# streamlit_app.py
import os
import sys
import time
import json
import base64
from io import StringIO
from datetime import datetime
from dotenv import load_dotenv

import streamlit as st
import pandas as pd
import requests
import httpx
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ---------------------------
# CONFIG: secrets (.streamlit/secrets.toml) OR .env
# ---------------------------
load_dotenv()  # local .env fallback

def get_secret(name: str, default=""):
    """Prefer st.secrets when available (Streamlit), else fallback to env var."""
    try:
        # st.secrets works only in Streamlit; catch if not present or missing key
        val = st.secrets.get(name)
        if val:
            return val
    except Exception:
        pass
    return os.getenv(name, default)

# Program 1 (pipeline that generates lineage CSV from program file)
API_KEY_P1 = get_secret("API_KEY_P1")
API_URL_P1 = get_secret("API_URL_P1")
WORKFLOW = {"name": "TSB_Data_Lineage_Generator_WF", "pipelineId": 7024}

# Program 2 (Decube/AVA APIs)
API_KEY = get_secret("API_KEY")
BASE_URL = get_secret("BASE_URL")
ASSET_URL = get_secret("ASSET_URL")
LINEAGE_URL = get_secret("LINEAGE_URL")
DQ_URL = get_secret("DQ_URL")
LOGO_URL = get_secret("LOGO_URL", "")

# Downloads path (local)
DOWNLOADS_DIR = os.path.join(os.path.expanduser("~"), "Downloads")
IS_LOCAL = os.path.isdir(DOWNLOADS_DIR)

# ---------------------------
# UTILITIES
# ---------------------------
def get_base64_image(image_path):
    if not image_path:
        return ""
    try:
        if image_path.startswith("http://") or image_path.startswith("https://"):
            res = requests.get(image_path, verify=False, timeout=10)
            res.raise_for_status()
            return base64.b64encode(res.content).decode()
        else:
            with open(image_path, "rb") as f:
                return base64.b64encode(f.read()).decode()
    except Exception:
        return ""

def create_session_p1():
    return httpx.Client(headers={
        "access-key": API_KEY_P1,
        "Content-Type": "application/json",
        "Connection": "keep-alive"
    })

def safe_save_csv_bytes(csv_bytes: bytes, filename: str) -> (bool, str):
    """
    Save CSV bytes to ~/Downloads if local. Returns (saved_flag, path_if_saved_or_empty).
    Always safe to call from cloud; it will simply return (False, "") on cloud.
    """
    if not IS_LOCAL:
        return False, ""
    try:
        downloads_path = DOWNLOADS_DIR
        os.makedirs(downloads_path, exist_ok=True)
        path = os.path.join(downloads_path, filename)
        with open(path, "wb") as f:
            f.write(csv_bytes)
        return True, path
    except Exception:
        return False, ""

def save_generated_lineage(df: pd.DataFrame, original_filename: str) -> (bytes, bool, str):
    """
    For the specific 'Generated_Lineage_Files/YYYYMMDD' requirement and versioning:
    - If local: create Downloads/Generated_Lineage_Files/YYYYMMDD and save file as {original}_Vn.csv
    - Return CSV bytes, saved_flag, saved_path
    - If not local: just return CSV bytes and saved_flag=False
    """
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    if not IS_LOCAL:
        return csv_bytes, False, ""
    try:
        base_dir = os.path.join(DOWNLOADS_DIR, "Generated_Lineage_Files")
        today_folder = datetime.now().strftime("%Y%m%d")
        output_dir = os.path.join(base_dir, today_folder)
        os.makedirs(output_dir, exist_ok=True)

        # versioning
        version = 1
        while True:
            candidate = os.path.join(output_dir, f"{original_filename}_V{version}.csv")
            if not os.path.exists(candidate):
                break
            version += 1

        with open(candidate, "wb") as f:
            f.write(csv_bytes)

        return csv_bytes, True, candidate
    except Exception:
        return csv_bytes, False, ""

# ---------------------------
# Program 1: call pipeline and extract raws in-memory
# ---------------------------
def process_agent(file_content: str):
    """
    Send the program file_content to Program1 pipeline API and return raw outputs in memory.
    returns {"success": bool, "raws": {agent_name: raw_text}, "api_duration": seconds} on success
    returns {"success": False, "error": ...} on failure
    """
    pipeline_id = WORKFLOW["pipelineId"]
    payload = {
        "pipeLineId": pipeline_id,
        "userInputs": {"{{Program_Files}}": file_content},
        "executionId": f"exec-{int(time.time())}",
        "user": "samuvel.isaac@ascendion.com"
    }
    try:
        session = create_session_p1()
        start = time.time()
        r = session.post(API_URL_P1, json=payload, timeout=None)
        r.raise_for_status()
        api_duration = round(time.time() - start, 2)
        output = r.json()
        agents = output.get("pipeline", {}).get("pipeLineAgents", [])
        tasks = output.get("pipeline", {}).get("tasksOutputs", [])
        raws = {}
        for agent, t in zip(agents, tasks):
            name = agent.get("agent", {}).get("name", "agent").strip()
            raw_val = t.get("raw", "")
            if raw_val:
                curated = raw_val.split("\n\n", 1)[0]
                raws[name] = curated
        return {"success": True, "raws": raws, "api_duration": api_duration}
    except httpx.HTTPStatusError as e:
        try:
            err = r.json()
        except Exception:
            err = {"error": str(e)}
        return {"success": False, "error": err}
    except Exception as e:
        return {"success": False, "error": {"error": str(e)}}

# ---------------------------
# Program 2 helpers (lineage + DQ)
# ---------------------------
def get_sources(asset_type, source_id):
    payload = {"asset_type": asset_type, "page": 1, "page_size": 200} if asset_type == "source" else {
        "asset_type": asset_type, "page": 1, "page_size": 200, "parent": {"id": source_id, "type": "source"}
    }
    try:
        res = requests.post(BASE_URL, headers={"X-Decube-Api-Key": API_KEY, "Content-Type": "application/json"},
                            data=json.dumps(payload), verify=False, timeout=20)
        if res.status_code != 200:
            st.error(f"Failed to fetch {asset_type}: {res.status_code} - {res.text}")
            return {}
        data = res.json().get("data", [])
        return {item["asset"]["name"]: item["asset"]["id"] for item in data}
    except Exception as e:
        st.error(f"Error calling API: {e}")
        return {}

def search_asset(query, parent_id, parent_type="source", page_size=100):
    payload = {"query": query, "search_type": "exact", "parent": {"id": parent_id, "type": parent_type},
               "show_deleted": False, "page": 1, "page_size": page_size}
    try:
        res = requests.post(BASE_URL, headers={"X-Decube-Api-Key": API_KEY, "Content-Type": "application/json"},
                            data=json.dumps(payload), verify=False, timeout=20)
        if res.status_code != 200:
            return []
        return res.json().get("data", [])
    except Exception:
        return []

def find_dataset_and_column(source_id, collection_id, table_name, column_name):
    datasets = search_asset(table_name, parent_id=source_id, parent_type="source")
    if not datasets:
        return None, None
    dataset_id = None
    for d in datasets:
        parents = d.get("parents", [])
        if len(parents) > 1 and parents[0]["id"] == source_id and parents[1]["id"] == collection_id:
            dataset_id = d["asset"]["id"]
            break
    columns = search_asset(column_name, parent_id=source_id, parent_type="source")
    if not columns:
        return dataset_id, None
    column_id = None
    for c in columns:
        parents = c.get("parents", [])
        if len(parents) > 2 and parents[0]["id"] == source_id and parents[1]["id"] == collection_id and parents[2]["id"] == dataset_id:
            column_id = c["asset"]["id"]
            break
    return dataset_id, column_id

def update_asset_desc(asset_id, description):
    payload = {"asset_id": asset_id, "asset_type": "property", "description": description}
    res = requests.patch(ASSET_URL, headers={"X-Decube-Api-Key": API_KEY, "Content-Type": "application/json"},
                         data=json.dumps(payload), verify=False, timeout=20)
    if res.status_code == 200:
        return {"status": "success", "id": asset_id}
    return {"status": "error", "id": asset_id, "detail": res.text}

def create_lineage(src_column_id, trg_column_id):
    payload = {"source": {"id": src_column_id, "type": "property"}, "target": {"id": trg_column_id, "type": "property"}, "data_job": None}
    res = requests.post(LINEAGE_URL, headers={"X-Decube-Api-Key": API_KEY, "Content-Type": "application/json"},
                        data=json.dumps(payload), verify=False, timeout=20)
    if res.status_code != 200:
        return {"error": res.text}
    try:
        return res.json()
    except Exception:
        return {"error": res.text}

def create_dq_monitor(SOURCE_ID, row, idx):
    payload = {
        "asset": {"type": "source", "id": SOURCE_ID},
        "test_type": "custom_sql",
        "mode": "on_demand",
        "name": row.get("Monitor Name", f"Monitor {idx+1}"),
        "description": row.get("Description", "No description provided."),
        "notify": True,
        "incident_level": "info",
        "custom_sql": row.get("Custom_SQL", "")
    }
    res = requests.post(DQ_URL, headers={"X-Decube-Api-Key": API_KEY, "Content-Type": "application/json"},
                        json=payload, verify=False, timeout=20)
    return {"Monitor Name": payload["name"], "Status": "✅ Created" if res.status_code == 200 else f"❌ Failed ({res.status_code})", "Response": res.text}

# ---------------------------
# Streamlit UI
# ---------------------------
hide_streamlit_style = """
    <style>
    /* Hide top-right processing spinner and stop button */
    .stStatusWidget, .st-emotion-cache-13ln4jf {
        display: none !important;
    }
    </style>
"""

st.markdown(hide_streamlit_style, unsafe_allow_html=True)

st.set_page_config(page_title="AAVA DTS Ally", layout="wide")

LOGO_BASE64 = get_base64_image(LOGO_URL)
if LOGO_BASE64:
    st.markdown(f"""
    <div style="display:flex;align-items:center;justify-content:space-between;">
      <div style="flex:1"><img src="data:image/png;base64,{LOGO_BASE64}" width="60"></div>
      <div style="flex:2;text-align:center"><h1>AAVA DTS Ally</h1></div>
      <div style="flex:1"></div>
    </div>
    """, unsafe_allow_html=True)
else:
    st.title("AAVA DTS Ally")

tabs = st.tabs(["📂 Determine Lineage", "🔗 Upload Lineage", "🛠️ Create DQ Rules", "✅ Upload DQ Rules"])
tab1, tab2, tab3, tab4 = tabs

# ------------------------
# TAB 1: Determine Lineage (Program1)
# ------------------------
with tab1:
    st.header("Determine Lineage")
    program_type = st.selectbox("Select Program Type", ["Stored Procedure", "COBOL"])
    uploaded_file = st.file_uploader("Upload Program File", type=["txt", "sql", "cbl", "csv"], key="progfile")

    if uploaded_file and program_type:
        content = uploaded_file.read().decode("utf-8", errors="replace")
        st.subheader("📄 Input File Preview")
        st.text_area("Program file content", content, height=240)

        if st.button("🚀 Process Agent (Generate Lineage CSV)"):
            with st.spinner("Calling AVA pipeline..."):
                res = process_agent(content)

            if not res.get("success"):
                st.error("❌ Processing failed.")
                st.json(res.get("error", {}))
            else:
                st.success(f"✅ API done in {res['api_duration']}s")
                # save in session for later tabs
                st.session_state["lineage_raws"] = res["raws"]
                st.session_state["lineage_original_filename"] = os.path.splitext(uploaded_file.name)[0]
                st.session_state["lineage_program_type"] = program_type

    # show outputs if present
    if st.session_state.get("lineage_raws"):
        raws = st.session_state["lineage_raws"]
        original_filename = st.session_state["lineage_original_filename"]
        program_type = st.session_state["lineage_program_type"]

        for agent_name, raw_output in raws.items():
            st.subheader(f"🔹 Output Preview - {agent_name}")

            # try parse as csv
            parsed_df = None
            try:
                parsed_df = pd.read_csv(StringIO(raw_output), sep=",", quotechar='"', skip_blank_lines=True, on_bad_lines="skip")
            except Exception:
                parsed_df = None

            if parsed_df is None:
                st.warning("Output is not a valid CSV — showing raw text")
                st.text_area(f"{agent_name} raw output", raw_output, height=240)
                continue

            # apply COBOL change if required
            if program_type == "COBOL":
                if "Source Table" in parsed_df.columns:
                    parsed_df["Source Table"] = parsed_df["Source Table"].astype(str).apply(lambda x: x if x.endswith(".csv") else f"{x}.csv")
                if "Target Table" in parsed_df.columns:
                    parsed_df["Target Table"] = parsed_df["Target Table"].astype(str).apply(lambda x: x if x.endswith(".csv") else f"{x}.csv")

            st.dataframe(parsed_df, use_container_width=True)

            # Save into Generated_Lineage_Files/YYYYMMDD with versioning when local; and always provide download button
            csv_bytes, saved_flag, saved_path = save_generated_lineage(parsed_df, original_filename)
            dl_filename = os.path.basename(saved_path) if saved_flag else f"{original_filename}.csv"

            if saved_flag:
                st.info(f"Saved locally to: {saved_path}")
            else:
                st.info("File not saved locally (running in cloud or no Downloads). Use download button below.")

            st.download_button(
                label=f"⬇️ Download CSV as {dl_filename}",
                data=csv_bytes,
                file_name=dl_filename,
                mime="text/csv",
                key=f"dl_prog_{agent_name}"
            )

            # hide preview after download if user clicked - we can't detect click programmatically beyond rerun,
            # but we do offer the download button and local save already.

# ------------------------
# TAB 2: Upload Lineage (Program2 lineage creation)
# ------------------------
with tab2:
    st.header("Upload Lineage")
    sources = get_sources("source", "")
    if not sources:
        st.info("No sources available or failed to fetch sources.")
    else:
        selected_source = st.selectbox("Select Source", list(sources.keys()))
        SOURCE_ID = sources[selected_source]
        collections = get_sources("collection", SOURCE_ID)
        if not collections:
            st.info("No collections found for selected source.")
        else:
            selected_collection = st.selectbox("Select Collection", list(collections.keys()))
            COLLECTION_ID = collections[selected_collection]

            uploaded = st.file_uploader("Upload Lineage (CSV/TXT)", type=["csv", "txt"], key="upload_lineage")
            if uploaded:
                # determine delimiter for txts
                try:
                    if uploaded.name.lower().endswith(".csv"):
                        df = pd.read_csv(uploaded, quotechar='"', skip_blank_lines=True, on_bad_lines="skip")
                    else:
                        first = uploaded.readline().decode("utf-8", errors="replace")
                        uploaded.seek(0)
                        delim = "\t" if "\t" in first else ","
                        df = pd.read_csv(uploaded, delimiter=delim, quotechar='"', skip_blank_lines=True, on_bad_lines="skip")
                except Exception as e:
                    st.error(f"Failed to parse uploaded file: {e}")
                    df = None

                if df is not None:
                    st.subheader("Input Preview")
                    st.dataframe(df, use_container_width=True)

                    if st.button("🚀 Process Lineage Creation"):
                        results = []
                        with st.spinner("Processing lineage creation..."):
                            for idx, row in df.iterrows():
                                src_table = str(row.get("Source Table", "")).strip()
                                src_column = str(row.get("Source Column", "")).strip()
                                trg_table = str(row.get("Target Table", "")).strip()
                                trg_column = str(row.get("Target Column", "")).strip()
                                desc_line = ",".join([f"\"{row[col]}\"" if "," in str(row[col]) else str(row[col]) for col in df.columns])
                                _, src_col_id = find_dataset_and_column(SOURCE_ID, COLLECTION_ID, src_table, src_column)
                                _, trg_col_id = find_dataset_and_column(SOURCE_ID, COLLECTION_ID, trg_table, trg_column)
                                if src_col_id and trg_col_id:
                                    src_update = update_asset_desc(src_col_id, desc_line)
                                    trg_update = update_asset_desc(trg_col_id, desc_line)
                                    lineage_response = create_lineage(src_col_id, trg_col_id)
                                    results.append({"Source Table": src_table, "Source Column": src_column, "Target Table": trg_table, "Target Column": trg_column, "Source Col ID": src_col_id, "Target Col ID": trg_col_id, "Source Desc Update": src_update, "Target Desc Update": trg_update, "Lineage Response": lineage_response})
                                else:
                                    results.append({"Source Table": src_table, "Source Column": src_column, "Target Table": trg_table, "Target Column": trg_column, "Source Col ID": src_col_id, "Target Col ID": trg_col_id, "Source Desc Update": "⚠️ Not found", "Target Desc Update": "⚠️ Not found", "Lineage Response": "⚠️ Skipped"})
                        res_df = pd.DataFrame(results)
                        st.subheader("✅ Lineage Results")
                        st.dataframe(res_df, use_container_width=True)
                        # export
                        csv_bytes = res_df.to_csv(index=False).encode("utf-8")
                        saved, path = safe_save_csv_bytes(csv_bytes, "Lineage_Results.csv")
                        if saved:
                            st.info(f"Saved locally to: {path}")
                        st.download_button("⬇️ Download Lineage_Results.csv", data=csv_bytes, file_name="Lineage_Results.csv", mime="text/csv")

# ------------------------
# TAB 3: Create DQ Rules (call DQ pipeline from DDL)
# ------------------------
with tab3:
    st.header("Create DQ Rules (from DDL)")
    uploaded_ddl = st.file_uploader("Upload DDL (.txt or .sql)", type=["txt", "sql"], key="dq_ddl_tab")
    if uploaded_ddl:
        ddl_content = uploaded_ddl.read().decode("utf-8", errors="replace")
        st.subheader("DDL Preview")
        st.text_area("DDL", ddl_content, height=240)
        if st.button("🚀 Generate DQ Rules from DDL"):
            dq_pipeline_id = 7193
            payload = {"pipeLineId": dq_pipeline_id, "userInputs": {"{{DDL_INPUT_FILE}}": ddl_content}, "executionId": f"dq-exec-{int(time.time())}", "user": "samuvel.isaac@ascendion.com"}
            try:
                sess = create_session_p1()
                r = sess.post(API_URL_P1, json=payload, timeout=None)
                r.raise_for_status()
                out = r.json()
                agents = out.get("pipeline", {}).get("pipeLineAgents", [])
                tasks = out.get("pipeline", {}).get("tasksOutputs", [])
                raws = {}
                for agent, t in zip(agents, tasks):
                    nm = agent.get("agent", {}).get("name", "agent").strip()
                    rv = t.get("raw", "")
                    if rv:
                        raws[nm] = rv.split("\n\n", 1)[0]
                if not raws:
                    st.warning("No output produced by pipeline.")
                for nm, raw in raws.items():
                    st.subheader(f"Output - {os.path.splitext(uploaded_ddl.name)[0]}")
                    # try parse csv
                    try:
                        df = pd.read_csv(StringIO(raw), sep=",", quotechar='"', skip_blank_lines=True, on_bad_lines="skip")
                        st.dataframe(df, use_container_width=True)
                        csv_bytes = df.to_csv(index=False).encode("utf-8")
                        # local save (Downloads) and download button
                        saved, path = safe_save_csv_bytes(csv_bytes, f"{os.path.splitext(uploaded_ddl.name)[0]}.csv")
                        if saved:
                            st.info(f"Saved locally to: {path}")
                        st.download_button(f"⬇️ Download {os.path.splitext(uploaded_ddl.name)[0]}.csv", data=csv_bytes, file_name=f"{os.path.splitext(uploaded_ddl.name)[0]}.csv", mime="text/csv")
                    except Exception as e:
                        st.warning(f"Could not parse pipeline output as CSV: {e}")
                        st.text_area(f"{nm} raw", raw, height=200)
            except Exception as e:
                st.error(f"Failed to call DQ pipeline: {e}")

# ------------------------
# TAB 4: Upload DQ Rules (CSV/TXT) -> create monitors
# ------------------------
with tab4:
    st.header("Upload DQ Rules (Create Monitors)")
    sources = get_sources("source", "")
    if not sources:
        st.info("No sources available or failed to fetch sources.")
    else:
        sel_src = st.selectbox("Select Source", list(sources.keys()), key="dq_src")
        SRC_ID = sources[sel_src]
        up = st.file_uploader("Upload DQ rules (CSV or TXT)", type=["csv", "txt"], key="dq_upload")
        if up:
            try:
                df = pd.read_csv(up, sep=",", quotechar='"', skip_blank_lines=True, on_bad_lines="skip")
            except Exception as e:
                st.error(f"Failed to parse file: {e}")
                df = None
            if df is not None:
                st.subheader("Preview")
                st.dataframe(df, use_container_width=True)
                if st.button("🚀 Create DQ Monitors"):
                    results = []
                    with st.spinner("Creating monitors..."):
                        for idx, row in df.iterrows():
                            results.append(create_dq_monitor(SRC_ID, row, idx))
                    result_df = pd.DataFrame(results)
                    st.subheader("Monitor Creation Results")
                    st.dataframe(result_df, use_container_width=True)
                    csv_bytes = result_df.to_csv(index=False).encode("utf-8")
                    saved, path = safe_save_csv_bytes(csv_bytes, "DQ_Monitors_Results.csv")
                    if saved:
                        st.info(f"Saved locally to: {path}")
                    st.download_button("⬇️ Download DQ_Monitors_Results.csv", data=csv_bytes, file_name="DQ_Monitors_Results.csv", mime="text/csv")
