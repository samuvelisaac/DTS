import streamlit as st
import pandas as pd
import requests
import httpx
import time
import json
import urllib3
import base64
from io import StringIO
from dotenv import load_dotenv
from datetime import datetime
import os

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ========================
# CONFIG
# ========================
load_dotenv()  # Load environment variables from .env

# Program 1 config
API_KEY_P1 = os.getenv("API_KEY_P1", "")  # fallback from .env if needed
API_URL_P1 = os.getenv("API_URL_P1")
workflow = {"name": "TSB_Data_Lineage_Generator_WF", "pipelineId": 7024}

# Program 2 config
API_KEY = os.getenv("API_KEY")
BASE_URL = os.getenv("BASE_URL")
ASSET_URL = os.getenv("ASSET_URL")
LINEAGE_URL = os.getenv("LINEAGE_URL")
DQ_URL = os.getenv("DQ_URL")
LOGO_URL = os.getenv("LOGO_URL")

# Get system Downloads folder
DOWNLOADS_DIR = os.path.join(os.path.expanduser("~"), "Downloads")

# ========================
# COMMON FUNCTIONS
# ========================
def get_base64_image(image_path):
    if image_path.startswith("http://") or image_path.startswith("https://"):
        response = requests.get(image_path, verify=False)
        response.raise_for_status()
        return base64.b64encode(response.content).decode()
    else:
        with open(image_path, "rb") as f:
            return base64.b64encode(f.read()).decode()


# -------- Program 1 Helpers --------
def create_session():
    return httpx.Client(headers={
        "access-key": API_KEY_P1,
        "Content-Type": "application/json",
        "Connection": "keep-alive"
    })

def process_agent(file_content):
    pipeline_id = workflow["pipelineId"]

    payload = {
        "pipeLineId": pipeline_id,
        "userInputs": {"Program_Files": file_content},  # ✅ fixed
        "executionId": f"exec-{int(time.time())}",      # unique exec ID
        "user": "your.email@ascendion.com"
    }

    try:
        session = create_session()
        start_time = time.time()
        response = session.post(API_URL_P1, json=payload, timeout=None)
        response.raise_for_status()
        api_duration = round(time.time() - start_time, 2)

        output_data = response.json()
        agents = output_data.get("pipeline", {}).get("pipeLineAgents", [])
        task_outputs = output_data.get("pipeline", {}).get("tasksOutputs", [])

        raws = {}
        for agent, output in zip(agents, task_outputs):
            name = agent["agent"]["name"].strip()
            raw_value = output.get("raw", "").strip()
            if raw_value:
                curated = raw_value.split("\n\n", 1)[0]
                raws[name] = curated

        return {"success": True, "raws": raws, "api_duration": api_duration}

    except httpx.HTTPStatusError as e:
        try:
            err_json = response.json()
        except Exception:
            err_json = {"error": str(e)}
        return {"success": False, "error": err_json}


# -------- Program 2 Helpers --------
def get_sources(asset_type, source_id):
    if asset_type == "source":
        payload = {"asset_type": asset_type, "page": 1, "page_size": 100}
    else:
        payload = {
            "asset_type": asset_type,
            "page": 1,
            "page_size": 100,
            "parent": {"id": source_id, "type": "source"},
        }

    response = requests.post(
        BASE_URL,
        headers={"X-Decube-Api-Key": API_KEY, "Content-Type": "application/json"},
        data=json.dumps(payload),
        verify=False,
    )

    if response.status_code != 200:
        st.error(f"Failed to fetch sources: {response.text}")
        return {}
    data = response.json().get("data", [])
    return {item["asset"]["name"]: item["asset"]["id"] for item in data}


def search_asset(query, parent_id, parent_type="source", page_size=100):
    payload = {
        "query": query,
        "search_type": "exact",
        "parent": {"id": parent_id, "type": parent_type},
        "show_deleted": False,
        "page": 1,
        "page_size": page_size,
    }
    response = requests.post(
        BASE_URL,
        headers={"X-Decube-Api-Key": API_KEY, "Content-Type": "application/json"},
        data=json.dumps(payload),
        verify=False,
    )
    if response.status_code != 200:
        return []
    return response.json().get("data", [])


def find_dataset_and_column(source_id, collection_id, table_name, column_name):
    datasets = search_asset(table_name, parent_id=source_id, parent_type="source")
    if not datasets:
        return None, None
    dataset_id = None
    for d in datasets:
        if d["parents"][0]["id"] == source_id and d["parents"][1]["id"] == collection_id:
            dataset_id = d["asset"]["id"]

    columns = search_asset(column_name, parent_id=source_id, parent_type="source")
    if not columns:
        return dataset_id, None
    column_id = None
    for c in columns:
        if (
            c["parents"][0]["id"] == source_id
            and c["parents"][1]["id"] == collection_id
            and c["parents"][2]["id"] == dataset_id
        ):
            column_id = c["asset"]["id"]
    return dataset_id, column_id


def update_asset_desc(asset_id, description):
    payload = {"asset_id": asset_id, "asset_type": "property", "description": description}
    response = requests.patch(
        ASSET_URL,
        headers={"X-Decube-Api-Key": API_KEY, "Content-Type": "application/json"},
        data=json.dumps(payload),
        verify=False,
    )
    return {"status": "success", "id": asset_id} if response.status_code == 200 else {"status": "error", "id": asset_id, "detail": response.text}


def create_lineage(src_column_id, trg_column_id):
    source = {"id": src_column_id, "type": "property"}
    target = {"id": trg_column_id, "type": "property"}
    response = requests.post(
        LINEAGE_URL,
        headers={"X-Decube-Api-Key": API_KEY, "Content-Type": "application/json"},
        data=json.dumps({"source": source, "target": target, "data_job": None}),
        verify=False,
    )
    if response.status_code != 200:
        return {"error": response.text}
    try:
        return response.json()
    except Exception:
        return {"error": response.text}


def create_dq_monitor(SOURCE_ID, row, idx):
    payload = {
        "asset": {"type": "source", "id": SOURCE_ID},
        "test_type": "custom_sql",
        "mode": "on_demand",
        "name": row.get("Monitor Name", f"Monitor {idx+1}"),
        "description": row.get("description", "No description provided."),
        "notify": True,
        "incident_level": "info",
        "custom_sql": row.get("custom_sql", ""),
    }
    headers = {"X-Decube-Api-Key": API_KEY, "Content-Type": "application/json"}
    response = requests.post(DQ_URL, headers=headers, json=payload, verify=False)
    return {
        "Monitor Name": payload["name"],
        "Status": "✅ Created" if response.status_code == 200 else f"❌ Failed ({response.status_code})",
        "Response": response.text,
    }

# ========================
# STREAMLIT APP
# ========================
hide_streamlit_style = """
    <style>
    /* Hide top-right processing spinner and stop button */
    .stStatusWidget, .st-emotion-cache-13ln4jf {
        display: none !important;
    }
    </style>
"""
st.markdown(hide_streamlit_style, unsafe_allow_html=True)

st.set_page_config(page_title="AAVA DTS Tools", layout="wide")

LOGO_BASE64 = get_base64_image(LOGO_URL)
st.markdown(
    f"""
    <div style="display: flex; align-items: center; justify-content: space-between; width: 100%;">
        <div style="flex: 1; display: flex; justify-content: flex-start; align-items: center;">
            <img src="data:image/png;base64,{LOGO_BASE64}" alt="Ascendion Logo" width="60"/>
        </div>
        <div style="flex: 2; display: flex; justify-content: center; align-items: center;">
            <h1 style="margin: 0; text-align: center;">AAVA DTS Ally</h1>
        </div>
        <div style="flex: 1;"></div>
    </div>
    """,
    unsafe_allow_html=True
)

tab1, tab2, tab3 = st.tabs(["📂 Generate Lineage File", "🔗 Create Lineage", "✅ Create DQ Rule"])

# ------------------------
# TAB 1: Program 1 (Generate Lineage File)
# ------------------------
with tab1:
    st.header("Generate Lineage File")
    program_type = st.selectbox("Select Program Type", ["Stored Procedure", "COBOL"])
    uploaded_file = st.file_uploader("Choose input file", type=["txt", "sql", "cbl", "csv"])

    if uploaded_file and program_type:
        content = uploaded_file.read().decode("utf-8")
        st.subheader("📄 Input File Preview")
        st.text_area("File Content", content, height=200)

        # ✅ Process button
        if st.button("🚀 Process Agent"):
            with st.spinner("Processing..."):
                result = process_agent(content)

            if result["success"]:
                st.success(f"✅ Process completed in {result['api_duration']}s")

                # Save results into session_state for persistence
                st.session_state["lineage_results"] = result["raws"]
                st.session_state["original_filename"] = os.path.splitext(uploaded_file.name)[0]
                st.session_state["program_type"] = program_type
            else:
                st.error("❌ Error while processing")
                errors = result["error"]
                st.json(errors if not (isinstance(errors, dict) and "errors" in errors) else errors["errors"])

    # ✅ Display results if available in session_state
    if "lineage_results" in st.session_state:
        raws = st.session_state["lineage_results"]
        original_filename = st.session_state["original_filename"]
        program_type = st.session_state["program_type"]

        # Setup output dir
        base_dir = os.path.join(DOWNLOADS_DIR, "Generated_Lineage_Files")
        today_folder = datetime.now().strftime("%Y%m%d")
        output_dir = os.path.join(base_dir, today_folder)

        for agent, raw_output in raws.items():
            st.subheader(f"🔹 Output Preview - {original_filename}")
            try:
                df = pd.read_csv(StringIO(raw_output), sep=",", quotechar='"')

                # COBOL table fix
                if program_type == "COBOL":
                    if "Source Table" in df.columns:
                        df["Source Table"] = df["Source Table"].astype(str).apply(
                            lambda x: x if x.endswith(".csv") else f"{x}.csv"
                        )
                    if "Target Table" in df.columns:
                        df["Target Table"] = df["Target Table"].astype(str).apply(
                            lambda x: x if x.endswith(".csv") else f"{x}.csv"
                        )

                st.dataframe(df, use_container_width=True)

                # ✅ Save only when button clicked
                if st.button(f"⬇️ Download CSV into {output_dir}", key=f"dl_{agent}"):
                    os.makedirs(output_dir, exist_ok=True)

                    # ✅ Compute version
                    version = 1
                    while True:
                        output_file = os.path.join(output_dir, f"{original_filename}_V{version}.csv")
                        if not os.path.exists(output_file):
                            break
                        version += 1

                    df.to_csv(output_file, index=False)
                    st.success(f"💾 Saved to: {output_file}")

                    # 🔑 Clear session_state → hide preview + button after save
                    del st.session_state["lineage_results"]
                    del st.session_state["original_filename"]
                    del st.session_state["program_type"]
                    st.rerun()

            except Exception as e:
                st.warning(f"⚠️ Could not parse output as CSV ({e}). Showing raw text instead.")
                st.text_area(f"{agent} raw", raw_output, height=200)

# ------------------------
# TAB 2: Program 2 Lineage
# ------------------------
with tab2:
    st.header("Create Lineage")
    sources_dict = get_sources("source", "")
    if sources_dict:
        selected_source_name = st.selectbox("Select Source", options=list(sources_dict.keys()), key="lineage_source")
        SOURCE_ID = sources_dict[selected_source_name]
        collection_dict = get_sources("collection", SOURCE_ID)
        if collection_dict:
            selected_collection_name = st.selectbox("Select Collection", options=list(collection_dict.keys()), key="lineage_collection")
            COLLECTION_ID = collection_dict[selected_collection_name]
            uploaded_file = st.file_uploader("Upload Lineage (CSV/TXT)", type=["csv", "txt"], key="lineage_file")
            if uploaded_file:
                if uploaded_file.name.endswith(".csv"):
                    df = pd.read_csv(uploaded_file, quotechar='"', skip_blank_lines=True, on_bad_lines="skip")
                else:
                    first_line = uploaded_file.readline().decode("utf-8")
                    uploaded_file.seek(0)
                    delimiter = "\t" if "\t" in first_line else ","
                    df = pd.read_csv(uploaded_file, delimiter=delimiter, quotechar='"', skip_blank_lines=True, on_bad_lines="skip")
                st.write("### Input Preview", df)
                if st.button("🚀 Process Lineage"):
                    results = []
                    with st.spinner("Processing..."):
                        for _, row in df.iterrows():
                            src_table = str(row["Source Table"]).strip()
                            src_column = str(row["Source Column"]).strip()
                            trg_table = str(row["Target Table"]).strip()
                            trg_column = str(row["Target Column"]).strip()
                            desc_line = ",".join([f"\"{row[col]}\"" if "," in str(row[col]) else str(row[col]) for col in df.columns])
                            _, src_col_id = find_dataset_and_column(SOURCE_ID, COLLECTION_ID, src_table, src_column)
                            _, trg_col_id = find_dataset_and_column(SOURCE_ID, COLLECTION_ID, trg_table, trg_column)
                            if src_col_id and trg_col_id:
                                src_update = update_asset_desc(src_col_id, desc_line)
                                trg_update = update_asset_desc(trg_col_id, desc_line)
                                lineage_response = create_lineage(src_col_id, trg_col_id)
                                results.append({
                                    "Source Table": src_table,
                                    "Source Column": src_column,
                                    "Target Table": trg_table,
                                    "Target Column": trg_column,
                                    "Source Col ID": src_col_id,
                                    "Target Col ID": trg_col_id,
                                    "Source Desc Update": src_update,
                                    "Target Desc Update": trg_update,
                                    "Lineage Response": lineage_response,
                                })
                            else:
                                results.append({
                                    "Source Table": src_table,
                                    "Source Column": src_column,
                                    "Target Table": trg_table,
                                    "Target Column": trg_column,
                                    "Source Col ID": src_col_id,
                                    "Target Col ID": trg_col_id,
                                    "Source Desc Update": "⚠️ Not found",
                                    "Target Desc Update": "⚠️ Not found",
                                    "Lineage Response": "⚠️ Skipped",
                                })
                    st.write("### ✅ Lineage Results")
                    st.dataframe(pd.DataFrame(results))

# ------------------------
# TAB 3: Program 2 DQ
# ------------------------
with tab3:
    st.header("Create DQ Rule")
    sources_dict = get_sources("source", "")
    if sources_dict:
        selected_source_name = st.selectbox("Select Source", options=list(sources_dict.keys()), key="dq_source")
        SOURCE_ID = sources_dict[selected_source_name]
        uploaded_file = st.file_uploader("Upload DQ Details (CSV)", type=["csv"], key="dq_file")
        if uploaded_file:
            df = pd.read_csv(uploaded_file)
            st.subheader("Input Data Preview")
            st.dataframe(df)
            if st.button("🚀 Process DQ Creation"):
                results = []
                with st.spinner("Processing..."):
                    for idx, row in df.iterrows():
                        results.append(create_dq_monitor(SOURCE_ID, row, idx))
                st.subheader("Monitor Creation Results")
                st.dataframe(pd.DataFrame(results))
