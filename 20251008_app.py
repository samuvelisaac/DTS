# streamlit_app.py
import os
import time
import json
import base64
from io import StringIO
from datetime import datetime
from dotenv import load_dotenv

import streamlit as st
import pandas as pd
import httpx
import certifi
import requests
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry   # ✅ Correct import

from streamlit_javascript import st_javascript

# Disable SSL verification warnings globally
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Patch requests globally to disable SSL verification
class UnsafeSession(requests.Session):
    def request(self, *args, **kwargs):
        kwargs["verify"] = False
        return super().request(*args, **kwargs)

requests.Session = UnsafeSession
requests.request = lambda *a, **kw: requests.api.request(*a, verify=False, **kw)

# --- Disable SSL verify in httpx too ---
class UnsafeHTTPX(httpx.Client):
    def __init__(self, *args, **kwargs):
        kwargs["verify"] = False
        super().__init__(*args, **kwargs)

httpx.Client = UnsafeHTTPX

load_dotenv()  # local .env fallback

# ---------------------------
# CONFIG: secrets (.streamlit/secrets.toml) OR .env
# ---------------------------
def get_secret(name: str, default=""):
    try:
        val = st.secrets.get(name)
        if val:
            return val
    except Exception:
        pass
    return os.getenv(name, default)

# Program 1
API_KEY_P1 = get_secret("API_KEY_P1")
API_URL_P1 = get_secret("API_URL_P1")
WORKFLOW = {"name": "TSB_Data_Lineage_Generator_WF", "pipelineId": 7024}

# Program 2
API_KEY = get_secret("API_KEY")
BASE_URL = get_secret("BASE_URL")
ASSET_URL = get_secret("ASSET_URL")
LINEAGE_URL = get_secret("LINEAGE_URL")
DQ_URL = get_secret("DQ_URL")
LOGO_URL = get_secret("LOGO_URL", "")

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
    retries = Retry(total=5, backoff_factor=0.5,
                    status_forcelist=[500, 502, 503, 504])
    client = httpx.Client(headers={
        "access-key": API_KEY_P1,
        "Content-Type": "application/json",
        "Connection": "keep-alive"
    })
    return client

def save_generated_file(df: pd.DataFrame, original_filename: str) -> (bytes, bool, str):
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    if not IS_LOCAL:
        return csv_bytes, False, ""
    try:
        base_dir = os.path.join(DOWNLOADS_DIR, "Generated_Files")
        today_folder = datetime.now().strftime("%Y%m%d")
        output_dir = os.path.join(base_dir, today_folder)
        os.makedirs(output_dir, exist_ok=True)

        if os.path.exists(os.path.join(output_dir, f"{original_filename}.csv")):
            candidate = os.path.join(output_dir, f"{original_filename}_res.csv")
        else:
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
# Program 1 API
# ---------------------------
def process_agent(file_content: str):
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
# Program 2 helpers
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
    if not columns or not dataset_id:
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

def github_file_dropdown(tab_name: str):
    options = github_filtered_files(tab_name)
    selected_file = st.selectbox(
        f"📂 Select File from GitHub ({tab_name})",
        options,
        index=None,
        placeholder="Select from list",
        key=f"file_select_{tab_name}"
    )
    if not selected_file:
        st.write("Select file from list")
        return None  # important to return None if not selected
    else:
        st.info(f"Selected GitHub file: {selected_file}")
        gh = st.session_state["github_settings"]
        file_url = f"https://raw.githubusercontent.com/{gh['owner']}/{gh['repo']}/main/{gh['folder']}/{selected_file}"
        return [selected_file, file_url]

def save_to_github(filename: str, content_bytes: bytes):
    """Save file to GitHub repo if settings exist in session."""
    gh = st.session_state.get("github_settings")
    if not gh:
        return False, "❌ GitHub not configured"

    try:
        url = f"https://api.github.com/repos/{gh['owner']}/{gh['repo']}/contents/{gh['folder']}/{filename}"
        headers = {
            "Authorization": f"token {gh['token']}",
            "Accept": "application/vnd.github.v3+json"
        }

        # Retry session for GitHub operations
        s = requests.Session()
        retry = Retry(total=5, backoff_factor=0.5,
                      status_forcelist=[500, 502, 503, 504])
        s.mount("https://", HTTPAdapter(max_retries=retry))

        # Check if file exists
        resp = s.get(url, headers=headers, timeout=20)
        sha = resp.json().get("sha") if resp.status_code == 200 else None

        payload = {
            "message": f"Add {filename} from Streamlit app",
            "content": base64.b64encode(content_bytes).decode("utf-8"),
            "branch": "main"
        }
        if sha:
            payload["sha"] = sha  # overwrite existing file

        put_resp = s.put(url, headers=headers, json=payload, timeout=30)
        if put_resp.status_code in (200, 201):
            return True, f"✅ Saved to GitHub: {filename}"
        return False, f"⚠️ GitHub save failed: {put_resp.text}"
    except Exception as e:
        return False, f"⚠️ GitHub error: {e}"

def github_filtered_files(tab_name: str):
    files = st.session_state.get("github_files", [])
    if not files:
        return []

    if tab_name == "tab1":
        return [f for f in files if not (f.endswith("_ln.csv") or f.endswith("_dq.csv") or f.endswith("_mp.csv") or f.endswith("_res.csv"))]
    if tab_name == "tab2":
        return [f for f in files if f.endswith("_ln.csv")]
    if tab_name == "tab3":
        return [f for f in files if not (f.endswith("_ln.csv") or f.endswith("_dq.csv") or f.endswith("_mp.csv") or f.endswith("_res.csv"))]
    if tab_name == "tab4":
        return [f for f in files if f.endswith("_dq.csv")]
    if tab_name == "tab51":
        return [f for f in files if not (f.endswith("_ln.csv") or f.endswith("_dq.csv") or f.endswith("_mp.csv") or f.endswith("_res.csv"))]
    if tab_name == "tab52":
        return [f for f in files if not (f.endswith("_ln.csv") or f.endswith("_dq.csv") or f.endswith("_mp.csv") or f.endswith("_res.csv"))]
    return files

def make_github_session():
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET", "POST", "PUT"]
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

def refresh_github_files():
    gh = st.session_state.get("github_settings")
    if not gh:
        return
    url = f"https://api.github.com/repos/{gh['owner']}/{gh['repo']}/contents/{gh['folder']}"
    headers = {"Authorization": f"token {gh['token']}", "Accept": "application/vnd.github.v3+json"}
    session = make_github_session()
    resp = session.get(url, headers=headers, timeout=15, verify=False)
    if resp.status_code == 200:
        st.session_state["github_files"] = [f["name"] for f in resp.json() if f["type"] == "file"]

def tab3_dq_rules():
    st.subheader("Create DQ Rules (from DDL)")

    github_res = None
    uploaded_ddl = None
    ddl_content = ""

    if st.session_state.get("github_files"):
        github_res = github_file_dropdown("tab3")
        if github_res and github_res[1]:
            try:
                ddl_content = requests.get(github_res[1], timeout=30).text
            except Exception as e:
                st.error(f"⚠️ Failed to fetch file from GitHub: {e}")
    else:
        uploaded_ddl = st.file_uploader("Upload DDL (.txt or .sql)", type=["txt", "sql"], key="dq_ddl_tab")
        if uploaded_ddl:
            ddl_content = uploaded_ddl.read().decode("utf-8", errors="replace")

    if ddl_content:
        st.markdown("##### DDL Preview")
        st.text_area("DDL", ddl_content, height=240, key="ddl_preview_tab3")

        if st.button("🚀 Generate DQ Rules from DDL"):
            dq_pipeline_id = 7193
            payload = {
                "pipeLineId": dq_pipeline_id,
                "userInputs": {"{{DDL_INPUT_FILE}}": ddl_content},
                "executionId": f"dq-exec-{int(time.time())}",
                "user": "samuvel.isaac@ascendion.com"
            }

            with st.spinner("Calling AVA pipeline..."):
                try:
                    sess = create_session_p1()
                    # Direct POST to pipeline API (the working one)
                    r = sess.post(API_URL_P1, json=payload, timeout=300)
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
                        return

                    for nm, raw in raws.items():
                        file_name_value = os.path.splitext(uploaded_ddl.name)[0] if uploaded_ddl else github_res[0]
                        last_dot_index = len(file_name_value) if file_name_value.lower().rfind('.') == -1 else file_name_value.lower().rfind('.')
                        file_name_f = file_name_value[0:last_dot_index]
                        st.markdown(f"##### 🔹 Create DQ Rules Output - {file_name_f}")

                        try:
                            df = pd.read_csv(StringIO(raw), sep=",", quotechar='"', skip_blank_lines=True, on_bad_lines="skip")
                            st.dataframe(df, use_container_width=True)

                            # Save into Generated_Files/YYYYMMDD with versioning when local; and always provide download button
                            csv_bytes, saved_flag, saved_path = save_generated_file(df, file_name_f)
                            dl_filename = f"{file_name_f}_dq.csv"

                            if saved_flag:
                                st.info(f"Saved to: {saved_path}")
                            else:
                                st.info("File not saved locally (running in cloud or no Downloads). Use download button below.")

                            gh_filename = dl_filename
                            ok,msg = save_to_github(gh_filename, csv_bytes)
                            refresh_github_files()

                            st.info(msg)
                            if not ok:
                                st.info("File not saved into Github. Use download button below.")

                            st.download_button(
                                label=f"⬇️ Download CSV as {dl_filename}",
                                data=csv_bytes,
                                file_name=dl_filename,
                                mime="text/csv"
                            )

                        except Exception as e:
                            st.warning(f"Could not parse output as CSV: {e}")
                            st.text_area(f"{nm} raw", raw, height=200)
                except httpx.TimeoutException:
                    st.error("⏱️ Request timed out while calling pipeline. Try again later.")
                except httpx.RequestError as e:
                    st.error(f"⚠️ Network error: {e}")
                except httpx.HTTPStatusError as e:
                    try:
                        err = r.json()
                    except Exception:
                        err = {"error": str(e)}
                    st.error("❌ Processing failed.")
                    st.json(err)
                except Exception as e:
                    st.error(f"⚠️ Failed to call DQ pipeline: {e}")

def tab5_link_mapping():
    st.subheader("Create Link Mapping (From DDL and Glossary)")

    github_res_ddl = None
    github_res_glossary = None
    uploaded_ddl = None
    uploaded_glossary = None
    ddl_content = ""
    glossary_content = None
    glossary_content1 = ""

    # ------------------------------------
    # ✅ Input source handling
    # ------------------------------------
    if st.session_state.get("github_files"):
        st.markdown("### 📁 Select files from GitHub")

        col1, col2 = st.columns(2)
        with col1:
            github_res_ddl = github_file_dropdown("tab51")
        with col2:
            github_res_glossary = github_file_dropdown("tab52")

        if github_res_ddl and github_res_ddl[1]:
            try:
                ddl_content = requests.get(github_res_ddl[1], timeout=30).text
            except Exception as e:
                st.error(f"⚠️ Failed to fetch DDL from GitHub: {e}")

        if github_res_glossary and github_res_glossary[1]:
            try:
                req = requests.get(github_res_glossary[1], timeout=30).text
                glossary_content = pd.read_csv(StringIO(req), sep=",", quotechar='"', skip_blank_lines=True, on_bad_lines="skip")
                glossary_content1 = glossary_content.to_csv(index=False)
            except Exception as e:
                st.error(f"⚠️ Failed to fetch Glossary file from GitHub: {e}")

    else:
        st.markdown("##### 📤 Upload Local Files")

        col1, col2 = st.columns(2)
        with col1:
            uploaded_ddl = st.file_uploader("Upload DDL (.txt or .sql)", type=["txt", "sql"], key="link_mapping_ddl_upload")
            if uploaded_ddl:
                ddl_content = uploaded_ddl.read().decode("utf-8", errors="replace")
        with col2:
            uploaded_glossary = st.file_uploader("Upload Glossary (.csv)", type=["csv"], key="link_mapping_glossary_upload")
            if uploaded_glossary:
                try:
                    glossary_content = pd.read_csv(uploaded_glossary, sep=",", quotechar='"', skip_blank_lines=True, on_bad_lines="skip")
                    glossary_content1 = glossary_content.to_csv(index=False)
                except Exception as e:
                    st.error(f"Failed to parse file: {e}")

    # ------------------------------------
    # ✅ Show previews
    # ------------------------------------
    if ddl_content:
        st.markdown("##### DDL Preview")
        st.text_area("DDL", ddl_content, height=240, key="ddl_preview_linkmap")

    if glossary_content is not None:
        st.markdown("##### Glossary Preview")
        st.dataframe(glossary_content, use_container_width=True)

    # ------------------------------------
    # ✅ Call Pipeline
    # ------------------------------------
    if ddl_content and glossary_content1:
        if st.button("🚀 Generate Link Mapping"):
            pipeline_id = 7288
            workflow_name = "DI_postgresDDL_glossary_mapping_wf"

            payload = {
                "pipeLineId": pipeline_id,
                "userInputs": {
                    "{{postgres_DDL}}": ddl_content,
                    "{{glossary_file}}": glossary_content1
                },
                "executionId": f"linkmap-exec-{int(time.time())}",
                "user": "samuvel.isaac@ascendion.com"
            }

            with st.spinner(f"Running pipeline: {workflow_name}..."):
                try:
                    sess = create_session_p1()
                    r = sess.post(API_URL_P1, json=payload, timeout=300)
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
                        return

                    for nm, raw in raws.items():
                        file_name_value = os.path.splitext(uploaded_glossary.name)[0] if uploaded_glossary else github_res_glossary[0]
                        last_dot_index = len(file_name_value) if file_name_value.lower().rfind('.') == -1 else file_name_value.lower().rfind('.')
                        file_name_f = file_name_value[0:last_dot_index]

                        st.markdown(f"##### 🔹 Create Link Mapping Output - {file_name_f}")

                        try:
                            df = pd.read_csv(StringIO(raw), sep=",", quotechar='"', skip_blank_lines=True, on_bad_lines="skip")
                            st.dataframe(df, use_container_width=True)

                            # Save into Generated_Files/YYYYMMDD with versioning when local; and always provide download button
                            csv_bytes, saved_flag, saved_path = save_generated_file(df, file_name_f)
                            dl_filename = f"{file_name_f}_mp.csv"

                            if saved_flag:
                                st.info(f"Saved to: {saved_path}")
                            else:
                                st.info("File not saved locally (running in cloud or no Downloads). Use download button below.")
    
                            gh_filename = dl_filename
                            ok,msg = save_to_github(gh_filename, csv_bytes)
                            refresh_github_files()

                            st.info(msg)
                            if not ok:
                                st.info("File not saved into Github. Use download button below.")

                            st.download_button(
                                label=f"⬇️ Download CSV as {dl_filename}",
                                data=csv_bytes,
                                file_name=dl_filename,
                                mime="text/csv"
                            )
                        except Exception as e:
                            st.warning(f"Could not parse output as CSV: {e}")
                            st.text_area(f"{nm} raw", raw, height=200)

                except httpx.TimeoutException:
                    st.error("⏱️ Request timed out while calling pipeline. Try again later.")
                except httpx.RequestError as e:
                    st.error(f"⚠️ Network error: {e}")
                except httpx.HTTPStatusError as e:
                    try:
                        err = r.json()
                    except Exception:
                        err = {"error": str(e)}
                    st.error("❌ Processing failed.")
                    st.json(err)
                except Exception as e:
                    st.error(f"⚠️ Failed to call pipeline: {e}")

    else:
        st.info("Please upload or select both DDL and Glossary files to proceed.")

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

LOGO_BASE64 = get_base64_image(LOGO_URL)

if LOGO_BASE64:
    LOGO_HTML = f'<img src="data:image/png;base64,{LOGO_BASE64}" width="48" style="margin-right:10px;"/>'
    header_height_px = 75
else:
    LOGO_HTML = '<div style="font-weight:bold;font-size:20px;margin-right:10px;">A</div>'
    header_height_px = 65

TITLE_HTML = '<h2 style="margin:0;">AAVA DTS Ally</h2>'

# ------------------- #
# CSS + Frozen Header #
# ------------------- #
st.markdown(
    f"""
    <style>
    /* Fixed custom header */
    .fixed-header {{
        position: fixed;
        top: 50px; /* below Streamlit toolbar */
        left: 0; right: 0;
        z-index: 1000;
        background: white;
        border-bottom: 1px solid #ddd;
        padding: 8px 16px;
    }}
    .header-content {{
        display: flex;
        align-items: center;
        justify-content: space-between;
    }}
    .header-left {{
        flex: 0 0 auto;
        display: flex;
        align-items: center;
    }}
    .header-center {{
        flex: 1;
        text-align: center;
    }}
    .header-right {{
        flex: 0 0 auto;
    }}
    /* Stretch main container full width */
    .block-container {{
        padding-top: {header_height_px + 40}px !important;
        padding-left: 5rem !important;
        padding-right: 5rem !important;
        max-width: 100% !important;
    }}
    </style>

    <div class="fixed-header">
        <div class="header-content">
            <div class="header-left">{LOGO_HTML}</div>
            <div class="header-center">{TITLE_HTML}</div>
            <div class="header-right"></div>
        </div>
    </div>
    """,
    unsafe_allow_html=True
)

# --------------------------
# Tab Definitions
# --------------------------
tab1, tab2, tab3, tab4, tab5, tab7 = st.tabs([
    "📂 Determine Lineage",
    "🔗 Upload Lineage",
    "🛠️ Create DQ Rules",
    "✅ Upload DQ Rules",
    "⇄ Create Link Mapping",
    "⚙️"
])

# -----------------------------------
# TAB 1: Determine Lineage (Program1)
# -----------------------------------
with tab1:
    st.subheader("Determine Lineage")
    # inside your tab (example: tab1)
    program_type = st.selectbox("Select Program Type", ["COBOL", "Stored Procedure"], index=None, placeholder="Select from list", key="program_type_tab1")

    if not program_type:
        st.write("Select Program Type from list")
    else:
        # Always initialize
        github_res = None
        uploaded_file = None
        content = ""
    
        if st.session_state.get("github_files"):
            github_res = github_file_dropdown("tab1")  # (filename, file_url)
            if github_res and github_res[1]:
                try:
                    content = requests.get(github_res[1]).text
                except Exception as e:
                    st.error(f"⚠️ Failed to fetch file content: {e}")
        else:
            uploaded_file = st.file_uploader(
                "Upload Program File",
                type=["txt", "sql", "cbl"],
                key="progfile_tab1"
            )
            if uploaded_file is not None:
                content = uploaded_file.read().decode("utf-8", errors="replace")
    
        # ✅ Safely handle preview
        if ((github_res and github_res[0]) or uploaded_file) and program_type and content:
            st.markdown("##### 📄 Input File Preview")
            st.text_area("Program file content", content, height=240, key="preview_tab1")

            if st.button("🚀 Process Agent (Generate Lineage CSV)"):
                with st.spinner("Calling AVA pipeline..."):
                    res = process_agent(content)
    
                if not res.get("success"):
                    st.error("❌ Processing failed.")
                    st.json(res.get("error", {}))
                else:
                    file_name_value = os.path.splitext(uploaded_file.name)[0] if uploaded_file else github_res[0]
                    last_dot_index = len(file_name_value) if file_name_value.lower().rfind('.') == -1 else file_name_value.lower().rfind('.')
                    file_name_f = file_name_value[0:last_dot_index]

                    st.success(f"✅ API done in {res['api_duration']}s")
                    # save in session for later tabs
                    st.session_state["lineage_raws"] = res["raws"]
                    st.session_state["lineage_original_filename"] = file_name_f
                    st.session_state["lineage_program_type"] = program_type

                # show outputs if present
                if st.session_state.get("lineage_raws"):
                    raws = st.session_state["lineage_raws"]
                    original_filename = st.session_state["lineage_original_filename"]
                    program_type = st.session_state["lineage_program_type"]
            
                    for agent_name, raw_output in raws.items():
                        st.markdown(f"##### 🔹 Determine Lineage Output - {original_filename}")
            
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

                        # Save into Generated_Files/YYYYMMDD with versioning when local; and always provide download button
                        csv_bytes, saved_flag, saved_path = save_generated_file(parsed_df, original_filename)
                        dl_filename = f"{original_filename}_ln.csv"

                        if saved_flag:
                            st.info(f"Saved locally to: {saved_path}")
                        else:
                            st.info("File not saved locally (running in cloud or no Downloads). Use download button below.")

                        gh_filename = dl_filename
                        ok,msg = save_to_github(gh_filename, csv_bytes)
                        refresh_github_files()

                        st.info(msg)
                        if not ok:
                            st.info("File not saved into Github. Use download button below.")

                        st.download_button(
                            label=f"⬇️ Download CSV as {dl_filename}",
                            data=csv_bytes,
                            file_name=dl_filename,
                            mime="text/csv",
                            key=f"dl_prog_{agent_name}"
                        )

# ------------------------
# TAB 2: Upload Lineage (Program2 lineage creation)
# ------------------------
with tab2:
    st.subheader("Upload Lineage")

    sources = get_sources("source", "")
    if not sources:
        st.info("No sources available or failed to fetch sources.")
    else:
        selected_source = st.selectbox("Select Source", list(sources.keys()), index=None, placeholder="Select from list", key="src_tab2")
        if not selected_source:
            st.write("Select Source from the list")
        else:
            SOURCE_ID = sources[selected_source]
            collections = get_sources("collection", SOURCE_ID)

            if not collections:
                st.info("No collections found for selected source.")
            else:
                selected_collection = st.selectbox("Select Collection", list(collections.keys()), index=None, placeholder="Select from list", key="coll_tab2")
                if not selected_collection:
                    st.write("Select Collection from the list")
                else:
                    COLLECTION_ID = collections[selected_collection]

                    github_res = None
                    uploaded = None
                    df = None

                    if st.session_state.get("github_files"):
                        github_res = github_file_dropdown("tab2")
                        if github_res and github_res[1]:
                            try:
                                csv_text = requests.get(github_res[1]).text
                                df = pd.read_csv(StringIO(csv_text), quotechar='"', skip_blank_lines=True, on_bad_lines="skip")
                            except Exception as e:
                                st.error(f"Failed to parse GitHub file: {e}")
                    else:
                        uploaded = st.file_uploader("Upload Lineage (CSV/TXT)", type=["csv", "txt"], key="upload_lineage")
                        if uploaded:
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

                    if df is not None:
                        st.markdown("##### Input Preview")
                        st.dataframe(df, use_container_width=True)

                        if st.button("🚀 Process Lineage Creation"):
                            results = []
                            with st.spinner("Processing lineage creation..."):
                                for idx, row in df.iterrows():
                                    src_table = str(row.get("Source Table", "")).strip()
                                    src_column = str(row.get("Source Column", "")).strip()
                                    trg_table = str(row.get("Target Table", "")).strip()
                                    trg_column = str(row.get("Target Column", "")).strip()

                                    desc_line = ",".join([
                                        f"\"{str(row[col]).replace('\"', '\"\"')}\"" if ("," in str(row[col]) or "\"" in str(row[col])) else str(row[col])
                                        for col in df.columns
                                    ])

                                    _, src_col_id = find_dataset_and_column(SOURCE_ID, COLLECTION_ID, src_table, src_column)
                                    _, trg_col_id = find_dataset_and_column(SOURCE_ID, COLLECTION_ID, trg_table, trg_column)

                                    if src_col_id and trg_col_id:
                                        src_update = update_asset_desc(src_col_id, desc_line)
                                        trg_update = update_asset_desc(trg_col_id, desc_line)
                                        lineage_response = create_lineage(src_col_id, trg_col_id)
                                        results.append({
                                            "Source Table": src_table, "Source Column": src_column,
                                            "Target Table": trg_table, "Target Column": trg_column,
                                            "Source Col ID": src_col_id, "Target Col ID": trg_col_id,
                                            "Source Desc Update": src_update, "Target Desc Update": trg_update,
                                            "Lineage Response": lineage_response
                                        })
                                    else:
                                        results.append({
                                            "Source Table": src_table, "Source Column": src_column,
                                            "Target Table": trg_table, "Target Column": trg_column,
                                            "Source Col ID": src_col_id, "Target Col ID": trg_col_id,
                                            "Source Desc Update": "⚠️ Not found", "Target Desc Update": "⚠️ Not found",
                                            "Lineage Response": "⚠️ Skipped"
                                        })

                                file_name_value = os.path.splitext(uploaded.name)[0] if uploaded else github_res[0]
                                last_dot_index = len(file_name_value) if file_name_value.lower().rfind('.') == -1 else file_name_value.lower().rfind('.')
                                file_name_f = file_name_value[0:last_dot_index]

                                res_df = pd.DataFrame(results)
                                st.markdown(f"##### 🔹 Lineage Output - {file_name_f}")
                                st.dataframe(res_df, use_container_width=True)
        
                                # Save into Generated_Files/YYYYMMDD with versioning when local; and always provide download button
                                csv_bytes, saved_flag, saved_path = save_generated_file(res_df, file_name_f)
                                dl_filename = f"{file_name_f}_res.csv"

                                if saved_flag:
                                    st.info(f"Saved to: {saved_path}")
                                else:
                                    st.info("File not saved locally (running in cloud or no Downloads). Use download button below.")

                                gh_filename = dl_filename
                                ok,msg = save_to_github(gh_filename, csv_bytes)
                                refresh_github_files()

                                st.info(msg)
                                if not ok:
                                    st.info("File not saved into Github. Use download button below.")

                                st.download_button(
                                    label=f"⬇️ Download CSV as {dl_filename}",
                                    data=csv_bytes,
                                    file_name=dl_filename,
                                    mime="text/csv"
                                )

# ------------------------
# TAB 3: Create DQ Rules (call DQ pipeline from DDL)
# ------------------------
with tab3:
    tab3_dq_rules()

# ------------------------
# TAB 4: Upload DQ Rules (CSV/TXT) -> create monitors
# ------------------------
with tab4:
    st.subheader("Upload DQ Rules (Create Monitors)")

    sources = get_sources("source", "")
    if not sources:
        st.info("No sources available or failed to fetch sources.")
    else:
        sel_src = st.selectbox("Select Source", list(sources.keys()), index=None, placeholder="Select from list", key="dq_src")
        if not sel_src:
            st.write("Select Source from the list")
        else:
            SRC_ID = sources[sel_src]
            github_res = None
            up = None
            df = None

            if st.session_state.get("github_files"):
                github_res = github_file_dropdown("tab4")
                if github_res and github_res[1]:
                    try:
                        csv_text = requests.get(github_res[1]).text
                        df = pd.read_csv(StringIO(csv_text), sep=",", quotechar='"', skip_blank_lines=True, on_bad_lines="skip")
                    except Exception as e:
                        st.error(f"Failed to parse GitHub file: {e}")
            else:
                up = st.file_uploader("Upload DQ rules (CSV or TXT)", type=["csv", "txt"], key="dq_upload")
                if up:
                    try:
                        df = pd.read_csv(up, sep=",", quotechar='"', skip_blank_lines=True, on_bad_lines="skip")
                    except Exception as e:
                        st.error(f"Failed to parse file: {e}")

            if df is not None:
                st.markdown("##### Preview")
                st.dataframe(df, use_container_width=True)

                if st.button("🚀 Create DQ Monitors"):
                    results = []
                    with st.spinner("Creating monitors..."):
                        for idx, row in df.iterrows():
                            results.append(create_dq_monitor(SRC_ID, row, idx))

                        file_name_value = os.path.splitext(up.name)[0] if up else github_res[0]
                        last_dot_index = len(file_name_value) if file_name_value.lower().rfind('.') == -1 else file_name_value.lower().rfind('.')
                        file_name_f = file_name_value[0:last_dot_index]

                        st.markdown(f"##### 🔹 DQ Rules Output - {file_name_f}")
                        result_df = pd.DataFrame(results)
                        st.dataframe(result_df, use_container_width=True)

                        # Save into Generated_Files/YYYYMMDD with versioning when local; and always provide download button
                        csv_bytes, saved_flag, saved_path = save_generated_file(result_df, file_name_f)
                        dl_filename = f"{file_name_f}_res.csv"

                        if saved_flag:
                            st.info(f"Saved to: {saved_path}")
                        else:
                            st.info("File not saved locally (running in cloud or no Downloads). Use download button below.")

                        gh_filename = dl_filename
                        ok,msg = save_to_github(gh_filename, csv_bytes)
                        refresh_github_files()

                        st.info(msg)
                        if not ok:
                            st.info("File not saved into Github. Use download button below.")

                        st.download_button(
                            label=f"⬇️ Download CSV as {dl_filename}",
                            data=csv_bytes,
                            file_name=dl_filename,
                            mime="text/csv"
                        )

# -----------------------------------------
# TAB 5: Create Link Mapping
# -----------------------------------------
with tab5:
    tab5_link_mapping()

# -----------------------------------------
# TAB 5: GitHub Settings (replace existing block)
# -----------------------------------------
with tab7:
    st.subheader("⚙️ GitHub Settings")

    # --- small message placeholders ---
    if "_gh_save_ok" not in st.session_state:
        st.session_state["_gh_save_ok"] = ""
    if "_gh_save_error" not in st.session_state:
        st.session_state["_gh_save_error"] = ""

    # --- callbacks (safe, run in their own rerun) ---
    def clear_inputs_callback():
        """Clear only the typed input fields (unsaved)."""
        st.session_state["gh_token"] = ""
        st.session_state["gh_owner"] = ""
        st.session_state["gh_repo"] = ""
        st.session_state["gh_folder"] = ""
        st.session_state["_gh_save_ok"] = ""
        st.session_state["_gh_save_error"] = ""
        # callback triggers a rerun automatically; message will show after rerun

    def delete_callback():
        """Delete saved GitHub settings + files and clear input boxes."""
        for k in ["github_settings", "github_files", "github_refreshed"]:
            if k in st.session_state:
                del st.session_state[k]
        # also clear the input widgets
        st.session_state["gh_token"] = ""
        st.session_state["gh_owner"] = ""
        st.session_state["gh_repo"] = ""
        st.session_state["gh_folder"] = ""
        st.session_state["_gh_save_ok"] = "🗑️ Deleted saved GitHub details and cleared all fields."
        st.session_state["_gh_save_error"] = ""

    def save_callback():
        """Save & validate GitHub details using current widget values (stored in session_state)."""
        token = st.session_state.get("gh_token", "").strip()
        owner = st.session_state.get("gh_owner", "").strip()
        repo = st.session_state.get("gh_repo", "").strip()
        folder = st.session_state.get("gh_folder", "").strip()

        if not (token and owner and repo and folder):
            st.session_state["_gh_save_error"] = "⚠️ All fields are required."
            st.session_state["_gh_save_ok"] = ""
            return

        # store settings
        st.session_state["github_settings"] = {
            "token": token,
            "owner": owner,
            "repo": repo,
            "folder": folder,
        }

        # validate by listing folder contents
        try:
            url = f"https://api.github.com/repos/{owner}/{repo}/contents/{folder}"
            headers = {
                "Authorization": f"token {token}",
                "Accept": "application/vnd.github.v3+json",
                "Connection": "keep-alive",
            }
            session = make_github_session()
            # NOTE: verify=False used here because app previously had SSL issues; keep only if necessary
            resp = session.get(url, headers=headers, timeout=15, verify=False)

            if resp.status_code == 200:
                files = [f["name"] for f in resp.json() if f.get("type") == "file"]
                st.session_state["github_files"] = files
                st.session_state["github_refreshed"] = True
                st.session_state["_gh_save_ok"] = f"✅ Found {len(files)} file(s) in {folder}"
                st.session_state["_gh_save_error"] = ""
            else:
                # show GitHub error message
                try:
                    msg = resp.json().get("message", resp.text)
                except Exception:
                    msg = resp.text
                st.session_state["_gh_save_error"] = f"⚠️ Failed to fetch files: {msg}"
                st.session_state["_gh_save_ok"] = ""
                st.session_state["github_files"] = []
        except Exception as e:
            st.session_state["_gh_save_error"] = f"⚠️ Error validating GitHub details: {e}"
            st.session_state["_gh_save_ok"] = ""
            st.session_state["github_files"] = {}

    # --- Action buttons (use on_click so callbacks run *before* rerendering widgets) ---
    col1, col2, col3, col4 = st.columns([0.9, 1, 1, 7])
    with col1:
        st.button("💾 Save", key="gh_save_btn", on_click=save_callback)
    with col2:
        st.button("🗑️ Delete", key="gh_delete_btn", on_click=delete_callback)
    with col3:
        st.button("🧹 Clear", key="gh_clear_btn", on_click=clear_inputs_callback)

    # --- Input widgets (their values are kept in session_state keys) ---
    # value argument uses current session_state (or empty string)
    token_val = st.session_state.get("gh_token", "")
    owner_val = st.session_state.get("gh_owner", "")
    repo_val = st.session_state.get("gh_repo", "")
    folder_val = st.session_state.get("gh_folder", "")

    token = st.text_input("GitHub Token", value=token_val, type="password", key="gh_token")
    owner = st.text_input("Repository Owner", value=owner_val, key="gh_owner")
    repo = st.text_input("Repository Name", value=repo_val, key="gh_repo")
    folder = st.text_input("Folder Path", value=folder_val, key="gh_folder")

    # --- Show success / error messages after widgets so user sees them in UI ---
    if st.session_state.get("_gh_save_ok"):
        st.success(st.session_state["_gh_save_ok"])
        st.session_state["_gh_save_ok"] = ""
        st.rerun()   # ✅ only rerun once after saving GitHub settings
    if st.session_state.get("_gh_save_error"):
        st.error(st.session_state["_gh_save_error"])
        st.session_state["_gh_save_error"] = ""
        st.rerun()   # ✅ only rerun once after saving GitHub settings
