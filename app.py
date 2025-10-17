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
import urllib
from urllib.parse import quote
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

GitHub_Token = get_secret("GitHub_Token")
Repository_Owner = get_secret("Repository_Owner")
Repository_Name = get_secret("Repository_Name")
Folder_Path = get_secret("Folder_Path")

s={"GitHub_Token": GitHub_Token, "Repository_Owner": Repository_Owner, "Repository_Name": Repository_Name, "Folder_Path": Folder_Path}
onetime = 0

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

def delete_lineage(src_column_id, trg_column_id):
    params = {
        "source_id": src_column_id,
        "source_type": "property",
        "target_id": trg_column_id,
        "target_type": "property"
    }

    try:
        res = requests.delete(
            LINEAGE_URL,
            headers={"X-Decube-Api-Key": API_KEY},
            params=params,      # ✅ use params, not data
            verify=False,
            timeout=20
        )

        if res.status_code != 200:
            return {"error": res.text}

        return res.json()

    except Exception as e:
        return {"error": str(e)}

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

def search_glossary(query,asset_type, parent_id = ""):
    parent_type = "glossary" if asset_type == "category" else "category"
    if asset_type == "glossary":
        payload = {"query": query, "search_type": "exact", "asset_type": asset_type, "show_deleted": False,
                    "page": 1, "page_size": 200}
    else:
        payload = {"query": query, "search_type": "exact", "asset_type": asset_type, "show_deleted": False,
                    "page": 1, "page_size": 200, "parent": {"id": parent_id, "type": parent_type}}
    try:
        res = requests.post(BASE_URL, headers={"X-Decube-Api-Key": API_KEY, "Content-Type": "application/json"},
                            data=json.dumps(payload), verify=False, timeout=20)
        if res.status_code != 200:
            return []
        return res.json().get("data", [])
    except Exception:
        return []

def find_glossary_category_term(glossary_name, category_name, term_name):
    glossaries = search_glossary(glossary_name, "glossary")
    if not glossaries:
        return None
    glossary_id = glossaries[0]["asset"]["id"]

    categories = search_glossary(category_name, "category", glossary_id)
    if not categories:
        return None
    category_id = ""
    for c in categories:
        if c["parents"][0]["id"] == glossary_id:
            category_id = c["asset"]["id"]
            break

    terms = search_glossary(term_name, "term", category_id)
    if not terms:
        return None
    term_id = ""
    for t in terms:
        if t["parents"][0]["id"] == glossary_id and t["parents"][1]["id"] == category_id:
            term_id = t["asset"]["id"]
            break

    return glossary_id, category_id, term_id

def update_link_mapping(col_id, term_id):
    payload = {"asset_id": col_id, "asset_type": "property", "linked_terms": [{"type": "term", "id": term_id}]}
    res = requests.patch(ASSET_URL, headers={"X-Decube-Api-Key": API_KEY, "Content-Type": "application/json"},
                        data=json.dumps(payload), verify=False, timeout=20)
    if res.status_code == 200:
        return {"status": "success", "column id": col_id, "term id": term_id}
    return {"status": "error", "column id": col_id, "term id": term_id, "detail": res.text}

# ============================================================
# Helper: GitHub File Dropdown
# ============================================================
def github_file_dropdown(tab_name: str, s):
    """
    Lists GitHub _gh.csv files and allows '-- None --' as a valid placeholder.
    Returns [filename, raw_url] or None.
    """
    if tab_name == "GitHub_Settings":
        url_path = f"{Repository_Owner}/{Repository_Name}/contents/{Folder_Path}"
        url = urllib.parse.urljoin("https://api.github.com/repos/", urllib.parse.quote(url_path))
        headers = {"Authorization": f"token {GitHub_Token}", "Accept": "application/vnd.github.v3+json"}
    
        files = ["-- None --"]
        try:
            ses = requests.Session()
            retry = Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
            ses.mount("https://", HTTPAdapter(max_retries=retry))
            r = ses.get(url, headers=headers, timeout=10)
            if r.status_code == 200:
                files += [f["name"] for f in r.json() if f["type"] == "file" and f["name"].endswith("_gh.csv")]
            else:
                st.warning(f"⚠️ Could not fetch credential list ({r.status_code})")
        except Exception as e:
            st.warning(f"⚠️ Error fetching credential files: {e}")
    else:
        # other tabs use session github_settings (not used for settings dropdown)
        options = github_filtered_files(tab_name)
        files = ["-- None --"] + options

    # Ensure a dedicated session key for this selectbox exists
    sel_key = f"file_select_{tab_name}"
    if sel_key not in st.session_state:
        st.session_state[sel_key] = files[0]  # default to "-- None --"
    # Also keep a higher level selected_value to reason about clearing
    if "selected_value" not in st.session_state:
        st.session_state["selected_value"] = None

    # Determine index to show
    try:
        index = files.index(st.session_state.get(sel_key, files[0]))
    except ValueError:
        index = 0

    if tab_name == "tab1":
        file_opt = "Program "
    elif tab_name == "tab2" or tab_name == "tab21":
        file_opt = "Lineage "
    elif tab_name == "tab3":
        file_opt = "DDL "
    elif tab_name == "tab4":
        file_opt = "DQ "
    elif tab_name == "tab51":
        file_opt = "DDL "
    elif tab_name == "tab52":
        file_opt = "Glossary "
    elif tab_name == "tab6":
        file_opt = "Link Mapping "
    elif tab_name == "GitHub_Settings":
        file_opt = "Credential "
    else:
        file_opt = ""

    selected_file = st.selectbox(
        f"📂 Select {file_opt}File from GitHub",
        files,
        index=index,
        key=sel_key,
        format_func=lambda x: x if x != "-- None --" else ""
    )

    # update selected_value for other logic
    if selected_file == "-- None --":
        st.session_state["selected_value"] = None
        return None
    else:
        st.session_state["selected_value"] = selected_file
        # Build raw.githubusercontent url for the selected file
        if tab_name == "GitHub_Settings":
            file_url1 = f"{Repository_Owner}/{Repository_Name}/main/{Folder_Path}/{selected_file}"
        else:
            gh = st.session_state.get("github_settings", {})
            file_url1 = f"{gh.get('owner')}/{gh.get('repo')}/main/{gh.get('folder')}/{selected_file}"
        file_url2 = urllib.parse.quote(file_url1)
        file_url = urllib.parse.urljoin("https://raw.githubusercontent.com/", file_url2)
        return [selected_file, file_url]


# ============================================================
# Helper: Clear Callback
# ============================================================
def clear_github_settings():
    """Callback for 🧹 Clear button — safely trigger a clear on next rerun."""
    st.session_state["_gh_clear_pending"] = True
    st.toast("🧹 Cleared all GitHub input fields and dropdown reset pending...", icon="✅")
    st.rerun()

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
        return [f for f in files if not (f.endswith("_ln.csv") or f.endswith("_dq.csv") or f.endswith("_mp.csv") or f.endswith("_res.csv") or f.endswith("_gh.csv"))]
    if tab_name == "tab2":
        return [f for f in files if f.endswith("_ln.csv")]
    if tab_name == "tab21":
        return [f for f in files if f.endswith("_ln.csv")]
    if tab_name == "tab3":
        return [f for f in files if not (f.endswith("_ln.csv") or f.endswith("_dq.csv") or f.endswith("_mp.csv") or f.endswith("_res.csv") or f.endswith("_gh.csv"))]
    if tab_name == "tab4":
        return [f for f in files if f.endswith("_dq.csv")]
    if tab_name == "tab51":
        return [f for f in files if not (f.endswith("_ln.csv") or f.endswith("_dq.csv") or f.endswith("_mp.csv") or f.endswith("_res.csv") or f.endswith("_gh.csv"))]
    if tab_name == "tab52":
        return [f for f in files if not (f.endswith("_ln.csv") or f.endswith("_dq.csv") or f.endswith("_mp.csv") or f.endswith("_res.csv") or f.endswith("_gh.csv"))]
    if tab_name == "tab6":
        return [f for f in files if f.endswith("_mp.csv")]
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

# -----------------------------
# Helper: Refresh GitHub Files
# -----------------------------
def refresh_github_files(s, source: str = "session"):
    """
    Refresh the list of GitHub files from either:
    - session (default): uses st.session_state["github_settings"]
    - env: uses variables GitHub_Token, Repository_Owner, Repository_Name, Folder_Path
    """
    if source == "session":
        gh = st.session_state.get("github_settings")
        if not gh:
            rt = "⚠️ No GitHub settings found in session to refresh files."
            return rt
        token, owner, repo, folder = gh["token"], gh["owner"], gh["repo"], gh["folder"]
    else:
        # for new GitHub Settings tab connection using environment/secure variables
        token, owner, repo, folder = s["GitHub_Token"], s["Repository_Owner"], s["Repository_Name"], s["Folder_Path"]

    url1 = f"{owner}/{repo}/contents/{folder}"
    url2 = urllib.parse.quote(url1)
    url = urllib.parse.urljoin("https://api.github.com/repos/",url2)
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json"
    }
    # Retry session for GitHub operations
    ses = requests.Session()
    retry = Retry(total=5, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
    ses.mount("https://", HTTPAdapter(max_retries=retry))
    try:
        resp = ses.get(url, headers=headers, timeout=15)
        if resp.status_code == 200:
            if source == "session":
                st.session_state["github_files"] = [
                    f["name"] for f in resp.json() if f["type"] == "file"
                ]
            rt = "✅ GitHub files refreshed successfully."
            return rt
        else:
            rt = f"❌ Failed to refresh files: {resp.status_code} - {resp.text}"
            return rt
    except Exception as e:
        rt = f"❌ Error refreshing GitHub files: {e}"
        return rt


# -----------------------------
# Helper functions (save/delete)
# -----------------------------
def github_save_or_update_csv(base_token, base_owner, base_repo, base_folder, filename, token, owner, repo, folder):
    """
    Save or overwrite the given GitHub details into the target GitHub repo using base_* credentials.
    """
    import base64, json

    url1 = f"{base_owner}/{base_repo}/contents/{base_folder}/{filename}"
    url2 = urllib.parse.quote(url1)
    url = urllib.parse.urljoin("https://api.github.com/repos/",url2)
    headers = {"Authorization": f"token {base_token}", "Accept": "application/vnd.github.v3+json"}
    content = f"token_ref,owner,repo,folder\n{token},{owner},{repo},{folder}\n"
    encoded_content = base64.b64encode(content.encode()).decode()

    s = requests.Session()
    retry = Retry(total=5, backoff_factor=0.5,
                  status_forcelist=[500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retry))

    # check if file exists
    resp = s.get(url, headers=headers)
    sha = resp.json().get("sha") if resp.status_code == 200 else None

    payload = {
        "message": f"Save or update {filename}",
        "content": encoded_content,
        "branch": "main"
    }
    if sha:
        payload["sha"] = sha  # update existing

    put_resp = s.put(url, headers=headers, json=payload)
    if put_resp.status_code in [200, 201]:
        return f"✅ File '{filename}' saved successfully."
    else:
        return f"❌ Failed to save file: {put_resp.status_code} - {put_resp.text}"


def delete_github_file(base_token, base_owner, base_repo, base_folder, filename):
    """Delete the selected GitHub file."""
    import json

    url = f"https://api.github.com/repos/{base_owner}/{base_repo}/contents/{base_folder}/{filename}"
    headers = {"Authorization": f"token {base_token}", "Accept": "application/vnd.github.v3+json"}

    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        sha = resp.json()["sha"]
        payload = {"message": f"Delete {filename}", "sha": sha, "branch": "main"}
        del_resp = requests.delete(url, headers=headers, json=payload)
        if del_resp.status_code == 200:
            st.success(f"🗑️ File '{filename}' deleted successfully.")
        else:
            st.error(f"❌ Delete failed: {del_resp.status_code} - {del_resp.text}")
    else:
        st.warning(f"⚠️ File '{filename}' not found on GitHub.")

# -----------------------------------
# TAB 1: Determine Lineage (Program1)
# -----------------------------------
def tab1_determine_lineage():
    st.markdown("#### 📂 Determine Lineage")

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
            github_res = github_file_dropdown("tab1",{})  # (filename, file_url)
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
                        rgf = refresh_github_files({})
                        if rgf[:1] == "⚠️":
                            st.warning(rgf)
                        elif rgf[:1] == "✅":
                            st.success(rgf)
                        else:
                            st.error(rgf)

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
def tab2_upload_lineage():
    st.markdown("#### 🔗 Upload Lineage")

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
                        github_res = github_file_dropdown("tab2",{})
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
                                rgf = refresh_github_files({})
                                if rgf[:1] == "⚠️":
                                    st.warning(rgf)
                                elif rgf[:1] == "✅":
                                    st.success(rgf)
                                else:
                                    st.error(rgf)

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
# TAB 21: Delete Lineage (Program2 lineage deletion)
# ------------------------
def tab2_delete_lineage():
    st.markdown("#### ⛓️‍💥 Delete Lineage")

    sources = get_sources("source", "")
    if not sources:
        st.info("No sources available or failed to fetch sources.")
    else:
        selected_source = st.selectbox("Select Source", list(sources.keys()), index=None, placeholder="Select from list", key="src_tab21")
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
                        github_res = github_file_dropdown("tab21",{})
                        if github_res and github_res[1]:
                            try:
                                csv_text = requests.get(github_res[1]).text
                                df = pd.read_csv(StringIO(csv_text), quotechar='"', skip_blank_lines=True, on_bad_lines="skip")
                            except Exception as e:
                                st.error(f"Failed to parse GitHub file: {e}")
                    else:
                        uploaded = st.file_uploader("Upload Lineage to be Deleted(CSV/TXT)", type=["csv", "txt"], key="delete_lineage")
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

                                    desc_line = ""

                                    _, src_col_id = find_dataset_and_column(SOURCE_ID, COLLECTION_ID, src_table, src_column)
                                    _, trg_col_id = find_dataset_and_column(SOURCE_ID, COLLECTION_ID, trg_table, trg_column)

                                    if src_col_id and trg_col_id:
                                        src_update = update_asset_desc(src_col_id, desc_line)
                                        trg_update = update_asset_desc(trg_col_id, desc_line)
                                        lineage_response = delete_lineage(src_col_id, trg_col_id)
                                        results.append({
                                            "Source Table": src_table, "Source Column": src_column,
                                            "Target Table": trg_table, "Target Column": trg_column,
                                            "Source Col ID": src_col_id, "Target Col ID": trg_col_id,
                                            "Source Desc Update": src_update, "Target Desc Update": trg_update,
                                            "Lineage Response": lineage_response or "Successfully Deleted"
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
                                dl_filename = f"{file_name_f}_del_res.csv"

                                if saved_flag:
                                    st.info(f"Saved to: {saved_path}")
                                else:
                                    st.info("File not saved locally (running in cloud or no Downloads). Use download button below.")

                                gh_filename = dl_filename
                                ok,msg = save_to_github(gh_filename, csv_bytes)
                                rgf = refresh_github_files({})
                                if rgf[:1] == "⚠️":
                                    st.warning(rgf)
                                elif rgf[:1] == "✅":
                                    st.success(rgf)
                                else:
                                    st.error(rgf)

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
def tab3_create_dq_rules():
    st.markdown("#### 🛠️ Create DQ Rules (from DDL)")

    github_res = None
    uploaded_ddl = None
    ddl_content = ""

    if st.session_state.get("github_files"):
        github_res = github_file_dropdown("tab3",{})
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
                            rgf = refresh_github_files({})
                            if rgf[:1] == "⚠️":
                                st.warning(rgf)
                            elif rgf[:1] == "✅":
                                st.success(rgf)
                            else:
                                st.error(rgf)

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

# ------------------------
# TAB 4: Upload DQ Rules (CSV/TXT) -> create monitors
# ------------------------
def tab4_upload_dq_rules():
    st.markdown("#### ✅ Upload DQ Rules (Create Monitors)")

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
                github_res = github_file_dropdown("tab4",{})
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
                        rgf = refresh_github_files({})
                        if rgf[:1] == "⚠️":
                            st.warning(rgf)
                        elif rgf[:1] == "✅":
                            st.success(rgf)
                        else:
                            st.error(rgf)

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
def tab5_create_link_mapping():
    st.markdown("#### ⇄ Create Link Mapping (From DDL and Glossary)")

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
        st.markdown("##### 📁 Select files from GitHub")

        col1, col2 = st.columns(2)
        with col1:
            github_res_ddl = github_file_dropdown("tab51",{})
        with col2:
            github_res_glossary = github_file_dropdown("tab52",{})

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
                            rgf = refresh_github_files({})
                            if rgf[:1] == "⚠️":
                                st.warning(rgf)
                            elif rgf[:1] == "✅":
                                st.success(rgf)
                            else:
                                st.error(rgf)

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

# ------------------------
# TAB 6: Upload Link Mapping (Link Mapping updation)
# ------------------------
def tab6_upload_link_mapping():
    st.markdown("#### 📤 Upload Link Mapping")

    sources = get_sources("source", "")
    if not sources:
        st.info("No sources available or failed to fetch sources.")
    else:
        selected_source = st.selectbox("Select Source", list(sources.keys()), index=None, placeholder="Select from list", key="src_tab6")
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
                        github_res = github_file_dropdown("tab6",{})
                        if github_res and github_res[1]:
                            try:
                                csv_text = requests.get(github_res[1]).text
                                df = pd.read_csv(StringIO(csv_text), quotechar='"', skip_blank_lines=True, on_bad_lines="skip")
                            except Exception as e:
                                st.error(f"Failed to parse GitHub file: {e}")
                    else:
                        uploaded = st.file_uploader("Upload Link Mapping (CSV/TXT)", type=["csv", "txt"], key="upload_lineage")
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

                        if st.button("🚀 Process Link Mapping Updation"):
                            results = []
                            with st.spinner("Processing Link Mapping Updation..."):
                                for idx, row in df.iterrows():
                                    table_name = str(row.get("physical table name", "")).strip()
                                    column_name = str(row.get("physical attribute name", "")).strip()
                                    glossary_name = str(row.get("glossary", "")).strip()
                                    category_name = str(row.get("category", "")).strip()
                                    term_name = str(row.get("term name", "")).strip()

                                    _, col_id = find_dataset_and_column(SOURCE_ID, COLLECTION_ID, table_name, column_name)
                                    glossary_id, category_id, term_id  = find_glossary_category_term(glossary_name, category_name, term_name)

                                    if col_id and term_id:
                                        #src_update = update_asset_desc(src_col_id, desc_line)
                                        #trg_update = update_asset_desc(trg_col_id, desc_line)
                                        link_mapping_response = update_link_mapping(col_id, term_id)
                                        results.append({
                                            "physical table name": table_name, "physical attribute name": column_name,
                                            "glossary": glossary_name, "category": category_name, "term name": term_name,
                                            "Column ID": col_id, "Term ID": term_id,
                                            #"Source Desc Update": src_update, "Target Desc Update": trg_update,
                                            "Link Mapping Response": link_mapping_response
                                        })
                                    else:
                                        results.append({
                                            "physical table name": table_name, "physical attribute name": column_name,
                                            "glossary": glossary_name, "category": category_name, "term name": term_name,
                                            "Column ID": col_id, "Term ID": term_id,
                                            #"Source Desc Update": "⚠️ Not found", "Target Desc Update": "⚠️ Not found",
                                            "Link Mapping Response": "⚠️ Skipped"
                                        })

                                file_name_value = os.path.splitext(uploaded.name)[0] if uploaded else github_res[0]
                                last_dot_index = len(file_name_value) if file_name_value.lower().rfind('.') == -1 else file_name_value.lower().rfind('.')
                                file_name_f = file_name_value[0:last_dot_index]

                                res_df = pd.DataFrame(results)
                                st.markdown(f"##### 🔹 Link Mapping Output - {file_name_f}")
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
                                rgf = refresh_github_files({})
                                if rgf[:1] == "⚠️":
                                    st.warning(rgf)
                                elif rgf[:1] == "✅":
                                    st.success(rgf)
                                else:
                                    st.error(rgf)

                                st.info(msg)
                                if not ok:
                                    st.info("File not saved into Github. Use download button below.")

                                st.download_button(
                                    label=f"⬇️ Download CSV as {dl_filename}",
                                    data=csv_bytes,
                                    file_name=dl_filename,
                                    mime="text/csv"
                                )


# ============================================================
# Tab 7 - GitHub Settings
# ============================================================
def tab7_github_settings(s):
    st.markdown("#### ⚙️ GitHub Settings")

    # Handle pending clear before rendering dropdown
    if st.session_state.get("_gh_clear_pending"):
        for key in list(st.session_state.keys()):
            if key.startswith(("GT_", "RO_", "RN_", "FP_")):
                del st.session_state[key]
        st.session_state["_gh_last_selected"] = None
        st.session_state["selected_value"] = None
        st.session_state["_gh_cleared_selected"] = True
        st.session_state["github_settings"] = {}
        st.session_state["tmp_github_settings"] = {}
        st.session_state.pop("file_select_GitHub_Settings", None)
        st.session_state["_gh_clear_pending"] = False
        st.toast("✅ GitHub settings cleared.", icon="🧹")
        st.rerun()

    st.session_state.setdefault("_gh_last_selected", None)
    st.session_state.setdefault("selected_value", None)
    if '_gh_cleared_selected' not in st.session_state:
        st.session_state["_gh_cleared_selected"] = False

    selected = github_file_dropdown("GitHub_Settings", s)

    GitHub_Token, Repository_Owner, Repository_Name, Folder_Path = (
        s["GitHub_Token"], s["Repository_Owner"], s["Repository_Name"], s["Folder_Path"]
    )

    token_val = owner_val = repo_val = folder_val = ""

    # --- If a file is selected ---
    if selected:
        try:
            df = pd.read_csv(selected[1])
            if {"token_ref", "owner", "repo", "folder"}.issubset(df.columns):
                token, owner, repo, folder = df.iloc[0].tolist()
                token_val = f"ghp_{token}"
                owner_val, repo_val, folder_val = owner, repo, folder
                st.session_state["github_settings"] = {
                    "token": token_val,
                    "owner": owner_val,
                    "repo": repo_val,
                    "folder": folder_val
                }
                if not st.session_state["_gh_last_selected"] == selected[0]:
                    st.session_state["_gh_last_selected"] = selected[0]
                    st.success(f"Loaded details from {selected[0]}")
                    res1 = refresh_github_files({},source="session")
                    if res1 and res1[:1] == "✅":
                        st.success(res1)
                        st.rerun()
                    elif res1[:2] == "⚠️":
                        st.warning(res1)
                    else:
                        st.error(res1)
            else:
                st.warning("⚠️ Invalid file format.")
        except Exception as e:
            st.error(f"❌ Error loading file: {e}")
    else:
        if 'selected_file_indicator' not in st.session_state:
            st.session_state["selected_file_indicator"] = False
        col = "col"
        try:
            st.session_state["github_settings"] = {}
            st.session_state["github_files"] = {}
            # ✅ Lazy refresh if GitHub files not available
            res1 = refresh_github_files({},source="session")
            if res1 and res1[:2] == "⚠️":
                st.success("No Github file seleted from the dropdown")
            if st.session_state["_gh_cleared_selected"]:
                st.session_state["_gh_cleared_selected"] = False
                token_val, owner_val, repo_val, folder_val = "", "", "", ""
                if st.session_state.get("selected_file_indicator") == False:
                    st.session_state["selected_file_indicator"] = True
                    st.rerun()
            else:
                st.error(res1)
        except Exception as e:
            st.error(f"❌ Error not loading file: {e}")

    st.session_state["selected_file_indicator"] = False

    if selected:
        # Textboxes for manual entry or loaded values
        st.markdown("##### Enter or Edit GitHub Details")
    else:
        # Textboxes for manual entry or loaded values
        st.markdown("##### Enter GitHub Details Manually")

    # --- Text boxes ---
    key_suffix = st.session_state.get("_gh_last_selected", "manual")
    token1 = st.text_input("GitHub Token", value=token_val, key=f"GT_{key_suffix}")
    owner1 = st.text_input("Repository Owner", value=owner_val, key=f"RO_{key_suffix}")
    repo1 = st.text_input("Repository Name", value=repo_val, key=f"RN_{key_suffix}")
    folder1 = st.text_input("Folder Path", value=folder_val, key=f"FP_{key_suffix}")

    st.session_state["tmp_github_settings"] = {
        "token": token1, "owner": owner1, "repo": repo1, "folder": folder1
    }

    # --- Action buttons ---
    col1, col2, col3, _ = st.columns([1, 1, 1, 6])

    with col1:
        if st.button("💾 Save", use_container_width=True):
            if all([token1, owner1, repo1, folder1]):
                save_name = f"{owner1}_{repo1}_gh.csv"
                msg = github_save_or_update_csv(
                    GitHub_Token, Repository_Owner, Repository_Name, Folder_Path,
                    save_name, token1[4:] if token1.startswith("ghp_") else token1,
                    owner1, repo1, folder1
                )
                st.toast(msg)

                # ✅ Use base (secure) credentials for refresh
                res1 = refresh_github_files(s, source="env")
                st.session_state["_gh_force_reload"] = True
                if res1.startswith("✅"):
                    st.toast("✅ Saved & refreshed successfully", icon="💾")
                else:
                    st.error(res1)
                
                # ✅ Clear and immediately reload new dropdown
                clear_github_settings()
            else:
                st.warning("⚠️ Fill in all fields before saving.")

    with col2:
        if st.button("🗑️ Delete", use_container_width=True):
            if selected:
                delete_github_file(
                    GitHub_Token, Repository_Owner, Repository_Name, Folder_Path, selected[0]
                )
                refresh_github_files(s, source="env")
                st.toast("🗑️ File deleted successfully", icon="✅")
                clear_github_settings()
            else:
                st.warning("⚠️ Select a file to delete.")

    with col3:
        st.button("🧹 Clear", use_container_width=True, on_click=clear_github_settings)



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

# -----------------------------
# Sidebar Navigation
# -----------------------------
def sidebar_navigation():
    st.sidebar.title("Navigation")
    sections = ["Physical Mapping", "DQ Rules", "Lineage", "Settings"]

    if "active_section" not in st.session_state:
        st.session_state["active_section"] = sections[0]

    for sec in sections:
        if st.sidebar.button(sec, use_container_width=True, type="secondary" if st.session_state["active_section"] != sec else "primary"):
            st.session_state["active_section"] = sec
            st.session_state["active_tab"] = "Create"  # default tab
            st.rerun()

# -----------------------------
# Main Layout with Dynamic Tabs
# -----------------------------
def main_layout(s):
    # ensure sidebar is created and active_section initialized
    sidebar_navigation()

    active_section = st.session_state.get("active_section", "Physical Mapping")

    # Header
    st.subheader(f"{active_section}")

    # Physical Mapping -> Create / Upload
    if active_section == "Physical Mapping":
        tab_create, tab_upload = st.tabs(["⇄ Create", "📤 Upload"])
        with tab_create:
            tab5_create_link_mapping()   # ⇄ Create Link Mapping
        with tab_upload:
            tab6_upload_link_mapping()   # 📤 Upload Link Mapping

    # DQ Rules -> Create / Upload
    elif active_section == "DQ Rules":
        tab_create, tab_upload = st.tabs(["🛠️ Create", "✅ Upload"])
        with tab_create:
            tab3_create_dq_rules()      # 🛠️ Create DQ Rules
        with tab_upload:
            tab4_upload_dq_rules()      # ✅ Upload DQ Rules

    # Lineage -> Create / Upload / Delete
    elif active_section == "Lineage":
        tab_create, tab_upload, tab_delete = st.tabs(["📂 Create", "🔗 Upload", "️‍⛓️‍💥 Delete"])
        with tab_create:
            tab1_determine_lineage()    # 📂 Determine Lineage
        with tab_upload:
            tab2_upload_lineage()       # 🔗 Upload Lineage
        with tab_delete:
            tab2_delete_lineage()       # ⛓️‍💥 Delete Lineage

    # Settings -> single GitHub Settings tab (PASS s to the function)
    elif active_section == "Settings":
        tab_settings, = st.tabs(["⚙️ GitHub Settings"])
        with tab_settings:
            # ensure 's' is the same session/config dict you pass from __main__
            tab7_github_settings(s)

    else:
        # fallback: show message and default to Physical Mapping tabs
        st.warning(f"Unknown section '{active_section}'. Showing Physical Mapping by default.")
        tab_create, tab_upload = st.tabs(["Create", "Upload"])
        with tab_create:
            tab5_create_link_mapping()
        with tab_upload:
            tab6_upload_link_mapping()

# -----------------------------
# Streamlit App Entry Point
# -----------------------------
if __name__ == "__main__":
    main_layout(s)

