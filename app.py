import streamlit as st
import pandas as pd
import requests
import json
import urllib3
import base64
import os

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ========================
# CONFIG
# ========================

API_KEY = st.secrets["API_KEY"]
BASE_URL = st.secrets["BASE_URL"]
ASSET_URL = st.secrets["ASSET_URL"]
LINEAGE_URL = st.secrets["LINEAGE_URL"]
DQ_URL = st.secrets["DQ_URL"]
LOGO_URL = st.secrets["LOGO_URL"]

# ========================
# COMMON FUNCTIONS
# ========================
def get_sources(asset_type, source_id):
    """Fetch available sources from API to populate dropdown"""
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
    sources = {item["asset"]["name"]: item["asset"]["id"] for item in data}
    return sources


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

# ========================
# LINEAGE FUNCTIONS
# ========================
def find_dataset_and_column(source_id, collection_id, table_name, column_name):
    datasets = search_asset(table_name, parent_id=source_id, parent_type="source")
    if not datasets:
        return None, None

    dataset_id = None
    for d in datasets:
        if d["parents"][0]["id"] == source_id and d["parents"][1]["id"] == collection_id:
            dataset_id = d["asset"]["id"]

    dataset_type = "dataset"

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

    if not column_id:
        return dataset_id, None

    return dataset_id, column_id


def update_asset_desc(asset_id, description):
    payload = {
        "asset_id": asset_id,
        "asset_type": "property",
        "description": description,
    }

    response = requests.patch(
        ASSET_URL,
        headers={"X-Decube-Api-Key": API_KEY, "Content-Type": "application/json"},
        data=json.dumps(payload),
        verify=False,
    )

    if response.status_code == 200:
        return {"status": "success", "id": asset_id}
    else:
        return {"status": "error", "id": asset_id, "detail": response.text}


def create_lineage(src_column_id, trg_column_id):
    source = {"id": src_column_id, "type": "property"}
    target = {"id": trg_column_id, "type": "property"}

    response = requests.post(
        LINEAGE_URL,
        headers={
            "X-Decube-Api-Key": API_KEY,
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
        data=json.dumps({"source": source, "target": target, "data_job": None}),
        verify=False,
    )

    if response.status_code != 200:
        return {"error": response.text}
    try:
        return response.json()
    except Exception:
        return {"error": response.text}

# ========================
# DQ FUNCTIONS
# ========================
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
def get_base64_image_from_url(url):
    response = requests.get(url)
    if response.status_code == 200:
        return base64.b64encode(response.content).decode()
    else:
        return ""

hide_streamlit_style = """
    <style>
    /* Hide top-right processing spinner and stop button */
    .stStatusWidget, .st-emotion-cache-13ln4jf {
        display: none !important;
    }
    </style>
"""
st.markdown(hide_streamlit_style, unsafe_allow_html=True)

LOGO_BASE64 = get_base64_image_from_url(LOGO_URL)

st.set_page_config(page_title="AAVA DTS Tools", layout="wide")

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

# Tabs appear directly below the logo+title
tab1, tab2 = st.tabs(["🔗 Lineage", "✅ Data Quality"])

st.markdown("</div>", unsafe_allow_html=True)

# ------------------------
# TAB 1: LINEAGE
# ------------------------
with tab1:
    st.header("Lineage Builder")
    st.write("Scroll down to test sticky header...")

    sources_dict = get_sources("source", "")
    if not sources_dict:
        st.stop()

    selected_source_name = st.selectbox("Select Source", options=list(sources_dict.keys()), key="lineage_source")
    SOURCE_ID = sources_dict[selected_source_name]

    collection_dict = get_sources("collection", SOURCE_ID)
    if not collection_dict:
        st.stop()

    selected_collection_name = st.selectbox("Select collection", options=list(collection_dict.keys()), key="lineage_collection")
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
# TAB 2: DQ
# ------------------------
with tab2:
    st.header("Data Quality Monitor Creator")

    sources_dict = get_sources("source", "")
    if not sources_dict:
        st.stop()

    selected_source_name = st.selectbox("Select Source", options=list(sources_dict.keys()), key="dq_source")
    SOURCE_ID = sources_dict[selected_source_name]

    uploaded_file = st.file_uploader("Upload DQ Details (CSV)", type=["csv"], key="dq_file")

    if uploaded_file:
        df = pd.read_csv(uploaded_file)
        st.subheader("Input Data Preview")
        st.dataframe(df)

        if st.button("🚀 Process DQ Creation"):
            results = []
            for idx, row in df.iterrows():
                results.append(create_dq_monitor(SOURCE_ID, row, idx))

            st.subheader("Monitor Creation Results")
            st.dataframe(pd.DataFrame(results))
