import streamlit as st
import pandas as pd
import json
import requests
import urllib3
import os

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ========================
# CONFIG
# ========================
# Get secrets from Streamlit Cloud (or local .streamlit/secrets.toml during development)
API_KEY = st.secrets.get("API_KEY") or os.environ.get("API_KEY")
BASE_URL = st.secrets.get("BASE_URL", "https://connect.avadatatrust.com/api/v1/data/assets/search")
ASSET_URL = st.secrets.get("ASSET_URL", "https://connect.avadatatrust.com/api/v1/data/assets")
LINEAGE_URL = st.secrets.get("LINEAGE_URL", "https://connect.avadatatrust.com/api/v1/data/catalog/lineage/manual_lineage")

# ========================
# FUNCTIONS
# ========================
def get_sources():
    """Fetch available sources from API to populate dropdown"""
    payload = {
        "asset_type": "source",
        "page": 1,
        "page_size": 100
    }
    response = requests.post(
        BASE_URL,
        headers={"X-Decube-Api-Key": API_KEY, "Content-Type": "application/json"},
        data=json.dumps(payload),
        verify=False
    )

    if response.status_code != 200:
        st.error(f"Failed to fetch sources: {response.text}")
        return []

    data = response.json().get("data", [])
    sources = {item["asset"]["name"]: item["asset"]["id"] for item in data}
    return sources


def search_asset(query, parent_id, parent_type="source", page_size=50):
    payload = {
        "query": query,
        "search_type": "exact",
        "parent": {"id": parent_id, "type": parent_type},
        "show_deleted": False,
        "page": 1,
        "page_size": page_size
    }

    response = requests.post(
        BASE_URL,
        headers={"X-Decube-Api-Key": API_KEY, "Content-Type": "application/json"},
        data=json.dumps(payload),
        verify=False
    )

    if response.status_code != 200:
        return []
    return response.json().get("data", [])


def find_dataset_and_column(source_id, table_name, column_name):
    datasets = search_asset(table_name, parent_id=source_id, parent_type="source")
    if not datasets:
        return None, None

    dataset = datasets[0]
    dataset_id = dataset["asset"]["id"]
    dataset_type = dataset["asset"]["type"]

    columns = search_asset(column_name, parent_id=source_id, parent_type="source")
    if not columns:
        return dataset_id, None

    filtered = [
        item for item in columns
        if any(p["id"] == dataset_id and p["type"] == dataset_type for p in item.get("parents", []))
    ]

    if not filtered:
        return dataset_id, None

    column_id = filtered[0]["asset"]["id"]
    return dataset_id, column_id


def get_asset_desc(asset_id):
    url = f"{ASSET_URL}/{asset_id}"
    response = requests.get(
        url,
        headers={"X-Decube-Api-Key": API_KEY, "Content-Type": "application/json"},
        verify=False
    )
    if response.status_code == 200:
        return response.json().get("asset", {}).get("description", "")
    return ""


def normalize_desc(desc):
    """Normalize description for comparison"""
    if not desc:
        return ""
    return desc.replace('"', '').strip().lower()


def update_asset_desc(asset_id, description):
    current_desc = get_asset_desc(asset_id)

    # Normalize both current and new descriptions
    if normalize_desc(current_desc) == normalize_desc(description):
        return {
            "status": "skipped",
            "id": asset_id,
            "reason": "Description already up-to-date"
        }

    payload = {
        "asset_id": asset_id,
        "asset_type": "property",
        "description": description
    }

    response = requests.patch(
        ASSET_URL,
        headers={
            "X-Decube-Api-Key": API_KEY,
            "Content-Type": "application/json"
        },
        data=json.dumps(payload),
        verify=False
    )

    if response.status_code == 200:
        return {"status": "success", "id": asset_id, "new_description": description}
    else:
        return {"status": "error", "id": asset_id, "detail": response.text}


def create_lineage(src_column_id, trg_column_id):
    source = {"id": src_column_id, "type": "property"}
    target = {"id": trg_column_id, "type": "property"}

    headers = {
        "X-Decube-Api-Key": API_KEY,
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    response = requests.post(
        LINEAGE_URL,
        headers=headers,
        data=json.dumps({"source": source, "target": target, "data_job": None}),
        verify=False
    )

    if response.status_code != 200:
        return {"error": response.text}

    try:
        return response.json()
    except Exception:
        return {"error": response.text}


# ========================
# STREAMLIT APP
# ========================
st.title("📊 AAVA DTS Lineage Builder")
st.write("Upload a CSV/TXT file with Source/Target table-column mappings to create lineage and update descriptions.")

# Fetch sources dynamically
sources_dict = get_sources()
if not sources_dict:
    st.stop()

selected_source_name = st.selectbox("Select Source", options=list(sources_dict.keys()))
SOURCE_ID = sources_dict[selected_source_name]

uploaded_file = st.file_uploader("Upload input.csv or input.txt", type=["csv", "txt"])

if uploaded_file:
    if uploaded_file.name.endswith(".csv"):
        df = pd.read_csv(uploaded_file, quotechar='"', skip_blank_lines=True, on_bad_lines="skip")
    elif uploaded_file.name.endswith(".txt"):
        first_line = uploaded_file.readline().decode("utf-8")
        uploaded_file.seek(0)
        delimiter = "\t" if "\t" in first_line else ","
        df = pd.read_csv(uploaded_file, delimiter=delimiter, quotechar='"', skip_blank_lines=True, on_bad_lines="skip")
    else:
        st.error("Unsupported file format.")
        st.stop()

    st.write("### Input Preview", df)

    if st.button("🚀 Process Lineage"):
        results = []

        for _, row in df.iterrows():
            src_table = str(row["Source Table"]).strip().lower()
            src_column = str(row["Source Column"]).strip().lower()
            trg_table = str(row["Target Table"]).strip().lower()
            trg_column = str(row["Target Column"]).strip().lower()

            value_line = ",".join([f"\"{row[col]}\"" if "," in str(row[col]) else str(row[col]) for col in df.columns])
            asset_description = value_line

            _, src_col_id = find_dataset_and_column(SOURCE_ID, src_table, src_column)
            _, trg_col_id = find_dataset_and_column(SOURCE_ID, trg_table, trg_column)

            if src_col_id and trg_col_id:
                src_asset_desc_response = update_asset_desc(src_col_id, asset_description)
                trg_asset_desc_response = update_asset_desc(trg_col_id, asset_description)
                lineage_response = create_lineage(src_col_id, trg_col_id)
                results.append({
                    "Source Table": src_table,
                    "Source Column": src_column,
                    "Target Table": trg_table,
                    "Target Column": trg_column,
                    "Target Column ID": trg_col_id,
                    "Source Column ID": src_col_id,
                    "Asset description": asset_description,
                    "Source Asset Description Update": src_asset_desc_response,
                    "Target Asset Description Update": trg_asset_desc_response,
                    "Lineage Response": lineage_response
                })
            else:
                results.append({
                    "Source Table": src_table,
                    "Source Column": src_column,
                    "Target Table": trg_table,
                    "Target Column": trg_column,
                    "Target Column ID": trg_col_id,
                    "Source Column ID": src_col_id,
                    "Asset description": asset_description,
                    "Source Asset Description Update": "⚠️ Skipped - column not found",
                    "Target Asset Description Update": "⚠️ Skipped - column not found",
                    "Lineage Response": "⚠️ Skipped - column not found"
                })

        st.write("### ✅ Lineage Results")
        st.dataframe(pd.DataFrame(results))

        json_bytes = json.dumps(results, indent=4).encode("utf-8")
        st.download_button(
            label="📥 Download Results JSON",
            data=json_bytes,
            file_name="lineage_results.json",
            mime="application/json"
        )
