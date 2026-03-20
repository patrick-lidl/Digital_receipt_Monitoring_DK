import base64
import json
from typing import Any, Dict, Optional

import pandas as pd
from bs4 import BeautifulSoup


def _extract_dataframe_from_src(src_data: str) -> pd.DataFrame:
    """
    Extract data from Arakawa string.

    Decodes a Base64 encoded Plotly JSON string and converts the data traces
    into a Pandas DataFrame.

    Args:
        src_data (str): The raw source string containing the data URI
                        (e.g., "data:application/vnd.plotly.v1+json;base64,...").

    Returns:
        pd.DataFrame: A DataFrame containing 'x', 'y', and 'trace_name' columns
                      for all traces found in the plot. Returns an empty DataFrame
                      if no valid traces are found.
    """
    # A. Split off the prefix to isolate the Base64 string
    try:
        encoded_str = src_data.split("base64,")[1]
    except IndexError:
        return pd.DataFrame()

    # B. Decode Base64 -> Bytes -> String
    decoded_bytes = base64.b64decode(encoded_str)
    decoded_str = decoded_bytes.decode("utf-8")

    # C. Parse the JSON string
    try:
        plot_json = json.loads(decoded_str)

        # Arakawa often double-stringifies the JSON, so we check if we need to parse again
        if isinstance(plot_json, str):
            plot_json = json.loads(plot_json)
    except json.JSONDecodeError:
        return pd.DataFrame()

    all_dfs = []

    # D. Extract X/Y data from traces
    if "data" in plot_json:
        for trace in plot_json["data"]:
            # We only extract traces that have explicit X and Y coordinates
            if "x" in trace and "y" in trace:
                df = pd.DataFrame(
                    {
                        "x": trace["x"],
                        "y": trace["y"],
                        "trace_name": trace.get("name", "unnamed"),
                    }
                )
                all_dfs.append(df)

    if not all_dfs:
        return pd.DataFrame()

    return pd.concat(all_dfs, ignore_index=True)


def _get_arakawa_data_block(html_file_path: str) -> Optional[Dict[str, Any]]:
    """
    Extract data block from Arakawa HTML file.

    Parses an Arakawa/Datapane HTML report to locate and extract the embedded
    application state (appData).

    Args:
        html_file_path (str): Path to the HTML file.

    Returns:
        Optional[Dict[str, Any]]: The 'result' dictionary from the app data if found,
                                  containing 'viewJson' and 'assets'. Returns None on failure.
    """
    try:
        with open(html_file_path, "r", encoding="utf-8") as f:
            html_content = f.read()

        soup = BeautifulSoup(html_content, "html.parser")

        # Find the specific script tag containing the appData assignment
        target_script = None
        for script in soup.find_all("script"):
            if script.string and 'window.reportProps["appData"]' in script.string:
                target_script = script.string
                break

        if not target_script:
            print("Error: Could not find the 'appData' script block in the HTML.")
            return None

        # Extract the JSON string following the variable assignment
        # We split by the assignment operator to get the right-hand side
        json_str = target_script.split('window.reportProps["appData"] =')[1].strip()

        # Clean up the trailing semicolon commonly found in JS assignments
        if json_str.endswith(";"):
            json_str = json_str[:-1]

        # Return the nested result object which contains the structure and assets
        return json.loads(json_str)["data"]["result"]

    except Exception as e:
        print(f"An error occurred while extracting the data block: {e}")
        return None


def _find_all_src_references(full_dict: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
    """
    Extract data references from report.

    Traverses the 'viewJson' block structure to map human-readable labels
    to their corresponding internal asset reference IDs (hashes).

    Note: This function relies on a specific nested structure of Blocks -> Tabs -> Groups.
    If the report layout changes significantly, this traversal path may need updating.

    Args:
        full_dict (Dict): The parsed appData dictionary containing 'viewJson'.

    Returns:
        Dict: A dictionary mapping forecast types (e.g., 'Wochen-Prognose') to
              sub-dictionaries mapping area names to asset hashes.
    """
    # Extract asset references based on the specific report hierarchy
    # Navigating deep into the nested block structure to find the Plot block
    return {
        wh_data["label"]: {
            area_data["label"]: area_data["blocks"][0]["src"].removeprefix("ref://")
            for area_data in wh_data["blocks"][0]["blocks"]
        }
        for wh_data in full_dict["viewJson"]["blocks"][2]["blocks"][0]["blocks"][1]["blocks"]
    }

    # OLD structure
    # return {
    #     label: {
    #         forecast["label"]: forecast["blocks"][0]["src"].removeprefix("ref://")
    #         for forecast in block["blocks"][0]["blocks"][1]["blocks"][1]["blocks"][1]["blocks"]
    #     }
    #     for block in full_dict["viewJson"]["blocks"]
    #     # Only process blocks with these specific labels
    #     if (label := block.get("label")) in ("Wochen-Prognose", "Tages-Prognose")
    # }


def extract_data_from_arakawa_report(file_path: str) -> pd.DataFrame:
    """
    Extract and build a consolidated DataFrame.

    Args:
        file_path (str): Path to the Arakawa HTML report.

    Returns:
        pd.DataFrame: A concatenated DataFrame containing data from all target plots,
                      enriched with 'grain' (Forecast type) and 'area' columns.
    """
    full_dict = _get_arakawa_data_block(file_path)

    if not full_dict:
        print("Failed to retrieve data dictionary.")
        return pd.DataFrame()

    try:
        # Map the UI labels to the underlying asset IDs
        references = _find_all_src_references(full_dict)

        extracted_data = []

        # Iterate through the mapping and extract actual data from the assets block
        for grain_key, areas_dict in references.items():
            for area_key, hash_reference in areas_dict.items():
                # locate the raw asset data using the hash
                asset_src = full_dict.get("assets", {}).get(hash_reference, {}).get("src")

                if asset_src:
                    # Extract and parse the Base64 data
                    df = _extract_dataframe_from_src(asset_src)

                    # Add metadata columns for context
                    df = df.assign(grain=grain_key, area=area_key)
                    extracted_data.append(df)

        if not extracted_data:
            return pd.DataFrame()

        return pd.concat(extracted_data, ignore_index=True)

    except Exception as e:
        print(f"Error during data consolidation: {e}")
        return pd.DataFrame()


if __name__ == "__main__":
    file_name = "data/kolli_prognosis/forecasts/2026-KW10_kolli_prognose.html"
    df = extract_data_from_arakawa_report(file_name)

    print(0)
