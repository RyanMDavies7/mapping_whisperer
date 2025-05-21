# utils/template_writer.py
# -----------------------
# This module writes parsed Dezign data into an Excel template
# (mapping_1-11.xlsx) while preserving formatting and formulas.
# It duplicates the template's row styles, then pastes in values-only
# to mimic Excel's "Paste as Values."

from __future__ import annotations
from pathlib import Path
from openpyxl import load_workbook
from utils.helpers import get_header_row, unwrap_merged_headers, normalize
import copy

# -----------------------------------------------------------------------------
# HDRS map: logical keys → exact header text in the Excel template
# These must match the header row in 'Transformation - Sourcing (1)'.
# -----------------------------------------------------------------------------
HDRS = {
    "name"     : "Attribute Name",
    "desc"     : "Attribute Description",
    "type"     : "Datatype",
    "sd"       : "Sourced/Derived",
    "src_t"    : "Source Table",
    "src_a"    : "Source Attribute",
    "nn"       : "Not Null",
    "def_v"    : "Default Values",
    "def_m1"   : "Default Records",
    "def_m2"   : "Default Records (2)",
    "key"      : "Keys",
    "clust"    : "Clustering",
    "part"     : "Partitioning",
    "ref_dim"  : "Referenced Dimension"
}


def _build_map(ws, hdr_row: int) -> dict[str, int]:
    """
    Build a map of normalized header text → column index (1-based).

    - Unwrap merged headers first so that each header cell has unique text.
    - Normalize (lowercase + strip) header values for matching.

    Example:
      hdr = get_header_row(ws, "#")
      cmap = _build_map(ws, hdr)
      # cmap might contain {'attribute name': 2, 'datatype': 4, ...}
    """
    unwrap_merged_headers(ws, hdr_row)
    return {
        normalize(cell.value): cell.column
        for cell in ws[hdr_row]
        if cell.value
    }


def _copy_style_only(ws, src_row: int, dst_row: int) -> None:
    """
    Copy ONLY the cell style and number_format from src_row to dst_row.

    - Does NOT copy cell.value, so existing formulas/values (e.g. "#" column)
      remain intact.
    - Ensures new rows inherit borders, fills, fonts, alignment, CF.

    Usage:
      # After inserting a row at position X, copy style from template row:
      _copy_style_only(ws, template_row, X)
    """
    for col in range(1, ws.max_column + 1):
        src = ws.cell(src_row, col)
        dst = ws.cell(dst_row, col)
        if src.has_style:
            dst._style = copy.copy(src._style)
        dst.number_format = src.number_format


def write_entity(entity: dict,
                 template_path: str | Path,
                 out_dir: str | Path) -> Path:
    """
    Populate one Excel workbook for a given entity using the template.

    Steps:
      1. Load the template and select the 'Transformation - Sourcing (1)' sheet.
      2. Fill top metadata (table name & description) in cells B3 & B4.
      3. Locate the header row (matching '#'), unwrap merged headers.
      4. Build a header→column map via _build_map().
      5. Ensure all required headers exist in the template.
      6. Calculate how many attribute rows are needed.
         - Template pre-seeds 500 rows; insert extras if needed.
      7. For each attribute row:
         a. Copy style-only from the first template data row.
         b. Write values into the columns of interest (skip '#').
      8. Save as '<out_dir>/<entity_name>.xlsx'.

    Args:
      entity: dict with keys 'name', 'description', 'fields' (list of dicts).
      template_path: path to mapping_1-11.xlsx template.
      out_dir: directory to save output files.

    Returns:
      Path of the saved workbook.
    """
    # 1) Load workbook and pick sheet
    wb = load_workbook(template_path, keep_vba=False)
    ws = wb["Transformation - Sourcing (1)"]

    # 2) Write table-level metadata
    ws["B3"] = entity["name"]         # Example: "SalesOrder"
    ws["B4"] = entity["description"]  # Example: "Customer sales orders"

    # 3) Find header row (first cell value '#') and build header map
    hdr_row = get_header_row(ws, "#")
    cmap    = _build_map(ws, hdr_row)

    # 4) Validate that all expected headers exist
    for expected in HDRS.values():
        norm = normalize(expected)
        if norm not in cmap:
            raise ValueError(f"Template missing header: '{expected}'")

    # 5) Determine where data rows start and how many we need
    first_data_row = hdr_row + 1
    needed = len(entity["fields"])

    # 6) Insert additional rows if we exceed template's pre-seeded capacity
    TEMPLATE_CAP = 500
    if needed > TEMPLATE_CAP:
        extra_at = first_data_row + TEMPLATE_CAP
        ws.insert_rows(extra_at, needed - TEMPLATE_CAP)
        # Copy style into the newly inserted rows
        for i in range(needed - TEMPLATE_CAP):
            _copy_style_only(ws, first_data_row + TEMPLATE_CAP - 1,
                                  extra_at + i)

    # 7) For each attribute, copy style and write values
    def col(key):
        """Helper to get the column index by logical key."""
        return cmap[normalize(HDRS[key])]

    for idx, fld in enumerate(entity["fields"]):
        row = first_data_row + idx
        # a) copy style only (value remains from template '#')
        _copy_style_only(ws, first_data_row, row)

        # b) write values into the correct columns
        ws.cell(row, col("name")).value      = fld["name"]
        ws.cell(row, col("desc")).value      = fld["description"]
        ws.cell(row, col("type")).value      = fld["datatype"]
        ws.cell(row, col("sd")).value        = ("Sourced" if fld.get("sourced") else "Derived")
        ws.cell(row, col("src_t")).value     = fld.get("src_table", "")
        ws.cell(row, col("src_a")).value     = fld.get("src_attr", "")
        ws.cell(row, col("ref_dim")).value   = fld.get("referenced_dimension", "")
        ws.cell(row, col("nn")).value        = ("Y" if fld.get("not_null") else "N")
        ws.cell(row, col("def_v")).value     = fld.get("def_val", "")
        ws.cell(row, col("def_m1")).value    = fld.get("def_m1", "")
        ws.cell(row, col("def_m2")).value    = fld.get("def_m2", "")
        ws.cell(row, col("key")).value       = fld.get("key_type", "")
        ws.cell(row, col("clust")).value     = fld.get("clustering", "")
        ws.cell(row, col("part")).value      = fld.get("partitioning", "")

    # 8) Save to outputs directory
    out_path = Path(out_dir) / f"{entity['name']}.xlsx"
    wb.save(out_path)
    return out_path
