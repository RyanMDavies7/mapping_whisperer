import copy
# === HELPERS ===

# Manual mapping for renamed columns (lowercased and normalized)
COLUMN_NAME_MAPPING = {
    "stagehist table name": "staging table name / static dataset name (xref tab)",
    "staging table name / static dataset name": "staging table name / static dataset name (xref tab)"
}

def copy_metadata(ws_src, ws_tgt):
    """Copy values from B2:B5 into target sheet, assuming metadata fields are in A2:A5."""
    for row in range(2, 6):  # Rows 2 to 5 inclusive
        val = ws_src.cell(row=row, column=2).value  # Column B
        if val not in (None, ""):
            ws_tgt.cell(row=row, column=2).value = val

def normalize(text):
    return str(text).strip().lower() if text else ""

def get_header_row(ws, match_value):
    """Find row where the first column matches a known header label."""
    for row in ws.iter_rows(min_row=1, max_row=100):
        if normalize(row[0].value) == normalize(match_value):
            return row[0].row
    raise Exception(f"Header '{match_value}' not found")

def get_column_map(ws, header_row):
    """Map normalized column names to Excel-style columns (A, B, C...)."""
    col_map = {}
    for cell in ws[header_row]:
        name = normalize(cell.value)
        if not name:
            continue
        count = 1
        original = name
        while name in col_map:
            count += 1
            name = f"{original} ({count})"
        col_map[name] = cell.column
    return col_map

def copy_table(ws_src, ws_tgt, src_header_row, tgt_header_row, label):
    src_cols = get_column_map(ws_src, src_header_row)
    tgt_cols = get_column_map(ws_tgt, tgt_header_row)

    row = 0
    copied_rows = 0

    while True:
        src_r = src_header_row + 1 + row
        tgt_r = tgt_header_row + 1 + row

        # ✅ Stop if source row is entirely blank
        if src_r > ws_src.max_row:
            break

        src_blank = all(ws_src.cell(src_r, c).value in (None, "") for c in src_cols.values())
        if src_blank:
            break

        for src_name, src_col in src_cols.items():
            mapped_name = normalize(COLUMN_NAME_MAPPING.get(src_name, src_name))
            if mapped_name not in tgt_cols:
                continue
            tgt_col = tgt_cols[mapped_name]
            src_val = ws_src.cell(src_r, src_col).value
            tgt_cell = ws_tgt.cell(tgt_r, tgt_col)
            tgt_cell.value = src_val
            tgt_cell.number_format = "General"

        row += 1
        copied_rows += 1

    print(f"✅ Copied {copied_rows} rows from {label}")


def unwrap_merged_headers(ws, header_row):
    """
    Unmerge header cells, assign enumerated names to duplicates, and preserve formatting.
    """
    merged_ranges = [rng for rng in ws.merged_cells.ranges if rng.min_row == header_row]
    seen = {}

    for rng in merged_ranges:
        if rng.min_row != rng.max_row:
            continue
        ws.unmerge_cells(str(rng))

        top_cell = ws.cell(row=header_row, column=rng.min_col)
        base_val = str(top_cell.value).strip()
        base_font = copy.copy(top_cell.font)
        base_fill = copy.copy(top_cell.fill)
        base_alignment = copy.copy(top_cell.alignment)
        base_border = copy.copy(top_cell.border)

        for col in range(rng.min_col, rng.max_col + 1):
            count = seen.get(base_val, 0) + 1
            seen[base_val] = count
            new_val = base_val if count == 1 else f"{base_val} ({count})"

            cell = ws.cell(row=header_row, column=col)
            cell.value = new_val
            cell.font = copy.copy(base_font)
            cell.fill = copy.copy(base_fill)
            cell.alignment = copy.copy(base_alignment)
            cell.border = copy.copy(base_border)

    # Second pass: deduplicate any already existing header collisions
    seen = {}
    for cell in ws[header_row]:
        val = str(cell.value).strip() if cell.value else ""
        if not val:
            continue
        count = seen.get(val, 0) + 1
        seen[val] = count
        if count > 1:
            cell.value = f"{val} ({count})"


