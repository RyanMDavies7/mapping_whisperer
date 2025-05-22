# utils/template_writer.py

"""
Takes one parsed entity (from dez_parser) and writes:
 - Table metadata (name/description)
 - Table options line (Partition/Cluster DDL)
 - Sourcing table (entity['sources'])
 - Transformation fields with all flags
while preserving all template formatting.
"""

from pathlib import Path
from openpyxl import load_workbook
from utils.helpers import get_header_row, unwrap_merged_headers, normalize
import copy

# Map our field-keys to exact Excel header captions:
HDRS = {
  "name":"Attribute Name","desc":"Attribute Description","type":"Datatype",
  "sd":"Sourced/Derived","src_t":"Source Table","src_a":"Source Attribute",
  "ref_dim":"Referenced Dimension","nn":"Not Null",
  "def_v":"Default Values","def_m1":"Default Records",
  "def_m2":"Default Records (2)","clust":"Clustering","part":"Partitioning",
  "key":"Keys"
}

def _build_map(ws, hdr_row:int) -> dict[str,int]:
    unwrap_merged_headers(ws, hdr_row)
    return {
      normalize(c.value): c.column
      for c in ws[hdr_row]
      if c.value
    }

def _copy_style_only(ws, src_row:int, dst_row:int):
    for col in range(1, ws.max_column+1):
        s, d = ws.cell(src_row,col), ws.cell(dst_row,col)
        if s.has_style:
            d._style = copy.copy(s._style)
        d.number_format = s.number_format

def _write_sourcing(ws, entity:dict):
    hdr = get_header_row(ws, "Dependency")
    cmap = { normalize(c.value): c.column for c in ws[hdr] if c.value }
    for i, src in enumerate(entity.get("sources",[])):
        r = hdr+1+i
        if "source database" in cmap:
            ws.cell(r, cmap["source database"]).value = src["database"]
        if "table name/dataset name" in cmap:
            ws.cell(r, cmap["table name/dataset name"]).value = src["table"]
        if "source column" in cmap:
            ws.cell(r, cmap["source column"]).value = src["column"]

def write_entity(entity:dict, template_path:str|Path, out_dir:str|Path) -> Path:
    wb = load_workbook(template_path)
    ws = wb["Transformation - Sourcing (1)"]

    # 1) Metadata
    ws["B3"], ws["B4"] = entity["name"], entity["description"]

    # 2) Table Options (one row above the '#' header)
    hdr = get_header_row(ws, "#")
    opts = entity.get("table_options","")
    if opts:
        ws.cell(hdr-1,1).value = f"Table options: {opts}"

    # 3) Sourcing
    _write_sourcing(ws, entity)

    # 4) Transformation headers
    trans_hdr = hdr
    cmap = _build_map(ws, trans_hdr)
    start = trans_hdr + 1
    needed = len(entity["fields"])
    TEMPLATE_CAP = 500

    # 5) Expand rows if >500
    if needed > TEMPLATE_CAP:
        extra = needed - TEMPLATE_CAP
        ins = start + TEMPLATE_CAP
        ws.insert_rows(ins, extra)
        for off in range(extra):
            _copy_style_only(ws, start+TEMPLATE_CAP-1, ins+off)

    # 6) Ensure style for each row
    for i in range(needed):
        _copy_style_only(ws, start, start+i)

    # 7) Write field values (skip '#' column)
    def C(k): return cmap[normalize(HDRS[k])]
    for i, f in enumerate(entity["fields"]):
        r = start + i
        ws.cell(r,C("name")).value    = f["name"]
        ws.cell(r,C("desc")).value    = f["description"]
        ws.cell(r,C("type")).value    = f["datatype"]
        ws.cell(r,C("sd")).value      = "Sourced" if f["sourced"] else "Derived"
        ws.cell(r,C("src_t")).value   = f["src_table"]
        ws.cell(r,C("src_a")).value   = f["src_attr"]
        ws.cell(r,C("ref_dim")).value = f["referenced_dimension"]
        ws.cell(r,C("nn")).value      = "Y" if f["not_null"] else "N"
        ws.cell(r,C("def_v")).value   = f["def_val"]
        ws.cell(r,C("def_m1")).value  = f["def_m1"]
        ws.cell(r,C("def_m2")).value  = f["def_m2"]
        ws.cell(r,C("clust")).value   = f["clustering"]
        ws.cell(r,C("part")).value    = f["partitioning"]
        ws.cell(r,C("key")).value     = f["key_type"]

    out_path = Path(out_dir) / f"{entity['name']}.xlsx"
    wb.save(out_path)
    return out_path
