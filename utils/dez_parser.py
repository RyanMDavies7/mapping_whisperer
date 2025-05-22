# utils/dez_parser.py

"""
Parses a .dez (XML) file into a list of entities with:
 - name, description
 - sources (from UDPs: source_database_N, source_table_N, source_column_N)
 - table_options (your PARTITION/CLUSTER DDL snippet UDP)
 - fields: with datatype, PK/FK, defaults, sourcing, partitioning, clustering, etc.
"""

import re
import xml.etree.ElementTree as ET
from pathlib import Path

# -------------------------------------------------------------------
# Default-values map based on BigQuery types & naming conventions
# -------------------------------------------------------------------
DEFAULTS = {
    "TIMESTAMP-Start": {
        "Default Values": '"1900-01-01 00:00:00.0000"',
        "Default Records": '"1900-01-01 00:00:00.0000"',
        "Default Records (2)": '"1900-01-01 00:00:00.0000"',
    },
    "TIMESTAMP-End": {
        "Default Values": '"9999-12-31 23:59:59.9999"',
        "Default Records": '"9999-12-31 23:59:59.9999"',
        "Default Records (2)": '"9999-12-31 23:59:59.9999"',
    },
    "TIMESTAMP": {
        "Default Values": '"1900-01-01 00:00:00.0000"',
        "Default Records": '"1900-01-01 00:00:00.0000"',
        "Default Records (2)": '"9999-12-31 23:59:59.9999"',
    },
    "DATETIME": {
        "Default Values": '"1900-01-01 00:00:00.0000"',
        "Default Records": '"1900-01-01 00:00:00.0000"',
        "Default Records (2)": '"9999-12-31 23:59:59.9999"',
    },
    "DATE": {
        "Default Values": '"1900-01-01"',
        "Default Records": '"1900-01-01"',
        "Default Records (2)": '"9999-12-31"',
    },
    "INT64": {
        "Default Values": "-1",
        "Default Records": "-1",
        "Default Records (2)": "-2",
    },
    "STRING": {
        "Default Values": '""',
        "Default Records": '""',
        "Default Records (2)": '""',
    },
    "BOOL": {
        "Default Values": "NULL",
        "Default Records": "NULL",
        "Default Records (2)": "NULL",
    },
    "NUMERIC": {
        "Default Values": "0",
        "Default Records": "0",
        "Default Records (2)": "0",
    },
    "FLOAT64": {
        "Default Values": '"0.0"',
        "Default Records": '"0.0"',
        "Default Records (2)": '"0.0"',
    },
}

def resolve_defaults(col_name: str, dtype: str) -> dict:
    """Pick the right defaults by column name (effective_*) or dtype."""
    key = dtype.upper()
    lower = col_name.lower()
    if "effective_start" in lower:
        key = "TIMESTAMP-Start"
    elif "effective_end" in lower:
        key = "TIMESTAMP-End"
    return DEFAULTS.get(key, {"Default Values":"", "Default Records":"", "Default Records (2)":""})

def parse_dez_file(filepath: str | Path) -> list[dict]:
    """
    Returns a list of entities, each:
      {
        "name": str,
        "description": str,
        "sources": [ {"database","table","column"}, ... ],
        "table_options": str,
        "fields": [
          {
            "name","description","datatype","sourced","not_null",
            "src_table","src_attr",
            "def_val","def_m1","def_m2",
            "key_type","referenced_dimension",
            "partitioning","clustering"
          }, ...
        ]
      }
    """
    tree = ET.parse(filepath)
    root = tree.getroot()

    # 1) Build entity-ID → name map
    id2name = {
        e.findtext("ID"): e.findtext("NAME")
        for e in root.findall(".//ENTITIES/ENT")
        if e.findtext("ID") and e.findtext("NAME")
    }

    # 2) Gather FK relationships (RELATIONSHIPS/REL)
    fk_for_entity: dict[str,dict[str,str]] = {}
    for rel in root.findall(".//RELATIONSHIPS/REL"):
        pid = rel.findtext("PARENTOBJECTID","")
        cid = rel.findtext("CHILDOBJECTID","")
        parent = id2name.get(pid,"")
        if cid and parent:
            m = fk_for_entity.setdefault(cid,{})
            for pair in rel.findall(".//PAIRS/PAIR"):
                fk_attr = pair.findtext("FOREIGNKEYID")
                if fk_attr:
                    m[fk_attr] = parent

    entities = []
    for ent in root.findall(".//ENTITIES/ENT"):
        ent_id   = ent.findtext("ID","")
        ent_name = ent.findtext("NAME","")
        ent_desc = ent.findtext("DESC","")

        # --- Primary keys from PKCON ---
        pk_ids = {
            a.text.strip()
            for a in ent.findall("./PKCON/ATTRIBUTEIDS/ATTRIBUTEID")
            if a.text
        }
        # --- Foreign-key map for this entity ---
        this_fk_map = fk_for_entity.get(ent_id, {})

        # --- Entity-level UDPs ---
        udps = { u.tag[4:]: (u.text or "")
                 for u in ent.findall("./USERDEFPROPS/*") }

        # 3a) Build sources list from UDPs like "01_source_database_1"
        src_re = re.compile(r"(\d+)_(source_database|source_table|source_column)_(\d+)")
        temp = {}
        for k, v in udps.items():
            m = src_re.match(k)
            if not m: continue
            kind = m.group(2).split("_",1)[1]   # "database"/"table"/"column"
            idx  = m.group(1)
            grp  = temp.setdefault(idx,{})
            grp[kind] = v
        sources = [
            {"database":g.get("database",""),
             "table":   g.get("table",""),
             "column":  g.get("column","")}
            for _,g in sorted(temp.items(), key=lambda x:int(x[0]))
        ]

        # 3b) Capture table_options UDP (Partition/Cluster DDL)
        table_opts = udps.get("table_options","")

        # 4) Parse fields
        fields = []
        # Pre-parse partition & cluster columns from table_opts
        part_cols, clust_cols = [], []
        # Partition: look inside the first TIMESTAMP_TRUNC(...)
        pm = re.search(r"PARTITION BY\s+[^(]+\(\s*([^) ,]+)", table_opts, re.IGNORECASE)
        if pm: part_cols = [pm.group(1).strip()]
        cm = re.search(r"CLUSTER BY\s+([^\n\r]+)", table_opts, re.IGNORECASE)
        if cm:
            clust_cols = [c.strip() for c in cm.group(1).split(",")]

        for attr in ent.findall("./ATTRIBUTES/ATTR"):
            aid   = attr.findtext("ID","")
            name  = attr.findtext("NAME","")
            desc  = attr.findtext("DESC","")
            dtype = attr.findtext("DT/DTLISTNAME","STRING")
            nn    = (attr.findtext("./NNCON/VALUE")=="1")

            # Key detection
            is_pk = aid in pk_ids
            is_fk = aid in this_fk_map
            if is_pk and is_fk: kt="PRIMARY, FOREIGN"
            elif is_pk:         kt="PRIMARY"
            elif is_fk:         kt="FOREIGN"
            else:               kt=""

            refdim = this_fk_map.get(aid,"")

            # Field-level UDP sourcing/clustering/partitioning
            st, sc = [], []
            part_flag = "Y" if name in part_cols else ""
            clust_flag= "Y" if name in clust_cols else ""
            for u in attr.findall("./USERDEFPROPS/*"):
                tag = u.tag[4:].lower()
                if tag.startswith("source_table"):
                    st.append(u.text or "")
                elif tag.startswith("source_column"):
                    sc.append(u.text or "")
                elif tag=="partitioning" and not part_flag:
                    part_flag = (u.text or "")
                elif tag=="clustering"  and not clust_flag:
                    clust_flag= (u.text or "")

            defs = resolve_defaults(name, dtype)
            fields.append({
                "name": name,
                "description": desc,
                "datatype": dtype,
                "sourced": not is_fk,
                "not_null": nn,
                "src_table": ", ".join(st),
                "src_attr":  ", ".join(sc),
                "def_val":   defs["Default Values"],
                "def_m1":    defs["Default Records"],
                "def_m2":    defs["Default Records (2)"],
                "key_type":  kt,
                "referenced_dimension": refdim,
                "partitioning": part_flag,
                "clustering":  clust_flag,
            })

        entities.append({
            "name":          ent_name,
            "description":   ent_desc,
            "sources":       sources,
            "table_options": table_opts,
            "fields":        fields,
        })

    return entities
