# utils/dez_parser.py

import re
import xml.etree.ElementTree as ET
from pathlib import Path

# -----------------------------------------------------------------------------
# Default‐values map (unchanged)
# -----------------------------------------------------------------------------
DEFAULTS = {
    "TIMESTAMP-Start": { "Default Values": '"1900-01-01 00:00:00.0000"' , "Default Records": '"1900-01-01 00:00:00.0000"' , "Default Records (2)": '"1900-01-01 00:00:00.0000"' },
    "TIMESTAMP-End"  : { "Default Values": '"9999-12-31 23:59:59.9999"' , "Default Records": '"9999-12-31 23:59:59.9999"' , "Default Records (2)": '"9999-12-31 23:59:59.9999"' },
    "TIMESTAMP"      : { "Default Values": '"1900-01-01 00:00:00.0000"' , "Default Records": '"1900-01-01 00:00:00.0000"' , "Default Records (2)": '"9999-12-31 23:59:59.9999"' },
    "DATETIME"       : { "Default Values": '"1900-01-01 00:00:00.0000"' , "Default Records": '"1900-01-01 00:00:00.0000"' , "Default Records (2)": '"9999-12-31 23:59:59.9999"' },
    "DATE"           : { "Default Values": '"1900-01-01"'             , "Default Records": '"1900-01-01"'             , "Default Records (2)": '"9999-12-31"'             },
    "INT64"          : { "Default Values": "-1"                       , "Default Records": "-1"                       , "Default Records (2)": "-2"                       },
    "STRING"         : { "Default Values": '""'                       , "Default Records": '""'                       , "Default Records (2)": '""'                       },
    "BOOL"           : { "Default Values": "NULL"                     , "Default Records": "NULL"                     , "Default Records (2)": "NULL"                     },
    "NUMERIC"        : { "Default Values": "0"                        , "Default Records": "0"                        , "Default Records (2)": "0"                        },
    "FLOAT64"        : { "Default Values": '"0.0"'                     , "Default Records": '"0.0"'                     , "Default Records (2)": '"0.0"'                     },
}

def resolve_defaults(col_name: str, dtype: str) -> dict:
    key = dtype.upper()
    name_l = col_name.lower()
    if "effective_start" in name_l:
        key = "TIMESTAMP-Start"
    elif "effective_end" in name_l:
        key = "TIMESTAMP-End"
    return DEFAULTS.get(key, {"Default Values": "", "Default Records": "", "Default Records (2)": ""})

def parse_dez_file(filepath: str | Path) -> list[dict]:
    tree = ET.parse(filepath)
    root = tree.getroot()

    # 1) id → name map for Entities
    id2name = {
        e.findtext("ID"): e.findtext("NAME")
        for e in root.findall(".//ENTITIES/ENT")
        if e.findtext("ID") and e.findtext("NAME")
    }

    # 2) Build FK map: child-entity-ID → { attr-ID : parent-entity-name }
    fk_for_entity = {}
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

        # primary keys
        pk_ids = {
            a.text.strip()
            for a in ent.findall("./PKCON/ATTRIBUTEIDS/ATTRIBUTEID")
            if a.text
        }

        # this entity's foreign-key map
        this_fk_map = fk_for_entity.get(ent_id, {})

        # ---------------------------------------------------------------------
        # 3) Collect entity-level UDPs (under <USERDEFPROPS>)
        #   e.g. <UDP_GUID>some_value</UDP_GUID>
        # ---------------------------------------------------------------------
        udps = {
            udp.tag: (udp.text or "")
            for udp in ent.findall("./USERDEFPROPS/*")
        }

        # 3a) Build “sources” from UDPs like "01_source_database_1"
        src_re = re.compile(r"(\d+)_(source_database|source_table|source_column)_(\d+)")
        temp   = {}
        for name, val in udps.items():
            m = src_re.match(name)
            if not m: continue
            kind = m.group(2).split("_",1)[1]   # database/table/column
            grp  = m.group(1)
            temp.setdefault(grp, {})[kind] = val

        sources = [
            {
                "database": g.get("database",""),
                "table":    g.get("table",""),
                "column":   g.get("column","")
            }
            for _,g in sorted(temp.items(), key=lambda x:int(x[0]))
        ]

        # 3b) Capture <TABOPT> text for table options
        table_options = ent.findtext("TABOPT","") or ""

        # Pre-parse partition & cluster columns from that DDL snippet
        part_cols, clust_cols = [], []
        pm = re.search(r"PARTITION\s+BY\s+[^(]+\(\s*([^) ,]+)", table_options, re.IGNORECASE)
        if pm: part_cols = [pm.group(1).strip()]
        cm = re.search(r"CLUSTER\s+BY\s+([^\n\r]+)", table_options, re.IGNORECASE)
        if cm:
            clust_cols = [c.strip() for c in cm.group(1).split(",")]

        # ---------------------------------------------------------------------
        # 4) Parse each <ATTR> into a field dict
        # ---------------------------------------------------------------------
        fields = []
        for attr in ent.findall("./ATTRIBUTES/ATTR"):
            aid   = attr.findtext("ID","")
            name  = attr.findtext("NAME","")
            desc  = attr.findtext("DESC","")
            dtype = attr.findtext("DT/DTLISTNAME","STRING")
            notnull = (attr.findtext("./NNCON/VALUE") == "1")

            # key logic
            is_pk = aid in pk_ids
            is_fk = aid in this_fk_map
            if is_pk and is_fk:
                key_type = "PRIMARY, FOREIGN"
            elif is_pk:
                key_type = "PRIMARY"
            elif is_fk:
                key_type = "FOREIGN"
            else:
                key_type = ""

            refdim = this_fk_map.get(aid,"")

            # field-level UDPs (sourcing/clustering/partitioning)
            st, sc = [], []
            part_flag  = "Y" if name in part_cols else ""
            clust_flag = "Y" if name in clust_cols else ""
            for udp in attr.findall("./USERDEFPROPS/*"):
                tag = udp.tag.lower()
                val = udp.text or ""
                if tag.startswith("udp_source_table"):
                    st.append(val)
                elif tag.startswith("udp_source_column"):
                    sc.append(val)
                elif "partitioning" in tag and not part_flag:
                    part_flag = val
                elif "clustering" in tag  and not clust_flag:
                    clust_flag = val

            defs = resolve_defaults(name, dtype)
            fields.append({
                "name": name,
                "description": desc,
                "datatype": dtype,
                "sourced": not is_fk,
                "not_null": notnull,
                "src_table": ", ".join(st),
                "src_attr":  ", ".join(sc),
                "def_val":   defs["Default Values"],
                "def_m1":    defs["Default Records"],
                "def_m2":    defs["Default Records (2)"],
                "key_type":  key_type,
                "referenced_dimension": refdim,
                "partitioning": part_flag,
                "clustering":  clust_flag,
            })

        entities.append({
            "name":          ent_name,
            "description":   ent_desc,
            "sources":       sources,
            "table_options": table_options,
            "fields":        fields,
        })

    return entities
