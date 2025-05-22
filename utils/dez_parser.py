# utils/dez_parser.py

"""
Parses a .dez (XML) file into a list of entities, each with:
  - name, description
  - sources: from USERDEFPROP Name/Value pairs like "01_source_database_1"
  - table_options: the full DDL snippet UDP
  - fields: detailed metadata + default values + key flags + partition/cluster flags
"""

import re
import xml.etree.ElementTree as ET
from pathlib import Path

# Default‐values map for BigQuery types & naming conventions
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
    """
    Pick the right default‐value set based on column name or dtype.
    """
    key = dtype.upper()
    lower = col_name.lower()
    if "effective_start" in lower:
        key = "TIMESTAMP-Start"
    elif "effective_end" in lower:
        key = "TIMESTAMP-End"
    return DEFAULTS.get(key, {
        "Default Values": "",
        "Default Records": "",
        "Default Records (2)": ""
    })

def parse_dez_file(filepath: str | Path) -> list[dict]:
    """
    Parse the .dez file and return a list of entities with:
      - name, description
      - sources: [{database,table,column},…]
      - table_options: str (DDL snippet)
      - fields: list of {
            name,description,datatype,sourced,not_null,
            src_table,src_attr,
            def_val,def_m1,def_m2,
            key_type,referenced_dimension,
            partitioning,clustering
        }
    """
    tree = ET.parse(filepath)
    root = tree.getroot()

    # 1) Map entity-ID → entity-name
    id2name = {
        e.findtext("ID"): e.findtext("NAME")
        for e in root.findall(".//ENTITIES/ENT")
        if e.findtext("ID") and e.findtext("NAME")
    }

    # 2) Build child-entity → {fk_attr_id: parent_name} via REL blocks
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

        # --- Primary keys set ---
        pk_ids = {
            a.text.strip() for a in ent.findall("./PKCON/ATTRIBUTEIDS/ATTRIBUTEID") if a.text
        }

        # --- This entity’s FK map ---
        this_fk_map = fk_for_entity.get(ent_id, {})

        # --- Read USERDEFPROP Name/Value pairs under this ENT ---
        udps = {}
        for udp in ent.findall(".//USERDEFPROP"):
            name = udp.findtext("NAME","").strip()
            val  = udp.findtext("VALUE","")
            if name:
                udps[name] = val or ""

        # --- Build 'sources' from 01_source_database_1, 02_source_table_1, etc. ---
        src_re = re.compile(r"(\d+)_(source_database|source_table|source_column)_(\d+)")
        temp = {}
        for key, val in udps.items():
            m = src_re.match(key)
            if not m:
                continue
            kind = m.group(2).split("_",1)[1]  # 'database','table','column'
            idx  = m.group(1)
            grp  = temp.setdefault(idx,{})
            grp[kind] = val
        sources = [
            {
                "database": grp.get("database",""),
                "table":    grp.get("table",""),
                "column":   grp.get("column","")
            }
            for _, grp in sorted(temp.items(), key=lambda x:int(x[0]))
        ]

        # --- Capture table_options UDP (single DDL snippet) ---
        table_options = udps.get("table_options","")

        # --- Pre-parse Partition & Cluster columns from that snippet ---
        part_cols, clust_cols = [], []
        pm = re.search(r"PARTITION\s+BY\s+[^(]+\(\s*([^) ,]+)", table_options, re.IGNORECASE)
        if pm:
            part_cols = [pm.group(1).strip()]
        cm = re.search(r"CLUSTER\s+BY\s+([^\n\r]+)", table_options, re.IGNORECASE)
        if cm:
            clust_cols = [c.strip() for c in cm.group(1).split(",")]

        # --- Parse each attribute ---
        fields = []
        for attr in ent.findall("./ATTRIBUTES/ATTR"):
            aid   = attr.findtext("ID","")
            name  = attr.findtext("NAME","")
            desc  = attr.findtext("DESC","")
            dtype = attr.findtext("DT/DTLISTNAME","STRING")
            nn    = (attr.findtext("./NNCON/VALUE")=="1")

            # -- Determine key type --
            is_pk = aid in pk_ids
            is_fk = aid in this_fk_map
            if is_pk and is_fk:
                kt = "PRIMARY, FOREIGN"
            elif is_pk:
                kt = "PRIMARY"
            elif is_fk:
                kt = "FOREIGN"
            else:
                kt = ""

            refdim = this_fk_map.get(aid,"")

            # -- Field-level UDP sourcing/clustering/partitioning --
            st, sc = [], []
            part_flag  = "Y" if name in part_cols else ""
            clust_flag = "Y" if name in clust_cols else ""
            for u in attr.findall(".//USERDEFPROP"):
                key = u.findtext("NAME","").strip().lower()
                val = u.findtext("VALUE","")
                if key.startswith("partitioning") and not part_flag:
                    part_flag = val or ""
                elif key.startswith("clustering")  and not clust_flag:
                    clust_flag = val or ""
                elif key.startswith("source_table"):
                    st.append(val or "")
                elif key.startswith("source_column"):
                    sc.append(val or "")

            defs = resolve_defaults(name, dtype)
            fields.append({
                "name": name,
                "description": desc,
                "datatype": dtype,
                "sourced": not is_fk,
                "not_null": nn,
                "src_table": ", ".join(st),
                "src_attr": ", ".join(sc),
                "def_val": defs["Default Values"],
                "def_m1":  defs["Default Records"],
                "def_m2":  defs["Default Records (2)"],
                "key_type": kt,
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
