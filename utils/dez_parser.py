import re
import xml.etree.ElementTree as ET
from pathlib import Path

# ----------------------------------------------------------------------------------------------------------------------
# Default‐values map for BigQuery types & naming conventions
# ----------------------------------------------------------------------------------------------------------------------
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
    key = dtype.upper()
    lower = col_name.lower()
    if "effective_start" in lower:
        key = "TIMESTAMP-Start"
    elif "effective_end" in lower:
        key = "TIMESTAMP-End"
    return DEFAULTS.get(key, {"Default Values": "", "Default Records": "", "Default Records (2)": ""})

def parse_dez_file(filepath: str|Path) -> list[dict]:
    tree = ET.parse(filepath)
    root = tree.getroot()

    # 1) Map entity-ID -> entity-name
    id2name = {
        e.findtext("ID"): e.findtext("NAME")
        for e in root.findall(".//ENTITIES/ENT")
        if e.findtext("ID") and e.findtext("NAME")
    }

    # 2) Build FK map: child-entity-ID -> { attr-ID: parent-name }
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

        # primary key IDs
        pk_ids = {
            a.text.strip()
            for a in ent.findall("./PKCON/ATTRIBUTEIDS/ATTRIBUTEID")
            if a.text
        }
        # this entity’s FK mapping
        this_fk_map = fk_for_entity.get(ent_id, {})

        # 3) Entity-level UDPs (under <USERDEFPROPS>)
        udps = {
            udp.tag[4:].lower(): (udp.text or "")
            for udp in ent.findall("./USERDEFPROPS/*")
        }

        # 3a) Build sources list from UDP keys like "01_source_table_1"
        src_pat = re.compile(r"(\d+)_source_table_(\d+)")
        col_pat = re.compile(r"(\d+)_source_column_(\d+)")
        groups = {}
        for key, val in udps.items():
            if src_pat.match(key):
                idx = int(src_pat.match(key).group(2))
                groups.setdefault(idx, {})["table"] = val
            elif col_pat.match(key):
                idx = int(col_pat.match(key).group(2))
                groups.setdefault(idx, {})["column"] = val
        sources = []
        if groups:
            # pick highest index only
            hi = max(groups.keys())
            g  = groups[hi]
            sources.append({
                "database": udps.get(f"{hi}_source_database_{hi}", ""),
                "table":     g.get("table",""),
                "column":    g.get("column","")
            })

        # 3b) Capture <TABOPT> for Partition/Cluster DDL
        table_options = ent.findtext("TABOPT","") or ""

        # pre-parse PARTITION and CLUSTER columns
        part_cols = []
        clust_cols = []
        pm = re.search(r"PARTITION\s+BY\s+[^(]+\(\s*([^) ,]+)", table_options, re.IGNORECASE)
        if pm:
            part_cols = [pm.group(1).strip()]
        cm = re.search(r"CLUSTER\s+BY\s+([^\n\r]+)", table_options, re.IGNORECASE)
        if cm:
            clust_cols = [c.strip() for c in cm.group(1).split(",")]

        # 4) Parse attributes
        fields = []
        for attr in ent.findall("./ATTRIBUTES/ATTR"):
            aid   = attr.findtext("ID","")
            name  = attr.findtext("NAME","")
            desc  = attr.findtext("DESC","")
            dtype = attr.findtext("DT/DTLISTNAME","STRING")
            nn    = (attr.findtext("./NNCON/VALUE")=="1")

            # key type
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

            ref_dim = this_fk_map.get(aid,"")

            # --- Sourcing logic with highest-index UDPs ---
            # (we already computed at entity level, but also attr-level UDP overrides)
            st, sc = "", ""
            # find attr-level UDPs if exist
            attr_udps = {
                udp.tag[4:].lower(): (udp.text or "")
                for udp in attr.findall("./USERDEFPROPS/*")
            }
            # same grouping logic per attribute
            gr = {}
            for k,v in attr_udps.items():
                mt = re.match(r"(\d+)_source_table_(\d+)", k)
                mc = re.match(r"(\d+)_source_column_(\d+)", k)
                if mt:
                    idx = int(mt.group(2)); gr.setdefault(idx,{})["table"] = v
                if mc:
                    idx = int(mc.group(2)); gr.setdefault(idx,{})["column"] = v
            if gr:
                hi2 = max(gr.keys()); g2 = gr[hi2]
                st = g2.get("table",""); sc = g2.get("column","")

            # alias logic
            alias = "".join([c for c in st if c.isupper()]) or st[:2].upper()
            src_table_disp = f"{st} AS {alias}" if st else ""
            src_attr_disp  = f"{alias}.{sc}" if sc else ""

            # derived expression
            derived_expr = ""
            if not (not is_fk):  # if Derived
                base = st or ent_name
                derived_expr = f"{base}.{name}"

            # partition/cluster flags
            part_flag  = "Y" if name in part_cols else ""
            clust_flag = "Y" if name in clust_cols else ""

            defs = resolve_defaults(name, dtype)
            fields.append({
                "name": name,
                "description": desc,
                "datatype": dtype,
                "sourced": not is_fk,
                "not_null": nn,
                "src_table": src_table_disp,
                "src_attr":  src_attr_disp,
                "def_val":   defs["Default Values"],
                "def_m1":    defs["Default Records"],
                "def_m2":    defs["Default Records (2)"],
                "key_type":  key_type,
                "referenced_dimension": ref_dim,
                "partitioning": part_flag,
                "clustering":  clust_flag,
                "derived_expr": derived_expr,
            })

        entities.append({
            "name":          ent_name,
            "description":   ent_desc,
            "sources":       sources,
            "table_options": table_options,
            "fields":        fields,
        })

    return entities
