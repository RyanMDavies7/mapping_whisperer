import re
import xml.etree.ElementTree as ET
from pathlib import Path

# -----------------------------------------------------------------------------
# Default‐values map for BigQuery types & naming conventions
# -----------------------------------------------------------------------------
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
    """Pick default‐value set based on column name or datatype."""
    key = dtype.upper()
    lower = col_name.lower()
    if "effective_start" in lower:
        key = "TIMESTAMP-Start"
    elif "effective_end" in lower:
        key = "TIMESTAMP-End"
    return DEFAULTS.get(key, {"Default Values": "", "Default Records": "", "Default Records (2)": ""})

def parse_dez_file(filepath: str | Path) -> list[dict]:
    """
    Parse a .dez file and return a list of entities with:
      - name, description
      - table_options (PARTITION/CLUSTER DDL)
      - fields: list of {
          name, description, datatype, sourced (bool), not_null,
          src_table, src_attr, key_type, referenced_dimension,
          def_val, def_m1, def_m2,
          partitioning, clustering,
          derived_expr (if Derived)
        }
    """
    tree = ET.parse(filepath)
    root = tree.getroot()

    # 1) Build ID→NAME map for FK resolution
    id2name = {
        e.findtext("ID"): e.findtext("NAME")
        for e in root.findall(".//ENTITIES/ENT")
        if e.findtext("ID") and e.findtext("NAME")
    }

    # 2) Gather all foreign key relationships
    fk_for_entity: dict[str, dict[str,str]] = {}
    for rel in root.findall(".//RELATIONSHIPS/REL"):
        pid = rel.findtext("PARENTOBJECTID","")
        cid = rel.findtext("CHILDOBJECTID","")
        parent = id2name.get(pid,"")
        if cid and parent:
            fk_map = fk_for_entity.setdefault(cid,{})
            for pair in rel.findall(".//PAIRS/PAIR"):
                fk_attr = pair.findtext("FOREIGNKEYID")
                if fk_attr:
                    fk_map[fk_attr] = parent

    entities = []
    for ent in root.findall(".//ENTITIES/ENT"):
        ent_name = ent.findtext("NAME","")
        ent_desc = ent.findtext("DESC","")

        # Primary key IDs
        pk_ids = {
            a.text.strip()
            for a in ent.findall("./PKCON/ATTRIBUTEIDS/ATTRIBUTEID")
            if a.text
        }
        this_fk_map = fk_for_entity.get(ent.findtext("ID",""), {})

        # Table-level options
        table_options = ent.findtext("TABOPT","") or ""
        # Pre-extract partition/cluster columns
        part_cols = []
        clust_cols = []
        pm = re.search(r"PARTITION\s+BY\s+[^(]+\(\s*([^) ,]+)", table_options, re.IGNORECASE)
        if pm: part_cols = [pm.group(1).strip()]
        cm = re.search(r"CLUSTER\s+BY\s+([^\n\r]+)", table_options, re.IGNORECASE)
        if cm:
            clust_cols = [c.strip() for c in cm.group(1).split(",")]

        fields = []
        for attr in ent.findall("./ATTRIBUTES/ATTR"):
            name  = attr.findtext("NAME","")
            dtype = attr.findtext("DT/DTLISTNAME","STRING")
            nn    = (attr.findtext("./NNCON/VALUE") == "1")

            # Determine key type
            aid = attr.findtext("ID","")
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

            # — Attribute‐level UDPs: take elements in order: [database, table, column]
            udp_elems = list(attr.findall("./USERDEFPROPS/*"))
            db_val    = udp_elems[0].text or "" if len(udp_elems) > 0 else ""
            table_val = udp_elems[1].text or "" if len(udp_elems) > 1 else ""
            col_val   = udp_elems[2].text or "" if len(udp_elems) > 2 else ""

            # Build alias from table_val
            alias = "".join([c for c in table_val if c.isupper()]) or table_val[:2].upper()
            src_table_disp = f"{table_val} AS {alias}" if table_val else ""
            src_attr_disp  = f"{alias}.{col_val}" if col_val and alias else ""

            # Partitioning / Clustering flags
            part_flag  = "Y" if name in part_cols else ""
            clust_flag = "Y" if name in clust_cols else ""

            # Derived expression for Derived fields
            sourced = not is_fk
            derived_expr = "" if sourced else f"{ent_name}.{name}"

            # Defaults
            defs = resolve_defaults(name, dtype)

            fields.append({
                "name": name,
                "description": attr.findtext("DESC","") or "",
                "datatype": dtype,
                "sourced": sourced,
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
            "table_options": table_options,
            "fields":        fields,
        })

    return entities
