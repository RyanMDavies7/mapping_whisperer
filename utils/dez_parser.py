import re
import xml.etree.ElementTree as ET
from pathlib import Path

# -------------------------------------------------------------------
# DEFAULTS unchanged...
# -------------------------------------------------------------------

def resolve_defaults(col_name: str, dtype: str) -> dict:
    # … same as before …
    key = dtype.upper()
    lower = col_name.lower()
    if "effective_start" in lower:
        key = "TIMESTAMP-Start"
    elif "effective_end" in lower:
        key = "TIMESTAMP-End"
    return DEFAULTS.get(key, {"Default Values": "", "Default Records": "", "Default Records (2)": ""})

def parse_dez_file(filepath: str | Path) -> list[dict]:
    tree = ET.parse(filepath)
    root = tree.getroot()

    # 1) id→name map
    id2name = {
        e.findtext("ID"): e.findtext("NAME")
        for e in root.findall(".//ENTITIES/ENT")
        if e.findtext("ID") and e.findtext("NAME")
    }

    # 2) FK map
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

        # PKs
        pk_ids = {
            a.text.strip()
            for a in ent.findall("./PKCON/ATTRIBUTEIDS/ATTRIBUTEID")
            if a.text
        }
        # this entity’s FKs
        this_fk_map = fk_for_entity.get(ent_id, {})

        # --- entity-level UDPs under USERDEFPROPS ---
        udps = { udp.tag: (udp.text or "")
                 for udp in ent.findall("./USERDEFPROPS/*") }

        # build sources from names like 01_source_database_1 etc.
        src_re = re.compile(r"(\d+)_(source_database|source_table|source_column)_(\d+)")
        tmp = {}
        for name, val in udps.items():
            m = src_re.match(name)
            if not m: continue
            kind = m.group(2).split("_",1)[1]   # database/table/column
            grp  = m.group(1)
            tmp.setdefault(grp, {})[kind] = val
        sources = [
            {"database": g.get("database",""),
             "table":    g.get("table",""),
             "column":   g.get("column","")}
            for _,g in sorted(tmp.items(), key=lambda x:int(x[0]))
        ]

        # capture <TABOPT> for DDL snippet
        table_options = ent.findtext("TABOPT","") or ""
        # pre‐extract partition/cluster columns
        part_cols, clust_cols = [], []
        pm = re.search(r"PARTITION\s+BY\s+[^(]+\(\s*([^) ,]+)", table_options, re.IGNORECASE)
        if pm: part_cols = [pm.group(1).strip()]
        cm = re.search(r"CLUSTER\s+BY\s+([^\n\r]+)", table_options, re.IGNORECASE)
        if cm: clust_cols = [c.strip() for c in cm.group(1).split(",")]

        # parse each field
        fields = []
        for attr in ent.findall("./ATTRIBUTES/ATTR"):
            aid   = attr.findtext("ID","")
            name  = attr.findtext("NAME","")
            desc  = attr.findtext("DESC","")
            dtype = attr.findtext("DT/DTLISTNAME","STRING")
            nn    = (attr.findtext("./NNCON/VALUE")=="1")

            # keyType
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

            # attribute-level USERDEFPROPS values (in order if no name match)
            st, sc = [], []
            # first look for named UDPs "udp_source_table", "udp_source_column"
            for udp in attr.findall("./USERDEFPROPS/*"):
                tag = udp.tag.lower()
                val = udp.text or ""
                if "source_table" in tag:
                    st.append(val)
                elif "source_column" in tag:
                    sc.append(val)
            # fallback: if none found, assume first three UDP-texts are [db,table,column]
            if not st or not sc:
                vals = [u.text or "" for u in attr.findall("./USERDEFPROPS/*")]
                if len(vals) >= 2 and not st:
                    st = [vals[1]]
                if len(vals) >= 3 and not sc:
                    sc = [vals[2]]

            # partition/cluster flags
            part_flag  = "Y" if name in part_cols  else ""
            clust_flag = "Y" if name in clust_cols else ""

            defs = resolve_defaults(name, dtype)
            fields.append({
                "name":                 name,
                "description":          desc,
                "datatype":             dtype,
                "sourced":              not is_fk,
                "not_null":             nn,
                "src_table":            ", ".join(st),
                "src_attr":             ", ".join(sc),
                "def_val":              defs["Default Values"],
                "def_m1":               defs["Default Records"],
                "def_m2":               defs["Default Records (2)"],
                "key_type":             kt,
                "referenced_dimension": refdim,
                "partitioning":         part_flag,
                "clustering":           clust_flag,
            })

        entities.append({
            "name":           ent_name,
            "description":    ent_desc,
            "sources":        sources,
            "table_options":  table_options,
            "fields":         fields,
        })

    return entities
