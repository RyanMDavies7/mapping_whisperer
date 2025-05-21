# utils/dez_parser.py
# -------------------
# This module parses a .dez file (Dezign-for-Databases)
# and extracts table definitions and attributes for mapping.
# It resolves default values, identifies primary/foreign keys,
# and collects source/partitioning/clustering metadata.

from __future__ import annotations
import xml.etree.ElementTree as ET
from pathlib import Path

# -----------------------------------------------------------------------------
# 1) Default values for each BigQuery datatype
#    If the column name indicates start/end, apply special timestamp defaults.
#    Otherwise, use the generic defaults defined below.
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
    """
    Determine the correct default-values set for a column.

    - If column name contains 'effective_start', use TIMESTAMP-Start defaults.
    - If column name contains 'effective_end', use TIMESTAMP-End defaults.
    - Otherwise, lookup by dtype (e.g. INT64, STRING).

    Returns a dict with keys: 'Default Values', 'Default Records', 'Default Records (2)'.
    """
    key = dtype.upper()
    lower = col_name.lower()
    if "effective_start" in lower:
        key = "TIMESTAMP-Start"
    elif "effective_end" in lower:
        key = "TIMESTAMP-End"
    return DEFAULTS.get(
        key,
        {"Default Values": "", "Default Records": "", "Default Records (2)": ""}
    )


def parse_dez_file(filepath: str | Path) -> list[dict]:
    """
    Parse a .dez XML file and extract entities (tables) with fields metadata.

    Returns a list of dicts, each with:
      - 'name': table name
      - 'description': table description
      - 'fields': list of field dicts with all required columns for mapping.

    Example output:
      [
        {
          'name': 'SalesOrder',
          'description': '...',
          'fields': [
              {
                 'name': 'OrderID',
                 'description': '',
                 'datatype': 'INT64',
                 'sourced': True,
                 'not_null': True,
                 'src_table': '...',
                 'src_attr': '...',
                 'def_val': '-1',
                 'def_m1': '-1',
                 'def_m2': '-2',
                 'key_type': 'PRIMARY',
                 'referenced_dimension': '',
                 'clustering': '',
                 'partitioning': ''
              },
              ...
          ]
        },
        ...
      ]
    """
    # Load XML
    tree = ET.parse(filepath)
    root = tree.getroot()

    # 1) Build a map from entity-ID to entity-name for later reference
    id2name = {
        ent.findtext("ID"): ent.findtext("NAME")
        for ent in root.findall(".//ENTITIES/ENT")
        if ent.findtext("ID") and ent.findtext("NAME")
    }

    # 2) Gather all foreign-key relationships via REL blocks
    #    Build a nested dict: { child_entity_id: { attribute_id: parent_table_name } }
    fk_for_entity: dict[str, dict[str, str]] = {}
    for rel in root.findall(".//RELATIONSHIPS/REL"):
        parent_id = rel.findtext("PARENTOBJECTID", "")
        child_id  = rel.findtext("CHILDOBJECTID", "")
        parent_name = id2name.get(parent_id, "")
        if not child_id or not parent_name:
            continue

        # For each pair in the relationship, map foreign key attr → parent name
        fk_map = fk_for_entity.setdefault(child_id, {})
        for pair in rel.findall(".//PAIRS/PAIR"):
            fk_attr_id = pair.findtext("FOREIGNKEYID")
            if fk_attr_id:
                fk_map[fk_attr_id] = parent_name

    entities: list[dict] = []

    # 3) Parse each entity block
    for ent in root.findall(".//ENTITIES/ENT"):
        ent_id   = ent.findtext("ID", "")  # unique ID of the entity
        ent_name = ent.findtext("NAME")
        ent_desc = ent.findtext("DESC", "")

        # Extract primary-key attribute IDs from PKCON
        pk_ids = {
            aid.text.strip()
            for aid in ent.findall("./PKCON/ATTRIBUTEIDS/ATTRIBUTEID")
            if aid.text
        }

        # Retrieve this entity's foreign-key map (may be empty)
        this_fk_map = fk_for_entity.get(ent_id, {})

        fields: list[dict] = []

        # 4) Parse each attribute of the entity
        for attr in ent.findall("./ATTRIBUTES/ATTR"):
            aid      = attr.findtext("ID", "")
            name     = attr.findtext("NAME", "")
            desc     = attr.findtext("DESC", "")
            dtype    = attr.findtext("DT/DTLISTNAME", "STRING")
            notnull  = (attr.findtext("./NNCON/VALUE") == "1")

            # Determine if attribute is PK, FK, or both
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

            # Get the parent table name if FK, else blank
            ref_dim = this_fk_map.get(aid, "")

            # Collect source-ledger/user-defined properties
            src_tables, src_cols = [], []
            partitioning = ""
            clustering   = ""
            for udp in attr.findall("./USERDEFPROPS/*"):
                tag = udp.tag[4:].lower()  # strip "UDP_"
                if tag.startswith("source_table"):
                    src_tables.append(udp.text or "")
                elif tag.startswith("source_column"):
                    src_cols.append(udp.text or "")
                elif tag == "partitioning":
                    partitioning = udp.text or ""
                elif tag == "clustering":
                    clustering = udp.text or ""

            # Resolve default values based on name/dtype logic
            defaults = resolve_defaults(name, dtype)

            # Append the field dict, matching the mapping template columns
            fields.append({
                "name"                : name,
                "description"         : desc,
                "datatype"            : dtype,
                "sourced"             : not is_fk,
                "not_null"            : notnull,
                "src_table"           : ", ".join(src_tables),
                "src_attr"            : ", ".join(src_cols),
                "def_val"             : defaults["Default Values"],
                "def_m1"              : defaults["Default Records"],
                "def_m2"              : defaults["Default Records (2)"],
                "key_type"            : key_type,
                "referenced_dimension": ref_dim,
                "clustering"          : clustering,
                "partitioning"        : partitioning,
            })

        # Add the parsed entity to the list
        entities.append({
            "name"       : ent_name,
            "description": ent_desc,
            "fields"     : fields,
        })

    return entities
