#!/usr/bin/env python
import sys
import argparse
from pathlib import Path
from utils.dez_parser import parse_dez_file
from utils.template_writer import write_entity
from utils.cli_selector import build_diagram_mapping, interactive_choice

TEMPLATE = "templates/mapping_1-11.xlsx"
OUT_DIR  = "outputs"

def main(dez_file: str, flags: list[str]):
    entities = parse_dez_file(dez_file)
    Path(OUT_DIR).mkdir(exist_ok=True)

    if flags:
        to_export = flags
    else:
        # Delegate interactive builder + picker
        _, diag2names = build_diagram_mapping(dez_file)
        to_export = interactive_choice(diag2names)

    for ent in entities:
        if not to_export or ent["name"] in to_export:
            write_entity(ent, TEMPLATE, OUT_DIR)
            print(f"✅  wrote {ent['name']}.xlsx")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Convert .dez to mapping XLSX with flexible selection."
    )
    parser.add_argument("dez_file", help="Path to the .dez file")
    parser.add_argument(
        "--entity",
        action="append",
        dest="entities",
        metavar="NAME",
        help="Specify an entity to export (repeatable)."
    )
    args = parser.parse_args()
    main(args.dez_file, args.entities or [])
