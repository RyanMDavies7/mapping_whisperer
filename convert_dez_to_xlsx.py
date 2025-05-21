#!/usr/bin/env python
import sys
from pathlib import Path
from utils.dez_parser import parse_dez_file
from utils.template_writer import write_entity

TEMPLATE = "templates/mapping_1-11.xlsx"
OUT_DIR  = "outputs"

def main(dez_file: str):
    entities = parse_dez_file(dez_file)
    Path(OUT_DIR).mkdir(exist_ok=True)
    for ent in entities:
        write_entity(ent, TEMPLATE, OUT_DIR)
        print("✅ wrote", ent["name"] + ".xlsx")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: convert_dez_to_xlsx.py <file.dez>")
        sys.exit(1)
    main(sys.argv[1])
