#!/usr/bin/env python
"""
Usage:
  # 1) Non-interactive (export all):
  python convert_dez_to_xlsx.py inputs/orders_bq.dez

  # 2) Non-interactive (export specific via flags):
  python convert_dez_to_xlsx.py inputs/orders_bq.dez --entity Party --entity SalesOrder

  # 3) Interactive (no --entity):
  python convert_dez_to_xlsx.py inputs/orders_bq.dez
"""
import sys
import argparse
from pathlib import Path
from utils.dez_parser import parse_dez_file
from utils.template_writer import write_entity

TEMPLATE = "templates/mapping_1-11.xlsx"
OUT_DIR  = "outputs"

def main(dez_file: str, selected: list[str]) -> None:
    entities = parse_dez_file(dez_file)
    Path(OUT_DIR).mkdir(exist_ok=True)

    # If no selection passed, export all
    if not selected:
        for ent in entities:
            write_entity(ent, TEMPLATE, OUT_DIR)
            print(f"✅ wrote {ent['name']}.xlsx")
        return

    # Otherwise, only export those in selected
    for ent in entities:
        if ent["name"] in selected:
            write_entity(ent, TEMPLATE, OUT_DIR)
            print(f"✅ wrote {ent['name']}.xlsx")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Convert a .dez file into one or more mapping XLSX files."
    )
    parser.add_argument(
        "dez_file",
        help="Path to the .dez (XML) file to parse"
    )
    parser.add_argument(
        "--entity",
        action="append",
        dest="entities",
        metavar="TABLE",
        help=(
            "Name of one entity/table to export. "
            "You can specify this option multiple times. "
            "If omitted, you'll be prompted interactively."
        )
    )

    args = parser.parse_args()
    dez_path = args.dez_file

    # If user specified --entity flags, use them directly
    if args.entities:
        selected = args.entities
    else:
        # Interactive mode: list all entities, let user pick by number
        all_ents = parse_dez_file(dez_path)
        names = [ent["name"] for ent in all_ents]

        print("\nFound the following entities:")
        for idx, name in enumerate(names, start=1):
            print(f"  {idx}. {name}")

        print("\nEnter the numbers of the entities you want to export,")
        print("separated by commas (or press Enter to export ALL):")
        choice = input("Selection: ").strip()

        if not choice:
            selected = []  # means “export all”
        else:
            # parse indices, ignore invalid entries
            picked = set()
            for part in choice.split(","):
                part = part.strip()
                if part.isdigit():
                    i = int(part)
                    if 1 <= i <= len(names):
                        picked.add(i-1)
            selected = [names[i] for i in sorted(picked)]

        print(f"\n→ Will export: {', '.join(selected) or 'ALL'}\n")

    main(dez_path, selected)
