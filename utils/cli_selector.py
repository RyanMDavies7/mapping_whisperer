import xml.etree.ElementTree as ET
from pathlib import Path


def build_diagram_mapping(dez_path: str | Path):
    """
    Parse the .dez XML and return two mappings:
      - id2name: { entity_id: entity_name }
      - diag2names: { diagram_name: [entity_name, ...] }

    Entities are sorted alphabetically per diagram.

    Example:
      id2name, diag2names = build_diagram_mapping("orders_bq.dez")
      # diag2names -> { "Sales Diagram": ["Order", "Party"], ... }
    """
    tree = ET.parse(dez_path)
    root = tree.getroot()

    # Map entity ID -> entity NAME
    id2name = {
        ent.findtext("ID"): ent.findtext("NAME")
        for ent in root.findall(".//ENTITIES/ENT")
        if ent.findtext("ID") and ent.findtext("NAME")
    }

    # Map diagram ID -> diagram NAME
    diag_id2name = {
        d.findtext("ID"): d.findtext("NAME")
        for d in root.findall(".//DIAGRAMS/DIAGRAM")
        if d.findtext("ID") and d.findtext("NAME")
    }

    # Collect entity IDs per diagram
    diag2ids: dict[str, set[str]] = {did: set() for did in diag_id2name}
    for entc in root.findall(".//DIAGRAMS/CONTROLS/ENTITYCONTROLS/ENTC"):
        did = entc.findtext("DIAGRAMID")
        eid = entc.findtext("ID")
        if did in diag2ids and eid in id2name:
            diag2ids[did].add(eid)

    # Convert to diagram_name -> sorted list of entity names
    diag2names = {
        diag_id2name[did]: sorted(id2name[eid] for eid in eids)
        for did, eids in diag2ids.items()
    }
    return id2name, diag2names


def interactive_choice(diag2names: dict[str, list[str]]) -> list[str]:
    """
    Display entities grouped by diagram, prompt user to select by number.
    Returns list of selected entity names (empty = all).

    Example:
      selected = interactive_choice(diag2names)
    """
    # Map global index -> name
    index_map: dict[int, str] = {}
    idx = 1
    print("\nAvailable entities by Diagram:\n")
    for diag in sorted(diag2names):
        print(f"{diag}:")
        for name in diag2names[diag]:
            print(f"  {idx:3d}. {name}")
            index_map[idx] = name
            idx += 1
        print()

    choice = input(
        "Enter numbers (comma-separated) to export those, or press Enter for ALL: "
    ).strip()
    if not choice:
        return []  # Export all

    selected = set()
    for part in choice.split(","):
        part = part.strip()
        if part.isdigit():
            num = int(part)
            if num in index_map:
                selected.add(index_map[num])
    print(f"\n→ You selected: {', '.join(sorted(selected))}\n")
    return sorted(selected)
