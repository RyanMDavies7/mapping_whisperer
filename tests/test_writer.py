from utils.dez_parser import parse_dez_file
from utils.template_writer import write_entity_to_template
from pathlib import Path

data = parse_dez_file("../inputs/orders.dez")
entity_name, entity_data = next(iter(data["entities"].items()))
entity_data["name"] = entity_name  # add name for writer

output = write_entity_to_template(
    entity_data,
    template_path="../templates/mapping_1-11.xlsx",
    output_dir="../outputs"
)
print("✅ wrote:", output)