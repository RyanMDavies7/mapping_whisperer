from utils.dez_parser import parse_dez_file

data = parse_dez_file("../inputs/orders.dez")

print("Versions :", data["versions"])
print("Diagrams :", data["diagrams"])
print("Entities :", list(data["entities"].keys())[:5])          # first 5 entities
first = next(iter(data["entities"].values()))
print("First entity fields:", first["fields"][:3])              # first 3 fields