#!/usr/bin/env python

TYPE_MAP = {
    "INTEGER": "INT64",
    "INT": "INT64",
    "SMALLINT": "INT64",
    "BIGINT": "INT64",
    "VARCHAR": "STRING",
    "CHAR": "STRING",
    "TEXT": "STRING",
    "NVARCHAR": "STRING",
    "DECIMAL": "NUMERIC",
    "FLOAT": "FLOAT64",
    "DOUBLE": "FLOAT64",
    "REAL": "FLOAT64",
    "NUMERIC": "NUMERIC",
    "DATE": "DATE",
    "DATETIME": "DATETIME",
    "TIMESTAMP": "TIMESTAMP",
    "BOOLEAN": "BOOL",
    "BOOL": "BOOL",
}

def convert_dez_file(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    for old, new in TYPE_MAP.items():
        content = content.replace(f"<DTLISTNAME>{old}</DTLISTNAME>", f"<DTLISTNAME>{new}</DTLISTNAME>")

    out_path = file_path.replace(".dez", "_bq.dez")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(content)

    print(f"✅ Converted and saved: {out_path}")

# Run like a script
if __name__ == "__main__":
    convert_dez_file("orders.dez")
