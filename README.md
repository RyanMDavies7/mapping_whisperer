# 🗺️ Mapping Whisperer

**Mapping Whisperer** is a Python-based tool for converting `.dez` files from ER/Studio into clean, readable Excel mapping documents. It also lets you update old mapping sheets into a new template structure with conditional formatting, sourcing logic, and column definitions.

## 🚀 Features

- ✅ Converts `.dez` models into Excel-based mapping documents
- ✅ Supports old template updating to the new format
- ✅ Handles foreign keys, primary keys, nullability, and data types
- ✅ Automatically marks clustering and partitioning
- ✅ Smart defaults based on data types
- ✅ Derives sourcing and derivation logic using user-defined properties
- ✅ Clean formatting with preserved Excel styles

## TO DO....

- Lowercase values in source table, source attribute
- Get staghist tables from GCP
- Get dim_source_record_set
- Get Jira ticket from entity and put into mapping
- Populate DDL 
- Does CLUSTER BY & PARTITION BY need to be in TABLE_OPTIONS
- Source/Business for source_key_value_number, source_key_value_string etc...
- Tidy up requirements.txt
- 

## 🏁 Getting Started

1. **Clone the repository**

```bash
git clone https://github.com/your-repo/mapping-whisperer.git
cd mapping-whisperer
```

2. **Set up your environment**

```bash
python -m venv .venv
source .venv/bin/activate   # or .venv\Scripts\activate on Windows
pip install -r requirements.txt
```

3. **Run the tool**

Convert a `.dez` file into mapping documents:

```bash
python convert_dez_to_xlsx.py path/to/your_file.dez
```

You will be prompted to pick an entity to convert.

4. **Update old mappings**

```bash
python update_old_mapping.py path/to/old_mapping.xlsx
```

This updates the content using the new mapping template.

## 🧠 How It Works

- **`dez_parser.py`**: Parses the XML `.dez` file. Extracts entities, attributes, keys, and user-defined properties.
- **`template_writer.py`**: Fills in an Excel template with the parsed values, maintaining styles.
- **`helpers.py`**: Utility functions for normalization and formatting.
- **`convert_dez_to_xlsx.py`**: Entry point to convert `.dez` to `.xlsx`.
- **`update_old_mapping.py`**: Entry point to upgrade old mappings to the new format.

## 🛠 Customization

- Modify `DEFAULTS` in `dez_parser.py` to change default values per datatype.
- Adjust `COLUMN_NAME_MAPPING` in `helpers.py` to align with template column changes.
- Add more sourcing logic in `template_writer.py` if needed.

## 📂 Folder Structure

```
.
├── inputs/              # Your .dez input files go here
├── outputs/             # Excel mappings are saved here
├── templates/           # Excel template with correct headers
├── utils/               # Helper scripts
├── convert_dez_to_xlsx.py
├── update_old_mapping.py
└── README.md
```

## 🧪 Testing

Test the parser by running:

```bash
python test_parser.py path/to/sample.dez
```

## 👏 Contributing

Open an issue or submit a pull request. This is designed to grow with your needs!

---

Happy mapping! 🎉
