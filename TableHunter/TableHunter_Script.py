import re
import os
import logging
import traceback
from datetime import datetime
from collections import defaultdict
from tkinter import Tk, messagebox

# ------------------ LOGGING SETUP ------------------ #

LOG_FOLDER = "logs"
os.makedirs(LOG_FOLDER, exist_ok=True)

log_file = os.path.join(
    LOG_FOLDER,
    f"sql_object_extraction_{datetime.now().strftime('%Y%m%d')}.log"
)

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# ------------------ BLOCK KEYWORDS (PREVENT FALSE TABLE DETECTION) ------------------ #

BLOCK_KEYWORDS = {
    "IF","EXISTS","THEN","ELSE","END","CASE","WHEN","ON","USING","VALUES",
    "SELECT","WHERE","GROUP","ORDER","HAVING","BY","AS",
    "AND","OR","NOT","IN","IS","NULL","LIKE","SET",
    "BEGIN","DECLARE","RETURN","LOOP","FOR","WHILE",
    "BETWEEN","DISTINCT","TOP","LIMIT","FETCH","OVER",
    "PARTITION","INNER","LEFT","RIGHT","FULL","OUTER","CROSS",
    "UNION","EXCEPT","INTERSECT","ALL","WITH",
    "DESC","ASC","INTO"
}

# ------------------ Load Configuration ------------------ #

def load_config(config_path="config.txt"):
    config = {}
    with open(config_path, "r") as f:
        for line in f:
            if "=" in line:
                key, value = line.strip().split("=", 1)
                config[key.strip()] = value.strip()
    return config

# ------------------ SQL Cleanup ------------------ #

def clean_sql(sql_text):
    sql_text = re.sub(r'/\*.*?\*/', '', sql_text, flags=re.DOTALL)
    sql_text = re.sub(r'--.*', '', sql_text)
    return sql_text.lower()

# ------------------ Extract Tables / Views WITH SCHEMA ------------------ #

def extract_objects(sql_text):
    pattern = r'''
        \bfrom\s+([a-z0-9_\.]+)|
        \bjoin\s+([a-z0-9_\.]+)|
        \bupdate\s+([a-z0-9_\.]+)|
        \binsert\s+into\s+(?:transient\s+|temporary\s+)?([a-z0-9_\.]+)|
        \bmerge\s+into\s+(?:transient\s+|temporary\s+)?([a-z0-9_\.]+)|
        \bdelete\s+from\s+([a-z0-9_\.]+)|
        \btruncate\s+table\s+(?:transient\s+|temporary\s+)?([a-z0-9_\.]+)|
        \balter\s+table\s+(?:transient\s+|temporary\s+)?([a-z0-9_\.]+)|
        \bcreate\s+(?:or\s+replace\s+)?(?:transient\s+|temporary\s+)?table\s+([a-z0-9_\.]+)|
        \bcreate\s+(?:or\s+replace\s+)?view\s+([a-z0-9_\.]+)|
        \bdrop\s+table\s+(?:transient\s+|temporary\s+)?([a-z0-9_\.]+)
    '''

    matches = re.findall(pattern, sql_text, flags=re.IGNORECASE | re.VERBOSE)

    objects = set()
    for m in matches:
        for obj in m:
            if obj:
                cleaned = obj.strip().replace(";", "").upper()
                if cleaned.split(".")[-1] not in BLOCK_KEYWORDS:
                    objects.add(cleaned)
    return objects

# ------------------ Remove Redundancy (Prefer schema version) ------------------ #

def remove_redundancy(objects):
    schema_objs = {obj for obj in objects if "." in obj}
    noschema_objs = {obj for obj in objects if "." not in obj}

    cleaned_objects = set(schema_objs)

    for obj in noschema_objs:
        if not any(schema_obj.split(".")[-1] == obj for schema_obj in schema_objs):
            cleaned_objects.add(obj)

    return cleaned_objects

# ------------------ Categorize Objects ------------------ #

def categorize(objects):
    categories = defaultdict(list)

    for full_obj in objects:
        table_name = full_obj.split('.')[-1]

        if table_name.startswith("LP_"):
            categories["LP_OBJECTS"].append(full_obj)
        elif table_name.startswith("DDT_"):
            categories["DDT_OBJECTS"].append(full_obj)
        elif table_name.startswith("E_"):
            categories["E_OBJECTS"].append(full_obj)
        elif table_name.startswith("SA_"):
            categories["SA_OBJECTS"].append(full_obj)
        elif table_name.startswith("DW_"):
            categories["DW_OBJECTS"].append(full_obj)
        else:
            categories["OTHER Table/Views/SP's"].append(full_obj)

    return categories

# ------------------ Stored Procedure Detection ------------------ #

def extract_stored_procedures(sql_text):
    procedures = set()

    create_pattern = r'create\s+(or\s+replace\s+)?procedure\s+([\w\.$]+)'
    for _, sp in re.findall(create_pattern, sql_text, flags=re.IGNORECASE):
        procedures.add(sp.upper())

    call_pattern = r'\b(?:exec|execute|call)\s+([\w\.$]+)'
    for sp in re.findall(call_pattern, sql_text, flags=re.IGNORECASE):
        procedures.add(sp.upper())

    return sorted(procedures)

# ------------------ Save Output ------------------ #

def save_output(categories, output_full_path):
    with open(output_full_path, "w") as f:
        f.write("===== CLASSIFIED DATABASE OBJECTS (SCHEMA INCLUDED) =====\n\n")
        for category, items in categories.items():
            f.write(f"{category} ({len(items)} items)\n")
            for item in sorted(items):
                f.write(f"  - {item}\n")
            f.write("\n")

# ------------------ Main Execution ------------------ #

def main():
    logging.info("SQL Object Extraction Started")

    try:
        config = load_config("config.txt")
        logging.info("Config loaded successfully")

        input_path = config["input_file_path"]
        output_folder = config["output_folder_path"]
        output_file = config["output_file_name"]

        os.makedirs(output_folder, exist_ok=True)
        output_path = os.path.join(output_folder, output_file)

        logging.info(f"Reading SQL file: {input_path}")
        with open(input_path, "r", encoding="utf-8") as f:
            sql_text = f.read()

        cleaned = clean_sql(sql_text)
        objects = extract_objects(cleaned)
        objects = remove_redundancy(objects)
        categories = categorize(objects)

        sp_list = extract_stored_procedures(cleaned)
        categories["STORED_PROCEDURES"] = sp_list

        save_output(categories, output_path)
        logging.info(f"Output saved to: {output_path}")

        root = Tk()
        root.withdraw()
        messagebox.showinfo(
            "Extraction Completed",
            f"SQL Object Extraction Completed Successfully!\nSaved to:\n{output_path}"
        )
        root.destroy()

        logging.info("SQL Object Extraction Completed Successfully")

    except Exception as e:
        logging.error("ERROR occurred during SQL Object Extraction")
        logging.error(str(e))
        logging.error(traceback.format_exc())

        root = Tk()
        root.withdraw()
        messagebox.showerror(
            "Extraction Failed",
            f"An error occurred.\nPlease check log file:\n{log_file}"
        )
        root.destroy()

# ------------------ Script Entry ------------------ #

if __name__ == "__main__":
    main()
    