import json
import re
import unicodedata
from typing import Optional
import os

RECORD_SET = "recordSet"
FIELD = "field"
NAME = "name"
DESCRIPTION = "description"
DISTRIBUTION = "distribution"
DATA_TYPE = "dataType"
NAIVE_BAYES = "Naive Bayes"

def extract_json(file_path: str) -> Optional[dict]:
    try:
        with open(file_path, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
    except json.JSONDecodeError:
        print("Error: Could not decode JSON from the file.")

def clean_text(value: str) -> str:
    text = unicodedata.normalize("NFKC", str(value))

    replacements = {
        "\u2018": "'",
        "\u2019": "'",
        "\u201c": '"',
        "\u201d": '"',
        "\u00a0": " ",
    }
    text = text.translate(str.maketrans(replacements))

    text = re.sub(r"[\r\n\t]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def extract_metadata(json_data: dict) -> list[dict[str, str]]:
    metadata = json_data[RECORD_SET][0][FIELD]
    cleaned_metadata = [
        {
            "data_type": clean_text(field[DATA_TYPE][0]),
            "field_name": clean_text(field[NAME]),
            "field_description": clean_text(field[DESCRIPTION]),
        }
        for field in metadata
        if DESCRIPTION in field and NAIVE_BAYES not in field[DESCRIPTION]
    ]
    return cleaned_metadata


if __name__ == '__main__':
    for root, dirs, files in os.walk(os.getcwd() + '/metadata'):
        for file in files:
            if file.endswith('.json'):
                full_path = os.path.join(root, file)
                json_metadata = extract_json(full_path)
                if json_metadata:
                    cleaned_data = extract_metadata(json_metadata)
                    print("Dataset description: " + json_metadata[DISTRIBUTION][1][DESCRIPTION])
                    print(json.dumps(cleaned_data, indent=4))
                    print("_" * 100)

