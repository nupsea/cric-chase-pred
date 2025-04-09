import csv
import json


def get_records_for_match(csv_file_path, desired_match_id):
    """
    Reads the CSV into memory, then filters out rows
    that match the 'desired_match_id'. Returns a list of dictionaries.
    """
    with open(csv_file_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        all_rows = list(reader)

    # Filter only rows with the given match_id (mergeid) and the second innings
    filtered = [row for row in all_rows if (row["mergeid"] == desired_match_id)]
    return filtered


def row_to_json(fields, row):
    row_dict = dict(zip(fields, row))
    return json.dumps(row_dict)
