import os
import pandas as pd

# Path to your CSV file
input_csv = "Every_ball_data.csv"

# Read the CSV file into a DataFrame
df = pd.read_csv(input_csv)

# Group the records by mergeid (match identifier)
for mergeid, group in df.groupby("mergeid"):
    # Create a folder for each match; folder name can be customized as needed
    folder_name = f"match_{mergeid}"
    os.makedirs(folder_name, exist_ok=True)

    # Filter records for first and second innings based on the 'inning' field
    first_innings = group[group["inning"] == 1]
    second_innings = group[group["inning"] == 2]

    # Save first_innings.csv if there are any records
    if not first_innings.empty:
        first_innings.to_csv(
            os.path.join(folder_name, "first_innings.csv"), index=False
        )

    # Save second_innings.csv if there are any records
    if not second_innings.empty:
        second_innings.to_csv(
            os.path.join(folder_name, "second_innings.csv"), index=False
        )
