import pandas as pd
import re
from uuid import uuid4


def create_parquet_file_from_txt(text_file_path, parquet_file_path):
    with open(text_file_path, "r") as f:
        lines = f.readlines()

    num_skipped_lines = 0
    data = []
    for line in lines:
        parts = line.split(":::")
        title_year = parts[1].strip()
        match = re.match(r"(.+) \((\d{4})\)", title_year)
        try:
            title, year = match.groups()
            year = int(year)
        # skip malformed lines
        except:
            num_skipped_lines += 1
            continue

        genre = parts[2].strip()
        description = parts[3].strip()

        year = int(year)

        data.append((str(uuid4()), title, year, genre, description))

    df = pd.DataFrame(
        data, columns=["movie_id", "title", "year", "genre", "description"]
    )

    print(df.head())
    print(
        f"Created a dataframe with shape {df.shape} while skipping {num_skipped_lines} lines."
        f"Saving to {parquet_file_path}."
    )

    df.to_parquet(parquet_file_path, index=False)