import pandas as pd
import re
from uuid import uuid4

with open("movie_data.txt", "r") as f:
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
    except:
        num_skipped_lines += 1
        continue  # skip this malformed lines

    genre = parts[2].strip()
    description = parts[3].strip()

    year = int(year)

    data.append((str(uuid4()), title, year, genre, description))

df = pd.DataFrame(data, columns=["movie_id", "title", "year", "genre", "description"])


print(df.head())
print(
    f"Created a dataframe with shape {df.shape} while skipping {num_skipped_lines} lines."
)

df.to_parquet("movie_data.parquet", index=False)
