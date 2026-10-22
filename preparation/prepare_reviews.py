import csv
from pathlib import Path

project_folder = Path(r"C:\BDA Project")
input_file = project_folder / "data" / "Meesho.csv"

with input_file.open(encoding="utf-8-sig", newline="") as file:
    reader = csv.DictReader(file)
    print("Columns:", reader.fieldnames)
    review_count = sum(1 for row in reader)
    print("Total reviews:", review_count)
    
with input_file.open(encoding="utf-8-sig", newline="") as file:
    reader = csv.DictReader(file)
    first_review = next(reader)

    print("Rating:", first_review["score"])
    print("Review:", first_review["content"])
    
output_file = project_folder / "data" / "ratings.txt"

with input_file.open(encoding="utf-8-sig", newline="") as source:
    reader = csv.DictReader(source)

    with output_file.open("w", encoding="utf-8") as target:
        for row in reader:
            target.write(row["score"] + "\n")

print("Created:", output_file)


with output_file.open(encoding="utf-8") as file:
    ratings = [line.strip() for line in file]

print("Ratings written:", len(ratings))
print("First 5 ratings:", ratings[:5])
print("All ratings valid:", all(r in {"1", "2", "3", "4", "5"} for r in ratings))
