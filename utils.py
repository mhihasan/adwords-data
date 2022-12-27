import csv
import os

TERMS = {
    "singe_word_terms": ["moneycard", "walmart", "keyword"],
    "two_word_terms": ["sams club", "nfl scores"],
    "three_word_terms": [
        "spanish english translate",
        "google knowledge panel",
        "mp3 to youtube",
    ],
}


def print_result(result):
    for r in result:
        print(r)
    print("\n")


def write_to_file(file_name, result):
    file_path = os.path.join(os.path.dirname(__file__), f"{file_name}.csv")
    with open(file_path, "w") as f:
        fieldnames = result[0].keys()
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for r in result:
            writer.writerow(r)
