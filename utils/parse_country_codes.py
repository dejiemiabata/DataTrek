import json
import os


def parse_country_codes():
    file_path = os.getenv("COUNTRY_POPULATION_DATA_FILE_PATH")

    if not file_path:
        raise ValueError(
            "Please set the COUNTRY_POPULATION_DATA_FILE_PATH env variable."
        )

    with open(file_path, "r") as f:
        data = json.load(f)

    country_codes = [item["Alpha_2"] for item in data]

    output_filename = f"country_codes.py"
    output_path = os.path.join("utils", output_filename)

    # Write to the output file
    with open(output_path, "w") as f:
        f.write(f"COUNTRY_CODES= {json.dumps(country_codes, indent=2)}")

    print(f"Completed writing country codes to {output_path} ")


if __name__ == "__main__":
    parse_country_codes()
