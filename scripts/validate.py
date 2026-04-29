import pandas as pd

def validate(file_path):
    df = pd.read_csv(file_path)

    null_count = df.isnull().sum().sum()
    duplicate_count = df.duplicated().sum()
    row_count = len(df)
    quality_score = 1 - (null_count + duplicate_count) / row_count

    print(f"Rows: {row_count}")
    print(f"Nulls: {null_count}")
    print(f"Duplicates: {duplicate_count}")
    print(f"Score: {quality_score}")

    assert row_count > 0, "No data loaded"
    assert null_count == 0, "Null values found"
    assert duplicate_count == 0, "Duplicates found"

    return True