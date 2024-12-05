import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from SnowFlake03_pyspark import lowercase_all_column_names, add_transformations  # Replace `your_script` with actual script name


@pytest.fixture(scope="module")
def spark():
    # Initialize SparkSession for testing
    return SparkSession.builder \
        .appName("PySparkTestSession") \
        .master("local[*]") \
        .getOrCreate()


@pytest.fixture
def sample_df(spark):
    # Create a sample DataFrame for testing
    data = [
        Row(C_CUSTKEY=1, C_NAME="Alice", C_ACCTBAL=6000.0),
        Row(C_CUSTKEY=2, C_NAME="Bob", C_ACCTBAL=3000.0),
        Row(C_CUSTKEY=3, C_NAME="Charlie", C_ACCTBAL=7000.0)
    ]
    return spark.createDataFrame(data)


def test_lowercase_all_column_names(sample_df):
    # Test lowercase_all_column_names function
    result_df = lowercase_all_column_names(sample_df)
    assert result_df.columns == ['c_custkey', 'c_name', 'c_acctbal'], "Column names should be lowercase"


def test_add_transformations(sample_df):
    # Apply lowercase transformation first for consistency
    result_df = lowercase_all_column_names(sample_df)
    transformed_df = add_transformations(result_df)

    # Check the new column exists
    assert "new_column" in transformed_df.columns, "New column 'new_column' should exist"

    # Check filtered rows
    filtered_rows = transformed_df.collect()
    assert len(filtered_rows) == 2, "Filtered DataFrame should contain 2 rows"

    # Verify constant column value
    for row in filtered_rows:
        assert row["new_column"] == "example_value", "New column should have constant value 'example_value'"

    # Verify columns in final DataFrame
    expected_columns = ['c_custkey', 'c_name', 'c_acctbal', "new_column"]
    assert transformed_df.columns == expected_columns, "Final DataFrame should only have specific columns"


def test_row_count(sample_df):
    # Check the row count of the original DataFrame
    original_count = sample_df.count()
    assert original_count == 3, f"Original DataFrame should have 3 rows, but found {original_count}"

    # After transformations
    result_df = lowercase_all_column_names(sample_df)
    transformed_df = add_transformations(result_df)
    transformed_count = transformed_df.count()
    assert transformed_count == 2, f"Transformed DataFrame should have 2 rows, but found {transformed_count}"