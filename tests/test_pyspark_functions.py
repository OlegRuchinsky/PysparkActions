import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from SnowFlake02_app import lowercase_all_column_names, \
    add_transformations  # Replace 'your_script' with the actual script name


@pytest.fixture(scope="module")
def spark():
    # Initialize a SparkSession for testing
    spark_session = SparkSession.builder \
        .appName("PySparkUnitTests") \
        .master("local[*]") \
        .getOrCreate()
    return spark_session


@pytest.fixture
def sample_df(spark):
    # Create a sample DataFrame for testing
    data = [
        (1, "Alice", 6000.0),
        (2, "Bob", 3000.0),
        (3, "Charlie", 7000.0)
    ]
    schema = ["C_CUSTKEY", "C_NAME", "C_ACCTBAL"]
    return spark.createDataFrame(data, schema)


def test_lowercase_all_column_names(spark, sample_df):
    # Test if all column names are converted to lowercase
    result_df = lowercase_all_column_names(sample_df)
    assert result_df.columns == ['c_custkey', 'c_name', 'c_acctbal'], "Column names should be lowercase"


def test_add_transformations(spark, sample_df):
    # Test if transformations are applied correctly
    result_df = lowercase_all_column_names(sample_df)  # Ensure consistent lowercase column names
    transformed_df = add_transformations(result_df)

    # Check if the new column is added
    assert "new_column" in transformed_df.columns, "New column 'new_column' should be added"

    # Check if rows are filtered correctly
    filtered_rows = transformed_df.collect()
    assert len(filtered_rows) == 2, "Only rows with acctbal > 5000 should remain"

    # Validate the content of the new column
    for row in filtered_rows:
        assert row.new_column == "example_value", "New column should have the constant value 'example_value'"

    # Ensure selected columns are present
    assert transformed_df.columns == ['c_custkey', 'c_name', 'c_acctbal',
                                      "new_column"], "Only selected columns should be in the result"


def test_no_side_effects(sample_df):
    # Ensure original DataFrame is not modified by functions
    original_columns = sample_df.columns
    lowercase_all_column_names(sample_df)
    assert sample_df.columns == original_columns, "Original DataFrame columns should remain unchanged"
