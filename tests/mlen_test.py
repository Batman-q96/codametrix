from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.types as T
import pyspark.sql.functions as F
import hashlib
import pytest

from app import main


@pytest.fixture(scope="session")
def df() -> DataFrame:
    """
    Pytest Fixture acts like a setup module function where the function gets invoked at the start
    of the execution. The function is expected to return a Spark Dataframe
    """
    # Step 1: Create a Spark Session

    # Step 2: Read the json file, fix data integrity issues, and verify the schema of the data
    """
    Expected Schema

    root
        |-- jira_ticket_id: integer (nullable = true)
        |-- date: date (nullable = true)
        |-- completed: boolean (nullable = true)
        |-- num_slack_messages: integer (nullable = true)
        |-- num_hours: float (nullable = true)
        |-- engineer: string (nullable = true)
        |-- ticket_description: string (nullable = true)
        |-- KPIs: array (nullable = true)
        |    |-- element: struct (containsNull = true)
        |    |    |-- initiative: string (nullable = true)
        |    |    |-- new_revenue: float (nullable = true)
        |-- lines_per_repo: array (nullable = true)
        |    |-- element: map (containsNull = true)
        |    |    |-- key: string
        |    |    |-- value: integer (valueContainsNull = true)
    """
    return main.get_and_cleanup_script()


def hash_util(obj) -> str:
    """Function to return the hash of the object

    Args:
        obj (_type_): Object can be of type string, int, float, Pyspark Row, list of Pyspark Rows,
        any objetct that can be projected in string format

    Returns:
        str: Hash of the object
    """
    return hashlib.md5(str(obj).encode("utf-8")).hexdigest()


def test_count_of_unique_tickets(df):
    """
    Sample Question: Get the count of unique tickets in the data
    ex: 100
    """
    res = df.select("jira_ticket_id").distinct()
    assert hash_util(res.count()) == "14ee22eaba297944c96afdbe5b16c65b"


def test_q1_longest_description(df):
    """
    Question: What is the longest Jira ticket description?

    Return the longest ticket description
    ex: [Row(ticket_description='...')]
    """
    res = ...
    assert hash_util(res.collect()) == "a0ef0d383ff7ee8a3af1669f9a8e0f14"


def test_q2_repo_with_max_lines(df):
    """
    Question: Which repo had the most lines of code added?

    Return the repo with maximum lines of code added
    ex: [Row(repo='...')]
    """
    res = ...
    assert hash_util(res.collect()) == "7f2650ec9b6159c18eba65f65615740d"


def test_q3_max_number_of_slack_messages_per_engineer(df):
    """
    Question: Provide the maximum number of Slack messages in any ticket for each engineer

    Return the maximum number of slack messages per engineer
    ex: [Row(engineer='...', max_messages=...),
        Row(engineer='...', max_messages=...),
                    ...,
        Row(engineer='...', max_messages=...)]

    hint: Results are ordered by engineer
    """
    res = ...
    assert hash_util(res.collect()) == "a237ab13f39d30d21b8b937359e9c01f"


def test_q4_mean_hours_spent(df):
    """
    Question: Mean hours spent on a ticket in June 2023

    Return Mean hours spent on a ticket of specific month and year
    ex: [Row(mean_hours=...)]
    """
    res = ...
    assert hash_util(res.collect()) == "7facdf09b955d4732ed4138d3fa48778"


def test_q5_total_lines_contributed(df):
    """
    Question: Total lines of code contributed by completed tickets to the repo 'A'

    Return the total_lines_of_code_contributed
    ex: [Row(total=...)]
    """
    res = ...
    assert hash_util(res.collect()) == "6adec64b2a723c9a52024c53068f264d"


def test_q6_total_new_revenue_per_engineer_per_company_initiative(df):
    """
    Question: Total new revenue per engineer per company initiative

    Return the total_lines_of_code_contributed
    ex: [Row(engineer='...', KPIs=[Row(initiative='...', total_revenue=...), Row(initiative='...', total_revenue=...), ...),
                        ...
    Row(engineer='...', KPIs=[Row(initiative='...', total_revenue=...), Row(initiative='...', total_revenue=...), ...)]

    hint: Order the results by engineer. Pay attention to the order of the intitiatives and total_revenue in KPI's
    """
    res = ...
    assert hash_util(res.collect()) == "c7b9f4457e8682313464d604f6f66581"
