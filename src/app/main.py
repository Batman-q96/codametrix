import pyspark.sql

from app import data_cleanup, data_load


def get_and_cleanup_script() -> pyspark.sql.DataFrame:

    spark: pyspark.sql.SparkSession = pyspark.sql.SparkSession.Builder().getOrCreate()
    data = data_load.load_file_into_pyspark_dataframe(spark)
    cleaner = data_cleanup.DataCleaner(spark, dirty_data=data)
    clean_data = cleaner.run_data_cleanup()
    print(clean_data.show())
    return clean_data


def main():
    get_and_cleanup_script()


if __name__ == "__main__":
    main()
