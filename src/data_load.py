import pyspark
import pyspark.sql

FILEPATH = "https://raw.githubusercontent.com/kingspp/mlen_assignment/main/"
FILENAME = "data.json"


def load_file_into_pyspark_dataframe(
    spark: pyspark.sql.SparkSession, filename: str = FILENAME, filepath: str = FILEPATH
) -> pyspark.sql.DataFrame:
    spark.sparkContext.addFile(FILEPATH + FILENAME)
    spark_df = spark.read.json(pyspark.SparkFiles.get(FILENAME))
    return spark_df


def main():
    spark: pyspark.sql.SparkSession = pyspark.sql.SparkSession.Builder().getOrCreate()
    # docs suggest SparkSession.builder.getOrCreate() but that syntax doesn't
    # have appropriate type checking and this is the same execution

    spark_df = load_file_into_pyspark_dataframe(spark)
    print(spark_df.show())


if __name__ == "__main__":
    main()
