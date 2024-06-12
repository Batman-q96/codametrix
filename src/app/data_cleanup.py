from typing import Optional

import dataclasses

import pyspark
import pyspark.sql
import pyspark.sql.dataframe
import pyspark.sql.functions

import pyspark.data
import pyspark.sql.types


@dataclasses.dataclass
class DataCleaner:
    # don't need overhead of pydantic data validation here so dataclass is fine
    spark: pyspark.sql.SparkSession

    dirty_data: pyspark.sql.DataFrame
    working_data: Optional[pyspark.sql.DataFrame] = None
    clean_data: Optional[pyspark.sql.DataFrame] = None

    def _get_unique_values(self, data: pyspark.sql.DataFrame, column_name: str) -> list:
        # Helper function to determine sanitization needs
        return data.select(column_name).distinct().collect()

    def _cleanup_kpis_column(
        self, data_to_clean: pyspark.sql.DataFrame, column_name: str = "KPIs"
    ) -> pyspark.sql.DataFrame:
        """Passthrough as this column looks to need no sanitization"""
        return data_to_clean

    def _cleanup_completed_column(
        self, data_to_clean: pyspark.sql.DataFrame, column_name: str = "completed"
    ) -> pyspark.sql.DataFrame:
        clean_data = data_to_clean.withColumn(
            column_name,
            data_to_clean[column_name]
            .rlike(r"(?i)^yes$|(?i)^true$")
            .cast(pyspark.sql.types.BooleanType()),
        )
        return clean_data

    def _cleanup_date_column(
        self, data_to_clean: pyspark.sql.DataFrame, column_name: str = "date"
    ) -> pyspark.sql.DataFrame:
        clean_data = data_to_clean.withColumn(
            column_name, pyspark.sql.functions.to_date(data_to_clean[column_name])
        )
        return clean_data

    def _cleanup_engineer_column(
        self, data_to_clean: pyspark.sql.DataFrame, column_name: str = "engineer"
    ) -> pyspark.sql.DataFrame:
        clean_data = data_to_clean.withColumn(
            column_name, pyspark.sql.functions.lcase(data_to_clean[column_name])
        )
        return clean_data

    def _cleanup_jira_ticket_id_column(
        self, data_to_clean: pyspark.sql.DataFrame, column_name: str = "jira_ticket_id"
    ) -> pyspark.sql.DataFrame:
        """Passthrough as this column looks to need no sanitization"""
        return data_to_clean

    def _cleanup_lines_per_repo_column(
        self, data_to_clean: pyspark.sql.DataFrame, column_name: str = "lines_per_repo"
    ) -> pyspark.sql.DataFrame:
        """Passthrough as this column looks to need no sanitization"""
        return data_to_clean

    def _cleanup_num_hours_column(
        self, data_to_clean: pyspark.sql.DataFrame, column_name: str = "num_hours"
    ) -> pyspark.sql.DataFrame:
        clean_data = data_to_clean.withColumn(
            column_name, data_to_clean[column_name].cast(pyspark.sql.types.FloatType())
        )
        return clean_data

    def _cleanup_num_slack_messages_column(
        self, data_to_clean: pyspark.sql.DataFrame, column_name: str = "slack_messages"
    ) -> pyspark.sql.DataFrame:
        """Passthrough as this column looks to need no sanitization"""
        return data_to_clean

    def _cleanup_num_ticket_description_column(
        self, data_to_clean: pyspark.sql.DataFrame, column_name: str = "ticket_description"
    ) -> pyspark.sql.DataFrame:
        """Passthrough as this column looks to need no sanitization"""
        return data_to_clean

    def run_data_cleanup(self) -> pyspark.sql.DataFrame:
        self.working_data = self.dirty_data
        self.working_data = self._cleanup_kpis_column(self.working_data)
        self.working_data = self._cleanup_completed_column(self.working_data)
        self.working_data = self._cleanup_date_column(self.working_data)
        self.working_data = self._cleanup_engineer_column(self.working_data)
        self.working_data = self._cleanup_jira_ticket_id_column(self.working_data)
        self.working_data = self._cleanup_lines_per_repo_column(self.working_data)
        self.working_data = self._cleanup_num_hours_column(self.working_data)
        self.working_data = self._cleanup_num_slack_messages_column(self.working_data)
        self.working_data = self._cleanup_num_ticket_description_column(self.working_data)
        self.clean_data = self.working_data
        return self.clean_data


def main():
    from app import data_load  # importing here because it's only used for tests

    spark_df = data_load.main()

    spark: pyspark.sql.SparkSession = pyspark.sql.SparkSession.Builder().getOrCreate()

    cleaner = DataCleaner(spark=spark, dirty_data=spark_df)
    clean_data = cleaner.run_data_cleanup()

    print(clean_data.schema)
    print(clean_data.show())

    return clean_data


if __name__ == "__main__":
    main()
