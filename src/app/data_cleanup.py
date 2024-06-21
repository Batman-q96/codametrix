from typing import Optional

import dataclasses

import json

import pyspark
import pyspark.sql
from pyspark.sql import functions, types as spark_types

import pyspark.data


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
            .cast(spark_types.BooleanType()),
        )
        return clean_data

    def _cleanup_date_column(
        self, data_to_clean: pyspark.sql.DataFrame, column_name: str = "date"
    ) -> pyspark.sql.DataFrame:
        clean_data = data_to_clean.withColumn(
            column_name, functions.to_date(data_to_clean[column_name])
        )
        return clean_data

    def _cleanup_engineer_column(
        self,
        data_to_clean: pyspark.sql.DataFrame,
        column_name: str = "engineer",
        remove_nulls: bool = False,
    ) -> pyspark.sql.DataFrame:
        clean_data = data_to_clean.withColumn(
            column_name, functions.initcap(functions.lcase(data_to_clean[column_name]))
        )
        if remove_nulls:
            clean_data = clean_data.filter(clean_data[column_name].isNotNull())
        return clean_data

    def _cleanup_jira_ticket_id_column(
        self, data_to_clean: pyspark.sql.DataFrame, column_name: str = "jira_ticket_id"
    ) -> pyspark.sql.DataFrame:
        clean_data = data_to_clean.withColumn(
            column_name, data_to_clean[column_name].cast(spark_types.IntegerType())
        )
        return clean_data

    def _cleanup_lines_per_repo_column(
        self,
        data_to_clean: pyspark.sql.DataFrame,
        column_name: str = "lines_per_repo",
        key_column_name: str = "jira_ticket_id",
    ) -> pyspark.sql.DataFrame:
        data_type = data_to_clean.schema[column_name].dataType
        if isinstance(data_type, spark_types.ArrayType):
            element_type = data_type.elementType
            if isinstance(element_type, spark_types.StructType):
                names = element_type.names
            else:
                raise TypeError
        else:
            raise TypeError
        working_data = data_to_clean.select(
            key_column_name, functions.inline(data_to_clean[column_name])
        )
        map_data = working_data
        map_data = map_data.withColumns(
            {
                name: functions.when(
                    working_data[name] > 0,
                    functions.create_map([functions.lit(name), working_data[name]]).alias(name),
                )
                for name in names
            }
        )
        # check assumption that only one valid value per column
        # xxx=map_data.withColumn('numNulls', sum(map_data[col].isNull().cast('int') for col in map_data.columns))
        # xxx.agg({"numNulls": "min"}).show()
        COALESCED_COLUMN_NAME = "coalesced"
        coalesced_data = map_data.withColumn(COALESCED_COLUMN_NAME, functions.coalesce(*names))
        coalesced_data = coalesced_data.groupBy(key_column_name).agg(
            functions.collect_list(COALESCED_COLUMN_NAME).alias(COALESCED_COLUMN_NAME)
        )

        clean_data = data_to_clean.join(coalesced_data, on=key_column_name, how="outer")
        clean_data = clean_data.withColumn(column_name, clean_data[COALESCED_COLUMN_NAME])
        clean_data = clean_data.drop(clean_data[COALESCED_COLUMN_NAME])

        return clean_data

    def _cleanup_num_hours_column(
        self, data_to_clean: pyspark.sql.DataFrame, column_name: str = "num_hours"
    ) -> pyspark.sql.DataFrame:
        clean_data = data_to_clean.withColumn(
            column_name, data_to_clean[column_name].cast(spark_types.FloatType())
        )
        # several things could be done to deal with negative values here
        # option 1
        # clean_data = clean_data.withColumn(column_name, functions.abs(clean_data[column_name]))
        # option 2
        clean_data = clean_data.filter(clean_data[column_name] >= 0)
        return clean_data

    def _cleanup_num_slack_messages_column(
        self, data_to_clean: pyspark.sql.DataFrame, column_name: str = "num_slack_messages"
    ) -> pyspark.sql.DataFrame:
        clean_data = data_to_clean.withColumn(
            column_name, data_to_clean[column_name].cast(spark_types.IntegerType())
        )
        return clean_data

    def _cleanup_num_ticket_description_column(
        self, data_to_clean: pyspark.sql.DataFrame, column_name: str = "ticket_description"
    ) -> pyspark.sql.DataFrame:
        """Passthrough as this column looks to need no sanitization"""
        return data_to_clean

    def _order_columns(
        self,
        data_to_clean: pyspark.sql.DataFrame,
        ordered_list_of_column_names: Optional[list[str]] = None,
    ):
        if not ordered_list_of_column_names:
            ordered_list_of_column_names = [
                "jira_ticket_id",
                "date",
                "completed",
                "num_slack_messages",
                "num_hours",
                "engineer",
                "ticket_description",
                "KPIs",
                "lines_per_repo",
            ]
        ordered_data = data_to_clean.select(*[ordered_list_of_column_names])
        return ordered_data

    def show_clean_schema(self) -> dict:
        if self.clean_data:
            return json.loads(self.clean_data.schema.json())
        else:
            raise ValueError

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
        self.working_data = self._order_columns(self.working_data)
        self.clean_data = self.working_data
        return self.clean_data


def main():
    from app import data_load  # importing here because it's only used for tests

    spark_df = data_load.main()

    spark: pyspark.sql.SparkSession = pyspark.sql.SparkSession.Builder().getOrCreate()

    cleaner = DataCleaner(spark=spark, dirty_data=spark_df)
    clean_data = cleaner.run_data_cleanup()

    print(cleaner.show_clean_schema())
    print(clean_data.show())

    return clean_data


if __name__ == "__main__":
    main()
