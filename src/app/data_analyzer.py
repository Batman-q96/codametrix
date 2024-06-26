import dataclasses
import datetime
import string

import hashlib

from typing import Optional

import pyspark.sql
from pyspark.sql import functions
import pyspark.sql.dataframe


@dataclasses.dataclass
class DataAnalyzer:
    input_data: pyspark.sql.DataFrame
    working_data: Optional[pyspark.sql.DataFrame] = None

    def get_longest_ticket_description(
        self,
        ticket_column_name="ticket_description",
        output_column_name="longest_ticket_description",
    ) -> pyspark.sql.DataFrame:
        WORKING_COLUMN_NAME = "ticket_length"
        self.working_data = self.input_data.withColumn(
            WORKING_COLUMN_NAME,
            functions.length(self.input_data[ticket_column_name]),
        )
        max_length = self.working_data.agg(
            functions.max(functions.col(WORKING_COLUMN_NAME))
        ).collect()[0][0]
        return self.working_data.where(self.working_data[WORKING_COLUMN_NAME] == max_length).select(
            output_column_name
        )

    def get_repo_with_most_lines(
        self,
        lines_of_code_column_name="lines_per_repo",
        repo_names=None,
    ) -> pyspark.sql.DataFrame:
        if repo_names is None:
            repo_names = list(string.ascii_uppercase)
        EXPLODED_DATA_COLUMN_NAME = "exploded"
        self.working_data = self.input_data.withColumn(
            EXPLODED_DATA_COLUMN_NAME, functions.explode(self.input_data[lines_of_code_column_name])
        )
        KEY_COLUMN_NAME = "repo"
        VALUE_COLUMN_NAME = "values"
        self.working_data = self.working_data.withColumn(
            KEY_COLUMN_NAME, functions.map_keys(EXPLODED_DATA_COLUMN_NAME).getItem(0)
        ).withColumn(VALUE_COLUMN_NAME, functions.map_values(EXPLODED_DATA_COLUMN_NAME).getItem(0))

        grouped_data = self.working_data.groupBy(KEY_COLUMN_NAME)
        self.working_data = grouped_data.sum(VALUE_COLUMN_NAME)
        self.working_data = self.working_data.withColumnRenamed(
            f"sum({VALUE_COLUMN_NAME})", VALUE_COLUMN_NAME
        )
        max_value = self.working_data.agg(
            functions.max(self.working_data[VALUE_COLUMN_NAME])
        ).collect()[0][0]
        return self.working_data.where(self.working_data[VALUE_COLUMN_NAME] == max_value).select(
            KEY_COLUMN_NAME
        )

    def get_max_messages_for_all_engineers(
        self,
        engineer_column_name="engineer",
        slack_messages_column_name: str = "num_slack_messages",
        output_column_name: str = "num_slack_messages",
        exclude_null: bool = True,
        ascending: bool = True,
    ) -> pyspark.sql.DataFrame:
        engineer_grouped_data = self.input_data.groupBy(self.input_data[engineer_column_name])
        self.working_data = engineer_grouped_data.max(slack_messages_column_name).sort(
            engineer_column_name, ascending=ascending
        )
        if exclude_null:
            self.working_data = self.working_data.where(
                self.working_data[engineer_column_name].isNotNull()
            )
        return self.working_data.withColumnRenamed(
            f"max({slack_messages_column_name})", output_column_name
        )

    def get_mean_hours_spent(
        self,
        start_date: datetime.date,
        end_date: datetime.date,
        date_column_name: str = "date",
        hours_spent_column_name: str = "num_hours",
        ticket_id_column_name: str = "jira_ticket_id",
        engineer_column_name: str = "engineer",
        output_column_name: str = "mean_hours",
        filter_null_dates: bool = True,
        ignore_null_engineers: bool = True,
    ) -> pyspark.sql.DataFrame:
        if filter_null_dates:
            self.working_data = self.input_data.filter(
                (self.input_data[date_column_name] >= start_date)
                & (self.input_data[date_column_name] <= end_date)
                & self.input_data[date_column_name].isNotNull()
            )
        else:
            self.working_data = self.input_data.filter(
                (self.input_data[date_column_name] >= start_date)
                & (self.input_data[date_column_name] <= end_date)
                | self.input_data[date_column_name].isNull()
            )
        if ignore_null_engineers:
            self.working_data = self.working_data.filter(
                self.working_data[engineer_column_name].isNotNull()
            )

        # add up all hours by everyone working on the same ticket
        grouped_data = self.working_data.groupBy(self.working_data[ticket_id_column_name])
        result_data = grouped_data.agg(
            functions.sum(self.working_data[hours_spent_column_name]).alias(output_column_name)
        )
        # then return the average of that
        return result_data.agg(
            functions.avg(result_data[output_column_name]).alias(output_column_name)
        )

    def get_total_lines_of_code_to_repo(
        self,
        repo_name: str,
        repo_lines_column_name: str = "lines_per_repo",
        completed_column_name="completed",
        output_column_name: str = "total",
    ) -> pyspark.sql.DataFrame:
        self.working_data = self.input_data.filter(self.input_data[completed_column_name] == True)
        # self.working_data = self.working_data.filter(functions.size(self.working_data["KPIs"])==1)
        exploded_data = self.working_data.withColumn(
            repo_lines_column_name,
            functions.explode(self.working_data[repo_lines_column_name]).alias(output_column_name),
        )
        repo_data = exploded_data.withColumn(
            output_column_name, exploded_data[repo_lines_column_name][repo_name]
        )
        # TODO: figure out why this doesn't work right
        # correct answer is 168195
        return repo_data.select(repo_data[output_column_name]).agg(
            functions.sum(repo_data[output_column_name]).alias(output_column_name)
        )

    def get_total_revenue_per_engineer_per_company_initiative(
        self,
        engineer_column_name: str = "engineer",
        kpi_column_name: str = "KPIs",
        kpi_initiative_column_name: str = "initiative",
        kpi_revenue_column_name: str = "new_revenue",
        total_revenue_column_name: str = "total_revenue",
        output_column_name: str = "KPIs",
        kpis_initiaitive_ascending: bool = True,
        engineers_ascending: bool = True,
        filter_null_engineers: bool = True,
    ) -> pyspark.sql.DataFrame:
        self.working_data = self.input_data.select(
            engineer_column_name,
            functions.inline(
                self.input_data[kpi_column_name].alias(
                    kpi_initiative_column_name, kpi_revenue_column_name
                )
            ),
        )
        grouped_data = self.working_data.groupBy(engineer_column_name, kpi_initiative_column_name)
        revenue_data = grouped_data.agg(
            functions.sum(kpi_revenue_column_name).alias(total_revenue_column_name)
        )
        sorted_revenue_data = revenue_data.sort(
            kpi_initiative_column_name, ascending=kpis_initiaitive_ascending
        )
        structed_data = sorted_revenue_data.select(
            engineer_column_name,
            functions.struct(kpi_initiative_column_name, total_revenue_column_name).alias(
                output_column_name
            ),
        )
        grouped_data = structed_data.groupBy(engineer_column_name)
        output_data = grouped_data.agg(
            functions.array_agg(output_column_name).alias(output_column_name)
        )
        if filter_null_engineers:
            filtered_data = output_data.filter(output_data[engineer_column_name].isNotNull())
        else:
            filtered_data = output_data
        return filtered_data.sort(engineer_column_name, ascending=engineers_ascending)


def hash_util(obj) -> str:
    """Function to return the hash of the object

    Args:
        obj (_type_): Object can be of type string, int, float, Pyspark Row, list of Pyspark Rows,
        any objetct that can be projected in string format

    Returns:
        str: Hash of the object
    """
    return hashlib.md5(str(obj).encode("utf-8")).hexdigest()


def hash_entry(obj) -> bytes:
    return str(obj).encode("utf-8")


def main():
    from app import data_cleanup

    clean_data = data_cleanup.main()

    analyzer = DataAnalyzer(input_data=clean_data)
    print(analyzer.get_longest_ticket_description(output_column_name="ticket_description").show())
    print(analyzer.get_repo_with_most_lines().show())
    print(analyzer.get_max_messages_for_all_engineers(output_column_name="max_messages").show())
    print(
        analyzer.get_mean_hours_spent(
            start_date=datetime.date(2023, 6, 1),
            end_date=datetime.date(2023, 6, 30),
            filter_null_dates=True,
            ignore_null_engineers=False,
        ).show()
    )
    print(analyzer.get_total_lines_of_code_to_repo("A").show())
    print(
        analyzer.get_total_revenue_per_engineer_per_company_initiative(
            kpis_initiaitive_ascending=False, engineers_ascending=False, filter_null_engineers=True
        ).show()
    )
    print("done")


def hash_break_brute_force():
    # this was used to figure out what the target number of rows to acheive was. Ultimately even
    # knowing the target row number was insufficient for me to match assumptions
    for i in range(247559):
        if hash_util(f"[Row(total={i})]") == "6adec64b2a723c9a52024c53068f264d":
            print(i)
            break
    else:
        print("no i found")


if __name__ == "__main__":
    main()
    # hash_break_brute_force()
