import pandas
from typing import Iterable, Dict, Optional, List

import uuid
import jinja2
import sqlalchemy
from sqlalchemy.dialects.mssql import dialect

from lazy_property import LazyProperty

import pyspark

from ..config import SparkMSSQLConfig
from jetavator.services import Service, SparkStorageService
from .MSSQLService import MSSQLService

DRIVER_GROUP_ID = "com.microsoft.sqlserver"
DRIVER_ARTIFACT_ID = "mssql-jdbc"
DRIVER_VERSION = "8.2.2.jre8"


class SparkMSSQLService(MSSQLService, SparkStorageService, Service[SparkMSSQLConfig], register_as='spark_mssql'):

    spark_jars_packages: List[str] = [
        f"{DRIVER_GROUP_ID}:{DRIVER_ARTIFACT_ID}:{DRIVER_VERSION}"
    ]

    def load_dataframe(self, dataframe: pandas.DataFrame, source_name: str, source_column_names: Iterable[str]) -> None:
        pass

    @LazyProperty
    def sqlalchemy_dialect(self) -> sqlalchemy.engine.interfaces.Dialect:
        return dialect()

    @property
    def spark(self):
        return self.owner.spark

    def load_csv(self, csv_file, source_name: str):
        raise NotImplementedError()

    def csv_file_path(self, source_name: str):
        raise NotImplementedError()

    def source_csv_exists(self, source_name: str):
        raise NotImplementedError()

    @property
    def url(self) -> str:
        return f"jdbc:sqlserver://{self.config.server}"

    def qualified_table_name(self, table_name: str) -> str:
        return f"{self.config.database}.{self.config.schema}.{table_name}"

    def read_table(self, table_name: str) -> pyspark.sql.DataFrame:
        return (
            self
            .spark
            .read
            .format("jdbc")
            .option("url", self.url)
            .option("dbtable", self.qualified_table_name(table_name))
            .option("user", self.config.username)
            .option("password", self.config.password)
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
            .load()
        )

    def write_table(
            self,
            table_name: str,
            df: pyspark.sql.DataFrame,
            mode: str = "append"
    ) -> None:
        self.check_valid_mode(mode)
        (
            df
            .write
            .format("jdbc")
            .mode(mode)
            .option("url", self.url)
            .option("dbtable", self.qualified_table_name(table_name))
            .option("user", self.config.username)
            .option("password", self.config.password)
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
            .save()
        )

    def connect_storage_view(self, table_name: str) -> None:
        self.spark.sql(
            f"""
            CREATE OR REPLACE TEMPORARY VIEW {table_name}
            USING org.apache.spark.sql.jdbc
            OPTIONS (
              url "{self.url}",
              dbtable "{self.qualified_table_name(table_name)}",
              user "{self.config.username}",
              password "{self.config.password}",
              driver "com.microsoft.sqlserver.jdbc.SQLServerDriver"
            )
            """
        )

    def disconnect_storage_view(self, table_name: str) -> None:
        self.spark.sql(f"DROP VIEW {table_name}")

    def merge_from_spark_view(
            self,
            storage_table_name: str,
            spark_view_name: str,
            key_column_name: str,
            column_names: Iterable[str],
            column_references: Dict[str, str],
            deleted_indicator: Optional[str] = None
    ) -> pyspark.sql.DataFrame:
        temp_table_suffix = str(uuid.uuid4()).replace('-', '_')
        temp_table_name = f"{storage_table_name}_{temp_table_suffix}"
        merge_sql_template = '''
            MERGE 
             INTO {{ target }} AS target
            USING {{ source }} AS source
               ON target.{{ key_column_name }}
                = source.{{ key_column_name }}
             {% if deleted_indicator %}
             WHEN MATCHED AND source.{{ deleted_indicator }} = 1 THEN DELETE
             {% endif %}
             {% for column, satellite_name in column_references.items() %}
             {{ "WHEN MATCHED THEN UPDATE SET" if loop.first }}
                 {{ column }} = (CASE WHEN update_ind_{{ satellite_name }} = 1
                 THEN source.{{ column }}
                 ELSE target.{{ column }}
                 END)
               {{ "," if not loop.last }}
             {% endfor %}
             WHEN NOT MATCHED THEN INSERT (
                {% for column in column_names %}
                {{ column }}{{ "," if not loop.last }}
                {% endfor %}
             ) VALUES (
                {% for column in column_names %}
                source.{{ column }}{{ "," if not loop.last }}
                {% endfor %}
             );
            '''
        original_df = self.owner.spark.table(spark_view_name)
        df = original_df.drop("key_source")
        self.write_table(temp_table_name, df, "overwrite")
        merge_sql = jinja2.Template(merge_sql_template).render(
            target=self.qualified_table_name(storage_table_name),
            source=self.qualified_table_name(temp_table_name),
            key_column_name=key_column_name,
            column_references=column_references,
            column_names=column_names,
            deleted_indicator=deleted_indicator
        )
        self.logger.debug("Pushing down SQL merge to MSSQL:\n" + merge_sql)
        self.execute(merge_sql)
        self.execute(f"DROP TABLE {self.qualified_table_name(temp_table_name)};")
        return original_df
