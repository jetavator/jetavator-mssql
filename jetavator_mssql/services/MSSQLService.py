from typing import Iterable, Set

import pandas
import sqlalchemy
from sqlalchemy.exc import ProgrammingError, DBAPIError

from lazy_property import LazyProperty

from jetavator.services import StorageService


class MSSQLService(StorageService, register_as='mssql'):

    index_option_kwargs: Set[str] = {"mssql_clustered"}

    @LazyProperty
    def sqlalchemy_connection(self):
        if self.config.trusted_connection:
            return sqlalchemy.create_engine(
                "mssql+pyodbc://{server}:1433/{database}"
                "?driver=ODBC+Driver+17+for+SQL+Server".format(
                    server=self.config.server,
                    database=self.config.database
                ),
                connect_args={'autocommit': True},
                deprecate_large_types=True
            )
        else:
            return sqlalchemy.create_engine(
                "mssql+pyodbc://{username}:{password}@{server}:1433/{database}"
                "?driver=ODBC+Driver+17+for+SQL+Server".format(
                    username=self.config.username,
                    password=self.config.password,
                    server=self.config.server,
                    database=self.config.database
                ),
                connect_args={'autocommit': True},
                deprecate_large_types=True
            )

    def execute(self, sql):
        sql_statement = sql.encode("ascii", "ignore").decode("ascii")
        try:
            result_proxy = self.sqlalchemy_connection.execute(
                sql_statement
            )
        except (ProgrammingError, DBAPIError) as e:
            raise Exception(
                f"""
                Config dump:
                {self.config}

                Error while strying to run script:
                {sql_statement}
                """ + str(e)
            )
        if result_proxy.returns_rows:
            df = pandas.DataFrame(result_proxy.fetchall())
            if df.shape != (0, 0):
                df.columns = result_proxy.keys()
            return df
        else:
            return pandas.DataFrame()

    def drop_schema(self):
        self.sqlalchemy_connection.execute(
            f"""
            DECLARE @drop_statements AS CURSOR
            DECLARE @statement AS VARCHAR(max)

            SET @drop_statements = CURSOR FOR

                SELECT 'DROP VIEW [{self.config.schema}].[' + TABLE_NAME + ']'
                  FROM INFORMATION_SCHEMA.VIEWS
                 WHERE TABLE_SCHEMA = '{self.config.schema}'
    
                UNION ALL
    
                SELECT 'DROP TABLE [{self.config.schema}].[' + TABLE_NAME + ']'
                  FROM INFORMATION_SCHEMA.TABLES
                 WHERE TABLE_SCHEMA = '{self.config.schema}'
                   AND TABLE_TYPE = 'BASE TABLE'

            OPEN @drop_statements

            FETCH NEXT FROM @drop_statements INTO @statement
            WHILE @@FETCH_STATUS = 0
            BEGIN
             EXECUTE (@statement)
             FETCH NEXT FROM @drop_statements INTO @statement
            END

            CLOSE @drop_statements
            DEALLOCATE @drop_statements
            """
        )
        self.sqlalchemy_connection.execute(
            f"DROP SCHEMA [{self.config.schema}]"
        )

    def create_schema(self):
        self.sqlalchemy_connection.execute(
            "CREATE SCHEMA [" + self.config.schema + "]"
        )

    @property
    def schema_empty(self):
        return (
                len(
                    self.sqlalchemy_connection.execute(
                        f"""
                    SELECT TOP 1
                           TABLE_NAME
                      FROM INFORMATION_SCHEMA.TABLES
                     WHERE TABLE_CATALOG = '{self.config.database}'
                       AND TABLE_SCHEMA = '{self.config.schema}'
                    """
                    ).fetchall()
                ) == 0
        )

    @property
    def schema_exists(self):
        return self._sql_exists(
            f"""
            SELECT SCHEMA_NAME
              FROM INFORMATION_SCHEMA.SCHEMATA
             WHERE CATALOG_NAME = '{self.config.database}'
               AND SCHEMA_NAME = '{self.config.schema}'
            """
        )

    def _sql_exists(self, sql):
        result_proxy = self.sqlalchemy_connection.execute(sql)
        return bool(result_proxy.first())

    def table_exists(self, table_name):
        return self._sql_exists(
            f"""
            SELECT TABLE_NAME
              FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_CATALOG = '{self.config.database}'
               AND TABLE_SCHEMA = '{self.config.schema}'
               AND TABLE_NAME = '{table_name}'
            """
        )

    def column_exists(self, table_name, column_name):
        return self._sql_exists(
            f"""
            SELECT COLUMN_NAME
              FROM INFORMATION_SCHEMA.COLUMNS
             WHERE TABLE_CATALOG = '{self.config.database}'
               AND TABLE_SCHEMA = '{self.config.schema}'
               AND TABLE_NAME = '{table_name}'
               AND COLUMN_NAME = '{column_name}'
            """
        )

    def sql_query_single_value(self, sql):
        try:
            return self.sqlalchemy_connection.execute(
                sql
            ).first()[0]
        except TypeError:
            return None

    # def execute_sql_element(
    #         self,
    #         sqlalchemy_element: sqlalchemy.sql.expression.Executable,
    #         async_cursor: bool = False
    # ) -> pandas.DataFrame:
    #     return self.sqlalchemy_connection.execute(sqlalchemy_element).fetchall()

    def test(self) -> None:
        self.execute("SELECT 1")

    def load_dataframe(self, dataframe: pandas.DataFrame, source_name: str, source_column_names: Iterable[str]) -> None:
        # TODO: Implement MSSQLService.load_dataframe
        raise NotImplementedError()

    # def compile_sqlalchemy(
    #         self,
    #         sqlalchemy_element: sqlalchemy.sql.expression.ClauseElement
    # ) -> str:
    #     return super().compile_sqlalchemy(sqlalchemy_element).replace("DATETIME", "DATETIME2")
