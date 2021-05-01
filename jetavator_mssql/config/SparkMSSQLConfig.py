import wysdom
from .MSSQLConfig import MSSQLConfig


class SparkMSSQLConfig(MSSQLConfig):
    type: str = wysdom.UserProperty(wysdom.SchemaConst('spark_mssql'))
