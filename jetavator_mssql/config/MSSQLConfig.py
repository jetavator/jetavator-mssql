from jetavator.config import StorageServiceConfig, ConfigProperty


class MSSQLConfig(StorageServiceConfig):

    database: str = ConfigProperty(str)
    server: str = ConfigProperty(str)
    username: str = ConfigProperty(str)
    password: str = ConfigProperty(str)
    trusted_connection: bool = ConfigProperty(bool, optional=True)
