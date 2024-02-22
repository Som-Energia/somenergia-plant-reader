from dotenv import dotenv_values
from abc import ABC
import enum


class UnknownConfigContext(Exception):
    pass


class Environment(str, enum.Enum):
    PROD = "prod"
    PRE = "pre"
    TESTING = "testing"


class AbstractConfig(ABC):
    _environment: Environment
    _dotenv_file: str

    @property
    def base_url(self) -> str:
        return dotenv_values(self._dotenv_file)["base_url"]

    @property
    def api_key(self) -> str:
        return dotenv_values(self._dotenv_file)["api_key"]

    @property
    def group_api_key(self) -> str:
        return dotenv_values(self._dotenv_file)["group_api_key"]

    @property
    def db_url(self) -> str:
        return dotenv_values(self._dotenv_file)["db_url"]


class TestConfig(AbstractConfig):
    _dotenv_file = ".env.testing"
    _environment = Environment.TESTING

    @property
    def base_url(self) -> str:
        return dotenv_values(self._dotenv_file).get(
            "base_url",
            "https://api.dset-energy.com",
        )

    @property
    def api_key(self) -> str:
        return dotenv_values(self._dotenv_file).get("api_key", "default_api_key")

    @property
    def group_api_key(self) -> str:
        return dotenv_values(self._dotenv_file).get(
            "group_api_key",
            "default_group_api_key",
        )

    @property
    def db_url(self) -> str:
        return dotenv_values(self._dotenv_file).get(
            "db_url",
            "postgresql://user:password@localhost:5432/db",
        )


class PreConfig(AbstractConfig):
    _environment = Environment.PRE
    _dotenv_file = ".env.pre"


class ProdConfig(AbstractConfig):
    _environment = Environment.PROD
    _dotenv_file = ".env.prod"


def get_config(environment: Environment) -> AbstractConfig:
    if environment == Environment.TESTING:
        return TestConfig()
    elif environment == Environment.PRE:
        return PreConfig()
    elif environment == Environment.PROD:
        return ProdConfig()
    else:
        raise UnknownConfigContext
