from dotenv import dotenv_values

class UnknownConfigContext(Exception):
    pass


def get_config(dbapi_argument):
    if dbapi_argument == "prod":
        env_dict = dotenv_values(".env.prod")
        return env_dict["dbapi"]
    elif dbapi_argument == "pre":
        env_dict = dotenv_values(".env.pre")
        return env_dict["dbapi"]
    elif dbapi_argument == "testing":
        env_dict = dotenv_values(".env.testing")
        return env_dict["dbapi"]
    else:
        return dbapi_argument


def get_config_dict(context):
    if context not in ["prod", "pre", "testing"]:
        raise UnknownConfigContext
    return dotenv_values(f".env.{context}")
