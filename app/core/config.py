from enum import Enum

from pydantic import AnyHttpUrl, ConfigDict
from pydantic_settings import BaseSettings
from starlette.config import Config

from helper.project_helper import get_project_path, get_folder_project_root_path

env_path = get_project_path() + '/.env'
# env_path = '/Users/tuantmtb/Documents/metric/ereport/git/metric-ereport-backend/.env'
config = Config(env_path)


class AppSettings(BaseSettings):
    APP_NAME: str = config("APP_NAME", default="E-Report Backend API")
    APP_DESCRIPTION: str | None = config("APP_DESCRIPTION", default=None)
    APP_VERSION: str | None = config("APP_VERSION", default=None)
    LICENSE_NAME: str | None = config("LICENSE", default=None)
    CONTACT_NAME: str | None = config("CONTACT_NAME", default=None)
    CONTACT_EMAIL: str | None = config("CONTACT_EMAIL", default=None)

    API_PATH: str | None = config("API_PATH", default=None)
    BACKEND_CORS_ORIGINS: list[AnyHttpUrl] | list[str] | None = config("BACKEND_CORS_ORIGINS", default=['*'])
    SWAGGER_URL: str | None = config("SWAGGER_URL", default=None)
    REDOC_URL: str | None = config("REDOC_URL", default=None)



class EnvironmentOption(Enum):
    LOCAL = "local"
    STAGING = "staging"
    PRODUCTION = "production"


class EnvironmentSettings(BaseSettings):
    ENVIRONMENT: EnvironmentOption = config("ENVIRONMENT", default="local")
    DEBUG: bool = config("DEBUG", default=False)


class MySQLSettings(BaseSettings):
    MYSQL_USER: str = config("MYSQL_USER", default="username")
    MYSQL_PASSWORD: str = config("MYSQL_PASSWORD", default="password")
    MYSQL_SERVER: str = config("MYSQL_SERVER", default="localhost")
    MYSQL_PORT: int = config("MYSQL_PORT", default=5432)
    MYSQL_DB: str = config("MYSQL_DB", default="dbname")
    MYSQL_URI: str = f"{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_SERVER}:{MYSQL_PORT}/{MYSQL_DB}"
    MYSQL_SYNC_PREFIX: str = config("MYSQL_SYNC_PREFIX", default="mysql://")
    MYSQL_ASYNC_PREFIX: str = config("MYSQL_ASYNC_PREFIX", default="mysql+aiomysql://")
    # MYSQL_URL: str = config("MYSQL_URL", default=None)
    MYSQL_URL: str = f"{MYSQL_ASYNC_PREFIX}{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_SERVER}:{MYSQL_PORT}/{MYSQL_DB}"


class ESSettings(BaseSettings):
    ES_URI_EREPORT: str | None = config("ES_URI_EREPORT", default=None)


# class OAuthSettings(BaseSettings):
#     OAUTH_SECRET_JWT: str = config("OAUTH_SECRET_JWT")
#     OAUTH_MAX_LIFETIME_TOKEN: int = config("OAUTH_MAX_LIFETIME_TOKEN")
#     OAUTH_GOOGLE_CLIENT_ID: str = config("OAUTH_GOOGLE_CLIENT_ID")
#     OAUTH_GOOGLE_CLIENT_SECRET: str = config("OAUTH_GOOGLE_CLIENT_SECRET")
#     OAUTH_REDIRECT_URL: str = ''


class Settings(
    AppSettings,
    EnvironmentSettings,
    # OAuthSettings,
    MySQLSettings,
    ESSettings
):
    model_config = ConfigDict(env_nested_delimiter="__", env_file=get_folder_project_root_path() + '/.env',
                              env_file_encoding="utf-8")


settings = Settings()
# settings = get_settings()
if __name__ == '__main__':
    print(settings.MYSQL_URL)
