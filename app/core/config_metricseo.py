from pydantic import ConfigDict
from pydantic_settings import BaseSettings
from starlette.config import Config

from helper.project_helper import get_project_path, get_folder_project_root_path

env_path = get_project_path() + '/.env_seo'
# env_path = '/Users/tuantmtb/Documents/metric/ereport/git/metric-ereport-backend/.env'
config = Config(env_path)


# print(f'load config from {env_path}')


class Settings(
    BaseSettings
):
    ES_URI_METRIC: str | None = config("ES_URI_METRIC", default=None)
    # ES_URI_METRICSEO: list[str] | None = config("ES_URI_METRICSEO", default=None)
    POSTGRESQL_URI_METRICSEO: str | None = config("POSTGRESQL_URI_METRICSEO", default=None)
    POSTGRESQL_URI_METRIC: str | None = config("POSTGRESQL_URI_METRIC", default=None)
    MARKET_TOOL_URL: str | None = config("MARKET_TOOL_URL", default=None)

    # SQL_URI_METRICSEO: str = config("SQL_URI_METRICSEO", default="")
    # ES_URI_METRICSEO: str = config("ES_URI_METRICSEO", default="")

    model_config = ConfigDict(
        env_nested_delimiter="__",
        env_file=get_folder_project_root_path() + '/.env_seo',
        env_file_encoding="utf-8",
        market_tool_url="https://metric.com",
    )


settings_seo = Settings()
if __name__ == '__main__':
    # print(settings_seo.SQL_URI_METRICSEO)
    pass
