from functools import lru_cache

from app.core import config
from helper.error_helper import log_error


@lru_cache()
def get_settings():
    setting = None
    try:
        setting = config.Settings()
    except Exception as e:
        log_error(e)
        pass
    if setting is None:
        try:
            setting = config.Settings(_env_file=get_project_path() + '/.env')
        except:
            pass
    return setting


def get_value_setting(key: str, default=None, type='default'):
    value = None
    if type == 'default':
        settings = get_settings()
        if settings is not None:
            value = settings.dict(exclude_none=True).get(key)
    return value or default


if __name__ == '__main__':
    from dotenv import load_dotenv
    from helper.project_helper import get_project_path

    load_dotenv(get_project_path() + '/.env')
    print(get_value_setting(key='PROJECT_NAME'))
