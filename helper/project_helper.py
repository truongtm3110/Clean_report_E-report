from pathlib import Path
import os


def get_project_path():
    project_path = os.environ['PYTHONPATH'].split(os.pathsep)[0]
    return project_path


def get_folder_project_root_path():
    CWF = Path(__file__)
    HOME_PROJECT_PATH = str(CWF.parent.parent)
    return HOME_PROJECT_PATH


def get_env_file_path():
    return os.path.join(get_folder_project_root_path(), ".env")


if __name__ == '__main__':
    print(get_project_path())
