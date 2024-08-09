import shutil

from helper.reader_helper import get_sub_folders_in_folder


def remove_folder_outdate(folder_output_include_versions, max_version_should_keep=2):
    # if not os.path.isdir(folder_output_include_versions):
    # if not os.path.exists(folder_output_include_versions):
    #     return
    sub_folders = get_sub_folders_in_folder(folder_path=folder_output_include_versions)
    sub_folders = [sub_folder for sub_folder in sub_folders if not sub_folder.endswith('temp/') and '_' in sub_folder and 'temp' not in sub_folder]
    # print(sub_folders)
    for file_name in sorted(sub_folders)[:-max_version_should_keep]:
        print('prepare delete ' + file_name)
        try:
            # todo: uncomment
            # pass
            # print('prepare remove', file_name)
            shutil.rmtree(file_name, ignore_errors=True)
        except Exception as e:
            print(e)


def get_newest_version(folder_output_include_versions):
    sub_folders = get_sub_folders_in_folder(folder_output_include_versions)
    if sub_folders is None:
        return None
    sub_folders = [sub_folder for sub_folder in sub_folders if not sub_folder.endswith('temp/')]
    sub_folders.sort()
    # print(sub_folders)
    if len(sub_folders) > 0:
        return sub_folders[-1]
    else:
        return None


def get_version_folders(folder_output_include_versions):
    sub_folders = get_sub_folders_in_folder(folder_output_include_versions)
    if sub_folders is None:
        return []
    sub_folders = [sub_folder for sub_folder in sub_folders if not sub_folder.endswith('temp/')]
    sub_folders.sort()
    if len(sub_folders) > 0:
        return sub_folders[::-1]
    else:
        return []

# if __name__ == '__main__':
#     get_newest_version('/bee/data/crawler/shopee.vn/reinput')
