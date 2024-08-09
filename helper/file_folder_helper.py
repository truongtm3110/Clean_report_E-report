import os
import shutil


def get_size_in_folder(folder_path):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(folder_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            # skip if it is symbolic link
            if not os.path.islink(fp):
                total_size += os.path.getsize(fp)
    return total_size


def count_files_in_folder(folder_path):
    # return len([name for name in os.listdir(folder_path) if os.path.isfile(name)])
    return len([f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))])


def statistic_folder_output(folder_path_output):
    from hurry.filesize import size
    total_size = get_size_in_folder(folder_path_output)
    count_file = count_files_in_folder(folder_path_output)
    avg_size_per_file = total_size / count_file if count_file > 0 else 0
    return {'folder_statistic': folder_path_output, 'total_size': size(total_size), 'count_file': count_file,
            'avg_size_per_file': size(avg_size_per_file)}


def move_and_create_folder(source_folder_path, destination_folder_path):
    try:
        # if not os.path.isdir(destination_folder_path):
        #     os.makedirs(destination_folder_path)
        shutil.move(source_folder_path, destination_folder_path)
    except Exception as e:
        print(e)


def copy_folder(source_folder_path, destination_folder_path, symlinks=False, ignore=None):
    if not os.path.isdir(destination_folder_path):
        os.makedirs(destination_folder_path)
    for item in os.listdir(source_folder_path):
        s = os.path.join(source_folder_path, item)
        d = os.path.join(destination_folder_path, item)
        if os.path.isdir(s):
            shutil.copytree(s, d, symlinks, ignore)
        else:
            shutil.copy2(s, d)

# if __name__ == '__main__':
# move_and_create_folder('/home/tuantmtb/Documents/data/bee_rnd/language_model/word_embedding/fasttext',
#                        '/home/tuantmtb/Documents/data/bee_rnd/language_model/word_embedding/fasttext2')
# copy_folder('/home/tuantmtb/Documents/data/tima/test/verify/output/group/20181202_121212',
#             '/home/tuantmtb/Documents/data/tima/test/verify/output/group/20181202_121212_new')
#
