import os
import magic
import shutil
import tarfile
import gzip
import argparse
import glob
from daily_tools import save_jsonl, load_jsonl

def detect_file_type(file_path):
    # 使用magic库检测文件类型
    mime = magic.Magic(mime=True)
    file_type = mime.from_file(file_path)
    
    # 根据MIME类型返回文件扩展名
    if file_type == 'application/x-gzip' or file_type == 'application/gzip':
        # 进一步检查是否是 tar.gz 文件
        try:
            with gzip.open(file_path, 'rb') as f:
                # 尝试读取 tar 文件头
                tarfile.open(fileobj=f)
            return '.tar.gz'
        except (tarfile.TarError, gzip.BadGzipFile, OSError):
            return '.gz'
    elif file_type == 'application/x-tar':
        return '.tar'
    elif file_type == 'application/zip':
        return '.zip'
    else:
        return None


def save_with_append(file_path, content):
    with open(file_path, 'a+', encoding='utf-8') as f:
        f.write(content)



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", type=str, required=True, help="Directory containing files to be renamed.")
    parser.add_argument("--save_text", type=str, default="./success_text_file_list.txt", help="Save the list of renamed files to a text file.")
    parser.add_argument("--failed_text", type=str, default="./failed_text_file_list.txt", help="Save the list of failed files to a text file.")
    args = parser.parse_args()

    for file_path in glob.glob(os.path.join(args.dir, "**", "source", "*"), recursive=True):
        if os.path.isfile(file_path):
            try:
                extension = detect_file_type(file_path)
                if extension:
                    new_file_path = file_path + extension

                    new_file_path = new_file_path.replace('/source/', '/source_extentions/')
                    os.makedirs(os.path.dirname(new_file_path), exist_ok=True)
                    # rename_file_with_extension(file_path, extension)
                    # 重命名文件
                    shutil.copyfile(file_path, new_file_path) # 注意这里是备份了一份源文件，但是如果不需要则可以改为重命名源文件
                    print(f"Copy {file_path} to {new_file_path}")
                    save_with_append(args.save_text, new_file_path+'\n')

                    # shutil.move(file_path, new_file_path) 
                    # print(f"Move {file_path} to {new_file_path}")
                else:
                    print(f"Cannot determine the type of {file_path}")
                    save_jsonl( [{"file_path": file_path, "error": "Unknown file type"}], args.failed_text)
            except Exception as e:
                print(f"Failed to process {file_path}: {e}")
                save_jsonl([{"file_path": file_path, "error": str(e)}], args.failed_text)


        # detected_ext = detect_file_type(filename)
        # if detected_ext is not None:
        #     rename_file_with_extension(filename, detected_ext
