import gzip
import shutil
import tarfile
import os
import pandas as pd
import numpy as np
import glob
import re
import shutil
from daily_tools import load_jsonl, save_jsonl
import regex
import logging
import chardet
from pathlib import Path
import logging
import sys
from mmdata import metadata_assemble
import argparse
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Manager
from ctypes import c_char_p
from render import render_formula_fina

# global_log_file = "./logs/log_file.log"  # 失败日志
# global_log_file_v = None  # 失败日志

# spectial_file_log_v = None  # 特殊矢量图文件日志
# output_dir_v =None
# spectial_file_log = "./logs/spectial_file_log.log"  # 特殊矢量图文件日志


# Path(global_log_file).touch()  # 创建全局日志文件
# Path(spectial_file_log).touch()  # 创建特殊矢量图文件日志


def extract_gz_file(file_path, to_path=None):
    if to_path is None:
        extract_path = file_path[:-3]
    with gzip.open(file_path, "rb") as f_in:
        with open(extract_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)


def extract_tar_file(file_path, to_path=None):
    if file_path.endswith(".tar"):
        if to_path is None:
            to_path = file_path[:-4]
        os.makedirs(to_path, exist_ok=True)
        with tarfile.open(file_path) as tar:

            tar.extractall(path=to_path)
        print("File Extracted in Current Directory", file_path)
    else:
        print("Not a tar file: '%s '" % file_path)


def extract_tar_gz(file_path, to_path=None):
    if file_path.endswith(".tar.gz"):
        if to_path is None:
            to_path = file_path[:-7]
        os.makedirs(to_path, exist_ok=True)
        with tarfile.open(file_path, "r:gz") as tar_ref:
            tar_ref.extractall(to_path)
        print("File Extracted in Current Directory", file_path)
    else:
        print("Not a tar.gz file: '%s '" % file_path)


def extract_compress_file(file_path):
    if file_path.endswith(".tar.gz"):
        extract_tar_gz(file_path)
    elif file_path.endswith(".tar"):
        extract_tar_file(file_path)
    elif file_path.endswith(".gz"):
        extract_gz_file(file_path)
    else:
        assert False, "Not a supported file format: '%s'" % file_path


def extract_tex_code(tex_path):
    # with open(file_path, 'rb') as f:
    #     rawdata = f.read()
    # result = chardet.detect(rawdata)
    # charenc = result['encoding']
    total_encodings = []
    try:
        # detected = chardet.detect(Path(tex_path).read_bytes())
        # encoding = detected.get("encoding")

        with open(tex_path, "rb") as f:
            rawdata = f.read()
        result = chardet.detect(rawdata)
        encoding = result["encoding"]

        total_encodings.append(encoding)
    except Exception as e:
        print(tex_path)
        print("Warning: ", e)

    total_encodings.append("cp850")
    for each_coding in total_encodings:
        try:
            with open(tex_path, "r", encoding=each_coding) as f:
                # content = f.read()
                tex_code = [
                    line for line in f.readlines() if not line.strip().startswith("%")
                ]

            return "\n".join(tex_code)
        except Exception as e:
            print(tex_path)
            print("Warning2: ", e)
    assert False, "Can't extract tex code from " + str(tex_path)


def extract_tableandequation_code(text_code):
    tables = []
    if "\\begin{table" in text_code:
        # print("Matching '\\begin{table' Mode")
        # print(len(re.findall(r'\\begin\{table\*?\}(.*?)\\end\{table\*?\}', text_code, re.DOTALL)))
        tables.extend(
            re.findall(
                r"(\\begin\{table\*?\}.*?\\end\{table\*?\})", text_code, re.DOTALL
            )
        )
    if "\\begin{equation" in text_code:
        # print("Matching '\\begin{equation' Mode")
        # print(len(re.findall(r'\\begin\{equation\*?\}.*?\\end\{equation\*?\}', text_code, re.DOTALL)))
        tables.extend(
            re.findall(
                r"(\\begin\{equation\*?\}.*?\\end\{equation\*?\})",
                text_code,
                re.DOTALL,
            )
        )
    if "\\begin{align" in text_code:
        tables.extend(
            re.findall(
                r"(\\begin\{align\*?\}.*?\\end\{align\*?\})",
                text_code,
                re.DOTALL,
            )
        )

    return tables


def extract_tex_codeAndtablesTex(tex_path):
    return extract_tableandequation_code(extract_tex_code(tex_path))


def find_multi_extensions(path, extensions):
    """
    在指定路径下查找具有多个扩展名的文件。

    Args:
        path (str): 要查找文件的目录路径。
        extensions (list): 包含要查找的文件扩展名的列表。

    Returns:
        list: 包含找到的所有文件的完整路径的列表。

    """
    fina_result = []
    for each_extension in extensions:
        fina_result += glob.glob(
            os.path.join(path, "**", each_extension), recursive=True
        )
    return fina_result


def obtain_compressed_dir(path):
    if path.endswith(".tar"):
        return path[:-4]
    elif path.endswith(".tar.gz"):
        return path[:-7]
    else:
        return path[:-3]


def validate_file_extension(file_path):
    """
    验证文件扩展名是否合法。

    Args:
        file_path (str): 要验证的文件路径。

    Returns:
        bool: 如果文件扩展名是 '.gz' 或 '.tar'，则返回 True；否则返回 False。

    """
    valid_extensions = [".gz", ".tar"]
    for ext in valid_extensions:
        if file_path.endswith(ext):
            return True
    return False


def process_a_compressed_file(paramters):
    # try:
    file_path, args = paramters
    # print(paramters)
    # global global_log_file, spectial_file_log, output_dir

    global_log_file = args.global_log_file
    spectial_file_log = args.spectial_file_log
    output_dir = args.output_dir

    print(global_log_file, "global_log_file")
    # 验证文件扩展名是否有效
    entity_id = os.path.basename(file_path)
    assert validate_file_extension(file_path), "Invalid file extension: " + str(
        file_path
    )
    try:
        # 解压缩文件
        extract_compress_file(file_path)
    except Exception as e:
        # 捕获解压缩过程中出现的异常
        exc_type, exc_value, exc_traceback = sys.exc_info()
        # 保存异常信息到日志文件中
        save_jsonl(
            {
                "reason": {
                    "e": str(e),
                    "exc_type": str(exc_type),
                    "exc_value": str(exc_value),
                    "exc_traceback": str(exc_traceback),
                },
                "file": file_path,
            },
            global_log_file,
        )
        # 返回空列表表示处理失败
        return []

    # 如果是.gz文件但不是.tar.gz文件
    if file_path.endswith(".gz") and not file_path.endswith(".tar.gz"):
        # 提取TeX代码
        tex_code = extract_tex_code(file_path[:-3])
        # 判断TeX代码中是否包含\begin{table
        if r"\begin{table" in tex_code:
            # 保存信息到特殊文件日志中
            save_jsonl(
                {
                    "reason": "vector diagram",
                    "file": file_path,
                },
                spectial_file_log,
            )
        else:
            # 保存信息到全局日志文件中
            save_jsonl(
                {
                    "reason": "no table in tex",
                    "file": file_path,
                },
                global_log_file,
            )

    # 获取压缩文件的解压目录
    base_path = obtain_compressed_dir(file_path)

    # 在指定目录中查找特定扩展名的文件
    tex_path = find_multi_extensions(
        base_path, ["*.ltx", "*.tex", "*.TEX", "*.TeX", "*.Tex"]
    )

    # 如果没有找到TeX文件
    if len(tex_path) == 0:
        # 保存信息到全局日志文件中
        save_jsonl(
            {
                "reason": "no tex file found",
                "file": file_path,
            },
            global_log_file,
        )
        # 返回空列表表示处理失败
        return []

    total_table_counts = 0
    total_meatadata = []

    # 遍历每个TeX文件路径
    for each_tex_path in tex_path:
        # 提取TeX代码和图形TeX代码
        table_tex = extract_tex_codeAndtablesTex(each_tex_path)

        # 如果没有提取到图形TeX代码
        if len(table_tex) == 0:
            # 保存信息到全局日志文件中
            save_jsonl(
                {
                    "reason": "no table in tex",
                    "file": file_path,
                    "tex_file": each_tex_path,
                },
                global_log_file,
            )
            # 继续处理下一个TeX文件
            continue

        # 遍历每个图形TeX代码
        for each_table_tex in table_tex:
            # 组装包含说明文字的元数据
            image_path = f"{output_dir}/" + str(entity_id)+"_" + str(total_table_counts) + ".png"
            try:
                render_formula_fina(
                    "$$" + each_table_tex + "$$", 
                    image_path
                )
            except Exception as e:
                save_jsonl(
                {
                    "reason": "render image failed",
                    "image_path": image_path,
                    "table_tex": each_table_tex
                    "exception": str(e),
                },
                global_log_file,
            )

            if os.path.exists(image_path):
                mete_data = metadata_assemble(
                    entity_id=entity_id,
                    block_id=total_table_counts,
                    image_path=image_path
                    image_data_meta={
                        "file_type": "table/equation",
                        "tex_code":each_table_tex
                    },
                )
                os.remove(image_path)
            else:
                mete_data = metadata_assemble(
                    entity_id=entity_id,
                    block_id=total_table_counts,
                    image_data=each_table_tex,
                    image_data_meta={
                        "file_type": "table/equation",
                        "tex_code":each_table_tex
                    },
                )
            # 将元数据添加到总元数据中
            total_meatadata.append(mete_data)
            # 图形计数加一
            total_table_counts += 1
    # 返回总元数据
    if len(total_meatadata):

        save_path = f"{output_dir}/" + str(entity_id) + ".parquet"

        print("saveing ...", save_path)
        try:
            pd.DataFrame(
                [block.to_pydict() for block in total_meatadata]
            ).to_parquet(save_path, index=False)
        except Exception as e:
            save_jsonl(
                {
                    "reason": "save image pair failed",
                    "save_path": save_path,
                    "exception": str(e),
                },
                global_log_file,
            )

    return total_meatadata
    


def main():
    parser = argparse.ArgumentParser(description="Docling Convert")
    parser.add_argument(
        "--input_file", "-i", type=str, default="list.txt", help="Input file"
    )
    parser.add_argument(
        "--workers_num", "-w", type=int, default=2, help="multi process workers num"
    )
    parser.add_argument(
        "--output_dir",
        "-o",
        type=str,
        default=r"data_output/data-table-parquet",
        help="Output directory",
    )
    parser.add_argument("--log_dir", "-l", type=str, default="logs", help="Log path")
    args = parser.parse_args()

    current_date = datetime.now().strftime("%Y-%m-%d")

    input_file = args.input_file
    workers_num = args.workers_num
    # log_dir = Path(args.log_dir)

    # global global_log_file, spectial_file_log, output_dir # 失败日志
    os.makedirs(args.log_dir, exist_ok=True)
    global_log_file = os.path.join(args.log_dir, "log_file_table.log")
    spectial_file_log = os.path.join(args.log_dir, "spectial_file_log_table.log")

    args.global_log_file = global_log_file
    args.spectial_file_log = spectial_file_log



    output_dir = args.output_dir

    Path(global_log_file).touch()  # 创建全局日志文件
    Path(spectial_file_log).touch()  # 创建特殊矢量图文件日志
    os.makedirs(output_dir, exist_ok=True)

    # txt 文件为在数据路径下生成的 list 文件
    # ex: find . -name "*.pdf" > list.txt
    if input_file.endswith(".txt"):
        input_file_list = Path(input_file).read_text().splitlines()
        input_file_path_list = [
            os.path.join(os.path.dirname(input_file), file_path)
            for file_path in input_file_list
        ]
        print(input_file_path_list)
        with ProcessPoolExecutor(max_workers=workers_num) as executor:
            executor.map(
                process_a_compressed_file,
                [[file_path, args] for file_path in input_file_path_list],
            )
    else:
        process_a_compressed_file([input_file, args])


if __name__ == "__main__":
    main()
