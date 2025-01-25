import argparse
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
import pandas as pd
import os
from daily_tools import load_jsonl, save_jsonl


def concat_data(paramters):
    try:
        file_paths, global_log_file, save_path = paramters

        print(
            "global_log_file",
            global_log_file,
        )
        print(
            "save_path",
            save_path,
        )

        total_parquet = []
        for each_path in file_paths:
            try:
                df = pd.read_parquet(each_path)
                total_parquet.append(df)
            except Exception as e:
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

        df_all = pd.concat(total_parquet)
        df_all.to_parquet(save_path, index=False)
    except Exception as e:
        print(e)


def main():
    parser = argparse.ArgumentParser(description="Docling Convert")
    parser.add_argument(
        "--input_file", "-i", type=str, default="list_parquet.txt", help="Input file"
    )
    parser.add_argument(
        "--workers_num", "-w", type=int, default=2, help="multi process workers num"
    )
    parser.add_argument(
        "--output_dir",
        "-o",
        type=str,
        default=r"data_output/data-table-total",
        help="Output directory",
    )
    parser.add_argument("--log_dir", "-l", type=str, default="logs", help="Log path")
    parser.add_argument(
        "--num_of_file", "-n", type=int, default=10000, help="concat num"
    )
    args = parser.parse_args()

    current_date = datetime.now().strftime("%Y-%m-%d")

    input_file = args.input_file
    workers_num = args.workers_num
    # log_dir = Path(args.log_dir)

    # global global_log_file, output_dir # 失败日志
    os.makedirs(args.log_dir, exist_ok=True)
    global_log_file = os.path.join(args.log_dir, "log_file_concat.log")
    output_dir = args.output_dir

    Path(global_log_file).touch()  # 创建全局日志文件
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
        # with ProcessPoolExecutor(max_workers=workers_num) as executor:
        #     process_file = []
        #     for start in range(0, len(input_file_path_list), args.num_of_file):
        #         save_path = os.path.join(
        #             output_dir, f"{start}_{start + args.num_of_file}.parquet"
        #         )
        #         process_file.append(
        #             [
        #                 input_file_path_list[start : start + args.num_of_file],
        #                 global_log_file,
        #                 save_path,
        #             ]
        #         )

        #     executor.map(
        #         concat_data,
        #         process_file,
        #     )

        # with ProcessPoolExecutor(max_workers=workers_num) as executor:
        for start in range(0, len(input_file_path_list), args.num_of_file):
            save_path = os.path.join(
                output_dir, f"{start}_{start + args.num_of_file}.parquet"
            )
            concat_data([
                    input_file_path_list[start : start + args.num_of_file],
                    global_log_file,
                    save_path,
                ])
    else:
        assert False, "Input file must be a text file"


if __name__ == "__main__":
    main()
