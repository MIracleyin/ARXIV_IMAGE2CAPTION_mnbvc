import pandas as pd
import numpy as np
import os
import glob
import time
import json
from functools import wraps
import threading
from copy import deepcopy


def lock_wraps(func):
    """
    该装饰器用于将一个函数加上线程锁，保证函数在多线程环境下只被一个线程执行。

    Args:
        func: 需要被装饰的函数。

    Returns:
        被线程锁包装后的函数。

    """
    lock = threading.Lock()

    @wraps(func)
    def wrapper(*args, **kwargs):
        with lock:
            return func(*args, **kwargs)

    return wrapper


def auto_remove_file(path):
    if os.path.exists(path):
        os.remove(path)


@lock_wraps
def save_jsonl(content, file_path, new=False, print_log=True):
    if print_log:
        # print(f"save to {file_path}")
        print(content)
    try:
        with open(file_path, "a+" if not new else "w") as outfile:
            # for entry in content:
            # json.dump(entry, outfile, ensure_ascii=False)
            outfile.write(json.dumps(content, ensure_ascii=False))
            outfile.write("\n")
    except Exception as e:
        print(e)
        print("Error: error when saving jsonl!!!!")


def load_jsonl(data_path):
    if os.path.exists(data_path):
        with open(data_path, "r", encoding="utf-8") as f:
            lines = f.readlines()

        jsonl_list = []
        for line in lines:
            try:
                jsonl_list.append(json.loads(line))
            except:
                print(f"{line} is not a valid json string")
        # jsonl_list = [json.loads(line) for line in lines]
    else:
        print(f"{data_path} not exists!")
        jsonl_list = []
    return jsonl_list
