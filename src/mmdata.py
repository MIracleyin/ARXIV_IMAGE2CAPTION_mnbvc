import pandas as pd
from typing import Dict, Optional
import json
from datetime import datetime
from Cryptodome.Hash import SHA1
import os


def calculate_sha1(bstr):
    # 该函数需要传入一个 b 字符串
    hash = SHA1.new(bstr).hexdigest()
    return hash


from typing import Dict


class mmblock:
    def __init__(self, **kwargs) -> None:
        self.entity_id = kwargs.get("entity_id")
        self.block_id = kwargs.get("block_id")
        self.timestamp = kwargs.get("timestamp")
        self.metadata = kwargs.get("metadata")
        self.text = kwargs.get("text")
        self.image = kwargs.get("image")
        self.ocr_text = kwargs.get("ocr_text")
        self.audio = kwargs.get("audio")
        self.stt_text = kwargs.get("stt_text")
        # self.other_block = kwargs.get("other_block") # ]
        self.block_type = kwargs.get("block_type")
        self.file_md5 = kwargs.get("file_md5")
        self.page_id = kwargs.get("page_id")

    def to_pydict(self) -> Dict:
        return {
            "实体ID": self.entity_id,
            "块ID": self.block_id,
            "时间": self.timestamp,
            "扩展字段": self.metadata,
            "文本": self.text,
            "图片": self.image,
            "OCR文本": self.ocr_text,
            "音频": self.audio,
            "STT文本": self.stt_text,
            "块类型": self.block_type,
            "文件md5": self.file_md5,
            "页ID": self.page_id,
        }


def read_file(file_path):
    # 打开文件
    with open(file_path, "rb") as file:
        # 读取文件，并返回二进制数据
        return file.read()


def write_file(file_path, data):
    # 打开文件
    with open(file_path, "wb") as file:
        # 将二进制数据写入文件
        file.write(data)


def metadata_assemble(
    entity_id,
    block_id,
    text=None,
    image_path=None,
    image_data=None,
    image_data_meta=None,
):
    """
    将实体ID、块ID、文本和图像路径等信息组装成元数据块。

    Args:
        entity_id (str): 实体ID。
        block_id (str): 块ID。
        text (str, optional): 文本内容。默认为None。
        image_path (str, optional): 图像文件路径。默认为None。

    Returns:
        dict: 组装后的元数据块。

    Raises:
        AssertionError: 如果未提供文本或图像路径，则抛出断言错误。

    """
    assert (
        text or image_path or (image_data and image_data_meta)
    ), "text or image_path is required"

    timestamp = datetime.now()
    formatted_time = timestamp.strftime("%Y%m%d")
    block_type = "图片" if image_path else "文本"

    if image_path or (image_data and image_data_meta):
        if image_path:
            image = read_file(image_path)
            meta_dict = {
                "file_name": os.path.basename(image_path),
                "text_length": "0",
            }
            if image_data_meta:
                meta_dict.update(image_data_meta)
        else:
            image = image_data
            meta_dict = image_data_meta
    else:
        image = None
        meta_dict = {"file_name": "null", "text_length": len(text)}

    return mmblock(
        entity_id=entity_id,
        block_id=block_id,
        timestamp=formatted_time,
        metadata=json.dumps(meta_dict, ensure_ascii=False),
        image=image,
        text=text,
        block_type=block_type,
    )


if __name__ == "__main__":
    print("test")
    print(metadata_assemble(entity_id="1", block_id="1", text="test").to_pydict())
