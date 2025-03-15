# step1 处理不带后缀的文件
# 由于通过爬虫爬取下来的文件不带有后缀名，所以需要预处理为带有后缀名的文件，具体做法为检测文件类型，然后将其复制到另一份文件夹，同时将
# 压缩文件路径写入到success_text_file_list.txt 中待处理，失败文件写入 failed_text_file_list.txt

python src/detect_file_type.py --dir xxx 下载目录

# step2 处理后缀文件
python src/arxiv_main.py --input_file success_text_file_list.txt # 批量处理file.txt
# 代码效果，会将多种图片路径和写法提取出来、校验图片路径，读取图片，转为二进制写入parquet，一个arxiv一个parquet