# step1 处理不带后缀的文件
# 由于通过爬虫爬取下来的文件不带有后缀名，所以需要预处理为带有后缀名的文件，具体做法为检测文件类型，然后将其复制到另一份文件夹，同时将
# 压缩文件路径写入到success_text_file_list.txt 中待处理，失败文件写入 failed_text_file_list.txt
mkdir resource_tmp

# --dir下载目录 download_dir
python3 src/detect_file_type.py --dir ./data 

# step2 处理图文对数据
python3 src/arxiv_main.py --input_file resource_tmp/image2caption_processed_file.txt 


# step3 处理表格和公式数据
python3 src/arxiv_main_tableandequation.py --input_file resource_tmp/tableequation_processed_file.txt 

# 代码效果，会将多种图片路径和写法提取出来、校验图片路径，读取图片，转为二进制写入parquet，一个arxiv一个parquet

# step4 合并parquet文件
python3 src/concat_parquet.py --input_dir data_output/data-image-parquet --output_dir data_output/data-image-total
python3 src/concat_parquet.py --input_dir data_output/data-table-parquet --output_dir data_output/data-table-total

# rm -rf data_output/data-image-parquet
# rm -rf data_output/data-table-parquet
# rm -rf resource_tmp