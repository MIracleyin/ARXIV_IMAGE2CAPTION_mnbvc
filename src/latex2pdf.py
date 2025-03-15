import os
import subprocess
from pdf2image import convert_from_path
import time
# current_file_dir = os.path.dirname(os.path.abspath(__file__))
import shutil
current_file_dir = './'
from daily_tools import load_jsonl

def latex_to_image(latex_code, output_image_path, dpi=300, tmp_res_dir=None):
    if tmp_res_dir is None:
        tmp_res_dir = os.path.join(current_file_dir, 'tmp_' + str(time.time()))
        # 这里需要考虑当多线程的时候多个进程在同一个时间戳创建了文件，然后导致的文件冲突问题
        # while os.path.exists(tmp_res_dir):

    tmp_res_dir = './'
    # os.makedirs(tmp_res_dir, exist_ok=True)
    """
    将 LaTeX 公式渲染为图片
    :param latex_code: 完整的 LaTeX 公式代码
    :param output_image_path: 输出图片路径（支持 PNG 或 PDF）
    :param dpi: 图片分辨率（仅对 PNG 有效）
    """
    # 创建临时 LaTeX 文件
    temp_tex = os.path.join(tmp_res_dir, "temp.tex")
    with open(temp_tex, "w") as f:
        f.write(
            r"""
\documentclass[preview]{standalone}
\usepackage{amsmath}
\usepackage{amssymb}
\usepackage{algorithm}
\usepackage{algpseudocode}
\usepackage{booktabs}
\usepackage{array}
\usepackage{graphicx}
\usepackage{multirow}
\usepackage{adjustbox}
\usepackage{caption}
\usepackage{float}
\usepackage{hyperref}
\begin{document}
""" + latex_code + r"""
\end{document}
"""
        )

    try:
        try:
        # 使用 pdflatex 编译 LaTeX 文件为 PDF
            subprocess.run(["pdflatex", "-interaction=nonstopmode", temp_tex, f'-output-directory="{tmp_res_dir}"'], check=True)
        except Exception as e:
            print(f"Error happdened during pdflatex compilation with e: {e}", )
            # 这里抛出异常了不一定代表pdf没有渲染成功，可能只是因为没有正常返回状态码

        # 如果输出路径是 PDF，直接重命名
        pdf_path = temp_tex.replace(".tex", ".pdf")
        assert os.path.exists(pdf_path), "convert latex2 pdf failded, no pdf generated"


        corp_pdf_path = pdf_path.replace(".pdf", "-cropped.pdf")
        # cropped_pdf = os.path.join(tmp_res_dir, "temp-cropped.pdf")
        subprocess.run(["pdfcrop", pdf_path, corp_pdf_path], check=True)

        
        if output_image_path.lower().endswith(".pdf"):
            os.rename(corp_pdf_path, output_image_path)

        else:
            # 使用 pdf2image 将 PDF 转换为 PNG
            images = convert_from_path(corp_pdf_path, dpi=dpi)
            if images:
                images[0].save(output_image_path)
            else:
                raise ValueError("Failed to convert PDF to image.")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        # 清理临时文件
        for ext in [".tex", ".pdf", ".aux", ".log", '.out']:
            if os.path.exists(f"temp{ext}"):
                os.remove(f"temp{ext}")
        
if __name__ == "__main__":

    # 示例：渲染公式、算法和表格
    latex_code = r"""
% 公式
\begin{equation*}
E = mc^2
\end{equation*}

% 算法
\begin{algorithm}[H]
\caption{示例算法}
\begin{algorithmic}[1]
\State 初始化变量 $x = 0$
\While{$x < 10$}
    \State $x = x + 1$
\EndWhile
\end{algorithmic}
\end{algorithm}

% 表格
\begin{table}[H]
\caption{示例表格}
\centering
\begin{tabular}{ccc}
\toprule
列1 & 列2 & 列3 \\
\midrule
1 & 2 & 3 \\
4 & 5 & 6 \\
\bottomrule
\end{tabular}
\end{table}
"""

    latex_code = r"""
% 表格
\begin{table}[H]
\caption{示例表格}
\centering
\begin{tabular}{ccc}
\toprule
列1 & 列2 & 列3 \\
\midrule
1 & 2 & 3 \\
4 & 5 & 6 \\
\bottomrule
\end{tabular}
\end{table}
"""

    # latex_code = data[3]['table/equation']

    latex_to_image(latex_code, "output.png", dpi=300)
    print("LaTeX content rendered and saved as output.png")

    for idx, each_data in enumerate(load_jsonl('/Users/leiyoubo/Coding/baidu/Codes/MNBVC_ARXIV_IMAGE2CAPTION/total_json.jsonl')):
        latex_to_image(each_data['table/equation'], f"/Users/leiyoubo/Coding/baidu/Codes/MNBVC_ARXIV_IMAGE2CAPTION/data_output/table_img_example/{idx}_output.png", dpi=300)
        print("LaTeX content rendered and saved as output.png")
