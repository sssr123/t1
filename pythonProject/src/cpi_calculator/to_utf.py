import os
from pathlib import Path

data_path = Path(__file__).resolve().parent.parent / 'data' / 'data_test'
filelist = ['products.csv']  # 需要上传的文件列表
for file in filelist:
    src_path = os.path.join(data_path, file)
    dest_path = os.path.join(data_path, f'utf8_{file}')

    with open(src_path, 'r', encoding='gbk') as f_in, open(dest_path, 'w', encoding='utf-8-sig') as f_out:
        for line in f_in:
            f_out.write(line)

# 然后将 output_path 上传到 OSS
