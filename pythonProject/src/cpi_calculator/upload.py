import os
from pathlib import Path
from config import settings
import oss2

# 替换为你自己的 AccessKey 和 Endpoint 信息
auth = oss2.Auth(settings.ACCESS_KEY, settings.ACCESS_KEY_SECRET)
bucket = oss2.Bucket(auth, settings.OSS.ENDPOINT, settings.OSS.BUCKET)

# 本地文件路径
base_path = Path(__file__).resolve().parent.parent.parent / 'data' / 'data'
filelist = [ 'products.csv', 'categories.csv', 'price.csv']  # 需要上传的文件列表
for file in filelist:
    local_file = os.path.join(base_path, file)  # 本地文件路径
    oss_key = f'{file}'  # OSS 中的路径
    # 上传文件
    bucket.put_object_from_file(oss_key, local_file)
    print(f"已上传至 OSS：oss://price-index-demo/{oss_key}")
