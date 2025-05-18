import csv
import re
from collections import defaultdict


def parse_category_hierarchy(doc_content):
    """
    解析文档内容生成分类层级结构
    返回格式：[{
        "code": "1100000000",
        "name": "居民消费价格总指数",
        "unit": "",
        "required": 0,
        "parent_code": None
    }, ...]
    """
    categories = []
    current_parent_stack = [None] * 5  # 支持最多5级分类
    code_pattern = re.compile(r'(\d{8,10})')

    for line in doc_content.split('\n'):
        if not line.strip() or '续表' in line:
            continue

        # 提取代码和名称
        code_match = code_pattern.search(line)
        if not code_match:
            continue

        code = code_match.group(1).ljust(10, '0')  # 统一补足10位代码
        parts = re.split(r'\s{2,}', line.strip())  # 按双空格切分列

        # 解析名称和规格要求
        name_part = parts[0].strip()
        name = re.sub(r'\(\d+\)', '', name_part).strip()
        required = int(re.search(r'\((\d+)\)', name_part).group(1)) if re.search(r'\(\d+\)', name_part) else 0

        # 解析计量单位
        unit = parts[1].strip() if len(parts) > 1 else ""
        unit = unit.replace('　', '').strip()

        # 确定层级和父级代码
        level = determine_level(code)
        parent_code = current_parent_stack[level - 1] if level > 1 else None

        # 更新父级栈
        current_parent_stack[level] = code
        for i in range(level + 1, len(current_parent_stack)):
            current_parent_stack[i] = None

        categories.append({
            "code": code,
            "name": name,
            "unit": unit,
            "required": required,
            "parent_code": parent_code,
            "level": level
        })

    return categories


def determine_level(code):
    """根据代码确定分类层级"""
    if code.endswith('00000000'):
        return 1
    elif code.endswith('000000'):
        return 2
    elif code.endswith('0000'):
        return 3
    elif code.endswith('00'):
        return 4
    else:
        return 5


def generate_category_csv(categories, filename):
    """生成符合国家标准的分类CSV"""
    with open(filename, 'w', newline='', encoding='gbk') as f:  # 使用GBK编码
        writer = csv.writer(f)
        # 写入表头
        writer.writerow([
            "分类代码",
            "分类名称",
            "计量单位",
            "必需规格数",
            "层级",
            "父级代码"
        ])

        for cat in categories:
            writer.writerow([
                cat["code"],
                cat["name"],
                cat["unit"],
                cat["required"],
                cat["level"],
                cat["parent_code"] or ""
            ])


# 示例文档内容解析（实际应读取docx文件）
doc_content = "raw.docx"

# 解析分类体系
categories = parse_category_hierarchy(doc_content)

# 生成CSV文件
generate_category_csv(categories, "国家统计分类标准.csv")

# 生成ClickHouse建表语句
print(f"""
CREATE TABLE national_category_stats
(
    `分类代码` String,
    `分类名称` String,
    `计量单位` String,
    `必需规格数` UInt8,
    `层级` UInt8,
    `父级代码` Nullable(String)
ENGINE = S3(
    'https://your-oss-endpoint/国家统计分类标准.csv',
    'ACCESS_KEY',
    'SECRET_KEY',
    'CSVWithNames'
);
""")