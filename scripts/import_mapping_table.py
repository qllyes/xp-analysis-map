"""
将采购公司和提报战区映射表写入数据库
"""

import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path
import sys

# 将项目根目录添加到Python路径中，以便可以导入config模块
# 这使得脚本无论从哪个位置运行都能找到config.py
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

try:
    # 从配置文件中导入数据库连接信息
    from config import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME
except ImportError:
    print("错误：无法从config.py导入数据库配置。请确保脚本位于'scripts'文件夹下，且config.py在项目根目录。")
    sys.exit(1)

# --- 配置 ---
# 要读取的Excel文件名 (请确保此文件与脚本在同一级或指定正确路径)
EXCEL_FILE_PATH = project_root / "采购公司与提报战区映射表(名称).xlsx"
# 数据库中创建的表名
DATABASE_TABLE_NAME = "purchase_company_warzone_mapping"

def main():
    """
    主函数，执行读取Excel并写入数据库的整个流程。
    """
    print("--- 开始执行Excel数据导入任务 ---")

    # 1. 读取Excel文件
    try:
        print(f"正在读取Excel文件: {EXCEL_FILE_PATH}...")
        if not EXCEL_FILE_PATH.exists():
            print(f"错误：找不到文件 '{EXCEL_FILE_PATH}'。请确保文件名正确且文件存在。")
            return
        
        df = pd.read_excel(EXCEL_FILE_PATH)
        print(f"✅ 文件读取成功，共 {len(df)} 行数据。")
        
    except Exception as e:
        print(f"❌ 读取Excel文件时出错: {e}")
        return

    # 2. 连接数据库并写入数据
    try:
        print("正在连接到MySQL数据库...")
        # 构建数据库连接URL
        db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
        engine = create_engine(db_url)

        print(f"正在将数据写入到数据表 '{DATABASE_TABLE_NAME}'...")
        # 使用pandas的to_sql功能，如果表已存在则替换它
        # index=False表示不将DataFrame的索引写入数据库
        df.to_sql(
            name=DATABASE_TABLE_NAME, 
            con=engine, 
            if_exists='replace', 
            index=False
        )
        print(f"✅ 数据成功写入数据库！")

    except Exception as e:
        print(f"❌ 连接数据库或写入数据时出错: {e}")
        return

    print("--- 任务执行完毕 ---")


if __name__ == "__main__":
    # 确保将'采购公司与提报战区映射表(名称).xlsx'文件放在项目根目录下
    # 然后在终端中，进入'scripts'文件夹，运行命令: python import_mapping_table.py
    main()
