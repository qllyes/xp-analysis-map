import logging
import warnings
from pathlib import Path

# --- 常量定义 (Constants) ---
CONFIG_FILE = Path("config.json")
DB_HOST = "10.243.0.221"
DB_PORT = 3306
DB_USER = "xinpin"
DB_PASSWORD = "xinpin"
DB_NAME = "new_goods_manage"
DEFAULT_SQL_FILE = Path("对标品.sql")
NATIONAL_DIR_SQL_FILE = Path("医保目录.sql")
PURCHASE_CO_MAPPING_FILE = Path("采购公司与提报战区映射表(名称).xlsx") # <-- 新增：采购公司映射文件名

def setup_logging():
    """配置日志记录器"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    return logger

def setup_warnings():
    """配置警告过滤器"""
    warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')
    warnings.filterwarnings('ignore', category=UserWarning, module='pandas.io.sql')

# 在模块加载时执行
setup_warnings()
