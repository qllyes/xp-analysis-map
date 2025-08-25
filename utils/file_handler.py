import pandas as pd
import io
import tempfile
import os
from config import setup_logging

logger = setup_logging()

class FileProcessor:
    """文件处理类，负责Excel文件的读取"""

    @staticmethod
    def read_excel_safe(file_path_or_buffer, dtype_spec=None) -> pd.DataFrame:
        """
        安全的Excel读取方法。
        新增功能：可以接收一个dtype参数来为特定列指定数据类型。
        """
        # 将传入的文件或缓冲区统一转换为BytesIO对象
        if hasattr(file_path_or_buffer, 'getvalue'):
            buffer = io.BytesIO(file_path_or_buffer.getvalue())
            file_name = getattr(file_path_or_buffer, 'name', 'in-memory-file')
        else:
            with open(file_path_or_buffer, 'rb') as f:
                buffer = io.BytesIO(f.read())
            file_name = str(file_path_or_buffer)

        # 创建一个带.xlsx后缀的临时文件
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as temp_file:
            temp_file_path = temp_file.name
            buffer.seek(0)
            temp_file.write(buffer.read())

        try:
            # 尝试使用不同的引擎读取
            engines = ['openpyxl', 'xlrd']
            for engine in engines:
                try:
                    # 在读取时传入dtype参数
                    return pd.read_excel(temp_file_path, engine=engine, dtype=dtype_spec)
                except Exception as e:
                    logger.warning(f"使用引擎 '{engine}' 读取 '{file_name}' 失败: {e}")
                    continue
            
            # 如果所有引擎都失败了
            raise ValueError(f"无法使用任何可用引擎读取文件 '{file_name}'。请确认文件格式正确且未损坏。")
            
        finally:
            # 确保临时文件被删除
            try:
                os.unlink(temp_file_path)
            except Exception as cleanup_e:
                logger.warning(f"清理临时文件 '{temp_file_path}' 失败: {cleanup_e}")
