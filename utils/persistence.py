import pandas as pd
import streamlit as st
from pathlib import Path
import pickle
from config import setup_logging

logger = setup_logging()

# 定义一个缓存目录来存放持久化文件
CACHE_DIR = Path(".cache")
CACHE_DIR.mkdir(exist_ok=True)

class PersistenceManager:
    """
    负责处理数据持久化，将DataFrame保存到本地文件或从本地文件加载。
    """

    @staticmethod
    def save_dataframe(df: pd.DataFrame, filename: str):
        """
        将DataFrame使用pickle格式保存到缓存目录。

        Args:
            df: 需要保存的DataFrame。
            filename: 保存的文件名 (例如 'map_df.pkl')。
        """
        if not isinstance(df, pd.DataFrame):
            logger.error("保存失败：提供的数据不是一个DataFrame。")
            return

        file_path = CACHE_DIR / filename
        try:
            with open(file_path, "wb") as f:
                pickle.dump(df, f)
            logger.info(f"DataFrame已成功保存到 {file_path}")
        except Exception as e:
            logger.error(f"保存DataFrame到 {file_path} 时出错: {e}")
            st.warning(f"无法保存映射表历史记录: {e}")

    @staticmethod
    def load_dataframe(filename: str) -> pd.DataFrame | None:
        """
        从缓存目录加载一个pickle格式的DataFrame。

        Args:
            filename: 要加载的文件名。

        Returns:
            加载的DataFrame，如果文件不存在或加载失败则返回None。
        """
        file_path = CACHE_DIR / filename
        if not file_path.exists():
            return None

        try:
            with open(file_path, "rb") as f:
                df = pickle.load(f)
            logger.info(f"DataFrame已成功从 {file_path} 加载")
            return df
        except Exception as e:
            logger.error(f"从 {file_path} 加载DataFrame时出错: {e}")
            st.warning(f"加载历史映射表失败，文件可能已损坏。请重新上传。")
            return None
