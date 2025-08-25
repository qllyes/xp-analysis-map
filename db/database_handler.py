import pandas as pd
import pymysql
from sqlalchemy import create_engine
from pathlib import Path
import re
import streamlit as st
from typing import List, Tuple
from config import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, DEFAULT_SQL_FILE, setup_logging

logger = setup_logging()

class SQLProcessor:
    """SQL处理器类，负责执行SQL查询"""

    def __init__(self):
        """初始化数据库连接信息"""
        self.db_url = (
            f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
        )
        self.engine = create_engine(self.db_url)

    @staticmethod
    def read_sql_file(file_path: Path) -> str:
        """读取SQL文件内容"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return f.read()
        except Exception as e:
            logger.error(f"读取SQL文件失败: {e}")
            raise ValueError(f"读取SQL文件失败: {str(e)}")

    def execute_simple_query(self, sql_query: str) -> Tuple[pd.DataFrame, str]:
        """
        执行一个简单的SQL查询，不进行任何参数化处理。
        返回：一个包含DataFrame和SQL语句的元组。
        """
        try:
            with self.engine.connect() as connection:
                df = pd.read_sql(sql_query, connection)
            return df, sql_query
        except Exception as e:
            logger.error(f"执行简单SQL查询失败: {e}")
            st.error(f"数据库查询失败: {str(e)}")
            return pd.DataFrame(), sql_query

    def execute_sql_query(self, sql_query: str, cgms: str = None, common_names: List[str] = None, strategy_categories: List[str] = None, lev3_org_name: List[str] = None) -> Tuple[pd.DataFrame, str]:
        """
        执行带有动态筛选条件的复杂SQL查询。
        返回：一个包含DataFrame和最终执行的SQL语句的元组。
        """
        final_sql = sql_query
        
        # 构建通用名和策略分类的筛选
        filter_conditions = []
        if common_names:
            names_str = "', '".join(name.replace("'", "''") for name in common_names)
            filter_conditions.append(f"goods_common_name IN ('{names_str}')")
        if strategy_categories:
            categories_str = "', '".join(cat.replace("'", "''") for cat in strategy_categories)
            filter_conditions.append(f"strategy_classify_name IN ('{categories_str}')")
        if filter_conditions:
            filter_clause = " OR ".join(filter_conditions)
            final_sql += f" AND ({filter_clause})"
        
        # 构建采购模式和战区的筛选
        filter_conditions01 = ""
        if cgms:
            if cgms == '统采':
                filter_conditions01 = "lev3_org_name ='集团'"
            # 增加对lev3_org_name是否存在的检查，修复bug
            elif lev3_org_name:
                names_str = "', '".join(name.replace("'", "''") for name in lev3_org_name)
                filter_conditions01 = f"lev3_org_name ='集团' OR lev3_org_name IN ('{names_str}')"
        
        if filter_conditions01:
            final_sql += f" AND ({filter_conditions01})"

        logger.info("已将动态筛选条件应用到SQL查询中。")

        # 执行查询
        try:
            with self.engine.connect() as connection:
                df = pd.read_sql(final_sql, connection)
            return df, final_sql
        except Exception as e:
            logger.error(f"执行SQL查询失败: {e}")
            st.error(f"数据库查询失败: {str(e)}")
            return pd.DataFrame(), final_sql
