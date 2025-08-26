from abc import ABC, abstractmethod
import pandas as pd
from pathlib import Path

# 导入项目中的其他模块
from db.database_handler import SQLProcessor
from processing.data_merger import DataMerger
from config import NATIONAL_DIR_SQL_FILE, PURCHASE_CO_MAPPING_FILE

class PurchaseStrategy(ABC):
    """处理策略的抽象基类，定义了所有策略必须实现的接口。"""
    
    def __init__(self, scm_df: pd.DataFrame):
        self.scm_df = scm_df
        self.sql_processor = SQLProcessor()
        self.data_merger = DataMerger()

    @abstractmethod
    def enrich_scm_data(self) -> pd.DataFrame:
        """定义如何丰富SCM数据。"""
        pass

    @abstractmethod
    def get_benchmark_query_params(self) -> dict:
        """定义如何为对标品查询生成参数。"""
        pass

    @abstractmethod
    def get_excel_export_params(self) -> dict:
        """定义导出Excel时需要的特定参数（如公式）。"""
        pass

class TongCaiStrategy(PurchaseStrategy):
    """统采模式的处理策略。"""
    
    def enrich_scm_data(self) -> pd.DataFrame:
        """统采模式只关联医保目录。"""
        current_df = self.scm_df.copy()
        if NATIONAL_DIR_SQL_FILE.exists():
            sql_query = self.sql_processor.read_sql_file(NATIONAL_DIR_SQL_FILE)
            national_dir_df, _ = self.sql_processor.execute_simple_query(sql_query)
            if not national_dir_df.empty and '国家药品编码' in current_df.columns:
                current_df['国家药品编码'] = current_df['国家药品编码'].astype(str)
                national_dir_df['国家药品编码'] = national_dir_df['国家药品编码'].astype(str)
                cols_to_replace = ['国家医保目录', '省医保目录', '省医保支付价']
                df_cleaned = current_df.drop(columns=[c for c in cols_to_replace if c in current_df.columns])
                current_df = pd.merge(df_cleaned, national_dir_df, on='国家药品编码', how='left')
        return current_df

    def get_benchmark_query_params(self) -> dict:
        """统采模式下，对标品只筛选集团数据。"""
        return {
            "cgms": "统采",
            "common_names": self.scm_df['通用名'].dropna().unique().tolist(),
            "strategy_categories": self.scm_df['策略分类'].dropna().unique().tolist(),
            "lev3_org_name": None
        }

    def get_excel_export_params(self) -> dict:
        """统采模式下的Excel公式和合并规则。"""
        return {
            "purchase_mode": "统采"
            # 未来可以将具体的公式字符串也定义在这里
        }

class DiCaiStrategy(PurchaseStrategy):
    """地采（非统采）模式的处理策略。"""

    def enrich_scm_data(self) -> pd.DataFrame:
        """地采模式需要关联医保目录和战区映射。"""
        current_df = self.scm_df.copy()
        # 1. 关联医保 (逻辑与统采相同)
        if NATIONAL_DIR_SQL_FILE.exists():
            sql_query = self.sql_processor.read_sql_file(NATIONAL_DIR_SQL_FILE)
            national_dir_df, _ = self.sql_processor.execute_simple_query(sql_query)
            if not national_dir_df.empty and '国家药品编码' in current_df.columns:
                current_df['国家药品编码'] = current_df['国家药品编码'].astype(str)
                national_dir_df['国家药品编码'] = national_dir_df['国家药品编码'].astype(str)
                cols_to_replace = ['国家医保目录', '省医保目录', '省医保支付价']
                df_cleaned = current_df.drop(columns=[c for c in cols_to_replace if c in current_df.columns])
                current_df = pd.merge(df_cleaned, national_dir_df, on='国家药品编码', how='left')
        
        # 2. 关联战区
        if PURCHASE_CO_MAPPING_FILE.exists():
            mapping_df = pd.read_excel(PURCHASE_CO_MAPPING_FILE)
            join_key = '采购公司'
            target_col = '提报战区'
            if join_key in current_df.columns and target_col in mapping_df.columns:
                current_df[join_key] = current_df[join_key].astype(str)
                mapping_df[join_key] = mapping_df[join_key].astype(str)
                if target_col in current_df.columns:
                    current_df = current_df.drop(columns=[target_col])
                current_df = pd.merge(current_df, mapping_df[[join_key, target_col]], on=join_key, how='left')
        
        return current_df

    def get_benchmark_query_params(self) -> dict:
        """地采模式下，对标品需要筛选集团和对应战区的数据。"""
        return {
            "cgms": "地采", # 或其他非统采的值
            "common_names": self.scm_df['通用名'].dropna().unique().tolist(),
            "strategy_categories": self.scm_df['策略分类'].dropna().unique().tolist(),
            "lev3_org_name": self.scm_df['提报战区'].dropna().unique().tolist()
        }

    def get_excel_export_params(self) -> dict:
        """地采模式下的Excel公式和合并规则。"""
        return {
            "purchase_mode": "地采"
        }

class StrategyFactory:
    """策略工厂，根据采购模式创建并返回相应的策略实例。"""
    
    @staticmethod
    def get_strategy(scm_df: pd.DataFrame) -> PurchaseStrategy:
        # 增加对空数据或缺少列的保护
        if scm_df.empty or '采购模式' not in scm_df.columns or scm_df['采购模式'].dropna().empty:
            # 默认为统采策略或抛出错误
            return TongCaiStrategy(scm_df)

        purchase_mode = scm_df['采购模式'].dropna().iloc[0]
        
        if purchase_mode == '统采':
            return TongCaiStrategy(scm_df)
        else: # 假设其他所有模式都按地采处理
            return DiCaiStrategy(scm_df)
