from abc import ABC, abstractmethod
import pandas as pd
import streamlit as st
from pathlib import Path

# 导入项目中的其他模块
from config import NATIONAL_DIR_SQL_FILE, PURCHASE_CO_MAPPING_FILE
from db.database_handler import SQLProcessor # 策略需要直接与数据库交互

class PurchaseStrategy(ABC):
    """
    处理策略的抽象基类，定义了所有与采购模式相关的操作接口。
    """
    
    def __init__(self):
        # 策略类应该是无状态的，但可以持有完成其任务所需的工具实例
        self.sql_processor = SQLProcessor()

    @abstractmethod
    def enrich_scm_data(self, scm_df: pd.DataFrame) -> pd.DataFrame:
        """
        根据不同策略丰富SCM数据（如关联医保、战区等）。
        """
        pass

    @abstractmethod
    def get_benchmark_query_params(self, scm_df: pd.DataFrame) -> dict:
        """
        根据SCM数据生成用于查询对标品的SQL参数。
        """
        pass
    
    @abstractmethod
    def filter_benchmark_data(self, scm_row: pd.Series, benchmark_base_df: pd.DataFrame) -> pd.DataFrame:
        """
        在数据合并阶段，根据单行SCM数据筛选匹配的对标品数据。
        """
        pass

    @abstractmethod
    def get_export_config(self) -> dict:
        """
        提供导出Excel时所需的特定配置（如模式名称、公式、合并规则）。
        """
        pass

class TongCaiStrategy(PurchaseStrategy):
    """统采模式的处理策略。"""
    
    def enrich_scm_data(self, scm_df: pd.DataFrame) -> pd.DataFrame:
        """统采模式只关联医保目录。"""
        current_df = scm_df.copy()
        try:
            if NATIONAL_DIR_SQL_FILE.exists():
                sql_query = self.sql_processor.read_sql_file(NATIONAL_DIR_SQL_FILE)
                national_dir_df, _ = self.sql_processor.execute_simple_query(sql_query)
                
                if not national_dir_df.empty and '国家药品编码' in current_df.columns and '国家药品编码' in national_dir_df.columns:
                    current_df['国家药品编码'] = current_df['国家药品编码'].astype(str)
                    national_dir_df['国家药品编码'] = national_dir_df['国家药品编码'].astype(str)
                    cols_to_replace = ['国家医保目录', '省医保目录', '省医保支付价']
                    df_cleaned = current_df.drop(columns=[col for col in cols_to_replace if col in current_df.columns])
                    current_df = pd.merge(df_cleaned, national_dir_df, on='国家药品编码', how='left')
                else:
                    st.warning("⚠️ [统采策略] 无法关联国家医保目录（缺少关联键或查询为空）。")
            else:
                st.warning(f"⚠️ [统采策略] 未找到 '{NATIONAL_DIR_SQL_FILE}' 文件，医保目录信息将不会关联。")
        except Exception as e:
            st.error(f"❌ [统采策略] 关联国家医保目录时出错: {e}")
        return current_df

    def get_benchmark_query_params(self, scm_df: pd.DataFrame) -> dict:
        """统采模式下，对标品只筛选集团数据。"""
        return {
            "cgms": "统采",
            "common_names": scm_df['通用名'].dropna().unique().tolist(),
            "strategy_categories": scm_df['策略分类'].dropna().unique().tolist(),
            "lev3_org_name": None
        }
        
    def filter_benchmark_data(self, scm_row: pd.Series, benchmark_base_df: pd.DataFrame) -> pd.DataFrame:
        """统采逻辑只按三级分类匹配"""
        return benchmark_base_df

    def get_export_config(self) -> dict:
        """统采模式下的Excel配置。"""
        return {
            "purchase_mode": "统采",
            "merge_rules": {
                "formula1_start_col": 27, "formula1_end_col": 38,
                "formula2_start_col": 44, "formula2_end_col": 59
            },
            "formulas": {
                "formula1": '=C{row}&"-"&D{row}&CHAR(10)&I{row}&"-"&J{row}&"-"&K{row}&CHAR(10)&"新品组压测意见："&CHAR(10)&"1.顾客："&CHAR(10)&"2.员工："&CHAR(10)&"3.公司："&DN{row}&DO{row}&CHAR(10)&"4.市场情况：该通用名中康月销"&CR{row}&CHAR(10)&"5.通用名结构："&CHAR(10)&"6.供应商条件："&DC{row}&"、"&DF{row}&"；"&DJ{row}&CHAR(10)&"7.医保："&CK{row}&"；"&"挂网价："&CM{row}&CHAR(10)&"8.铺货："&"标准："&CU{row}&"通"&"（新品费："&CV{row}&"元）；买手洽谈："&CW{row}&"（新品费："&CZ{row}&"元）"',
                "formula2": '="【引进理由】"&L{row}&CHAR(10)&"【成份】"&ES{row}&CHAR(10)&"【适应症】"&EU{row}&CHAR(10)&"【采购总结卖点】"&CHAR(10)&EX{row}&CHAR(10)&"【搜索关键词】"&EY{row}'
            }
        }

class DiCaiStrategy(PurchaseStrategy):
    """地采（非统采）模式的处理策略。"""

    def enrich_scm_data(self, scm_df: pd.DataFrame) -> pd.DataFrame:
        """地采模式需要关联医保目录和战区映射。"""
        current_df = scm_df.copy()
        # 1. 关联医保 (逻辑与统采相同)
        try:
            if NATIONAL_DIR_SQL_FILE.exists():
                sql_query = self.sql_processor.read_sql_file(NATIONAL_DIR_SQL_FILE)
                national_dir_df, _ = self.sql_processor.execute_simple_query(sql_query)
                if not national_dir_df.empty and '国家药品编码' in current_df.columns and '国家药品编码' in national_dir_df.columns:
                    current_df['国家药品编码'] = current_df['国家药品编码'].astype(str)
                    national_dir_df['国家药品编码'] = national_dir_df['国家药品编码'].astype(str)
                    cols_to_replace = ['国家医保目录', '省医保目录', '省医保支付价']
                    df_cleaned = current_df.drop(columns=[c for c in cols_to_replace if c in current_df.columns])
                    current_df = pd.merge(df_cleaned, national_dir_df, on='国家药品编码', how='left')
            else:
                 st.warning(f"⚠️ [地采策略] 未找到 '{NATIONAL_DIR_SQL_FILE}' 文件，医保目录信息将不会关联。")
        except Exception as e:
            st.error(f"❌ [地采策略] 关联国家医保目录时出错: {e}")

        # 2. 关联战区
        try:
            if PURCHASE_CO_MAPPING_FILE.exists():
                mapping_df = pd.read_excel(PURCHASE_CO_MAPPING_FILE)
                join_key = '采购公司'
                target_col = '提报战区'
                if join_key in current_df.columns and join_key in mapping_df.columns and target_col in mapping_df.columns:
                    current_df[join_key] = current_df[join_key].astype(str)
                    mapping_df[join_key] = mapping_df[join_key].astype(str)
                    if target_col in current_df.columns:
                        current_df = current_df.drop(columns=[target_col])
                    current_df = pd.merge(current_df, mapping_df[[join_key, target_col]], on=join_key, how='left')
                else:
                    st.warning("⚠️ [地采策略] 无法关联战区信息（缺少关联键或目标列）。")
            else:
                st.warning(f"⚠️ [地采策略] 未找到 '{PURCHASE_CO_MAPPING_FILE}' 文件，战区信息将不会关联。")
        except Exception as e:
            st.error(f"❌ [地采策略] 关联战区信息时出错: {e}")
        
        return current_df

    def get_benchmark_query_params(self, scm_df: pd.DataFrame) -> dict:
        """地采模式下，对标品需要筛选集团和对应战区的数据。"""
        return {
            "cgms": "地采",
            "common_names": scm_df['通用名'].dropna().unique().tolist(),
            "strategy_categories": scm_df['策略分类'].dropna().unique().tolist(),
            "lev3_org_name": scm_df['提报战区'].dropna().unique().tolist()
        }
        
    def filter_benchmark_data(self, scm_row: pd.Series, benchmark_base_df: pd.DataFrame) -> pd.DataFrame:
        """非统采逻辑：筛选集团或对应战区的数据"""
        lev3_org_name = scm_row['提报战区']
        condition = (
            (benchmark_base_df['取数维度（战区/集团）'] == '集团') |
            (benchmark_base_df['取数维度（战区/集团）'] == lev3_org_name)
        )
        return benchmark_base_df[condition]

    def get_export_config(self) -> dict:
        """地采模式下的Excel配置。"""
        return {
            "purchase_mode": "地采",
            "merge_rules": {
                "formula1_start_col": 25, "formula1_end_col": 38,
                "formula2_start_col": 48, "formula2_end_col": 58
            },
            "formulas": {
                "formula1": '=I{row}&CHAR(10)&"1.顾客：；"&CHAR(10)&"2.公司：；"&CHAR(10)&"3.市场分析：；"&CHAR(10)&"4.供应商条件："&DB{row}&"，"&DE{row}&"，"&DI{row}&"；"&CHAR(10)&"5.医保："&CK{row}&"，"&"支付价"&"："&CL{row}&"；"&CHAR(10)&"6.铺货通道："&CV{row}&"；"&CHAR(10)&"挑战点：1.；"&CHAR(10)&"修改点：1.；"',
                "formula2": '="【引进理由】"&L{row}&CHAR(10)&"【成份】"&EX{row}&CHAR(10)&"【适应症】"&EZ{row}&CHAR(10)&"【卖点】"&FB{row}&CHAR(10)&"【关键搜索词】"&FC{row}'
            }
        }

class StrategyFactory:
    """策略工厂，根据采购模式创建并返回相应的策略实例。"""
    
    @staticmethod
    def get_strategy(scm_df: pd.DataFrame) -> PurchaseStrategy:
        if scm_df.empty or '采购模式' not in scm_df.columns or scm_df['采购模式'].dropna().empty:
            raise ValueError("无法确定采购模式，请检查SCM数据文件。")

        purchase_mode = scm_df['采购模式'].dropna().iloc[0]
        
        if purchase_mode == '统采':
            return TongCaiStrategy()
        else:
            return DiCaiStrategy()
