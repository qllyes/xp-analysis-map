"""
定义了所有采购模式分析流程的策略类。
每个策略类都封装了一种特定采购模式（如地采、统采）的完整端到端处理逻辑。
"""

from abc import ABC, abstractmethod
import pandas as pd
import streamlit as st
from config import NATIONAL_DIR_SQL_FILE, PURCHASE_CO_MAPPING_FILE
from pathlib import Path


class AnalysisStrategy(ABC):
    """分析策略的抽象基类，定义了所有策略必须遵循的接口。"""

    def __init__(self, processors):
        """
        初始化策略。

        Args:
            processors (dict): 包含所有处理器实例的字典。
        """
        self.sql_processor = processors["sql"]
        self.mapping_processor = processors["mapper"]
        self.data_merger = processors["merger"]
        self.data_processor = processors["processor"]
        self.data_formatter = processors["formatter"]
        self.result_exporter = processors["exporter"]
        self.status_updater = processors.get("status_updater", lambda label, state: None)

    @abstractmethod
    def execute(self, map_df: pd.DataFrame, scm_df: pd.DataFrame) -> dict:
        """
        执行完整的分析流程。

        Args:
            map_df (pd.DataFrame): 映射关系表。
            scm_df (pd.DataFrame): SCM新品申报数据。

        Returns:
            dict: 包含处理结果的字典，例如最终的DataFrame和导出的Excel文件。
        """
        pass


class DicaiStrategy(AnalysisStrategy):
    """地采模式的具体分析策略。"""

    def _enrich_scm_data(self, scm_df: pd.DataFrame) -> pd.DataFrame:
        """
        为地采模式丰富SCM数据，主要是关联战区信息。
        """
        current_df = scm_df.copy()
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
                    
                    current_df = pd.merge(
                        current_df,
                        mapping_df[[join_key, target_col]],
                        on=join_key,
                        how='left'
                    )
                else:
                    st.warning("⚠️ 无法关联战区信息（缺少关联键或目标列）。")
            else:
                st.warning(f"⚠️ 未找到 '{PURCHASE_CO_MAPPING_FILE}' 文件，战区信息将不会关联。")
        except Exception as e:
            st.error(f"❌ 关联战区信息时出错: {e}")
        return current_df

    def execute(self, map_df: pd.DataFrame, scm_df: pd.DataFrame) -> dict:
        self.status_updater(label="丰富地采数据...", state="running")
        enriched_scm_df = self._enrich_scm_data(scm_df)
        
        # 提取筛选条件
        self.status_updater(label="提取筛选条件...", state="running")
        scm_common_names = enriched_scm_df['通用名'].dropna().unique().tolist()
        scm_strategy_categories = enriched_scm_df['策略分类'].dropna().unique().tolist()
        scm_lev3_org_name = enriched_scm_df['提报战区'].dropna().unique().tolist()

        # 查询对标品数据
        self.status_updater(label="🔎 正在从数据库按[地采]规则查询对标品数据…", state="running")
        sql_path = Path("对标品.sql")
        sql_query = self.sql_processor.read_sql_file(sql_path)
        benchmark_df, executed_sql = self.sql_processor.execute_sql_query(
            sql_query, cgms='地采', common_names=scm_common_names, 
            strategy_categories=scm_strategy_categories, lev3_org_name=scm_lev3_org_name
        )
        if benchmark_df.empty: st.warning("⚠️ 对标品数据查询为空。")

        # 映射与合并
        self.status_updater(label="🧭 正在进行映射转换与数据合并…", state="running")
        map_scm_df = self.mapping_processor.run_mapping(map_df, enriched_scm_df, source_type='table2')
        map_benchmark_df = self.mapping_processor.run_mapping(map_df, benchmark_df, source_type='table3')
        # --- 核心修复：传入 'strategy' 参数 ---
        target_df = self.data_merger.merge_and_sort_data(map_scm_df, map_benchmark_df, strategy='地采')
        
        # 【地采特有】构建分组结构
        self.status_updater(label="📊 正在构建分组结构...", state="running")
        processed_df, sep_indices, scm_indices = self.data_processor.insert_group_separators(target_df)
        
        # 格式化与导出
        self.status_updater(label="🎨 正在清理与格式化数据...", state="running")
        formatted_df = self.data_formatter.format_data(processed_df)
        
        self.status_updater(label="📦 正在按[地采]模板生成Excel文件…", state="running")
        output, filename = self.result_exporter.export_to_excel(formatted_df, sep_indices, scm_indices, purchase_mode='地采')
        
        return {
            "result_df": formatted_df,
            "result_output": output,
            "result_filename": filename,
            "executed_sql": executed_sql,
            "new_product_count": len(scm_indices)
        }


class TongcaiStrategy(AnalysisStrategy):
    """统采模式的具体分析策略。"""

    def execute(self, map_df: pd.DataFrame, scm_df: pd.DataFrame) -> dict:
        # 提取筛选条件
        self.status_updater(label="提取筛选条件...", state="running")
        scm_common_names = scm_df['通用名'].dropna().unique().tolist()
        scm_strategy_categories = scm_df['策略分类'].dropna().unique().tolist()

        # 查询对标品数据
        self.status_updater(label="🔎 正在从数据库按[统采]规则查询对标品数据…", state="running")
        sql_path = Path("对标品.sql")
        sql_query = self.sql_processor.read_sql_file(sql_path)
        benchmark_df, executed_sql = self.sql_processor.execute_sql_query(
            sql_query, cgms='统采', common_names=scm_common_names, 
            strategy_categories=scm_strategy_categories
        )
        if benchmark_df.empty: st.warning("⚠️ 对标品数据查询为空。")

        # 映射与合并
        self.status_updater(label="🧭 正在进行映射转换与数据合并…", state="running")
        map_scm_df = self.mapping_processor.run_mapping(map_df, scm_df, source_type='table2')
        map_benchmark_df = self.mapping_processor.run_mapping(map_df, benchmark_df, source_type='table3')
        # --- 核心修复：传入 'strategy' 参数 ---
        target_df = self.data_merger.merge_and_sort_data(map_scm_df, map_benchmark_df, strategy='统采')
        
        # 【统采特有】不插入分隔行，直接获取 SCM 行索引
        self.status_updater(label="📊 正在识别新品行...", state="running")
        scm_indices = target_df[target_df['__source__'] == 'scm'].index.tolist()
        processed_df = target_df
        
        # 格式化与导出
        self.status_updater(label="🎨 正在清理与格式化数据...", state="running")
        formatted_df = self.data_formatter.format_data(processed_df)
        
        self.status_updater(label="📦 正在按[统采]模板生成Excel文件…", state="running")
        # 注意：为 separator_indices 传入空列表
        output, filename = self.result_exporter.export_to_excel(formatted_df, [], scm_indices, purchase_mode='统采')
        
        return {
            "result_df": formatted_df,
            "result_output": output,
            "result_filename": filename,
            "executed_sql": executed_sql,
            "new_product_count": len(scm_indices)
        }

