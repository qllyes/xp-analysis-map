"""
定义了分析流程的执行器 (Pipeline)。
它会根据传入的采购模式，自动选择并执行正确的分析策略。
这是策略模式的上下文 (Context) 部分。
"""

import pandas as pd
from .strategies import AnalysisStrategy, DicaiStrategy, TongcaiStrategy

class AnalysisPipeline:
    """分析流程的执行器。"""

    def __init__(self, purchase_mode: str, processors: dict):
        """
        根据采购模式选择合适的策略。

        Args:
            purchase_mode (str): 采购模式 ('统采' 或 '地采')。
            processors (dict): 包含所有处理器实例的字典。
        """
        self.processors = processors
        
        if purchase_mode == '统采':
            self.strategy: AnalysisStrategy = TongcaiStrategy(self.processors)
        else: # 默认为地采
            self.strategy: AnalysisStrategy = DicaiStrategy(self.processors)

    def run(self, map_df: pd.DataFrame, scm_df: pd.DataFrame) -> dict:
        """
        执行所选策略的分析流程。

        Args:
            map_df (pd.DataFrame): 映射关系表。
            scm_df (pd.DataFrame): SCM新品申报数据。

        Returns:
            dict: 包含处理结果的字典。
        """
        return self.strategy.execute(map_df, scm_df)
