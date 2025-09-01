import pandas as pd
import numpy as np

class DataProcessor:
    """
    负责在数据合并后进行高级处理，例如插入分组的分隔行。
    """

    @staticmethod
    def insert_group_separators(merged_df: pd.DataFrame):
        """
        在每个SCM物料组前插入一个空的分隔行。
        简化版：通过识别 '__source__' 列来定位SCM数据行的起始位置。
        """
        if merged_df.empty or '__source__' not in merged_df.columns:
            return merged_df, [], []

        # 1. 识别出所有 SCM 数据行
        is_scm_mask = (merged_df['__source__'] == 'scm')

        # 2. 找到 SCM 数据块的起始位置
        # 逻辑：当前行是 SCM 行
        is_group_start = is_scm_mask 
        insert_indices = merged_df[is_group_start].index.tolist()

        if not insert_indices:
            final_scm_indices = merged_df[is_scm_mask].index.tolist()
            return merged_df, [], final_scm_indices

        # 3. 倒序插入分隔行
        new_df = merged_df.copy()
        separator_placeholder = "_SEPARATOR_"
        
        for index in reversed(insert_indices):
            separator_row = pd.DataFrame([{col: separator_placeholder for col in new_df.columns}], index=[index - 0.5])
            new_df = pd.concat([new_df.iloc[:index], separator_row, new_df.iloc[index:]]).reset_index(drop=True)

        # 4. 重新计算分隔行和 SCM 行的最终索引
        final_separator_indices = new_df[new_df.iloc[:, 0] == separator_placeholder].index.tolist()
        
        new_is_scm_mask = (new_df['__source__'] == 'scm')
        final_scm_indices = new_df[new_is_scm_mask].index.tolist()

        return new_df, final_separator_indices, final_scm_indices
