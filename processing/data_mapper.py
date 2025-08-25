import pandas as pd

class MappingProcessor:
    """映射处理类，负责字段映射逻辑"""

    @staticmethod
    def run_mapping(map_df: pd.DataFrame, source_df: pd.DataFrame, source_type: str = 'table2') -> pd.DataFrame:
        """
        执行字段映射
        Args:
            map_df: 映射关系表（包含三列：目标字段名、table2字段名、table3字段名）
            source_df: 源数据表
            source_type: 源数据类型 ('table2' 或 'table3')
        """
        if map_df.empty or source_df.empty:
            return pd.DataFrame()

        # 根据source_type选择对应的映射列
        if source_type == 'table2':
            source_field_col_idx = 1  # table2字段名在第二列
        elif source_type == 'table3':
            source_field_col_idx = 2  # table3字段名在第三列
        else:
            raise ValueError("source_type 必须是 'table2' 或 'table3'")

        # 清理映射表并提取映射关系
        map_df_clean = map_df.dropna(subset=[map_df.columns[0], map_df.columns[source_field_col_idx]], how='any')
        
        target_fields = map_df_clean.iloc[:, 0].astype(str)
        source_fields = map_df_clean.iloc[:, source_field_col_idx].astype(str)
        
        # 构建映射字典 {源字段: 目标字段}
        field_map = dict(zip(source_fields, target_fields))
        
        # 从 source_df 中选取并重命名存在的列
        available_cols = [col for col in field_map if col in source_df.columns]
        result_df = source_df[available_cols].rename(columns=field_map)
        
        # 添加在源数据中缺失但在映射表中定义的目标列，并填充为空
        all_target_fields = map_df.iloc[:, 0].dropna().astype(str).unique()
        for col in all_target_fields:
            if col not in result_df.columns:
                result_df[col] = ''
        
        # 确保最终结果的列顺序与映射表中的目标字段顺序一致
        final_columns = [col for col in all_target_fields if col in result_df.columns]
        
        return result_df[final_columns]
