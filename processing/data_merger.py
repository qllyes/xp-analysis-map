import pandas as pd
import numpy as np

class DataMerger:
    """数据合并处理器，负责合并映射后的数据并排序"""

    @staticmethod
    def merge_and_sort_data(map_scm_df: pd.DataFrame, map_benchmark_df: pd.DataFrame, strategy: str) -> pd.DataFrame:
        """
        根据复杂的分组、筛选和排序规则合并SCM和对标品数据。
        
        Args:
            map_scm_df (pd.DataFrame): 映射后的SCM数据。
            map_benchmark_df (pd.DataFrame): 映射后的对标品数据。
            strategy (str): 采购模式策略 ('统采' 或 '地采')。
        """
        if map_scm_df.empty:
            return map_benchmark_df
        if map_benchmark_df.empty:
            return map_scm_df

        # 为数据添加来源标记
        scm_df = map_scm_df.copy()
        scm_df['__source__'] = 'scm'
        benchmark_df = map_benchmark_df.copy()
        benchmark_df['__source__'] = 'benchmark'
        
        if '近90天月均销售数量' in benchmark_df.columns:
            benchmark_df['近90天月均销售数量'] = pd.to_numeric(benchmark_df['近90天月均销售数量'], errors='coerce').fillna(0)

        all_parts = []
        
        for index, scm_row in scm_df.iterrows():
            current_scm_part = scm_row.to_frame().T
            all_parts.append(current_scm_part)

            category = scm_row['三级大类']
            if pd.isna(category):
                continue

            current_benchmark_base = benchmark_df[benchmark_df['三级大类'] == category]
            if current_benchmark_base.empty:
                continue
            
            # --- 核心修复：使用传入的 strategy 参数 ---
            final_benchmark_group = pd.DataFrame()
            if strategy != '统采':
                # 地采逻辑
                lev3_org_name = scm_row['提报战区']
                condition = (
                    (current_benchmark_base['取数维度（战区/集团）'] == '集团') |
                    (current_benchmark_base['取数维度（战区/集团）'] == lev3_org_name)
                )
                final_benchmark_group = current_benchmark_base[condition]
            else:
                # 统采逻辑
                final_benchmark_group = current_benchmark_base
            
            if not final_benchmark_group.empty:
                if strategy != '统采':
                    sort_keys = ['取数维度（战区/集团）', '商品名称','近90天月均销售数量']
                    if all(key in final_benchmark_group.columns for key in sort_keys):
                        final_benchmark_group = final_benchmark_group.sort_values(
                            by=sort_keys,
                            ascending=[False, False, False]
                        )
                else:
                    sort_keys = ['取数维度（战区/集团）','近90天月均销售数量']
                    if all(key in final_benchmark_group.columns for key in sort_keys):
                        final_benchmark_group = final_benchmark_group.sort_values(
                            by=sort_keys,
                            ascending=[False, False]
                        )
                all_parts.append(final_benchmark_group)

        if not all_parts:
            return pd.DataFrame(columns=scm_df.columns)

        final_df = pd.concat(all_parts).reset_index(drop=True)
        
        return final_df
