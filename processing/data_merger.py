import pandas as pd
import numpy as np

class DataMerger:
    """数据合并处理器，负责合并映射后的数据并排序"""

    @staticmethod
    def merge_and_sort_data(map_scm_df: pd.DataFrame, map_benchmark_df: pd.DataFrame, strategy) -> pd.DataFrame:
        """
        根据复杂的分组、筛选和排序规则合并SCM和对标品数据。
        
        Args:
            map_scm_df: 映射后的SCM DataFrame。
            map_benchmark_df: 映射后的对标品 DataFrame。
            strategy: 当前采购模式对应的策略实例。
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
        
        # 确保排序列为数值类型以便排序
        if '近90天月均销售数量' in benchmark_df.columns:
            benchmark_df['近90天月均销售数量'] = pd.to_numeric(benchmark_df['近90天月均销售数量'], errors='coerce').fillna(0)

        all_parts = []
        
        # 遍历scm数据，以每一行作为分组的起点
        for index, scm_row in scm_df.iterrows():
            # 1. 将当前scm行本身作为一个分组部分
            current_scm_part = scm_row.to_frame().T
            all_parts.append(current_scm_part)

            # 2. 根据当前scm行的'三级分类'筛选benchmark数据
            category = scm_row['三级大类']
            if pd.isna(category):
                continue

            current_benchmark_base = benchmark_df[benchmark_df['三级大类'] == category]
            if current_benchmark_base.empty:
                continue

            # 3. --- 核心改动：使用策略对象来执行筛选 ---
            #    不再需要 if purchase_mode != '统采' 的判断
            final_benchmark_group = strategy.filter_benchmark_data(scm_row, current_benchmark_base)
            
            # 4. 对筛选后的benchmark组进行排序
            if not final_benchmark_group.empty:
                sort_keys = ['取数维度（战区/集团）', '商品名称','近90天月均销售数量']
                if all(key in final_benchmark_group.columns for key in sort_keys):
                    final_benchmark_group = final_benchmark_group.sort_values(
                        by=sort_keys,
                        ascending=[False, False, False]
                    )
                all_parts.append(final_benchmark_group)

        # 5. 合并所有处理好的部分
        if not all_parts:
            return pd.DataFrame(columns=scm_df.columns)

        final_df = pd.concat(all_parts).reset_index(drop=True)
        
        return final_df
