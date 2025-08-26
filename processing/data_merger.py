import pandas as pd
import numpy as np

class DataMerger:
    """数据合并处理器，负责合并映射后的数据并排序"""

    @staticmethod
    def merge_and_sort_data(map_scm_df: pd.DataFrame, map_benchmark_df: pd.DataFrame) -> pd.DataFrame:
        """
        根据复杂的分组、筛选和排序规则合并SCM和对标品数据。
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
            print('三级大类：',category)
            if pd.isna(category):
                continue # 如果scm行没有三级分类，则跳过对标品查找

            current_benchmark_base = benchmark_df[benchmark_df['三级大类'] == category]
            if current_benchmark_base.empty:
                continue

            # 3. 根据当前scm行的'采购模式'应用额外筛选条件
            purchase_mode = scm_row['采购模式']
            final_benchmark_group = pd.DataFrame()
            if purchase_mode != '统采':
                # 非统采逻辑
                lev3_org_name = scm_row['提报战区']
                condition = (
                    (current_benchmark_base['取数维度（战区/集团）'] == '集团') |
                    (current_benchmark_base['取数维度（战区/集团）'] == lev3_org_name)
                )
                final_benchmark_group = current_benchmark_base[condition]
            else:
                # 统采逻辑（只按三级分类匹配）
                final_benchmark_group = current_benchmark_base
            
            # 4. 对筛选后的benchmark组进行排序
            if not final_benchmark_group.empty:
                # 确保排序键存在
                sort_keys = ['取数维度（战区/集团）', '近90天月均销售金额']
                if all(key in final_benchmark_group.columns for key in sort_keys):
                    final_benchmark_group = final_benchmark_group.sort_values(
                        by=sort_keys,
                        ascending=[False, False]
                    )
                all_parts.append(final_benchmark_group)

        # 5. 合并所有处理好的部分
        if not all_parts:
            return pd.DataFrame(columns=scm_df.columns) # 返回一个空的DataFrame

        final_df = pd.concat(all_parts).reset_index(drop=True)

        # if scm_df['采购模式'].dropna().iloc[0] =='统采':
        #     # 删除统采辅助列，地采专用 
        #     final_df = final_df.drop(columns=['lev3_org_name','采购公司'], errors='ignore')
        
        return final_df
