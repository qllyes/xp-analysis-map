import pandas as pd
import numpy as np

class DataFormatter:
    """
    数据格式化处理器，负责在导出前对DataFrame进行最终的数据清理和转换。
    """

    @staticmethod
    def format_data(df: pd.DataFrame) -> pd.DataFrame:
        """
        对合并后的DataFrame进行全面的数据格式化。
        该版本增加了健壮的类型转换逻辑，以防止未来的类型错误。
        """
        if df.empty:
            return df

        df_copy = df.copy()

        # 步骤 1: 将空字符串统一替换为NaN，为数值计算做准备
        df_copy.replace('', np.nan, inplace=True)

        # 定义需要操作的列名列表
        force_str_cols = ['过会编码','新品编码','商品编码', '国际条码'] 
        percent_cols_div100 = ['返利率(%)']
        decimal_2_cols = [
            '日服/使用成交价（顾客）', '日服/使用底价', '标准单位进价', '标准单位底价',
            '标准单位成交价', '标准单位综合毛利额', '标准单位零售定价', '进价',
            '新品底价/对标品最低底价', '底价 *(返利后)'
        ]
        decimal_1_cols = ['预估/实际成交价', '建议零售价']
        int_cols = [
            '动销战区数', '效期（天）', # <-- 将已知列加入以便进行四舍五入
            '近90天月均销售数量', '近90天月均销售金额', '近90天月均前台含税毛利额',
            '近90天月均补偿后含税毛利额', '超级旗舰店铺货商品数量', '旗舰店铺货商品数量',
            '大店铺货商品数量', '中店铺货商品数量', '小店铺货商品数量', '成长店铺货商品数量',
            '通用名月均销量', '通用名月均销售额', '通用名月均前台毛利额', '通用名月均补偿后毛利额'
        ]
        
        # 步骤 2: 循环处理，进行特定的数值计算和类型转换
        for col in df_copy.columns:
            if col == '__source__' or df_copy[col].isnull().all():
                continue

            series = df_copy[col]

            # if col == '过会编码':
            #     df_copy[col] = pd.to_numeric(series, errors='coerce').astype('Int64').astype(str).replace('<NA>', np.nan)
            if  col == '填报日期':
                df_copy[col] = series.astype(str).replace('nan', np.nan).replace('NaT', np.nan)
            elif col in force_str_cols:
                df_copy[col] = series.astype(str).replace('nan', np.nan)
            elif col in percent_cols_div100:
                df_copy[col] = pd.to_numeric(series, errors='coerce') / 100
            elif col in decimal_2_cols:
                df_copy[col] = pd.to_numeric(series, errors='coerce').round(2)
            elif col in decimal_1_cols:
                df_copy[col] = pd.to_numeric(series, errors='coerce').round(1)
            # 对于整数列，只做四舍五入，暂时保留为数值类型
            elif col in int_cols:
                 df_copy[col] = pd.to_numeric(series, errors='coerce').round(0)
        # 步骤 4: 将所有剩余的NaN值统一替换为'-'
        df_copy.fillna('-', inplace=True)
        
        # 步骤 5: 在返回前，移除临时的'__source__'列
        if '__source__' in df_copy.columns:
            df_copy.drop(columns=['__source__'], inplace=True)
        
        return df_copy
