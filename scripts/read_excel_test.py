import pandas as pd

# 读取“地采新品申报单导出明细”Excel文件
file_path = r"D:\lbx_work\xinpin\python_scripts\scm表头顺序映射\地采新品申报单导出明细.xlsx"  # 请根据实际文件路径修改
try:
    df = pd.read_excel(file_path,dtype={'过会编码':str})
    print("文件读取成功，数据预览：")
    print(df.head())
except Exception as e:
    print(f"读取Excel文件时出错: {e}")
