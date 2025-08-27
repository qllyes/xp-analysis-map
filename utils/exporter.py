import pandas as pd
from io import BytesIO
from datetime import datetime
from typing import Tuple, List, Dict
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
from openpyxl.cell import MergedCell
from openpyxl.utils import get_column_letter

class ResultExporter:
    """结果导出类，负责生成和下载结果文件，并应用复杂的格式。"""

    @staticmethod
    def export_to_excel(df: pd.DataFrame, separator_indices: List[int], scm_indices: List[int], export_config: Dict) -> Tuple[BytesIO, str]:
        """
        导出DataFrame到Excel，并应用所有指定的格式。
        
        Args:
            df: 待导出的DataFrame。
            separator_indices: 分隔行的索引列表。
            scm_indices: SCM数据行的索引列表。
            export_config: 从策略对象获取的包含导出设置的字典。
        """
        output = BytesIO()
        
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df_to_write = df.replace("_SEPARATOR_", "")
            df_to_write.to_excel(writer, index=False, sheet_name='目标表')
            
            workbook = writer.book
            worksheet = writer.sheets['目标表']

            # --- 定义样式 (此部分不变) ---
            default_font = Font(name='微软雅黑', size=9)
            header_font = Font(name='微软雅黑', size=9, bold=True)
            red_font = Font(name='微软雅黑', size=9, color="FF0000")
            wrap_alignment = Alignment(horizontal='left', vertical='center', wrap_text=True) 
            no_wrap_alignment = Alignment(horizontal='left', vertical='center', wrap_text=False)
            yellow_fill = PatternFill(start_color='FFFF00', end_color='FFFF00', fill_type='solid')
            separator_fill = PatternFill(start_color='F2F2F2', end_color='F2F2F2', fill_type='solid')
            thin_border_side = Side(style='thin', color='000000')
            thin_border = Border(left=thin_border_side, right=thin_border_side, top=thin_border_side, bottom=thin_border_side)
            
            # --- 定义数字格式 (此部分不变) ---
            percent_format = '0.00%;-0.00%;0.00%;@'
            decimal_2_format = '0.00;-0.00;0.00;@'
            # ... 其他格式
            text_format = '@'
            column_formats = {
                '返利率(%)': percent_format, '通用名补偿后毛利率': percent_format,
                '日服/使用成交价（顾客）': decimal_2_format, # ... 其他列
                '过会编码': text_format, '新品编码': text_format, '国际条码': text_format,
            }
            red_font_columns = ['新品底价/对标品最低底价', '底价 *(返利后)']

            # --- 应用常规样式和格式 (此部分不变) ---
            for col_idx, col_name in enumerate(df.columns, 1):
                # ...
                pass

            # --- 核心改动：从 export_config 获取配置 ---
            purchase_mode = export_config["purchase_mode"]
            merge_rules = export_config["merge_rules"]
            formulas = export_config["formulas"]

            # --- 处理特殊行：背景色、合并、公式 ---
            for sep_idx in separator_indices:
                excel_row = sep_idx + 2
                worksheet.row_dimensions[excel_row].height = 150
                for i in range(1, len(df.columns) + 1):
                    worksheet.cell(row=excel_row, column=i).fill = separator_fill
                
                # --- 使用从配置中读取的合并规则 ---
                worksheet.merge_cells(start_row=excel_row, start_column=merge_rules["formula1_start_col"], end_row=excel_row, end_column=merge_rules["formula1_end_col"])
                worksheet.merge_cells(start_row=excel_row, start_column=merge_rules["formula2_start_col"], end_row=excel_row, end_column=merge_rules["formula2_end_col"])
                
                worksheet.cell(row=excel_row, column=merge_rules["formula1_start_col"]).alignment = wrap_alignment
                worksheet.cell(row=excel_row, column=merge_rules["formula2_start_col"]).alignment = wrap_alignment
                
                scm_data_row = excel_row + 1
                
                # --- 使用从配置中读取的公式 ---
                formula1 = formulas["formula1"].format(row=scm_data_row)
                formula2 = formulas["formula2"].format(row=scm_data_row)
                
                worksheet.cell(row=excel_row, column=merge_rules["formula1_start_col"]).value = formula1
                worksheet.cell(row=excel_row, column=merge_rules["formula2_start_col"]).value = formula2

            for scm_idx in scm_indices:
                excel_row = scm_idx + 2
                for i in range(1, len(df.columns) + 1):
                    worksheet.cell(row=excel_row, column=i).fill = yellow_fill

            # --- 后处理步骤 (此部分不变) ---
            for row in worksheet.iter_rows(min_row=1, max_row=worksheet.max_row, min_col=1, max_col=worksheet.max_column):
                for cell in row:
                    cell.border = thin_border

        output.seek(0)
        filename = f'{purchase_mode}新品过会分析表_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
        return output, filename
