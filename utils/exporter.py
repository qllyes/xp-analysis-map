import pandas as pd
from io import BytesIO
from datetime import datetime
from typing import Tuple, List
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side


class ResultExporter:
    """结果导出类，负责生成和下载结果文件，并应用复杂的格式。"""

    @staticmethod
    def export_to_excel(df: pd.DataFrame, separator_indices: List[int], scm_indices: List[int]) -> Tuple[BytesIO, str]:
        """
        导出DataFrame到Excel，并应用所有指定的格式，包括插入空行和公式。
        """
        output = BytesIO()
        
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # 在写入前，将占位符替换为空字符串，这样Excel中就是空行
            df_to_write = df.replace("_SEPARATOR_", "")
            df_to_write.to_excel(writer, index=False, sheet_name='目标表')
            
            workbook = writer.book
            worksheet = writer.sheets['目标表']

            # --- 定义样式 ---
            default_font = Font(name='微软雅黑', size=9)
            header_font = Font(name='微软雅黑', size=9, bold=True)
            red_font = Font(name='微软雅黑', size=9, color="FF0000")
            
            # 定义两种对齐方式：一种带自动换行，一种不带
            wrap_alignment = Alignment(horizontal='left', vertical='center', wrap_text=True) 
            no_wrap_alignment = Alignment(horizontal='left', vertical='center', wrap_text=False)

            yellow_fill = PatternFill(start_color='FFFF00', end_color='FFFF00', fill_type='solid')
            separator_fill = PatternFill(start_color='F2F2F2', end_color='F2F2F2', fill_type='solid') # 灰色
            thin_border_side = Side(style='thin', color='000000')
            thin_border = Border(left=thin_border_side, 
                                 right=thin_border_side, 
                                 top=thin_border_side, 
                                 bottom=thin_border_side)
            # --- 定义数字格式 ---
            percent_format = '0.00%'
            decimal_2_format = '0.00'
            decimal_1_format = '0.0'
            integer_format = '0'
            text_format = '@'
            column_formats = {
                '返利率(%)': percent_format, '通用名补偿后毛利率': percent_format,
                '日服/使用成交价（顾客）': decimal_2_format, '日服/使用底价': decimal_2_format,
                '标准单位进价': decimal_2_format, '标准单位底价': decimal_2_format,
                '标准单位成交价': decimal_2_format, '标准单位综合毛利额': decimal_2_format,
                '标准单位零售定价': decimal_2_format, '进价': decimal_2_format,
                '新品底价/对标品最低底价': decimal_2_format, '底价 *(返利后)': decimal_2_format,
                '预估/实际成交价': decimal_1_format, '建议零售价': decimal_1_format,
                '过会编码': text_format, '新品编码': text_format, '国际条码': text_format,
                '近90天月均销售数量': integer_format, '近90天月均销售金额': integer_format,
                '近90天月均前台含税毛利额': integer_format, '近90天月均补偿后含税毛利额': integer_format,
                '超级旗舰店铺货商品数量': integer_format, '旗舰店铺货商品数量': integer_format,
                '大店铺货商品数量': integer_format, '中店铺货商品数量': integer_format,
                '小店铺货商品数量': integer_format, '成长店铺货商品数量': integer_format,
                '通用名月均销量': integer_format, '通用名月均销售额': integer_format,
                '通用名月均前台毛利额': integer_format, '通用名月均补偿后毛利额': integer_format,
            }
            red_font_columns = ['新品底价/对标品最低底价', '底价 *(返利后)']

            # --- 应用常规样式和格式 ---
            for col_idx, col_name in enumerate(df.columns, 1):
                # 为第一行（列名）设置样式，并启用自动换行
                header_cell = worksheet.cell(row=1, column=col_idx)
                header_cell.font = header_font
                header_cell.alignment = wrap_alignment
                
                # 为数据行设置样式，并禁用自动换行
                for row_idx in range(2, len(df) + 2):
                    cell = worksheet.cell(row=row_idx, column=col_idx)
                    cell.alignment = no_wrap_alignment
                    cell.font = red_font if col_name in red_font_columns else default_font
                    if col_name in column_formats:
                        cell.number_format = column_formats[col_name]

            # --- 处理特殊行 ---
            # 1. 处理分隔行 (合并单元格、设置背景色、插入公式)
            for sep_idx in separator_indices:
                excel_row = sep_idx + 2
                # 设置分隔行高度
                worksheet.row_dimensions[excel_row].height = 150
                for i in range(1, len(df.columns) + 1):
                    worksheet.cell(row=excel_row, column=i).fill = separator_fill
                
                worksheet.merge_cells(start_row=excel_row, start_column=27, end_row=excel_row, end_column=38) # AA to AL
                worksheet.merge_cells(start_row=excel_row, start_column=44, end_row=excel_row, end_column=59) # AR to BG

                # 为包含公式的合并单元格，单独设置自动换行
                worksheet.cell(row=excel_row, column=27).alignment = wrap_alignment
                worksheet.cell(row=excel_row, column=44).alignment = wrap_alignment

                scm_data_row = excel_row + 1
                
                # M1 公式 (使用硬编码的列字母)
                formula1 = f'=C{scm_data_row}&"-"&D{scm_data_row}&CHAR(10)&I{scm_data_row}&"-"&J{scm_data_row}&"-"&K{scm_data_row}&CHAR(10)&"新品组压测意见："&CHAR(10)&"1.顾客："&CHAR(10)&"2.员工："&CHAR(10)&"3.公司："&DN{scm_data_row}&DO{scm_data_row}&CHAR(10)&"4.市场情况：该通用名中康月销"&CR{scm_data_row}&CHAR(10)&"5.通用名结构："&CHAR(10)&"6.供应商条件："&DC{scm_data_row}&"、"&DF{scm_data_row}&"；"&DJ{scm_data_row}&CHAR(10)&"7.医保："&CK{scm_data_row}&"；"&"挂网价："&CM{scm_data_row}&CHAR(10)&"8.铺货："&"标准："&CU{scm_data_row}&"通"&"（新品费："&CV{scm_data_row}&"元）；买手洽谈："&CW{scm_data_row}&"（新品费："&CZ{scm_data_row}&"元）"'
                worksheet.cell(row=excel_row, column=27).value = formula1

                # M2 公式 (使用硬编码的列字母)
                formula2 = f'="【引进理由】"&L{scm_data_row}&CHAR(10)&"【成份】"&EO{scm_data_row}&CHAR(10)&"【适应症】"&EQ{scm_data_row}&CHAR(10)&"【采购总结卖点】"&CHAR(10)&ET{scm_data_row}&CHAR(10)&"【搜索关键词】"&EU{scm_data_row}'
                worksheet.cell(row=excel_row, column=44).value = formula2

            # 2. 处理SCM行 (黄色背景)
            for scm_idx in scm_indices:
                excel_row = scm_idx + 2
                for i in range(1, len(df.columns) + 1):
                    worksheet.cell(row=excel_row, column=i).fill = yellow_fill
            # --- 最后一步：一次性为所有单元格添加边框 ---
            for row in worksheet.iter_rows(min_row=1, max_row=worksheet.max_row, min_col=1, max_col=worksheet.max_column):
                for cell in row:
                    cell.border = thin_border

        output.seek(0)
        filename = f'新品过会分析表_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
        return output, filename
