import streamlit as st
import pandas as pd
from pathlib import Path

# 从各个模块导入所需的类和函数
from config import setup_logging, NATIONAL_DIR_SQL_FILE,PURCHASE_CO_MAPPING_FILE  # <-- 导入新常量
from ui.components import FileUploadWidget
from db.database_handler import SQLProcessor
from processing.data_mapper import MappingProcessor
from processing.data_merger import DataMerger
from processing.data_processor import DataProcessor 
from processing.data_formatter import DataFormatter 
from utils.exporter import ResultExporter
from utils.file_handler import FileProcessor
from utils.persistence import PersistenceManager

# 设置日志
logger = setup_logging()

class NewProductAnalysisApp:
    """新品分析应用主类"""

    def __init__(self):
        """初始化应用，实例化所有处理器"""
        self.file_processor = FileProcessor()
        self.upload_widget = FileUploadWidget()
        self.mapping_processor = MappingProcessor()
        self.sql_processor = SQLProcessor()
        self.data_merger = DataMerger()
        self.data_processor = DataProcessor() 
        self.data_formatter = DataFormatter() 
        self.result_exporter = ResultExporter()
        self.persistence_manager = PersistenceManager()

    def _load_persisted_map(self):
        """在应用会话开始时尝试加载持久化的映射表"""
        if "map_df" not in st.session_state:
            st.session_state["map_df"] = self.persistence_manager.load_dataframe("map_df.pkl")
            if st.session_state["map_df"] is not None:
                st.session_state["map_source"] = "history"
            else:
                st.session_state["map_source"] = None
    
    def _enrich_scm_data(self, scm_df: pd.DataFrame) -> pd.DataFrame:
        """
        使用'国家目录.sql'和本地Excel文件来丰富SCM DataFrame。
        """
        current_df = scm_df.copy()

        # --- 第一步：关联国家医保目录 ---
        status = st.status("正在关联国家医保目录数据...", expanded=False)
        try:
            if NATIONAL_DIR_SQL_FILE.exists():
                sql_query = self.sql_processor.read_sql_file(NATIONAL_DIR_SQL_FILE)
                national_dir_df, _ = self.sql_processor.execute_simple_query(sql_query)
                
                if not national_dir_df.empty and '国家药品编码' in current_df.columns and '国家药品编码' in national_dir_df.columns:
                    current_df['国家药品编码'] = current_df['国家药品编码'].astype(str)
                    national_dir_df['国家药品编码'] = national_dir_df['国家药品编码'].astype(str)
                    
                    cols_to_replace = ['国家医保目录', '省医保目录', '省医保支付价']
                    df_cleaned = current_df.drop(columns=[col for col in cols_to_replace if col in current_df.columns])
                    
                    current_df = pd.merge(df_cleaned, national_dir_df, on='国家药品编码', how='left')
                    #st.success("✅ SCM数据已成功关联国家医保目录信息。")
                else:
                    st.warning("⚠️ 无法关联国家医保目录（缺少关联键或查询为空）。")
            else:
                st.warning(f"⚠️ 未找到 '{NATIONAL_DIR_SQL_FILE}' 文件，医保目录信息将不会关联。")
            status.update(label="医保目录关联完成。", state="complete")
        except Exception as e:
            st.error(f"❌ 关联国家医保目录时出错: {e}")
            status.update(label="医保目录关联失败！", state="error", expanded=True)

        # --- 第二步：关联采购公司与战区映射表 ---
        status = st.status("正在关联采购公司与战区映射...", expanded=False)
        try:
            if PURCHASE_CO_MAPPING_FILE.exists():
                mapping_df = pd.read_excel(PURCHASE_CO_MAPPING_FILE)
                
                join_key = '采购公司'
                target_col = 'lev3_org_name'
                if join_key in current_df.columns and join_key in mapping_df.columns and target_col in mapping_df.columns:
                    current_df[join_key] = current_df[join_key].astype(str)
                    mapping_df[join_key] = mapping_df[join_key].astype(str)

                    # 如果目标列已存在，先删除以避免合并冲突
                    if target_col in current_df.columns:
                        current_df = current_df.drop(columns=[target_col])
                    
                    current_df = pd.merge(
                        current_df,
                        mapping_df[[join_key, target_col]],
                        on=join_key,
                        how='left'
                    )
                    #st.success("✅ SCM数据已成功关联战区信息。")
                else:
                    st.warning("⚠️ 无法关联战区信息（缺少关联键或目标列）。")
            else:
                st.warning(f"⚠️ 未找到 '{PURCHASE_CO_MAPPING_FILE}' 文件，战区信息将不会关联。")
            status.update(label="战区信息关联完成。", state="complete")
        except Exception as e:
            st.error(f"❌ 关联战区信息时出错: {e}")
            status.update(label="战区信息关联失败！", state="error", expanded=True)

        return current_df

    def _inject_custom_css(self):
        st.markdown(
            """
            <style>
                .card { background-color: #f9fafb; border-radius: 12px; padding: 20px; margin-bottom: 18px; box-shadow: 0 2px 8px rgba(0,0,0,0.06); }
                .card-title { font-size: 16px; font-weight: 700; color: #333; margin-bottom: 12px; }
                .muted { color: #666; font-size: 13px; }
                section[data-testid="stFileUploadDropzone"] button::after { content: '浏览文件'; font-size: 14px !important; display: block; }
                section[data-testid="stFileUploadDropzone"] p::after { content: '拖拽文件到此处'; font-size: 1rem !important; display: block; }
            </style>
            """,
            unsafe_allow_html=True,
        )

    def render_header(self):
        st.set_page_config(page_title="新品过会分析表生成工具", layout="wide")
        st.title("🔍 新品过会分析表生成工具")
        st.markdown(
            """
            **操作步骤：** 1. 上传映射关系表 → 2. 上传新品申报数据 → 3. 运行生成 → 4. 下载结果
            """
        )
        st.divider()

    def render_input_section(self):
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown('<div class="card-title">① 数据上传</div>', unsafe_allow_html=True)
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**映射关系表**")
            if st.session_state.get("map_source") == "history":
                st.info("ℹ️ 已自动加载历史映射表。您可以上传新文件进行覆盖。")
            map_file = st.file_uploader("上传定义字段映射关系的Excel文件", type=["xlsx", "xls"], key="map_uploader_new")
            if map_file:
                with st.spinner("正在读取并保存新映射表..."):
                    new_map_df = self.file_processor.read_excel_safe(map_file)
                    st.session_state["map_df"] = new_map_df
                    st.session_state["map_source"] = "upload"
                    self.persistence_manager.save_dataframe(new_map_df, "map_df.pkl")
        with col2:
            st.markdown("**新品申报数据**")
            scm_file = st.file_uploader("上传从SCM系统导出的新品申报Excel文件", type=["xlsx", "xls"], key="scm_uploader_new")
            if scm_file:
                with st.spinner("正在读取新品数据..."):
                    # <-- 核心改动：读取后立即进行数据关联 -->
                    scm_df = self.file_processor.read_excel_safe(scm_file,dtype_spec={'过会编码':str})
                    enriched_scm_df = self._enrich_scm_data(scm_df)
                    st.session_state["scm_df"] = enriched_scm_df
        if st.session_state.get("map_df") is not None:
            df = st.session_state["map_df"]
            source_text = "历史记录" if st.session_state.get("map_source") == "history" else "新上传"
            st.success(f"✅ 映射关系表已加载 ({source_text} - {df.shape[0]} 行, {df.shape[1]} 列)")
        if st.session_state.get("scm_df") is not None:
            df = st.session_state["scm_df"]
            st.success(f"✅ 新品申报数据已加载 ({df.shape[0]} 行，{df.shape[1]} 列)")
        st.markdown('</div>', unsafe_allow_html=True)

    def render_action_section(self):
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown('<div class="card-title">② 执行生成</div>', unsafe_allow_html=True)
        if st.button("🚀 运行", type="primary", use_container_width=True):
            self._process_analysis()
        st.markdown('</div>', unsafe_allow_html=True)

    def _process_analysis(self):
        """核心处理流程"""
        map_df = st.session_state.get("map_df")
        scm_df = st.session_state.get("scm_df")

        if map_df is None or scm_df is None:
            st.error("❌ 请先上传映射关系表和新品申报数据！")
            return

        try:
            status = st.status("准备开始生成…", expanded=True)
            
            status.update(label="提取筛选条件...", state="running")
            scm_common_names = scm_df['通用名'].dropna().unique().tolist()
            scm_strategy_categories = scm_df['策略分类'].dropna().unique().tolist()
            scm_lev3_org_name =scm_df['lev3_org_name'].dropna().unique().tolist()
            scm_cgms = scm_df['采购模式'].dropna().iloc[0]

            status.update(label="🔎 正在从数据库按条件查询对标品数据…", state="running")
            sql_path = Path("对标品.sql")
            if not sql_path.exists():
                status.update(label="❌ 后台SQL文件缺失。", state="error"); return
            
            sql_query = self.sql_processor.read_sql_file(sql_path)
            benchmark_df, executed_sql = self.sql_processor.execute_sql_query(
                sql_query, cgms=scm_cgms,common_names=scm_common_names, strategy_categories=scm_strategy_categories
                ,lev3_org_name=scm_lev3_org_name
            )
            st.session_state["executed_sql"] = executed_sql
            if benchmark_df.empty: st.warning("⚠️ 对标品数据查询为空。")

            status.update(label="🧭 正在进行映射转换与数据合并…", state="running")
            map_scm_df = self.mapping_processor.run_mapping(map_df.copy(), scm_df.copy(), source_type='table2')
            map_benchmark_df = pd.DataFrame()
            if not benchmark_df.empty:
                 map_benchmark_df = self.mapping_processor.run_mapping(map_df.copy(), benchmark_df.copy(), source_type='table3')
            target_df = self.data_merger.merge_and_sort_data(map_scm_df.copy(), map_benchmark_df.copy())
            status.update(label="📊 正在构建分组结构...", state="running")
            processed_df, sep_indices, scm_indices = self.data_processor.insert_group_separators(target_df)
            status.update(label="🎨 正在清理与格式化数据...", state="running")
            formatted_df = self.data_formatter.format_data(processed_df.copy())
            status.update(label="📦 正在生成高级格式的Excel文件…", state="running")
            output, filename = self.result_exporter.export_to_excel(formatted_df, sep_indices, scm_indices)

            st.session_state["result_df"] = formatted_df 
            st.session_state["result_output"] = output
            st.session_state["result_filename"] = filename
            
            status.update(label="🎉 生成完成！", state="complete")

        except Exception as e:
            st.error(f"❌ 分析过程中发生错误: {e}")
            logger.error(f"分析处理错误: {e}", exc_info=True)

    def render_results_section(self):
        """③ 分析结果区域"""
        if "result_df" not in st.session_state:
            return
            
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown('<div class="card-title">③ 生成结果</div>', unsafe_allow_html=True)
        st.success("生成成功，以下是结果摘要与下载：")
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("总计行数", st.session_state["result_df"].shape[0])
        with col2:
            st.metric("总计列数", st.session_state["result_df"].shape[1])
            
        st.download_button(
            label="📥 下载Excel结果文件",
            data=st.session_state["result_output"].getvalue(),
            file_name=st.session_state["result_filename"],
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
        
        with st.expander("点击预览结果数据"):
            st.dataframe(st.session_state["result_df"])
        
        if "executed_sql" in st.session_state:
            with st.expander("点击查看对标品SQL"):
                st.code(st.session_state["executed_sql"], language='sql')
            
        st.markdown('</div>', unsafe_allow_html=True)

    def run(self):
        """运行应用"""
        self.render_header()
        self._inject_custom_css()
        self._load_persisted_map()
        self.render_input_section()
        self.render_action_section()
        self.render_results_section()

def main():
    """应用入口函数"""
    app = NewProductAnalysisApp()
    app.run()

if __name__ == "__main__":
    main()
