import streamlit as st
import pandas as pd
from pathlib import Path

# 从各个模块导入所需的类和函数
from config import setup_logging
from ui.components import FileUploadWidget
from db.database_handler import SQLProcessor
from processing.data_mapper import MappingProcessor
from processing.data_merger import DataMerger
from processing.data_processor import DataProcessor 
from processing.data_formatter import DataFormatter 
from processing.strategies import StrategyFactory # <-- 核心改动：导入策略工厂
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
        self.strategy = None # <-- 新增属性，用于持有当前选择的策略实例

    def _load_persisted_map(self):
        """在应用会话开始时尝试加载持久化的映射表"""
        if "map_df" not in st.session_state:
            st.session_state["map_df"] = self.persistence_manager.load_dataframe("map_df.pkl")
            if st.session_state["map_df"] is not None:
                st.session_state["map_source"] = "history"
            else:
                st.session_state["map_source"] = None
    
    # --- 核心改动：删除了 _enrich_scm_data 方法，逻辑已移至策略类 ---

    def _inject_custom_css(self):
        st.markdown(
            """
            <style>
                /* --- 全局与布局 --- */
                .stApp {
                    background-color: #f0f2f6; /* 更柔和的背景色 */
                }
                /* 核心改动：使顶部工具栏背景透明 */
                [data-testid="stHeader"] {
                    background-color: transparent;
                }
                .block-container {
                    padding-top: 2rem;
                    padding-bottom: 2rem;
                }
                
                /* --- 标题与分隔线 --- */
                h1 {
                    color: #1e3a8a; /* 深蓝色主题 */
                    text-align: center;
                    font-weight: 700;
                }
                .main-description p { /* 为主描述文本创建一个特定类 */
                    text-align: center;
                    color: #4b5563;
                }
                hr {
                    background-color: #d1d5db;
                    height: 1px;
                    border: none;
                }

                /* --- 卡片美化 --- */
                .card { 
                    background-color: #ffffff; 
                    border-radius: 12px; 
                    padding: 24px; 
                    margin-bottom: 2rem; /* 增加卡片间距 */
                    box-shadow: 0 10px 25px rgba(0,0,0,0.05), 0 4px 10px rgba(0,0,0,0.02);
                    border: 1px solid #e5e7eb;
                }
                .card-title { 
                    font-size: 1.25rem; 
                    font-weight: 600; 
                    color: #1e3a8a; 
                    margin-bottom: 1.5rem; 
                    padding-bottom: 0.5rem;
                    border-bottom: 2px solid #dbeafe; /* 标题下划线 */
                }

                /* --- 上传区域子标题 --- */
                .uploader-subheader {
                    display: flex;
                    align-items: center;
                    margin-bottom: 0.5rem;
                }
                .uploader-icon {
                    font-size: 1.5rem;
                    margin-right: 0.75rem;
                    line-height: 1;
                }
                .uploader-title {
                    font-size: 1.1rem;
                    font-weight: 600;
                    color: #374151;
                }
                .uploader-description {
                    font-size: 0.9rem;
                    color: #6b7280;
                    margin-bottom: 1rem;
                    text-align: left !important; /* 覆盖全局居中对齐 */
                }
                
                /* --- 悬浮提示 --- */
                .tooltip-container {
                    position: relative;
                    display: inline-block;
                    margin-left: 8px;
                    cursor: help;
                }
                .tooltip-icon {
                    font-size: 1rem;
                    color: #6b7280;
                    border: 1px solid #d1d5db;
                    border-radius: 50%;
                    width: 20px;
                    height: 20px;
                    display: inline-flex;
                    align-items: center;
                    justify-content: center;
                }
                .tooltip-text {
                    visibility: hidden;
                    width: 280px;
                    background-color: #374151;
                    color: #fff;
                    text-align: center;
                    border-radius: 6px;
                    padding: 8px;
                    position: absolute;
                    z-index: 1;
                    bottom: 125%;
                    left: 50%;
                    margin-left: -140px;
                    opacity: 0;
                    transition: opacity 0.3s;
                    font-size: 0.85rem;
                    font-weight: 400;
                }
                .tooltip-container:hover .tooltip-text {
                    visibility: visible;
                    opacity: 1;
                }
                
                /* --- 列分隔线 --- */
                div[data-testid="stHorizontalBlock"] > div:first-child {
                    border-right: 1px dashed #d1d5db;
                    padding-right: 2rem;
                }
                div[data-testid="stHorizontalBlock"] > div:nth-child(2) {
                    padding-left: 2rem;
                }


                /* --- 文件上传组件美化与汉化 --- */
                section[data-testid="stFileUploadDropzone"] {
                    background-color: #f9fafb;
                    border: 2px dashed #d1d5db;
                    border-radius: 8px;
                    padding-top: 1.5rem; /* 增加内部上边距 */
                }
                section[data-testid="stFileUploadDropzone"]:hover {
                    border-color: #3b82f6;
                }
                section[data-testid="stFileUploadDropzone"] button::after { content: '浏览文件'; font-size: 14px !important; display: block; }
                section[data-testid="stFileUploadDropzone"] p::after { content: '拖拽文件到此处'; font-size: 1rem !important; display: block; }

                /* --- 按钮美化 --- */
                div[data-testid="stButton"] > button {
                    border-radius: 8px;
                    padding: 12px 28px;
                    font-weight: 600;
                    transition: all 0.2s ease-in-out;
                    border: none;
                    font-size: 1rem;
                }
                div[data-testid="stButton"] > button[kind="primary"] {
                    background-image: linear-gradient(45deg, #3b82f6, #2563eb);
                    color: white;
                    box-shadow: 0 4px 15px rgba(59, 130, 246, 0.2);
                }
                div[data-testid="stButton"] > button[kind="primary"]:hover {
                    transform: translateY(-2px);
                    box-shadow: 0 6px 20px rgba(59, 130, 246, 0.3);
                }
                div[data-testid="stButton"] > button[kind="secondary"] {
                    background-color: #fee2e2;
                    color: #ef4444;
                    border: 1px solid #fca5a5;
                }
                div[data-testid="stButton"] > button[kind="secondary"]:hover {
                    background-color: #ef4444;
                    color: white;
                }
                div[data-testid="stButton"] > button[kind="secondary"]::before {
                    content: '🛑 ';
                    margin-right: 8px;
                }
                
                /* --- 结果展示区美化 --- */
                div[data-testid="stMetric"] {
                    background-color: #f9fafb;
                    border-left: 5px solid #3b82f6;
                    padding: 1rem;
                    border-radius: 8px;
                }
                div[data-testid="stMetric"] label {
                    font-weight: 500;
                    color: #6b7280;
                }
                div[data-testid="stMetric"] p {
                    font-size: 2rem;
                    font-weight: 700;
                    color: #1e3a8a;
                }
                
                /* --- Expander (可展开区域) 美化 --- */
                div[data-testid="stExpander"] summary {
                    font-weight: 500;
                    color: #374151;
                }
            </style>
            """,
            unsafe_allow_html=True,
        )

    def render_header(self):
        st.set_page_config(page_title="新品过会分析表生成工具", layout="wide")
        st.title("新品过会分析表生成工具")
        st.divider()

    def render_input_section(self):
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown('<div class="card-title">① 数据上传</div>', unsafe_allow_html=True)
        col1, col2 = st.columns(2)
        
        with col1:
            tooltip_text = ""
            if st.session_state.get("map_source") == "history":
                tooltip_text = "已自动加载历史映射表。您可以上传新文件进行覆盖。"
            else:
                tooltip_text = "您上传的映射表将被自动保存，供下次使用。"

            history_tooltip_html = f"""
                <div class="tooltip-container">
                    <span class="tooltip-icon">ℹ️</span>
                    <span class="tooltip-text">{tooltip_text}</span>
                </div>
            """

            st.html(f"""
                <div class="uploader-subheader">
                    <span class="uploader-icon">🔄</span>
                    <span class="uploader-title">映射关系表</span>
                    {history_tooltip_html}
                </div>
                <p class="uploader-description">上传定义字段映射规则的Excel文件。</p>
            """)
            
            map_file = st.file_uploader("上传定义字段映射关系的Excel文件", type=["xlsx", "xls"], key="map_uploader_new", label_visibility="collapsed")

            if map_file:
                with st.spinner("正在读取并保存新映射表..."):
                    new_map_df = self.file_processor.read_excel_safe(map_file)
                    st.session_state["map_df"] = new_map_df
                    st.session_state["map_source"] = "upload"
                    self.persistence_manager.save_dataframe(new_map_df, "map_df.pkl")
            else:
                if st.session_state.get("map_source") == "upload":
                    st.session_state["map_df"] = None
                    st.session_state["map_source"] = None

        with col2:
            st.html("""
                <div class="uploader-subheader">
                    <span class="uploader-icon">📊</span>
                    <span class="uploader-title">新品申报数据</span>
                </div>
                <p class="uploader-description">上传从SCM系统导出的新品Excel文件。</p>
            """)

            scm_file = st.file_uploader("上传从SCM系统导出的新品申报Excel文件", type=["xlsx", "xls"], key="scm_uploader_new", label_visibility="collapsed")
            
            if scm_file:
                with st.spinner("正在读取新品数据..."):
                    dtype_spec = {
                        '过会编码': str,
                        '新品编码': str,
                        '国际条码': str,
                        '国家药品编码': str
                    }
                    scm_df = self.file_processor.read_excel_safe(scm_file, dtype_spec=dtype_spec)
                    # --- 核心改动：此处不再进行数据丰富，直接保存原始DataFrame ---
                    st.session_state["scm_df"] = scm_df 
            else:
                if "scm_df" in st.session_state:
                    st.session_state["scm_df"] = None

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

        if not st.session_state.get("is_running", False):
            if st.button("🚀 运行", type="primary", use_container_width=True):
                if st.session_state.get("map_df") is None or st.session_state.get("scm_df") is None:
                    st.error("❌ 请先上传映射关系表和新品申报数据！")
                else:
                    st.session_state.is_running = True
                    st.rerun()
        else:
            if st.button("中止运行", type="secondary", use_container_width=True):
                st.session_state.is_running = False
                st.warning("操作已中止。")
                if "result_df" in st.session_state:
                    del st.session_state["result_df"]
                st.rerun()

        st.markdown('</div>', unsafe_allow_html=True)

    def _process_analysis(self):
        """核心处理流程 (已使用策略模式重构)"""
        try:
            map_df = st.session_state.get("map_df")
            scm_df = st.session_state.get("scm_df") # <-- 这是原始的SCM数据

            status = st.status("准备开始生成…", expanded=True)
            
            # 1. 获取策略
            status.update(label="识别采购模式...", state="running")
            self.strategy = StrategyFactory.get_strategy(scm_df)
            if not st.session_state.is_running: return

            # 2. 使用策略来丰富数据
            status.update(label="正在准备新品数据 (关联医保/战区)...", state="running")
            enriched_scm_df = self.strategy.enrich_scm_data(scm_df)
            if not st.session_state.is_running: return

            # 3. 使用策略获取SQL查询参数
            status.update(label="提取筛选条件...", state="running")
            query_params = self.strategy.get_benchmark_query_params(enriched_scm_df)
            
            status.update(label="🔎 正在从数据库按条件查询对标品数据…", state="running")
            sql_path = Path("对标品.sql")
            if not sql_path.exists():
                status.update(label="❌ 后台SQL文件缺失。", state="error"); return
            
            sql_query = self.sql_processor.read_sql_file(sql_path) 
            benchmark_df, executed_sql = self.sql_processor.execute_sql_query(sql_query, **query_params)
            st.session_state["executed_sql"] = executed_sql
            if benchmark_df.empty: st.warning("⚠️ 对标品数据查询为空。")
            if not st.session_state.is_running: return

            status.update(label="🧭 正在进行映射转换与数据合并…", state="running")
            map_scm_df = self.mapping_processor.run_mapping(map_df.copy(), enriched_scm_df.copy(), source_type='table2')
            map_benchmark_df = self.mapping_processor.run_mapping(map_df.copy(), benchmark_df.copy(), source_type='table3')
            if not st.session_state.is_running: return
            
            # 4. 在合并时传入策略实例
            target_df = self.data_merger.merge_and_sort_data(map_scm_df.copy(), map_benchmark_df.copy(), self.strategy)
            if not st.session_state.is_running: return
            
            status.update(label="📊 正在构建分组结构...", state="running")
            processed_df, sep_indices, scm_indices = self.data_processor.insert_group_separators(target_df)
            if not st.session_state.is_running: return

            status.update(label="🎨 正在清理与格式化数据...", state="running")
            formatted_df = self.data_formatter.format_data(processed_df.copy())
            if not st.session_state.is_running: return
            
            status.update(label="📦 正在生成高级格式的Excel文件…", state="running")
            # 5. 使用策略获取导出配置
            export_config = self.strategy.get_export_config()
            output, filename = self.result_exporter.export_to_excel(formatted_df, sep_indices, scm_indices, export_config)
            
            st.session_state["new_product_count"] = len(scm_indices) 
            st.session_state["result_df"] = formatted_df 
            st.session_state["result_output"] = output
            st.session_state["result_filename"] = filename
            
            status.update(label="🎉 生成完成！", state="complete")

        except Exception as e:
            st.error(f"❌ 分析过程中发生错误: {e}")
            logger.error(f"分析处理错误: {e}", exc_info=True)
        finally:
            st.session_state.is_running = False

    def render_results_section(self):
        """③ 分析结果区域"""
        if "result_df" not in st.session_state:
            return
            
        result_df = st.session_state["result_df"]
        new_product_count = st.session_state.get("new_product_count", 0)

        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown('<div class="card-title">③ 生成结果</div>', unsafe_allow_html=True)
        st.success("生成成功，以下是结果摘要与下载：")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("新品数", new_product_count)
        with col2:
            st.metric("总计行数", result_df.shape[0])
        with col3:
            st.metric("总计列数", result_df.shape[1])
            
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
        if "is_running" not in st.session_state:
            st.session_state.is_running = False

        self.render_header()
        self._inject_custom_css()
        self._load_persisted_map()
        self.render_input_section()
        self.render_action_section()

        if st.session_state.is_running:
            self._process_analysis()
            st.rerun()
            
        self.render_results_section()

def main():
    """应用入口函数"""
    app = NewProductAnalysisApp()
    app.run()

if __name__ == "__main__":
    main()
