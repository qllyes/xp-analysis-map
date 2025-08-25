import streamlit as st
import pandas as pd
from typing import Optional
from pathlib import Path
from utils.file_handler import FileProcessor # 确保导入FileProcessor

class FileUploadWidget:
    """文件上传组件类，提供更好的状态管理和备选加载方式"""

    def __init__(self):
        self.file_processor = FileProcessor()

    def render(self, label: str, session_state_key: str) -> Optional[pd.DataFrame]:
        """
        渲染文件上传组件并返回加载的数据
        
        Args:
            label: 上传组件的标签
            session_state_key: session state中保存数据的键
            
        Returns:
            加载的DataFrame或None
        """
        # 初始化session state
        if session_state_key not in st.session_state:
            st.session_state[session_state_key] = None
        
        uploader_key = f"uploader_{session_state_key}"
        
        # 文件上传器
        uploaded_file = st.file_uploader(
            label, 
            type=['xlsx', 'xls'], 
            key=uploader_key,
            help="支持.xlsx和.xls格式的Excel文件"
        )
        
        # 处理文件上传
        if uploaded_file:
            return self._handle_file_upload(uploaded_file, session_state_key)
        
        # 如果文件被移除，清空session_state
        if not uploaded_file and st.session_state.get(session_state_key) is not None:
             # 这个逻辑在Streamlit的rerun机制下可能需要更复杂的处理，
             # 但对于简单场景，依赖于每次上传都重新加载是可行的。
             pass

        # 添加手动文件路径输入作为备选方案
        with st.expander("🔧 高级选项: 从本地路径加载"):
            manual_path = st.text_input(
                "手动指定文件路径:",
                key=f"manual_path_{session_state_key}",
                help="例如: C:/Users/用户名/Desktop/文件.xlsx"
            )
            
            if st.button("📁 从路径加载", key=f"load_manual_{session_state_key}"):
                if manual_path and Path(manual_path).exists():
                    try:
                        with st.spinner("正在读取文件..."):
                            data = self.file_processor.read_excel_safe(Path(manual_path))
                        st.session_state[session_state_key] = data
                        st.success(f"✅ 成功加载: {Path(manual_path).name} ({len(data)} 行)")
                        # st.experimental_rerun() # 加载后可以刷新页面
                        return data
                    except Exception as e:
                        st.error(f"❌ 读取失败: {str(e)}")
                        return None
                elif manual_path:
                    st.error("❌ 文件路径不存在")
                else:
                    st.warning("⚠️ 请输入有效路径")
        
        return st.session_state.get(session_state_key)

    def _handle_file_upload(self, uploaded_file, session_state_key: str) -> Optional[pd.DataFrame]:
        """处理文件上传逻辑"""
        try:
            file_size_mb = len(uploaded_file.getvalue()) / (1024 * 1024)
            if file_size_mb > 50:
                st.warning(f"⚠️ 文件较大({file_size_mb:.2f}MB)，读取可能需要一些时间...")
            
            with st.spinner(f"正在读取文件 {uploaded_file.name}..."):
                data = self.file_processor.read_excel_safe(uploaded_file)
            
            st.session_state[session_state_key] = data
            st.success(f"✅ 成功加载: {uploaded_file.name} ({len(data)} 行, {len(data.columns)} 列)")
            return data
            
        except Exception as e:
            st.error(f"❌ 读取文件失败: {uploaded_file.name}. 错误: {e}")
            st.info("💡 建议：请检查文件是否已损坏或格式不兼容。")
            st.session_state[session_state_key] = None
            return None
