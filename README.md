# 新品分析工具

这是一个基于 Streamlit 的 Web 应用，用于分析 SCM 导出的新品申报数据和新品所属的对标品数据，并生成目标表。

## 功能特性

1. 上传三张原始基础表：
   - SCM新品申报字段与目标表字段的映射关系表（Excel）
   - SCM的新品申报数据（Excel）
   - 对标品数据字段与目标表字段的映射关系表（Excel）

2. 数据处理：
   - 将SCM新品申报数据映射到目标表字段
   - 执行对标品数据的SQL查询并筛选样本
   - 将对标品数据映射到目标表字段
   - 合并映射后的数据并按指定规则排序

3. 结果导出：
   - 生成带有黄色背景标记的Excel目标表

## 安装和运行

### 使用 uv（推荐）

1. 安装依赖：
   ```bash
   uv sync --extra web
   ```

2. 运行应用：
   ```bash
   uv run streamlit run new_product_analysis.py
   ```

### 使用 pip

1. 安装依赖：
   ```bash
   pip install pandas openpyxl streamlit
   ```

2. 运行应用：
   ```bash
   streamlit run new_product_analysis.py
   ```

## 使用说明

1. 打开应用后，您会看到三个文件上传区域：
   - SCM新品申报映射表
   - SCM新品申报数据
   - 对标品数据映射表

2. 依次上传所需的三个文件

3. 点击"运行分析"按钮开始处理数据

4. 处理完成后，点击"下载Excel结果文件"按钮下载结果

## 注意事项

- 由于缺少数据库连接信息，对标品数据目前使用SCM数据的一个副本来模拟。在实际使用中，请修改代码以连接到真实的数据库并执行SQL查询。
- 确保上传的Excel文件格式正确，支持.xlsx和.xls格式
- SQL文件应包含有效的查询语句