# ds-management-demo（DataTrace Pro）

一个用于演示“数据集登记 + 数据血缘追踪（Lineage）”的最小可运行项目：提供 API / CLI / Python SDK，并用 Streamlit 做一个可视化界面。

## 功能概览

- Dataset Hub：注册、搜索、按标签过滤数据集
- Workstation：选择输入数据集，提交一次“数据变换”，生成一个或多个输出数据集
- Lineage Graph：按时间范围/操作类型过滤，查看血缘图（inputs → operation → outputs）
- CLI：`register / commit / search`（像 git 一样记录数据变更）
- SDK：`datatrace.py` 提供 `init/get_dataset/log/trace`，`demo_script.py` 演示链式血缘

## 代码结构

- `app.py`：Streamlit 前端（DataTrace Pro）
- `api_server.py`：FastAPI 后端（Dataset/Transform API）
- `cli.py`：命令行工具（调用后端 API）
- `datatrace.py`：Python SDK（给业务脚本用）
- `database.py`：SQLite 存取层（默认数据库文件 `datatrace.db`）
- `demo_script.py`：SDK 使用示例

## 快速开始

### 1) 安装依赖

在你自己的虚拟环境里安装（示例命令）：

```bash
python -m pip install -r requirements.txt
```

### 2) 一键启动（推荐）

同时启动 API（FastAPI/uvicorn）+ Web UI（Streamlit）：

```bash
python start_demo.py
```

默认：
- API：`http://127.0.0.1:8000`
- Web UI：`http://127.0.0.1:8501`

### 2) 启动 API 服务

```bash
uvicorn api_server:app --reload
```

默认地址：`http://127.0.0.1:8000`（`datatrace.py` / `cli.py` 默认都指向这个地址）。

### 3) 启动 Web UI（可视化）

```bash
streamlit run app.py
```

### 4) CLI 用法（可选）

先注册一个数据集：

```bash
python cli.py register
```

再提交一次变换（会生成一个输出数据集并写入血缘记录）：

```bash
python cli.py commit
```

搜索：

```bash
python cli.py search coco
```

### 5) SDK / Demo（可选）

先确保 API 已启动，然后运行：

```bash
python demo_script.py
```

## 数据存储说明

- 使用 SQLite，默认文件：`datatrace.db`
- `datasets` 表：数据集元信息（`id/name/description/tags/created_at`）
- `records` 表：血缘事件（`input_ids`、`output_id` 用逗号分隔字符串存储，读取时会解析成列表）

重置本地数据（清空所有数据集/血缘记录）：

```bash
rm -f datatrace.db
```

`database.py` 在导入时会自动初始化表结构并重建空库。

## 已知限制 / 下一步

- 后端 `/transform/` 已支持多输出，并在单输出时兼容保留 `output_dataset` 字段；建议后续为 records 增加专门的 API（例如 `/records/search`）以避免前端直连数据库

## 新增 API（血缘可查询）

- `GET /records?limit=50&offset=0&start=2026-01-01&end=2026-01-31&op_types=Clean,Merge&q=keyword`
- `GET /records?actor=alice&source=web&run_id=batch_001`（按操作者/来源/批次过滤）
- `GET /records?dataset_id=ae4ebd5b&direction=both&depth=3`（限定为某数据集“祖先/后代”范围内的 records）
- `GET /operations`
- `GET /operations?dataset_id=ae4ebd5b&direction=upstream&depth=2`（按血缘范围聚合 operation 计数）
- `GET /lineage/{dataset_id}?direction=both&depth=2`
- `GET /lineage/{dataset_id}?direction=both&depth=2&start=2026-01-01&end=2026-01-31&op_types=Clean,Merge&q=keyword`（血缘子图过滤）
- `GET /report/{dataset_id}?direction=both&depth=2`（一键导出 Markdown 报告）

## 时间序列 API（小规模试验）

- `POST /timeseries/{dataset_id}`（写入时间序列点）
- `POST /timeseries/{dataset_id}/generate?freq=daily&periods=60&amplitude=10&noise=1`（生成样例）
- `GET /timeseries/{dataset_id}?start=2026-01-01&end=2026-03-01&metric=value`
- 继承规则：由数据集生成新数据集时，会默认复制时间序列（多输入会自动加前缀区分来源）

### /timeseries/{dataset_id}/generate 参数说明

- `freq`：采样频率，可选 `daily` / `hourly`，默认 `daily`
- `periods`：点数，默认 `60`（1 ~ 5000）
- `amplitude`：季节波动幅度，默认 `10.0`
- `noise`：噪声幅度，默认 `1.0`
- `trend`：线性趋势斜率，默认 `0.05`
- `metric`：指标名，默认 `value`
- `start`：起始时间（YYYY-MM-DD 或 ISO datetime），默认“向前回溯 periods 个单位”
