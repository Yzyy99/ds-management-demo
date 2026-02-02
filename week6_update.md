# 第六周升级记录（Week 6）

日期：2026-01-08

## 本周目标

- 让血缘数据不只在 Web UI 里可视化，而是可以通过后端 API 被查询/复用。
- 提供“一键启动”，降低 demo 启动成本。
- 对齐多端能力：API/CLI/SDK 支持一次操作生成多个输出数据集（与 Web UI Workstation 的能力一致）。

## 完成内容

### 1) Transform 多输出对齐（API/CLI/SDK）

- 后端 `POST /transform/` 支持多输出：
  - 请求可传 `output_count`（自动生成 N 个输出名）或 `outputs=[{name, description}]`（显式指定输出列表）。
  - 响应新增 `output_datasets`；当只有 1 个输出时，仍兼容保留旧字段 `output_dataset`。
- CLI `python cli.py commit` 支持多输出：
  - `--outputs N`：自动生成 N 个输出（由后端按规则命名）。
  - `--out-name`（可重复）/`--out-desc`：显式指定输出数据集名称与描述。
- SDK `datatrace.log(..., output_name=...)` 现在会把 `output_name` 作为后端 `outputs` 传入，实现“强制命名”。

涉及文件：
- `api_server.py`
- `cli.py`
- `datatrace.py`

### 2) Records / Lineage 查询 API（核心新功能）

- `GET /records`：查询血缘事件（records），支持分页与筛选：
  - 分页：`limit`、`offset`
  - 筛选：`start`、`end`（YYYY-MM-DD 或 ISO datetime）、`op_types`（逗号分隔）、`q`（搜索描述或记录 ID）
- `GET /operations`：列出所有操作类型及其出现次数（用于筛选器/统计）。
- `GET /lineage/{dataset_id}`：查询某个数据集的血缘子图：
  - 参数：`direction=upstream|downstream|both`、`depth`（展开层数）
  - 返回：`nodes`（dataset/operation 节点）与 `edges`（输入→操作→输出）

涉及文件：
- `api_server.py`
- `database.py`

### 3) 一键启动脚本

- 新增 `start_demo.py`：一条命令同时启动后端（uvicorn）+ Web UI（streamlit）。

运行：

```bash
python start_demo.py
```

默认地址：
- API：`http://127.0.0.1:8000`
- Web UI：`http://127.0.0.1:8501`

涉及文件：
- `start_demo.py`
- `README.md`

## 快速验证（建议）

0) 安装依赖（在虚拟环境中执行）：

```bash
python -m pip install -r requirements.txt
```

1) 一键启动：

```bash
python start_demo.py
```

2) 打开 Web UI，生成几条变换记录后，在浏览器访问：

- `http://127.0.0.1:8000/records`
- `http://127.0.0.1:8000/operations`
- `http://127.0.0.1:8000/lineage/ae4ebd5b?direction=both&depth=2`

## 兼容性说明

- 旧客户端如果只理解 `output_dataset` 也能继续用：当输出数量为 1 时，后端仍返回 `output_dataset`。
- Web UI 当前仍是“直连数据库”的模式（未强制改为走 API）；本周新增 API 为后续“Web UI 全改走 API”打基础。
