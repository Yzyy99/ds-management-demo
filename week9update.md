# 第九周升级记录（Week 9）

日期：2026-01-29

## 本周目标

- 提供“一键导出可分享报告”的能力，面向数据分析师/AI 工程师使用场景。
- 把血缘图 + 关键操作记录汇总成一份 Markdown 报告，便于分享与汇报。
- 引入真实时间序列数据（小规模试验），贴合“时间序列工具集”的开题方向。

## 完成内容

### 1) 一键导出可分享报告（Markdown）

- 前端 Lineage 页面支持一键导出 Markdown 报告。
- 报告包含：Scope（方向/深度/时间/操作类型）+ Summary（节点/边/记录数）+ 最近 20 条操作记录。
- 后端新增导出接口：`GET /report/{dataset_id}`，可叠加过滤（方向/深度/时间/操作类型/actor/source/run_id）。

涉及文件：
- `app.py`
- `api_server.py`
- `README.md`
- `cli.py`
- `datatrace.py`

## 如何使用

### 方式 A：前端一键导出（推荐）

1) 启动：

```bash
python start_demo.py
```

2) 进入 `Lineage Intelligence`：
   - 选择 Focus Dataset
   - 选择方向/深度/时间范围/操作类型
   - 点击 `Export Report (Markdown)` 即可下载

### 方式 B：API（工具/集成用）

- 基础导出：
  - `GET /report/{dataset_id}?direction=both&depth=2`
- 带过滤导出：
  - `GET /report/{dataset_id}?direction=upstream&depth=3&start=2026-01-01&end=2026-01-31&op_types=Clean,Merge`
  - `GET /report/{dataset_id}?actor=alice&source=web&run_id=batch_001`

### 方式 C：CLI / SDK（可选）

- CLI：
  - `python cli.py report <dataset_id> --out report.md`
- SDK：
- `datatrace.get_report(<dataset_id>)`

---

### 2) 时间序列数据小规模试验

- 新增 `timeseries` 表，用于存放真实时间序列点（timestamp + value + metric）。
- 提供 API 与前端入口，可为某个数据集生成/查看时间序列样例。
- 目的：不再只是“空壳数据集名”，而是有实际时间序列数据可展示与试验。
- **直接继承**：当基于某数据集生成新数据集时，默认复制其时间序列（多输入时会自动加前缀以区分来源）。

涉及文件：
- `database.py`
- `api_server.py`
- `app.py`
- `datatrace.py`
- `README.md`

## 如何使用（时间序列）

### 方式 A：前端（推荐）

1) 启动：

```bash
python start_demo.py
```

2) 进入 `Time Series Lab`：
   - 选择一个 Dataset
   - 点击 `Generate Sample Series` 生成样例数据
   - 查看折线图与原始数据

### 方式 B：API

- 生成样例：
  - `POST /timeseries/{dataset_id}/generate?freq=daily&periods=60&amplitude=10&noise=1`
- 读取数据：
  - `GET /timeseries/{dataset_id}?start=2026-01-01&end=2026-03-01&metric=value`

### 方式 C：SDK（可选）

- 生成样例：
  - `datatrace.generate_timeseries(<dataset_id>, periods=120)`
- 读取数据：
  - `datatrace.get_timeseries(<dataset_id>)`
