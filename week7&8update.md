# 第七&八周升级记录（Week 7 & Week 8）

日期：2026-01-14（Week 7） / 2026-01-22（Week 8）

## Week 7 目标

- 溯源图检索支持“指定一个数据集作为 focus”，并能筛选它相关的操作：它的祖先/后代（upstream/downstream/both）与展开层数（depth）。
- 让后端的 records / operations / lineage 三套查询能力能围绕同一个“数据集血缘范围”协同工作，便于 UI/外部工具复用。

## Week 7 完成内容

### 1) 血缘范围（Dataset-scoped）过滤能力打通

- `GET /records` 新增按数据集血缘范围筛选：
  - 参数：`dataset_id`、`direction=upstream|downstream|both`、`depth`
  - 与原有筛选可叠加：`start`、`end`、`op_types`、`q` + `limit/offset`
- `GET /operations` 新增按数据集血缘范围聚合统计：
  - 参数：`dataset_id`、`direction`、`depth`（可选叠加 `start/end/q`）
- `GET /lineage/{dataset_id}` 支持过滤后再生成子图：
  - 新增参数：`start`、`end`、`op_types`、`q`
  - 去重 `edges`，避免多次遍历导致的重复边

涉及文件：
- `api_server.py`
- `database.py`

### 2) Streamlit：Lineage Intelligence 支持 focus dataset + 祖先/后代

- 侧边栏新增：
  - Focus Dataset（可选）
  - `Direction`（both/upstream/downstream）
  - `Depth`（展开层数）
- 操作类型列表会根据“时间范围 + focus 血缘范围”动态生成，更贴合实际筛选体验。
- 图中 dataset 节点改用 `dataset_id` 作为 Graphviz node id（避免同名数据集冲突），并高亮 focus 数据集节点。

涉及文件：
- `app.py`

### 3) CLI / SDK 增补查询能力

- CLI 新增：
  - `python cli.py lineage <dataset_id> [--direction ... --depth ... --start ... --end ... --op-types ... --q ...]`
  - `python cli.py ops [--dataset-id ... --direction ... --depth ... --start ... --end ... --q ...]`
- SDK 新增：
  - `datatrace.get_records(...)`
  - `datatrace.get_operations(...)`
  - `datatrace.get_lineage(...)`

涉及文件：
- `cli.py`
- `datatrace.py`

## Week 7 快速验证（建议）

1) 启动：

```bash
python start_demo.py
```

2) 生成一些变换记录后，访问示例：

- `http://127.0.0.1:8000/records?dataset_id=ae4ebd5b&direction=both&depth=3`
- `http://127.0.0.1:8000/operations?dataset_id=ae4ebd5b&direction=both&depth=3`
- `http://127.0.0.1:8000/lineage/ae4ebd5b?direction=both&depth=3&op_types=Clean,Merge`

3) Web UI：进入 `Lineage Intelligence`，选择 Focus Dataset + Direction/Depth，观察子图与操作筛选联动。

---

# Week 8 更新（图更好看 / 更好记 / 更快）

日期：2026-01-22

## Week 8 目标

- 图更好看：Lineage 图视觉统一、重点更突出。
- 更好记：每条血缘记录能知道“是谁做的/从哪来的/属于哪次运行”。
- 更快：常用查询加索引，记录多了也不卡。

## Week 8 完成内容

### 1) 图更好看（Lineage Graph 视觉优化）

- 数据集/操作节点配色统一，Focus 数据集更显眼。
- 操作节点显示操作者（@actor），更直观。
- Graphviz 参数优化（间距、箭头、字体）。

涉及文件：
- `app.py`

### 2) 更好记（记录元信息）

- records 新增字段：`actor` / `source` / `run_id`
- API `POST /transform/` 支持写入这些字段（默认 `actor=anonymous`、`source=api`）
- `GET /records` 支持按 `actor/source/run_id` 过滤
- Lineage 子图节点携带 `actor/source/run_id`
- Web UI / CLI / SDK 都可写入 actor/run_id

涉及文件：
- `api_server.py`
- `database.py`
- `app.py`
- `cli.py`
- `datatrace.py`

### 3) 更快（索引优化）

- 为 `records.timestamp` / `operation_name` / `actor` / `source` / `run_id` 增加索引

涉及文件：
- `database.py`

## Week 8 如何使用

### 方式 A：只用前端（推荐）

1) 启动：

```bash
python start_demo.py
```

2) 进入 `Lineage Intelligence`：
   - 选择 Focus Dataset（可选）
   - 选择方向（上游/下游/双向）与 Depth
   - 图会自动高亮 Focus 数据集并显示操作者

3) 在 `Workstation` 里提交操作：
   - 填 `Actor`（是谁做的）
   - 可选 `Run ID`（一次批量运行的标识）
   - 这些信息会写进血缘记录，便于追溯

### 方式 B：API（给工具/脚本用）

- 按操作者/来源/批次过滤 records：
  - `GET /records?actor=alice&source=web&run_id=batch_001`
- 仍支持按数据集血缘范围 + 时间筛选：
  - `GET /records?dataset_id=ae4ebd5b&direction=both&depth=2&start=2026-01-01&end=2026-01-31`

### 方式 C：CLI/SDK（可选）

- CLI 记录 actor/run_id：
  - `python cli.py commit --actor alice --run-id batch_001`
- SDK 自动填 `actor=CONFIG["USER"]`，如需可显式传 `run_id`：
  - `datatrace.log(..., run_id="batch_001")`
