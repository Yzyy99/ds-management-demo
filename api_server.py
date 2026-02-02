from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel
from typing import List, Optional
import database as db
import uuid
from datetime import datetime, timedelta
from collections import Counter
import random
import math

app = FastAPI(title="DataTrace API", version="1.0")

# --- Pydantic 模型 (用于请求体验证) ---
class DatasetCreate(BaseModel):
    name: str
    description: str
    tags: List[str]

class OutputSpec(BaseModel):
    name: str
    description: str = ""

class RecordCreate(BaseModel):
    input_ids: List[str]
    operation: str
    description: str
    actor: Optional[str] = None
    source: Optional[str] = None
    run_id: Optional[str] = None
    # 兼容旧字段：目前后端不使用该字段来命名
    output_suffix: str = "_processed"
    # 与 Web UI 对齐：支持单次操作生成多个输出数据集
    output_count: int = 1
    outputs: Optional[List[OutputSpec]] = None

    class Config:
        extra = "ignore"

class TimeseriesPoint(BaseModel):
    timestamp: str
    value: float
    metric: Optional[str] = None

class TimeseriesBatch(BaseModel):
    points: List[TimeseriesPoint]

def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    v = value.strip()
    if not v:
        return None
    # 支持 YYYY-MM-DD 或 ISO datetime
    try:
        if len(v) == 10 and v[4] == "-" and v[7] == "-":
            return datetime.strptime(v, "%Y-%m-%d")
        return datetime.fromisoformat(v)
    except Exception:
        raise HTTPException(status_code=400, detail=f"Invalid datetime format: {value}")

# --- API 路由 ---

@app.get("/")
def health_check():
    return {"status": "running", "system": "DataTrace Pro"}

# api_server.py 修改部分

@app.post("/datasets/")
def create_dataset(item: DatasetCreate):
    # 1. 先检查是否已存在同名数据集
    existing = db.search_datasets(query=item.name)
    # 精确匹配名称
    target = next((d for d in existing if d['name'] == item.name), None)
    
    if target:
        # 如果存在，直接返回旧的 ID，不报错 (Idempotency)
        return {
            "id": target['id'], 
            "name": target['name'], 
            "message": "Dataset already exists, returning existing ID.",
            "new": False
        }

    # 2. 不存在则创建
    ds_id = str(uuid.uuid4())[:8]
    db.add_dataset(ds_id, item.name, item.description, item.tags)
    return {
        "id": ds_id, 
        "name": item.name, 
        "message": "Dataset registered successfully",
        "new": True
    }

@app.get("/datasets/search")
def search_datasets(q: Optional[str] = None, tags: Optional[str] = None):
    # tags 传入逗号分隔字符串
    tag_list = tags.split(",") if tags else []
    results = db.search_datasets(query=q, tags=tag_list)
    return {"count": len(results), "results": results}

@app.get("/records")
def list_records(
    start: Optional[str] = None,
    end: Optional[str] = None,
    op_types: Optional[str] = None,
    q: Optional[str] = None,
    actor: Optional[str] = None,
    source: Optional[str] = None,
    run_id: Optional[str] = None,
    dataset_id: Optional[str] = None,
    direction: str = "both",
    depth: int = 2,
    limit: int = 50,
    offset: int = 0,
):
    """
    查询血缘事件 records（支持分页/筛选），用于 UI/外部工具把血缘当作“可查询的数据产品”。
    - start/end: YYYY-MM-DD 或 ISO datetime
    - op_types: 逗号分隔，例如 Clean,Merge
    - q: 搜索 operation_desc 或 record id
    - actor/source/run_id: 记录来源/操作者筛选
    - limit/offset: 分页
    """
    if limit < 1 or limit > 200:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 200")
    if offset < 0:
        raise HTTPException(status_code=400, detail="offset must be >= 0")

    start_dt = _parse_datetime(start)
    end_dt = _parse_datetime(end)
    ops = [s.strip() for s in op_types.split(",") if s.strip()] if op_types else None

    records = db.get_filtered_records(
        start_date=start_dt,
        end_date=end_dt,
        op_types=ops,
        search_q=q,
        actor=actor,
        source=source,
        run_id=run_id,
        limit=None,
        offset=0,
    )

    if dataset_id:
        scope_ds = db.get_dataset_by_id(dataset_id)
        if not scope_ds:
            raise HTTPException(status_code=404, detail=f"Dataset {dataset_id} not found")
        try:
            record_ids, _ = db.collect_lineage_record_ids(dataset_id, records, direction=direction, depth=depth)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        records = [r for r in records if r.get("id") in record_ids]

    total = len(records)
    results = records[int(offset or 0) : int(offset or 0) + int(limit)]
    payload = {"count": total, "limit": limit, "offset": offset, "results": results}
    if dataset_id:
        payload.update({"dataset_id": dataset_id, "direction": direction, "depth": depth})
    return payload

@app.get("/timeseries/{dataset_id}")
def get_timeseries(
    dataset_id: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    metric: Optional[str] = None,
    limit: int = 1000,
):
    if limit < 1 or limit > 10000:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 10000")
    ds = db.get_dataset_by_id(dataset_id)
    if not ds:
        raise HTTPException(status_code=404, detail=f"Dataset {dataset_id} not found")
    results = db.get_timeseries(dataset_id, start=start, end=end, metric=metric, limit=limit)
    return {"dataset_id": dataset_id, "count": len(results), "results": results}

@app.post("/timeseries/{dataset_id}")
def add_timeseries(dataset_id: str, batch: TimeseriesBatch):
    ds = db.get_dataset_by_id(dataset_id)
    if not ds:
        raise HTTPException(status_code=404, detail=f"Dataset {dataset_id} not found")
    points = [{"timestamp": p.timestamp, "value": p.value} for p in batch.points]
    metric = batch.points[0].metric if batch.points and batch.points[0].metric else "value"
    inserted = db.add_timeseries_points(dataset_id, points, metric=metric)
    return {"dataset_id": dataset_id, "inserted": inserted, "metric": metric}

@app.post("/timeseries/{dataset_id}/generate")
def generate_timeseries(
    dataset_id: str,
    start: Optional[str] = None,
    freq: str = "daily",
    periods: int = 60,
    amplitude: float = 10.0,
    noise: float = 1.0,
    trend: float = 0.05,
    metric: str = "value",
):
    """
    生成简单的时间序列样例（sin + trend + noise），用于小规模实验。
    freq: daily | hourly
    """
    ds = db.get_dataset_by_id(dataset_id)
    if not ds:
        raise HTTPException(status_code=404, detail=f"Dataset {dataset_id} not found")
    if periods < 1 or periods > 5000:
        raise HTTPException(status_code=400, detail="periods must be between 1 and 5000")
    if freq not in ("daily", "hourly"):
        raise HTTPException(status_code=400, detail="freq must be daily or hourly")

    if start:
        base_dt = _parse_datetime(start)
    else:
        base_dt = datetime.now() - (timedelta(days=periods) if freq == "daily" else timedelta(hours=periods))
    step = timedelta(days=1) if freq == "daily" else timedelta(hours=1)

    points = []
    for i in range(periods):
        t = base_dt + step * i
        seasonal = amplitude * math.sin(2 * math.pi * i / max(10, periods // 3))
        value = seasonal + (trend * i) + random.uniform(-noise, noise)
        points.append({"timestamp": t.strftime("%Y-%m-%d %H:%M:%S"), "value": float(value)})
    inserted = db.add_timeseries_points(dataset_id, points, metric=metric)
    return {"dataset_id": dataset_id, "inserted": inserted, "metric": metric}

@app.get("/operations")
def list_operations(
    start: Optional[str] = None,
    end: Optional[str] = None,
    q: Optional[str] = None,
    actor: Optional[str] = None,
    source: Optional[str] = None,
    run_id: Optional[str] = None,
    dataset_id: Optional[str] = None,
    direction: str = "both",
    depth: int = 2,
):
    """列出 operation 类型及其次数（支持按时间 / 搜索 / 数据集血缘范围聚合）。"""
    start_dt = _parse_datetime(start)
    end_dt = _parse_datetime(end)

    records = db.get_filtered_records(
        start_date=start_dt,
        end_date=end_dt,
        op_types=None,
        search_q=q,
        actor=actor,
        source=source,
        run_id=run_id,
        limit=None,
        offset=0,
    )

    if dataset_id:
        scope_ds = db.get_dataset_by_id(dataset_id)
        if not scope_ds:
            raise HTTPException(status_code=404, detail=f"Dataset {dataset_id} not found")
        try:
            record_ids, _ = db.collect_lineage_record_ids(dataset_id, records, direction=direction, depth=depth)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        records = [r for r in records if r.get("id") in record_ids]

    counter = Counter()
    for r in records:
        op = r.get("operation_name")
        if op:
            counter[op] += 1

    results = [{"operation": op, "count": int(cnt)} for op, cnt in sorted(counter.items(), key=lambda kv: (-kv[1], kv[0]))]
    payload = {"count": len(results), "results": results}
    if dataset_id:
        payload.update({"dataset_id": dataset_id, "direction": direction, "depth": depth})
    return payload

@app.post("/transform/")
def create_transformation(item: RecordCreate):
    # 1. 验证输入数据集是否存在
    inputs = []
    for i_id in item.input_ids:
        ds = db.get_dataset_by_id(i_id)
        if not ds:
            raise HTTPException(status_code=404, detail=f"Input dataset {i_id} not found")
        inputs.append(ds)
    
    # 2. 生成输出配置（支持多输出）
    input_names = [d['name'] for d in inputs]
    base_name = "merged_dataset" if len(input_names) > 1 else input_names[0]
    op_slug = (item.operation or "").strip().lower().replace(" ", "_") or "transformed"

    if item.outputs:
        output_specs = item.outputs
    else:
        count = max(1, int(item.output_count or 1))
        if count == 1:
            default_names = [f"{base_name}_{op_slug}"]
        else:
            default_names = [f"{base_name}_{op_slug}_{idx + 1}" for idx in range(count)]
        output_specs = [OutputSpec(name=n, description="") for n in default_names]
    
    # 3. 继承标签
    all_tags = set()
    for d in inputs:
        if d['tags']: all_tags.update(d['tags'].split(","))
    all_tags.add(op_slug)
    all_tags.add("generated")
    
    # 4. 写入数据库
    rec_id = str(uuid.uuid4())[:8]

    created_outputs = []
    output_ids = []
    for spec in output_specs:
        name = (spec.name or "").strip()
        if not name:
            raise HTTPException(status_code=400, detail="Output dataset name cannot be empty")

        new_id = str(uuid.uuid4())[:8]
        new_desc = spec.description.strip() if spec.description else ""
        if not new_desc:
            new_desc = f"Generated via {item.operation} from {', '.join(input_names)}. {item.description}"

        db.add_dataset(new_id, name, new_desc, list(all_tags))
        output_ids.append(new_id)
        created_outputs.append({"id": new_id, "name": name})

        # 时间序列数据继承：默认复制输入数据集的序列
        db.copy_timeseries(item.input_ids, new_id, prefix_metric=(len(item.input_ids) > 1))

    actor = (item.actor or "").strip() or "anonymous"
    source = (item.source or "").strip() or "api"
    run_id = (item.run_id or "").strip() or None
    db.add_record(rec_id, item.input_ids, item.operation, item.description, output_ids, actor=actor, source=source, run_id=run_id)
    
    payload = {
        "record_id": rec_id,
        "output_datasets": created_outputs,
    }
    # 兼容旧客户端：单输出时保留 output_dataset 字段
    if len(created_outputs) == 1:
        payload["output_dataset"] = created_outputs[0]
    return payload

@app.get("/lineage/{dataset_id}")
def get_lineage(
    dataset_id: str,
    direction: str = "both",
    depth: int = 2,
    start: Optional[str] = None,
    end: Optional[str] = None,
    op_types: Optional[str] = None,
    q: Optional[str] = None,
):
    """
    查询某个数据集的血缘子图（上游/下游/双向）。
    - direction: upstream | downstream | both
    - depth: 展开层数（按“数据集节点”计）
    """
    direction = (direction or "").strip().lower()
    if direction not in ("upstream", "downstream", "both"):
        raise HTTPException(status_code=400, detail="direction must be upstream, downstream, or both")
    if depth < 0 or depth > 10:
        raise HTTPException(status_code=400, detail="depth must be between 0 and 10")

    root = db.get_dataset_by_id(dataset_id)
    if not root:
        raise HTTPException(status_code=404, detail=f"Dataset {dataset_id} not found")

    start_dt = _parse_datetime(start)
    end_dt = _parse_datetime(end)
    ops = [s.strip() for s in op_types.split(",") if s.strip()] if op_types else None

    all_datasets = {d["id"]: d for d in db.get_all_datasets()}
    all_records = db.get_filtered_records(start_date=start_dt, end_date=end_dt, op_types=ops, search_q=q, limit=None, offset=0)

    inputs_index = {}
    outputs_index = {}
    for rec in all_records:
        for i in rec.get("input_ids", []):
            inputs_index.setdefault(i, []).append(rec)
        for o in rec.get("output_ids", []):
            outputs_index.setdefault(o, []).append(rec)

    def ds_node(ds_id: str):
        ds = all_datasets.get(ds_id) or db.get_dataset_by_id(ds_id)
        if not ds:
            return {"id": ds_id, "type": "dataset", "name": ds_id}
        return {"id": ds_id, "type": "dataset", "name": ds.get("name"), "tags": ds.get("tags"), "created_at": ds.get("created_at")}

    nodes = {}
    edges = []
    edge_set = set()
    nodes[dataset_id] = ds_node(dataset_id)

    visited = set([dataset_id])
    frontier = set([dataset_id])

    def add_edge(source: str, target: str):
        key = (source, target)
        if key in edge_set:
            return
        edge_set.add(key)
        edges.append({"source": source, "target": target})

    for _ in range(depth):
        if not frontier:
            break
        next_frontier = set()
        for ds_id in list(frontier):
            if direction in ("downstream", "both"):
                for rec in inputs_index.get(ds_id, []):
                    op_id = f"op:{rec['id']}"
                    if op_id not in nodes:
                        nodes[op_id] = {
                            "id": op_id,
                            "type": "operation",
                            "name": rec.get("operation_name"),
                            "timestamp": rec.get("timestamp"),
                            "desc": rec.get("operation_desc"),
                            "record_id": rec.get("id"),
                            "actor": rec.get("actor"),
                            "source": rec.get("source"),
                            "run_id": rec.get("run_id"),
                        }
                    for inp in rec.get("input_ids", []):
                        if inp not in nodes:
                            nodes[inp] = ds_node(inp)
                        add_edge(inp, op_id)
                    for out in rec.get("output_ids", []):
                        if out not in nodes:
                            nodes[out] = ds_node(out)
                        add_edge(op_id, out)
                        if out not in visited:
                            visited.add(out)
                            next_frontier.add(out)

            if direction in ("upstream", "both"):
                for rec in outputs_index.get(ds_id, []):
                    op_id = f"op:{rec['id']}"
                    if op_id not in nodes:
                        nodes[op_id] = {
                            "id": op_id,
                            "type": "operation",
                            "name": rec.get("operation_name"),
                            "timestamp": rec.get("timestamp"),
                            "desc": rec.get("operation_desc"),
                            "record_id": rec.get("id"),
                            "actor": rec.get("actor"),
                            "source": rec.get("source"),
                            "run_id": rec.get("run_id"),
                        }
                    for inp in rec.get("input_ids", []):
                        if inp not in nodes:
                            nodes[inp] = ds_node(inp)
                        add_edge(inp, op_id)
                        if inp not in visited:
                            visited.add(inp)
                            next_frontier.add(inp)
                    for out in rec.get("output_ids", []):
                        if out not in nodes:
                            nodes[out] = ds_node(out)
                        add_edge(op_id, out)

        frontier = next_frontier

    return {
        "root": dataset_id,
        "direction": direction,
        "depth": depth,
        "filters": {"start": start, "end": end, "op_types": op_types, "q": q},
        "nodes": list(nodes.values()),
        "edges": edges,
    }

@app.get("/report/{dataset_id}")
def export_report(
    dataset_id: str,
    direction: str = "both",
    depth: int = 2,
    start: Optional[str] = None,
    end: Optional[str] = None,
    op_types: Optional[str] = None,
    q: Optional[str] = None,
    actor: Optional[str] = None,
    source: Optional[str] = None,
    run_id: Optional[str] = None,
    format: str = "md",
):
    """导出可分享报告（Markdown）。"""
    if format not in ("md", "markdown"):
        raise HTTPException(status_code=400, detail="format must be 'md'")

    root = db.get_dataset_by_id(dataset_id)
    if not root:
        raise HTTPException(status_code=404, detail=f"Dataset {dataset_id} not found")

    start_dt = _parse_datetime(start)
    end_dt = _parse_datetime(end)
    ops = [s.strip() for s in op_types.split(",") if s.strip()] if op_types else None

    records = db.get_filtered_records(
        start_date=start_dt,
        end_date=end_dt,
        op_types=ops,
        search_q=q,
        actor=actor,
        source=source,
        run_id=run_id,
        limit=None,
        offset=0,
    )

    try:
        record_ids, dataset_ids = db.collect_lineage_record_ids(dataset_id, records, direction=direction, depth=depth)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    records = [r for r in records if r.get("id") in record_ids]

    op_set = sorted({r.get("operation_name") for r in records if r.get("operation_name")})
    ds_set = set(dataset_ids)

    edge_set = set()
    op_nodes = set()
    for rec in records:
        op_id = f"op:{rec.get('id')}"
        op_nodes.add(op_id)
        for i_id in rec.get("input_ids", []) or []:
            edge_set.add((i_id, op_id))
        for o_id in rec.get("output_ids", []) or []:
            edge_set.add((op_id, o_id))

    timestamps = [r.get("timestamp") for r in records if r.get("timestamp")]
    time_min = min(timestamps) if timestamps else None
    time_max = max(timestamps) if timestamps else None

    title = f"DataTrace Report - {root.get('name')} ({dataset_id})"
    lines = [
        f"# {title}",
        "",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        "",
        "## Scope",
        f"- direction: {direction}",
        f"- depth: {depth}",
    ]
    if start or end:
        lines.append(f"- time: {start or 'N/A'} ~ {end or 'N/A'}")
    if ops:
        lines.append(f"- op_types: {', '.join(ops)}")
    if q:
        lines.append(f"- q: {q}")
    if actor:
        lines.append(f"- actor: {actor}")
    if source:
        lines.append(f"- source: {source}")
    if run_id:
        lines.append(f"- run_id: {run_id}")

    lines += [
        "",
        "## Summary",
        f"- records: {len(records)}",
        f"- datasets: {len(ds_set)}",
        f"- operations: {len(op_set)}",
        f"- nodes: {len(ds_set) + len(op_nodes)}",
        f"- edges: {len(edge_set)}",
    ]
    if time_min or time_max:
        lines.append(f"- time_span: {time_min or 'N/A'} ~ {time_max or 'N/A'}")

    lines += ["", "## Recent Operations"]
    if not records:
        lines.append("No records found.")
    else:
        lines.append("| Time | Operation | Actor | Inputs | Outputs | Description |")
        lines.append("| --- | --- | --- | --- | --- | --- |")
        recent = records[-20:]
        for r in recent:
            op = r.get("operation_name") or ""
            actor_v = r.get("actor") or ""
            inputs = ",".join(r.get("input_ids", []) or [])
            outputs = ",".join(r.get("output_ids", []) or [])
            desc = (r.get("operation_desc") or "").replace("\n", " ")
            if len(desc) > 80:
                desc = desc[:77] + "..."
            lines.append(f"| {r.get('timestamp','')} | {op} | {actor_v} | {inputs} | {outputs} | {desc} |")

    content = "\n".join(lines) + "\n"
    return PlainTextResponse(content, media_type="text/markdown")

# 启动方式：uvicorn api_server:app --reload
