import sqlite3
import json
from datetime import datetime

DB_FILE = "datatrace.db"

def _connect():
    conn = sqlite3.connect(DB_FILE, timeout=30)
    return conn

def init_db():
    conn = _connect()
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS datasets
                 (id TEXT PRIMARY KEY, 
                  name TEXT, 
                  description TEXT, 
                  tags TEXT, 
                  created_at TEXT)''')
    # input_ids 存储为逗号分隔字符串
    c.execute('''CREATE TABLE IF NOT EXISTS records
                 (id TEXT PRIMARY KEY, 
                  timestamp TEXT, 
                  input_ids TEXT, 
                  operation_name TEXT, 
                  operation_desc TEXT, 
                  output_id TEXT,
                  actor TEXT,
                  source TEXT,
                  run_id TEXT)''')
    # 兼容旧库：增量补充列
    c.execute("PRAGMA table_info(records)")
    existing_cols = {row[1] for row in c.fetchall()}
    for col, col_type in (("actor", "TEXT"), ("source", "TEXT"), ("run_id", "TEXT")):
        if col not in existing_cols:
            c.execute(f"ALTER TABLE records ADD COLUMN {col} {col_type}")
    # 索引：提升常见查询性能
    c.execute("CREATE INDEX IF NOT EXISTS idx_records_timestamp ON records(timestamp)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_records_operation_name ON records(operation_name)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_records_run_id ON records(run_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_records_actor ON records(actor)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_records_source ON records(source)")

    # 时间序列数据表（面向时间序列工具集）
    c.execute('''CREATE TABLE IF NOT EXISTS timeseries
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  dataset_id TEXT,
                  timestamp TEXT,
                  value REAL,
                  metric TEXT)''')
    c.execute("CREATE INDEX IF NOT EXISTS idx_ts_dataset_time ON timeseries(dataset_id, timestamp)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_ts_metric ON timeseries(metric)")
    conn.commit()
    conn.close()

# --- 基础写入操作 ---

def add_dataset(ds_id, name, desc, tags):
    conn = _connect()
    c = conn.cursor()
    tags_str = ",".join(tags)
    created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    c.execute("INSERT INTO datasets VALUES (?, ?, ?, ?, ?)", 
              (ds_id, name, desc, tags_str, created_at))
    conn.commit()
    conn.close()
    return ds_id

def _normalize_ids(ids):
    if ids is None:
        return []
    if isinstance(ids, str):
        ids_list = [ids]
    else:
        ids_list = list(ids)
    return [i.strip() for i in ids_list if i and i.strip()]

def add_record(rec_id, input_id_list, op_name, op_desc, output_ids, actor=None, source=None, run_id=None):
    conn = _connect()
    c = conn.cursor()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    input_ids = _normalize_ids(input_id_list)
    output_ids = _normalize_ids(output_ids)
    input_ids_str = ",".join(input_ids)
    output_ids_str = ",".join(output_ids)
    c.execute(
        "INSERT INTO records (id, timestamp, input_ids, operation_name, operation_desc, output_id, actor, source, run_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (rec_id, timestamp, input_ids_str, op_name, op_desc, output_ids_str, actor, source, run_id),
    )
    conn.commit()
    conn.close()
    return rec_id

def _normalize_ts(ts):
    if isinstance(ts, datetime):
        return ts.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(ts, str):
        return ts.strip()
    return str(ts)

def add_timeseries_points(dataset_id, points, metric="value"):
    """
    写入时间序列点：points = [{"timestamp": "...", "value": 1.23}, ...] 或 [(ts, v), ...]
    """
    if not points:
        return 0
    conn = _connect()
    c = conn.cursor()
    rows = []
    for p in points:
        if isinstance(p, dict):
            ts = _normalize_ts(p.get("timestamp"))
            val = p.get("value")
        else:
            ts = _normalize_ts(p[0])
            val = p[1]
        rows.append((dataset_id, ts, float(val), metric))
    c.executemany("INSERT INTO timeseries (dataset_id, timestamp, value, metric) VALUES (?, ?, ?, ?)", rows)
    conn.commit()
    conn.close()
    return len(rows)

def get_timeseries(dataset_id, start=None, end=None, metric=None, limit=1000):
    conn = _connect()
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    sql = "SELECT * FROM timeseries WHERE dataset_id = ?"
    params = [dataset_id]
    if metric:
        sql += " AND metric = ?"
        params.append(metric)
    if start:
        sql += " AND timestamp >= ?"
        params.append(_normalize_ts(start))
    if end:
        sql += " AND timestamp <= ?"
        params.append(_normalize_ts(end))
    sql += " ORDER BY timestamp ASC LIMIT ?"
    params.append(int(limit or 1000))
    c.execute(sql, params)
    rows = c.fetchall()
    conn.close()
    return [dict(r) for r in rows]

def copy_timeseries(from_dataset_ids, to_dataset_id, prefix_metric=False):
    src_ids = _normalize_ids(from_dataset_ids)
    if not src_ids:
        return 0
    conn = _connect()
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    placeholders = ",".join("?" * len(src_ids))
    c.execute(
        f"SELECT dataset_id, timestamp, value, metric FROM timeseries WHERE dataset_id IN ({placeholders}) ORDER BY timestamp ASC",
        src_ids,
    )
    rows = c.fetchall()
    if not rows:
        conn.close()
        return 0
    out_rows = []
    use_prefix = prefix_metric and len(src_ids) > 1
    for r in rows:
        metric = r["metric"]
        if use_prefix:
            metric = f"{r['dataset_id']}:{metric}"
        out_rows.append((to_dataset_id, r["timestamp"], float(r["value"]), metric))
    c.executemany(
        "INSERT INTO timeseries (dataset_id, timestamp, value, metric) VALUES (?, ?, ?, ?)",
        out_rows,
    )
    conn.commit()
    conn.close()
    return len(out_rows)

# --- 高级查询与搜索 ---

def get_dataset_by_id(ds_id):
    conn = _connect()
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("SELECT * FROM datasets WHERE id=?", (ds_id,))
    row = c.fetchone()
    conn.close()
    return dict(row) if row else None

def get_all_datasets():
    # 默认获取所有，用于初始化
    return search_datasets()

def search_datasets(query=None, tags=None):
    """
    Hugging Face 风格搜索：支持名称/描述模糊匹配 + 标签过滤
    """
    conn = _connect()
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    
    sql = "SELECT * FROM datasets WHERE 1=1"
    params = []
    
    if query:
        # 搜索名称或描述
        sql += " AND (name LIKE ? OR description LIKE ?)"
        params.extend([f"%{query}%", f"%{query}%"])
    
    if tags:
        # 筛选标签 (简单实现：只要包含其中一个标签即可)
        # 也可以改为必须包含所有标签，视需求而定
        tag_conditions = []
        for tag in tags:
            tag_conditions.append("tags LIKE ?")
            params.append(f"%{tag}%")
        if tag_conditions:
            sql += " AND (" + " OR ".join(tag_conditions) + ")"
            
    sql += " ORDER BY created_at DESC"
    
    c.execute(sql, params)
    rows = c.fetchall()
    conn.close()
    return [dict(r) for r in rows]

# database.py 中补上这段代码

def _row_to_record(row):
    record = dict(row)
    raw_inputs = record.pop('input_ids', "") or ""
    raw_outputs = record.pop('output_id', "") or ""
    record['input_ids'] = [i for i in raw_inputs.split(",") if i]
    record['output_ids'] = [i for i in raw_outputs.split(",") if i]
    return record

def get_all_records():
    """获取所有记录，用于初始化过滤器等"""
    conn = _connect()
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("SELECT * FROM records ORDER BY timestamp ASC")
    rows = c.fetchall()
    conn.close()
    return [_row_to_record(r) for r in rows]

def _build_records_filter_sql(start_date=None, end_date=None, op_types=None, search_q=None, actor=None, source=None, run_id=None):
    sql = " FROM records WHERE 1=1"
    params = []

    if start_date:
        sql += " AND timestamp >= ?"
        params.append(start_date.strftime("%Y-%m-%d 00:00:00"))
    if end_date:
        sql += " AND timestamp <= ?"
        params.append(end_date.strftime("%Y-%m-%d 23:59:59"))

    if op_types:
        placeholders = ",".join("?" * len(op_types))
        sql += f" AND operation_name IN ({placeholders})"
        params.extend(op_types)

    if search_q:
        sql += " AND (operation_desc LIKE ? OR id LIKE ?)"
        params.extend([f"%{search_q}%", f"%{search_q}%"])
    
    if actor:
        sql += " AND actor = ?"
        params.append(actor)
    if source:
        sql += " AND source = ?"
        params.append(source)
    if run_id:
        sql += " AND run_id = ?"
        params.append(run_id)

    return sql, params

def get_records_count(start_date=None, end_date=None, op_types=None, search_q=None, actor=None, source=None, run_id=None):
    conn = _connect()
    c = conn.cursor()
    where_sql, params = _build_records_filter_sql(
        start_date=start_date, end_date=end_date, op_types=op_types, search_q=search_q, actor=actor, source=source, run_id=run_id
    )
    c.execute("SELECT COUNT(*)" + where_sql, params)
    total = c.fetchone()[0]
    conn.close()
    return int(total)

def get_filtered_records(start_date=None, end_date=None, op_types=None, search_q=None, actor=None, source=None, run_id=None, limit=None, offset=0):
    """
    图谱过滤核心逻辑：
    - start_date/end_date: 时间范围
    - op_types: 操作类型筛选 (e.g. ['Clean', 'Merge'])
    - search_q: 搜索操作描述或ID
    - actor/source/run_id: 记录来源筛选
    - limit/offset: 分页
    """
    conn = _connect()
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    
    where_sql, params = _build_records_filter_sql(
        start_date=start_date, end_date=end_date, op_types=op_types, search_q=search_q, actor=actor, source=source, run_id=run_id
    )
    sql = "SELECT *" + where_sql + " ORDER BY timestamp ASC"

    if limit is not None:
        sql += " LIMIT ? OFFSET ?"
        params.extend([int(limit), int(offset or 0)])
    
    c.execute(sql, params)
    rows = c.fetchall()
    conn.close()
    return [_row_to_record(r) for r in rows]

def get_operation_stats():
    """返回操作类型列表及其出现次数，用于过滤器等"""
    conn = _connect()
    c = conn.cursor()
    c.execute(
        "SELECT operation_name, COUNT(*) AS cnt FROM records GROUP BY operation_name ORDER BY cnt DESC, operation_name ASC"
    )
    rows = c.fetchall()
    conn.close()
    return [{"operation": r[0], "count": int(r[1])} for r in rows if r and r[0]]

def _build_record_indices(records):
    inputs_index = {}
    outputs_index = {}
    for rec in records or []:
        for i in rec.get("input_ids", []) or []:
            inputs_index.setdefault(i, []).append(rec)
        for o in rec.get("output_ids", []) or []:
            outputs_index.setdefault(o, []).append(rec)
    return inputs_index, outputs_index

def collect_lineage_record_ids(dataset_id, records, direction="both", depth=2):
    """
    在给定 records 子集中，收集与 dataset_id 相关的“血缘记录（record ids）”：
    - downstream: dataset 作为 input 的记录 + 其 outputs 继续扩展
    - upstream: dataset 作为 output 的记录 + 其 inputs 继续扩展
    depth 按“数据集节点扩展层数”计（与 /lineage 的定义一致）。
    返回：(record_id_set, dataset_id_set)
    """
    direction = (direction or "").strip().lower()
    if direction not in ("upstream", "downstream", "both"):
        raise ValueError("direction must be upstream, downstream, or both")
    depth = int(depth or 0)
    if depth < 0:
        raise ValueError("depth must be >= 0")

    inputs_index, outputs_index = _build_record_indices(records)

    record_ids = set()
    dataset_ids = set([dataset_id])

    visited = set([dataset_id])
    frontier = set([dataset_id])

    for _ in range(depth):
        if not frontier:
            break
        next_frontier = set()
        for ds_id in list(frontier):
            if direction in ("downstream", "both"):
                for rec in inputs_index.get(ds_id, []):
                    rec_id = rec.get("id")
                    if rec_id:
                        record_ids.add(rec_id)
                    for out_id in rec.get("output_ids", []) or []:
                        dataset_ids.add(out_id)
                        if out_id not in visited:
                            visited.add(out_id)
                            next_frontier.add(out_id)

            if direction in ("upstream", "both"):
                for rec in outputs_index.get(ds_id, []):
                    rec_id = rec.get("id")
                    if rec_id:
                        record_ids.add(rec_id)
                    for in_id in rec.get("input_ids", []) or []:
                        dataset_ids.add(in_id)
                        if in_id not in visited:
                            visited.add(in_id)
                            next_frontier.add(in_id)

        frontier = next_frontier

    return record_ids, dataset_ids

# 初始化
init_db()
