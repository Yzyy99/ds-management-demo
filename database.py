import sqlite3
import json
from datetime import datetime

DB_FILE = "datatrace.db"

def init_db():
    conn = sqlite3.connect(DB_FILE)
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
                  output_id TEXT)''')
    conn.commit()
    conn.close()

# --- 基础写入操作 ---

def add_dataset(ds_id, name, desc, tags):
    conn = sqlite3.connect(DB_FILE)
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

def add_record(rec_id, input_id_list, op_name, op_desc, output_ids):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    input_ids = _normalize_ids(input_id_list)
    output_ids = _normalize_ids(output_ids)
    input_ids_str = ",".join(input_ids)
    output_ids_str = ",".join(output_ids)
    c.execute("INSERT INTO records VALUES (?, ?, ?, ?, ?, ?)",
              (rec_id, timestamp, input_ids_str, op_name, op_desc, output_ids_str))
    conn.commit()
    conn.close()
    return rec_id

# --- 高级查询与搜索 ---

def get_dataset_by_id(ds_id):
    conn = sqlite3.connect(DB_FILE)
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
    conn = sqlite3.connect(DB_FILE)
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
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("SELECT * FROM records ORDER BY timestamp ASC")
    rows = c.fetchall()
    conn.close()
    return [_row_to_record(r) for r in rows]

def get_filtered_records(start_date=None, end_date=None, op_types=None, search_q=None):
    """
    图谱过滤核心逻辑：
    - start_date/end_date: 时间范围
    - op_types: 操作类型筛选 (e.g. ['Clean', 'Merge'])
    - search_q: 搜索操作描述或ID
    """
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    
    sql = "SELECT * FROM records WHERE 1=1"
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
        
    sql += " ORDER BY timestamp ASC"
    
    c.execute(sql, params)
    rows = c.fetchall()
    conn.close()
    return [_row_to_record(r) for r in rows]

# 初始化
init_db()
