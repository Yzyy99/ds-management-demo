import requests
import sys
import os

# 默认配置
CONFIG = {
    "API_URL": "http://127.0.0.1:8000",
    "USER": "anonymous"
}

class Dataset:
    """数据集对象，包装 ID 和 Name，方便代码传递"""
    def __init__(self, id, name, tags=None):
        self.id = id
        self.name = name
        self.tags = tags

    def __repr__(self):
        return f"<Dataset: {self.name} ({self.id})>"

def init(api_url=None, user=None):
    """初始化 SDK 配置"""
    if api_url:
        CONFIG["API_URL"] = api_url
    if user:
        CONFIG["USER"] = user
    
    # 测试连接
    try:
        requests.get(CONFIG["API_URL"])
        print(f"✅ DataTrace connected to {CONFIG['API_URL']}")
    except:
        print(f"❌ Connection Failed. Is api_server.py running?")

def get_dataset(name, description="", tags=None, auto_create=True):
    """
    获取数据集对象。
    如果 auto_create=True，且数据集不存在，则自动注册它（类似 SwanLab 自动创建实验）。
    """
    if tags is None: tags = []
    
    # 1. 尝试注册/获取 (利用后端的 Get-or-Create 逻辑)
    payload = {
        "name": name,
        "description": description or f"Auto-registered by SDK user {CONFIG['USER']}",
        "tags": tags
    }
    
    try:
        res = requests.post(f"{CONFIG['API_URL']}/datasets/", json=payload)
        res.raise_for_status()
        data = res.json()
        return Dataset(id=data['id'], name=data['name'])
    except Exception as e:
        print(f"❌ Failed to get dataset {name}: {e}")
        return None

def log(inputs, op_name, output_name, description=None, output_tags=None):
    """
    核心操作：记录一次数据变换 (Transformation)
    类似 swanlab.log，但在 DataTrace 中意味着“生成了新数据”
    """
    if not isinstance(inputs, list):
        inputs = [inputs]
    
    # 提取 Input IDs
    input_ids = []
    for i in inputs:
        if isinstance(i, Dataset):
            input_ids.append(i.id)
        elif isinstance(i, str):
            # 如果用户只传了 ID 字符串
            input_ids.append(i)
        else:
            raise ValueError("Inputs must be Dataset objects or ID strings")

    payload = {
        "input_ids": input_ids,
        "operation": op_name,
        "description": description or f"Executed via SDK script",
        "output_suffix": "" # 这里我们由 output_name 决定
    }

    # 注意：目前的 API 是自动生成 output name 的，
    # 为了支持 SDK 指定 output_name，我们需要稍微变通一下，
    # 或者修改 API。这里演示直接调用 API，让 API 自动处理命名逻辑，
    # 如果你想强行指定名字，可能需要修改 api_server 的逻辑。
    # 既然是模拟 SwanLab，我们先假设后端会自动处理。
    
    try:
        res = requests.post(f"{CONFIG['API_URL']}/transform/", json=payload)
        res.raise_for_status()
        data = res.json()
        out_ds = data['output_dataset']
        
        print(f"🚀 Operation '{op_name}' logged.")
        print(f"   └── New Dataset: {out_ds['name']} (ID: {out_ds['id']})")
        
        return Dataset(id=out_ds['id'], name=out_ds['name'])
    except Exception as e:
        print(f"❌ Failed to log operation: {e}")
        return None

# --- 高级功能：装饰器 ---
# 这样用户完全不用改函数内部逻辑，只要加一行 @dt.trace
def trace(op_name, output_name_suffix="_processed"):
    def decorator(func):
        def wrapper(*args, **kwargs):
            # 1. 尝试从参数中寻找 Dataset 对象作为输入
            inputs = []
            for arg in args:
                if isinstance(arg, Dataset):
                    inputs.append(arg)
            for k, v in kwargs.items():
                if isinstance(v, Dataset):
                    inputs.append(v)
            
            # 2. 执行原函数
            result = func(*args, **kwargs)
            
            # 3. 记录日志 (如果有输入的话)
            if inputs:
                log(inputs, op_name, output_name=f"auto_{output_name_suffix}", description=f"Auto-traced function: {func.__name__}")
            
            return result
        return wrapper
    return decorator