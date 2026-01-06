import datatrace as dt  

# 1. 初始化 
dt.init(user="developer_yzy")

# 2. 获取输入数据集 (像 SwanLab 一样，没有就自动创建)
# 假设我有一个原始图片集
ds_raw = dt.get_dataset(
    name="raw_coco_2024", 
    description="原始COCO数据集下载", 
    tags=["image", "raw"]
)

print(f"正在处理数据集: {ds_raw}")

# --- 模拟业务逻辑 ---
# 假设我们要进行清洗操作
# ... 代码执行中 ...
# ... 清洗完毕 ...

# 3. 记录操作 (Log Operation)
# 告诉系统：我用 "Clean" 操作处理了 ds_raw，生成了新数据
ds_cleaned = dt.log(
    inputs=[ds_raw], 
    op_name="Clean_Remove_Nulls", 
    output_name="coco_cleaned", 
    description="移除了模糊图片和空标签"
)

# 4. 继续处理 (链式调用)
# 假设我要把清洗后的数据和另一个数据集融合
ds_audio = dt.get_dataset("raw_audio_files", tags=["audio"])

ds_multimodal = dt.log(
    inputs=[ds_cleaned, ds_audio], # 多对一融合！
    op_name="Merge_Multimodal",
    output_name="coco_audio_merged",
    description="合并图片和音频用于多模态训练"
)

print("流程结束！")