import streamlit as st
import uuid
import database as db
import pandas as pd
import graphviz
from datetime import datetime, timedelta

# --- 页面配置 ---
st.set_page_config(page_title="DataTrace Pro", layout="wide", page_icon="🕸️")

# 自定义样式
st.markdown("""
<style>
    .main-header {font-size: 2rem; font-weight: 700; color: #333;}
    .card {background-color: #f9f9f9; padding: 15px; border-radius: 8px; border: 1px solid #ddd; margin-bottom: 10px;}
    .tag-badge {background-color: #e1ecf4; color: #39739d; padding: 2px 8px; border-radius: 4px; font-size: 0.85em; margin-right: 5px;}
</style>
""", unsafe_allow_html=True)

# --- 侧边栏：全局控制 ---
with st.sidebar:
    st.title("🎛️ Control Panel")
    page = st.radio("Mode", ["Search & Explore", "Workstation", "Lineage Intelligence"])
    
    st.divider()
    
    if page == "Search & Explore":
        st.subheader("🔎 Search Filters")
        search_query = st.text_input("Keywords", placeholder="Name or Description...")
        
        # 动态获取所有标签供筛选
        all_ds = db.get_all_datasets()
        all_tags = set()
        for d in all_ds:
            if d['tags']: all_tags.update(d['tags'].split(","))
        
        selected_tags = st.multiselect("Filter by Tags", options=list(all_tags))
        
    elif page == "Lineage Intelligence":
        st.subheader("🕸️ Graph Filters")
        # 时间范围选择
        today = datetime.now()
        date_range = st.date_input(
            "Time Range",
            value=(today - timedelta(days=7), today),
            max_value=today
        )
        
        # 操作类型筛选
        all_recs = db.get_all_records()
        all_ops = list(set([r['operation_name'] for r in all_recs])) if all_recs else []
        selected_ops = st.multiselect("Operation Types", options=all_ops, default=all_ops)

# --- 页面 1: Search & Explore (Hugging Face 风格) ---
if page == "Search & Explore":
    st.markdown('<div class="main-header">📦 Dataset Hub</div>', unsafe_allow_html=True)

    with st.expander("➕ Register New Dataset", expanded=(len(all_ds) == 0)):
        with st.form("create_dataset_form"):
            new_ds_name = st.text_input("Dataset Name")
            new_ds_desc = st.text_area("Description", height=100)
            new_ds_tags = st.text_input("Tags (comma separated)")
            create_ds = st.form_submit_button("Create Dataset")
            
            if create_ds:
                if not new_ds_name:
                    st.error("Please provide a dataset name.")
                else:
                    tags = [t.strip() for t in new_ds_tags.split(",") if t.strip()]
                    new_id = str(uuid.uuid4())[:8]
                    db.add_dataset(new_id, new_ds_name, new_ds_desc, tags)
                    st.success(f"Dataset '{new_ds_name}' created (ID: {new_id}).")
                    st.rerun()
    
    # 执行搜索
    results = db.search_datasets(query=search_query, tags=selected_tags)
    
    col1, col2 = st.columns([3, 1])
    col1.caption(f"Showing {len(results)} datasets")
    
    if not results:
        st.info("No datasets match your search criteria.")
    else:
        # 卡片式布局
        for ds in results:
            with st.container():
                st.markdown(f"""
                <div class="card">
                    <h4>📂 {ds['name']} <small style="color:grey; font-weight:normal">({ds['id']})</small></h4>
                    <p>{ds['description']}</p>
                    <p>
                        {' '.join([f'<span class="tag-badge">{t}</span>' for t in ds['tags'].split(',')])}
                        <span style="float:right; color:grey; font-size:0.8em">Created: {ds['created_at']}</span>
                    </p>
                </div>
                """, unsafe_allow_html=True)

# --- 页面 2: Workstation (数据处理) ---
elif page == "Workstation":
    st.markdown('<div class="main-header">🛠️ Data Processor</div>', unsafe_allow_html=True)
    
    datasets = db.get_all_datasets()
    ds_options = {f"{d['name']} ({d['id']})": d for d in datasets}
    
    c1, c2 = st.columns([1, 1], gap="large")
    
    with c1:
        st.subheader("1. Select Inputs")
        # 多选输入
        selected_labels = st.multiselect("Source Datasets", options=list(ds_options.keys()))
        selected_ds_objects = [ds_options[l] for l in selected_labels]
        
        if selected_ds_objects:
            st.code("\n".join([f"- {d['name']} ({d['tags']})" for d in selected_ds_objects]), language="text")

    with c2:
        st.subheader("2. Define Action")
        with st.form("action_form"):
            op_name = st.text_input("Operation (e.g. Join, Clean)")
            op_desc = st.text_area("Commit Message / Logic Details")
            output_count = st.number_input("Number of output datasets", min_value=1, max_value=5, value=1, step=1, key="action_output_count")
            op_slug = op_name.lower().replace(" ", "_") if op_name else "transformed"
            if len(selected_ds_objects) == 1:
                base_hint = selected_ds_objects[0]['name']
            elif len(selected_ds_objects) > 1:
                base_hint = "merged_dataset"
            else:
                base_hint = "dataset"
            output_configs = []
            for idx in range(int(output_count)):
                suggested_name = f"{base_hint}_{op_slug}_{idx+1}" if selected_ds_objects else f"{op_slug}_{idx+1}"
                out_name = st.text_input(f"Output {idx+1} Name", value=suggested_name, key=f"action_output_name_{idx}")
                out_desc = st.text_area(f"Output {idx+1} Description", value="", height=70, key=f"action_output_desc_{idx}")
                output_configs.append({
                    "name": out_name.strip(),
                    "desc": out_desc.strip()
                })
            run = st.form_submit_button("🚀 Run Transformation")
            
            if run:
                if not op_name:
                    st.error("Operation name is required.")
                elif not selected_ds_objects:
                    st.error("Select at least one source dataset.")
                elif any(not cfg['name'] for cfg in output_configs):
                    st.error("Please provide names for all outputs.")
                else:
                    input_ids = [d['id'] for d in selected_ds_objects]
                    input_names = [d['name'] for d in selected_ds_objects]
                    
                    tags = set()
                    for d in selected_ds_objects:
                        if d['tags']:
                            tags.update([t.strip() for t in d['tags'].split(",") if t.strip()])
                    tags.add(op_slug)
                    tags.add("generated")
                    tag_list = sorted(tags)
                    
                    created_outputs = []
                    for cfg in output_configs:
                        new_id = str(uuid.uuid4())[:8]
                        desc = cfg['desc'] or f"Generated via {op_name} from {', '.join(input_names)}."
                        db.add_dataset(new_id, cfg['name'], desc, tag_list)
                        created_outputs.append({"id": new_id, "name": cfg['name']})
                    
                    db.add_record(str(uuid.uuid4())[:8], input_ids, op_name, op_desc, [o['id'] for o in created_outputs])
                    
                    st.success(f"Created {len(created_outputs)} dataset(s): {', '.join([o['name'] for o in created_outputs])}")
                    st.rerun()

# --- 页面 3: Lineage Intelligence (过滤图谱) ---
elif page == "Lineage Intelligence":
    st.markdown('<div class="main-header">🕸️ Lineage Graph</div>', unsafe_allow_html=True)
    
    # 解析侧边栏的时间过滤器
    start_d, end_d = None, None
    if len(date_range) == 2:
        start_d = datetime.combine(date_range[0], datetime.min.time())
        end_d = datetime.combine(date_range[1], datetime.max.time())
    
    # 获取过滤后的记录
    records = db.get_filtered_records(start_date=start_d, end_date=end_d, op_types=selected_ops)
    
    st.caption(f"Visualizing {len(records)} operations based on current filters.")
    
    if not records:
        st.warning("No lineage data found for the selected time range/filters.")
    else:
        graph = graphviz.Digraph()
        graph.attr(rankdir='LR', bgcolor='transparent')
        graph.attr('node', shape='box', style='rounded,filled', fontname='Helvetica', fontsize='10')
        graph.attr('edge', color='#666666')
        
        # 仅绘制筛选出的记录
        for rec in records:
            input_ids = rec.get('input_ids', [])
            output_ids = rec.get('output_ids', [])
            
            # 操作节点
            op_id = f"op_{rec['id']}"
            graph.node(op_id, label=f"{rec['operation_name']}\n(Op)", shape='circle', 
                       fillcolor='#222222', fontcolor='white', width='0.7', fixedsize='true')
            
            # 输入边
            for i_id in input_ids:
                inp_ds = db.get_dataset_by_id(i_id)
                if inp_ds: # 只有当输入数据集存在时才画（可能因过滤原因？）
                     # 实际场景中，数据集本身可能不应该被时间过滤，只过滤“操作事件”
                    graph.node(inp_ds['name'], label=inp_ds['name'], fillcolor='white', color='#cccccc')
                    graph.edge(inp_ds['name'], op_id)
            
            # 输出边（支持多个输出数据集）
            for o_id in output_ids:
                out_ds = db.get_dataset_by_id(o_id)
                if out_ds:
                    graph.node(out_ds['name'], label=out_ds['name'], fillcolor='white', color='#cccccc')
                    graph.edge(op_id, out_ds['name'])
                
        st.graphviz_chart(graph, use_container_width=True)
        
        with st.expander("Show Raw Event Logs"):
            st.dataframe(pd.DataFrame(records))
