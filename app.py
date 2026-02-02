import streamlit as st
import uuid
import database as db
import pandas as pd
import graphviz
from datetime import datetime, timedelta
import random
import math

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
    page = st.radio("Mode", ["Search & Explore", "Workstation", "Lineage Intelligence", "Time Series Lab"])
    
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
        datasets = db.get_all_datasets()
        ds_options = {f"{d['name']} ({d['id']})": d for d in datasets}
        focus_label = st.selectbox(
            "Focus Dataset (optional)",
            options=["(All datasets)"] + list(ds_options.keys()),
            index=0,
        )
        focus_dataset_id = None if focus_label == "(All datasets)" else ds_options[focus_label]["id"]

        lineage_direction = st.selectbox("Direction", options=["both", "upstream", "downstream"], index=0)
        lineage_depth = st.slider("Depth", min_value=0, max_value=6, value=2, step=1)

        # 时间范围选择
        today = datetime.now()
        date_range = st.date_input(
            "Time Range",
            value=(today - timedelta(days=7), today),
            max_value=today
        )

        # 操作类型筛选（按当前 focus + 时间范围动态更新）
        start_d, end_d = None, None
        if len(date_range) == 2:
            start_d = datetime.combine(date_range[0], datetime.min.time())
            end_d = datetime.combine(date_range[1], datetime.max.time())

        candidate_records = db.get_filtered_records(start_date=start_d, end_date=end_d)
        if focus_dataset_id:
            record_ids, _ = db.collect_lineage_record_ids(
                focus_dataset_id, candidate_records, direction=lineage_direction, depth=int(lineage_depth)
            )
            candidate_records = [r for r in candidate_records if r.get("id") in record_ids]

        all_ops = sorted({r.get("operation_name") for r in candidate_records if r.get("operation_name")})
        selected_ops = st.multiselect("Operation Types", options=all_ops, default=all_ops)
    elif page == "Time Series Lab":
        st.subheader("⏱️ Time Series Lab")
        ts_datasets = db.get_all_datasets()
        ts_options = {f"{d['name']} ({d['id']})": d for d in ts_datasets}
        ts_label = st.selectbox(
            "Dataset",
            options=["(Select a dataset)"] + list(ts_options.keys()),
            index=0,
        )
        ts_dataset_id = None if ts_label == "(Select a dataset)" else ts_options[ts_label]["id"]
        ts_metric = st.text_input("Metric", value="value")
        ts_days = st.slider("Lookback Days", min_value=1, max_value=365, value=60, step=1)

        st.markdown("**Generate Sample Series (optional)**")
        ts_periods = st.number_input("Points", min_value=10, max_value=1000, value=120, step=10)
        ts_freq = st.selectbox("Frequency", options=["daily", "hourly"], index=0)
        ts_amplitude = st.number_input("Amplitude", min_value=0.1, max_value=100.0, value=10.0, step=0.5)
        ts_noise = st.number_input("Noise", min_value=0.0, max_value=10.0, value=1.0, step=0.1)
        ts_trend = st.number_input("Trend", min_value=-1.0, max_value=1.0, value=0.05, step=0.01)
        generate_ts = st.button("Generate Sample Series")

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
            actor = st.text_input("Actor (who did this?)", value="web")
            run_id = st.text_input("Run ID (optional)", placeholder="e.g. batch_20260114")
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
                        db.copy_timeseries(input_ids, new_id, prefix_metric=(len(input_ids) > 1))
                    
                    db.add_record(
                        str(uuid.uuid4())[:8],
                        input_ids,
                        op_name,
                        op_desc,
                        [o['id'] for o in created_outputs],
                        actor=actor.strip() or "web",
                        source="web",
                        run_id=run_id.strip() or None,
                    )
                    
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

    if focus_dataset_id:
        record_ids, _ = db.collect_lineage_record_ids(
            focus_dataset_id, records, direction=lineage_direction, depth=int(lineage_depth)
        )
        records = [r for r in records if r.get("id") in record_ids]
    
    scope_hint = ""
    if focus_dataset_id:
        scope_hint = f" | Focus={focus_dataset_id}, dir={lineage_direction}, depth={lineage_depth}"
    st.caption(f"Visualizing {len(records)} operations based on current filters.{scope_hint}")

    # 一键导出报告（Markdown）
    if focus_dataset_id:
        root_ds = db.get_dataset_by_id(focus_dataset_id) or {"name": focus_dataset_id}
        ds_set = set([focus_dataset_id])
        op_set = set()
        edge_set = set()
        timestamps = []
        for r in records:
            op_set.add(r.get("operation_name"))
            if r.get("timestamp"):
                timestamps.append(r.get("timestamp"))
            for i_id in r.get("input_ids", []) or []:
                ds_set.add(i_id)
                edge_set.add((i_id, f"op:{r.get('id')}"))
            for o_id in r.get("output_ids", []) or []:
                ds_set.add(o_id)
                edge_set.add((f"op:{r.get('id')}", o_id))
        time_min = min(timestamps) if timestamps else None
        time_max = max(timestamps) if timestamps else None
        title = f"DataTrace Report - {root_ds.get('name')} ({focus_dataset_id})"
        report_lines = [
            f"# {title}",
            "",
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            "",
            "## Scope",
            f"- direction: {lineage_direction}",
            f"- depth: {lineage_depth}",
            f"- time: {date_range[0]} ~ {date_range[1]}" if len(date_range) == 2 else "- time: N/A",
            f"- op_types: {', '.join(selected_ops)}" if selected_ops else "- op_types: N/A",
            "",
            "## Summary",
            f"- records: {len(records)}",
            f"- datasets: {len(ds_set)}",
            f"- operations: {len([o for o in op_set if o])}",
            f"- nodes: {len(ds_set) + len(records)}",
            f"- edges: {len(edge_set)}",
        ]
        if time_min or time_max:
            report_lines.append(f"- time_span: {time_min or 'N/A'} ~ {time_max or 'N/A'}")
        report_lines += ["", "## Recent Operations"]
        if not records:
            report_lines.append("No records found.")
        else:
            report_lines.append("| Time | Operation | Actor | Inputs | Outputs | Description |")
            report_lines.append("| --- | --- | --- | --- | --- | --- |")
            recent = records[-20:]
            for r in recent:
                op = r.get("operation_name") or ""
                actor_v = r.get("actor") or ""
                inputs = ",".join(r.get("input_ids", []) or [])
                outputs = ",".join(r.get("output_ids", []) or [])
                desc = (r.get("operation_desc") or "").replace("\n", " ")
                if len(desc) > 80:
                    desc = desc[:77] + "..."
                report_lines.append(f"| {r.get('timestamp','')} | {op} | {actor_v} | {inputs} | {outputs} | {desc} |")
        report_md = "\n".join(report_lines) + "\n"
        st.download_button(
            "📄 Export Report (Markdown)",
            data=report_md,
            file_name=f"datatrace_report_{focus_dataset_id}.md",
            mime="text/markdown",
        )
    else:
        st.info("Select a Focus Dataset to export a report.")
    
    if not records:
        st.warning("No lineage data found for the selected time range/filters.")
    else:
        graph = graphviz.Digraph()
        graph.attr(rankdir='LR', bgcolor='transparent', fontname='Helvetica', ranksep='0.6', nodesep='0.5')
        graph.attr('node', shape='box', style='rounded,filled', fontname='Helvetica', fontsize='10', color='#adb5bd')
        graph.attr('edge', color='#868e96', arrowsize='0.7')

        seen_ds_nodes = set()
        def add_dataset_node(ds_id: str):
            if ds_id in seen_ds_nodes:
                return
            ds = db.get_dataset_by_id(ds_id)
            label = f"{ds['name']}\n({ds_id})" if ds else ds_id
            fillcolor = "#f8f9fa"
            color = "#ced4da"
            penwidth = "1"
            if focus_dataset_id and ds_id == focus_dataset_id:
                fillcolor = "#ffe066"
                color = "#f08c00"
                penwidth = "2.2"
            graph.node(f"ds_{ds_id}", label=label, fillcolor=fillcolor, color=color, penwidth=penwidth)
            seen_ds_nodes.add(ds_id)
        
        # 仅绘制筛选出的记录
        for rec in records:
            input_ids = rec.get('input_ids', [])
            output_ids = rec.get('output_ids', [])
            
            # 操作节点
            op_id = f"op_{rec['id']}"
            op_actor = rec.get("actor") or "unknown"
            graph.node(
                op_id,
                label=f"{rec['operation_name']}\n@{op_actor}",
                shape='circle',
                fillcolor='#2f3640',
                fontcolor='white',
                width='0.85',
                fixedsize='true'
            )
            
            # 输入边
            for i_id in input_ids:
                add_dataset_node(i_id)
                graph.edge(f"ds_{i_id}", op_id)
            
            # 输出边（支持多个输出数据集）
            for o_id in output_ids:
                add_dataset_node(o_id)
                graph.edge(op_id, f"ds_{o_id}")
                
        st.graphviz_chart(graph, use_container_width=True)
        
        with st.expander("Show Raw Event Logs"):
            st.dataframe(pd.DataFrame(records))

# --- 页面 4: Time Series Lab ---
elif page == "Time Series Lab":
    st.markdown('<div class="main-header">⏱️ Time Series Lab</div>', unsafe_allow_html=True)

    if not ts_dataset_id:
        st.info("Select a dataset to view or generate time series.")
    else:
        if generate_ts:
            base_dt = datetime.now() - (timedelta(days=ts_periods) if ts_freq == "daily" else timedelta(hours=ts_periods))
            step = timedelta(days=1) if ts_freq == "daily" else timedelta(hours=1)
            points = []
            for i in range(int(ts_periods)):
                t = base_dt + step * i
                seasonal = ts_amplitude * math.sin(2 * math.pi * i / max(10, int(ts_periods) // 3))
                value = seasonal + (ts_trend * i) + random.uniform(-ts_noise, ts_noise)
                points.append({"timestamp": t.strftime("%Y-%m-%d %H:%M:%S"), "value": float(value)})
            inserted = db.add_timeseries_points(ts_dataset_id, points, metric=ts_metric)
            st.success(f"Inserted {inserted} points for dataset {ts_dataset_id}.")
            st.rerun()

        ts_start = datetime.now() - timedelta(days=int(ts_days))
        ts_end = datetime.now()
        series = db.get_timeseries(ts_dataset_id, start=ts_start, end=ts_end, metric=ts_metric, limit=5000)
        if not series:
            st.warning("No time series data found. Try generating sample series.")
        else:
            df = pd.DataFrame(series)
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            df = df.sort_values("timestamp")
            st.line_chart(df.set_index("timestamp")["value"])
            with st.expander("Show Raw Series"):
                st.dataframe(df)
