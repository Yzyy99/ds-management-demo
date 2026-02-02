import click
import requests
import sys
import json

API_URL = "http://127.0.0.1:8000"

@click.group()
def cli():
    """DataTrace 命令行工具 - 像 Git 一样管理数据血缘"""
    pass

@cli.command()
@click.option('--name', prompt='Dataset Name', help='数据集名称')
@click.option('--desc', prompt='Description', help='描述')
@click.option('--tags', prompt='Tags (comma separated)', help='标签')
def register(name, desc, tags):
    """注册一个新的原始数据集"""
    tag_list = [t.strip() for t in tags.split(",")]
    payload = {"name": name, "description": desc, "tags": tag_list}
    
    try:
        r = requests.post(f"{API_URL}/datasets/", json=payload)
        if r.status_code == 200:
            data = r.json()
            click.echo(click.style(f"✔ Success! Dataset ID: {data['id']}", fg='green', bold=True))
        else:
            click.echo(click.style(f"✘ Error: {r.text}", fg='red'))
    except requests.exceptions.ConnectionError:
        click.echo(click.style("✘ Error: API Server not running. Run 'uvicorn api_server:app' first.", fg='red'))

@cli.command()
@click.option('--inputs', prompt='Input IDs (comma separated)', help='输入数据集ID')
@click.option('--op', prompt='Operation Name', help='操作名称 (e.g. Clean, Join)')
@click.option('--msg', prompt='Commit Message', help='操作描述')
@click.option('--outputs', type=int, default=1, show_default=True, help='输出数据集数量')
@click.option('--out-name', multiple=True, help='指定输出数据集名称（可重复传入）')
@click.option('--out-desc', multiple=True, help='指定输出数据集描述（可重复传入，与 out-name 对齐）')
@click.option('--actor', default='cli', show_default=True, help='操作者标识（用于追溯）')
@click.option('--run-id', default=None, help='Run ID（用于一次批量操作的归档）')
def commit(inputs, op, msg, outputs, out_name, out_desc, actor, run_id):
    """提交一次数据变换 (Transform)"""
    input_list = [i.strip() for i in inputs.split(",")]
    payload = {
        "input_ids": input_list,
        "operation": op,
        "description": msg,
        "actor": actor,
        "source": "cli",
        "run_id": run_id
    }

    if out_desc and not out_name:
        click.echo(click.style("✘ Error: --out-desc must be used together with --out-name.", fg='red'))
        sys.exit(1)

    if out_name:
        descs = list(out_desc) if out_desc else []
        outputs_spec = []
        for idx, name in enumerate(out_name):
            name = (name or "").strip()
            if not name:
                click.echo(click.style("✘ Error: output name cannot be empty.", fg='red'))
                sys.exit(1)
            outputs_spec.append({
                "name": name,
                "description": (descs[idx] if idx < len(descs) else "").strip()
            })
        payload["outputs"] = outputs_spec
    else:
        payload["output_count"] = int(outputs or 1)
    
    try:
        r = requests.post(f"{API_URL}/transform/", json=payload)
        if r.status_code == 200:
            data = r.json()
            click.echo(click.style("✔ Transformation Recorded!", fg='green'))
            output_datasets = data.get("output_datasets")
            if output_datasets:
                for idx, out_ds in enumerate(output_datasets, start=1):
                    click.echo(f"  └── Output {idx}: {out_ds['name']} (ID: {out_ds['id']})")
            else:
                out_ds = data.get("output_dataset")
                if out_ds:
                    click.echo(f"  └── Output Dataset: {out_ds['name']} (ID: {out_ds['id']})")
        else:
            click.echo(click.style(f"✘ Error: {r.text}", fg='red'))
    except Exception as e:
        click.echo(f"Error: {e}")

@cli.command()
@click.argument('query', required=False)
def search(query):
    """搜索数据集"""
    params = {"q": query} if query else {}
    r = requests.get(f"{API_URL}/datasets/search", params=params)
    results = r.json()['results']
    
    if not results:
        click.echo("No datasets found.")
        return

    click.echo(f"Found {len(results)} datasets:")
    for d in results:
        click.echo(f"[{d['id']}] {click.style(d['name'], bold=True)} (Tags: {d['tags']})")

@cli.command()
@click.argument("dataset_id")
@click.option("--direction", type=click.Choice(["upstream", "downstream", "both"]), default="both", show_default=True)
@click.option("--depth", type=int, default=2, show_default=True)
@click.option("--start", help="YYYY-MM-DD or ISO datetime")
@click.option("--end", help="YYYY-MM-DD or ISO datetime")
@click.option("--op-types", help="Comma separated operation types, e.g. Clean,Merge")
@click.option("--q", help="Search operation_desc or record id")
@click.option("--pretty/--no-pretty", default=True, show_default=True)
def lineage(dataset_id, direction, depth, start, end, op_types, q, pretty):
    """查询某个数据集的血缘子图（支持 direction/depth + 过滤）。"""
    params = {"direction": direction, "depth": depth}
    if start:
        params["start"] = start
    if end:
        params["end"] = end
    if op_types:
        params["op_types"] = op_types
    if q:
        params["q"] = q

    r = requests.get(f"{API_URL}/lineage/{dataset_id}", params=params)
    if r.status_code != 200:
        click.echo(click.style(f"✘ Error: {r.text}", fg="red"))
        sys.exit(1)

    data = r.json()
    click.echo(click.style(f"✔ OK: nodes={len(data.get('nodes', []))}, edges={len(data.get('edges', []))}", fg="green"))
    if pretty:
        click.echo(json.dumps(data, ensure_ascii=False, indent=2))
    else:
        click.echo(json.dumps(data, ensure_ascii=False))

@cli.command(name="ops")
@click.option("--start", help="YYYY-MM-DD or ISO datetime")
@click.option("--end", help="YYYY-MM-DD or ISO datetime")
@click.option("--q", help="Search operation_desc or record id")
@click.option("--dataset-id", help="Scope to a dataset's lineage")
@click.option("--direction", type=click.Choice(["upstream", "downstream", "both"]), default="both", show_default=True)
@click.option("--depth", type=int, default=2, show_default=True)
def ops(start, end, q, dataset_id, direction, depth):
    """列出 operation 类型及其出现次数（可按数据集血缘范围聚合）。"""
    params = {}
    if start:
        params["start"] = start
    if end:
        params["end"] = end
    if q:
        params["q"] = q
    if dataset_id:
        params["dataset_id"] = dataset_id
        params["direction"] = direction
        params["depth"] = depth

    r = requests.get(f"{API_URL}/operations", params=params)
    if r.status_code != 200:
        click.echo(click.style(f"✘ Error: {r.text}", fg="red"))
        sys.exit(1)
    data = r.json()
    for item in data.get("results", []):
        click.echo(f"{item['operation']}\t{item['count']}")

@cli.command()
@click.argument("dataset_id")
@click.option("--direction", type=click.Choice(["upstream", "downstream", "both"]), default="both", show_default=True)
@click.option("--depth", type=int, default=2, show_default=True)
@click.option("--start", help="YYYY-MM-DD or ISO datetime")
@click.option("--end", help="YYYY-MM-DD or ISO datetime")
@click.option("--op-types", help="Comma separated operation types, e.g. Clean,Merge")
@click.option("--q", help="Search operation_desc or record id")
@click.option("--actor", help="Filter by actor")
@click.option("--source", help="Filter by source")
@click.option("--run-id", help="Filter by run_id")
@click.option("--out", type=click.Path(), help="Write report to a file")
def report(dataset_id, direction, depth, start, end, op_types, q, actor, source, run_id, out):
    """导出可分享报告（Markdown）。"""
    params = {"direction": direction, "depth": depth}
    if start:
        params["start"] = start
    if end:
        params["end"] = end
    if op_types:
        params["op_types"] = op_types
    if q:
        params["q"] = q
    if actor:
        params["actor"] = actor
    if source:
        params["source"] = source
    if run_id:
        params["run_id"] = run_id

    r = requests.get(f"{API_URL}/report/{dataset_id}", params=params)
    if r.status_code != 200:
        click.echo(click.style(f"✘ Error: {r.text}", fg="red"))
        sys.exit(1)

    if out:
        with open(out, "w", encoding="utf-8") as f:
            f.write(r.text)
        click.echo(click.style(f"✔ Report written to {out}", fg="green"))
    else:
        click.echo(r.text)

if __name__ == '__main__':
    cli()
