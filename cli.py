import click
import requests
import sys

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
def commit(inputs, op, msg):
    """提交一次数据变换 (Transform)"""
    input_list = [i.strip() for i in inputs.split(",")]
    payload = {
        "input_ids": input_list,
        "operation": op,
        "description": msg
    }
    
    try:
        r = requests.post(f"{API_URL}/transform/", json=payload)
        if r.status_code == 200:
            data = r.json()
            click.echo(click.style("✔ Transformation Recorded!", fg='green'))
            click.echo(f"  └── Output Dataset: {data['output_dataset']['name']} (ID: {data['output_dataset']['id']})")
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

if __name__ == '__main__':
    cli()