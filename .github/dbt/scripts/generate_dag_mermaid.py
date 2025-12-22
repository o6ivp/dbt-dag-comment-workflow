#!/usr/bin/env python3
"""
dbt manifest.json から変更されたモデルの DAG を Mermaid 形式で生成するスクリプト

Usage:
    python generate_dag_mermaid.py --manifest path/to/manifest.json --models "model1 model2" --output dag.md
    python generate_dag_mermaid.py --manifest path/to/manifest.json --changed-files "models/staging/model1.sql models/marts/model2.sql" --output dag.md
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Optional


def extract_model_name_from_path(file_path: str) -> Optional[str]:
    """ファイルパスからモデル名を抽出する"""
    path = Path(file_path)
    if path.suffix == ".sql":
        return path.stem
    return None


def filter_test_nodes(nodes: dict) -> dict:
    """テストノードを除外する"""
    return {k: v for k, v in nodes.items() if v.get("resource_type") != "test"}


def collect_lineage(
    manifest: dict,
    target_models: set[str],
    depth_upstream: int = 2,
    depth_downstream: int = 2,
    exclude_tests: bool = True,
) -> tuple[set[str], list[tuple[str, str]]]:
    """
    対象モデルの上流・下流のリネージを収集する

    Returns:
        relevant_nodes: 関連するノードIDのセット
        edges: (source_id, target_id) のリスト
    """
    nodes = manifest.get("nodes", {})
    sources = manifest.get("sources", {})

    if exclude_tests:
        nodes = filter_test_nodes(nodes)

    all_nodes = {**nodes, **sources}

    # モデル名からノードIDへのマッピング
    name_to_id = {}
    for node_id, node in all_nodes.items():
        name = node.get("name")
        if name:
            # 複数のノードが同じ名前を持つ場合があるため、リストで保持
            if name not in name_to_id:
                name_to_id[name] = []
            name_to_id[name].append(node_id)

    # 対象モデルのノードIDを特定
    target_node_ids = set()
    for model_name in target_models:
        if model_name in name_to_id:
            target_node_ids.update(name_to_id[model_name])

    relevant_nodes = set(target_node_ids)
    edges = []

    # 下流ノードのマッピングを作成
    downstream_map: dict[str, list[str]] = {}
    for node_id, node in nodes.items():
        for dep in node.get("depends_on", {}).get("nodes", []):
            if dep not in downstream_map:
                downstream_map[dep] = []
            downstream_map[dep].append(node_id)

    # 変更対象モデル間のエッジを先に収集
    for node_id in target_node_ids:
        node = all_nodes.get(node_id, {})
        for dep in node.get("depends_on", {}).get("nodes", []):
            if dep in all_nodes:
                edges.append((dep, node_id))

    # 上流を収集（BFS）
    def collect_upstream(start_ids: set[str], max_depth: int) -> None:
        current_level = start_ids
        depth = 0
        while current_level and (max_depth == -1 or depth < max_depth):
            next_level = set()
            for node_id in current_level:
                node = all_nodes.get(node_id, {})
                for dep in node.get("depends_on", {}).get("nodes", []):
                    if dep in all_nodes:
                        edges.append((dep, node_id))
                        if dep not in relevant_nodes:
                            relevant_nodes.add(dep)
                            next_level.add(dep)
            current_level = next_level
            depth += 1

    # 下流を収集（BFS）
    def collect_downstream(start_ids: set[str], max_depth: int) -> None:
        current_level = start_ids
        depth = 0
        while current_level and (max_depth == -1 or depth < max_depth):
            next_level = set()
            for node_id in current_level:
                for child_id in downstream_map.get(node_id, []):
                    if child_id not in relevant_nodes:
                        relevant_nodes.add(child_id)
                        edges.append((node_id, child_id))
                        next_level.add(child_id)
            current_level = next_level
            depth += 1

    collect_upstream(target_node_ids, depth_upstream)
    collect_downstream(target_node_ids, depth_downstream)

    return relevant_nodes, edges


def generate_mermaid(
    manifest: dict,
    relevant_nodes: set[str],
    edges: list[tuple[str, str]],
    changed_models: set[str],
    exclude_tests: bool = True,
) -> str:
    """Mermaid形式のDAGを生成する"""
    nodes = manifest.get("nodes", {})
    sources = manifest.get("sources", {})

    if exclude_tests:
        nodes = filter_test_nodes(nodes)

    all_nodes = {**nodes, **sources}

    lines = ["```mermaid", "flowchart LR"]

    # スタイル定義
    lines.append("    %% Style definitions")
    lines.append("    classDef changed fill:#ff6b6b,stroke:#c92a2a,color:#fff")
    lines.append("    classDef default fill:#fff,stroke:#333,color:#333")
    lines.append("")

    # ノードIDからサニタイズされたIDへのマッピング
    node_id_map = {}
    name_counter: dict[str, int] = {}  # 重複する名前を処理

    def get_unique_id(name: str, resource_type: str) -> str:
        """ノード名からユニークなIDを生成"""
        # プレフィックスを追加して識別しやすくする
        prefix = {"source": "src", "snapshot": "snp"}.get(resource_type, "")
        base_name = f"{prefix}_{name}" if prefix else name

        # 重複チェック
        if base_name in name_counter:
            name_counter[base_name] += 1
            return f"{base_name}_{name_counter[base_name]}"
        else:
            name_counter[base_name] = 0
            return base_name

    # ノード定義
    lines.append("    %% Nodes")
    for node_id in sorted(relevant_nodes):
        node = all_nodes.get(node_id, {})
        if not node:
            continue

        name = node.get("name", node_id.split(".")[-1])
        resource_type = node.get("resource_type", "model")

        safe_id = get_unique_id(name, resource_type)
        node_id_map[node_id] = safe_id

        # ノードの形状とラベル
        if resource_type == "source":
            # ソースは円筒形
            source_name = node.get("source_name", "")
            label = f"{source_name}.{name}" if source_name else name
            shape = f'[("{label}")]'
        elif resource_type == "snapshot":
            # スナップショットは六角形
            shape = f'{{{{"{name}"}}}}'
        else:
            # モデルは角丸四角形
            shape = f'["{name}"]'

        # 変更されたモデルのみハイライト、それ以外はデフォルト
        if name in changed_models:
            lines.append(f"    {safe_id}{shape}:::changed")
        else:
            lines.append(f"    {safe_id}{shape}")

    # エッジ定義
    lines.append("")
    lines.append("    %% Edges")
    unique_edges = set(edges)
    for src_id, dst_id in sorted(unique_edges):
        # 両方のノードがマッピングに存在する場合のみエッジを追加
        if src_id in node_id_map and dst_id in node_id_map:
            src_safe = node_id_map[src_id]
            dst_safe = node_id_map[dst_id]
            lines.append(f"    {src_safe} --> {dst_safe}")

    lines.append("```")

    return "\n".join(lines)


def generate_summary(
    manifest: dict,
    changed_models: set[str],
    relevant_nodes: set[str],
    exclude_tests: bool = True,
) -> str:
    """変更サマリーを生成"""
    nodes = manifest.get("nodes", {})
    sources = manifest.get("sources", {})

    if exclude_tests:
        nodes = filter_test_nodes(nodes)

    all_nodes = {**nodes, **sources}

    # モデル名からノードIDへのマッピング
    name_to_id = {}
    for node_id, node in all_nodes.items():
        name = node.get("name")
        if name:
            if name not in name_to_id:
                name_to_id[name] = []
            name_to_id[name].append(node_id)

    # 変更されたモデルをmanifestに存在するものと削除されたものに分類
    changed_existing = set()
    changed_deleted = set()
    for model in changed_models:
        if model in name_to_id:
            changed_existing.add(model)
        else:
            changed_deleted.add(model)

    # 変更されたモデルのノードIDを取得
    changed_node_ids = set()
    for model in changed_existing:
        if model in name_to_id:
            changed_node_ids.update(name_to_id[model])

    # 統計（変更モデルを除いた関連モデル）
    related_model_count = sum(
        1 for nid in relevant_nodes
        if all_nodes.get(nid, {}).get("resource_type") == "model" and nid not in changed_node_ids
    )
    source_count = sum(1 for nid in relevant_nodes if all_nodes.get(nid, {}).get("resource_type") == "source")
    snapshot_count = sum(1 for nid in relevant_nodes if all_nodes.get(nid, {}).get("resource_type") == "snapshot")

    # 変更スナップショットを別カウント
    changed_snapshot_count = sum(
        1 for nid in relevant_nodes
        if all_nodes.get(nid, {}).get("resource_type") == "snapshot"
        and all_nodes.get(nid, {}).get("name") in changed_models
    )
    related_snapshot_count = snapshot_count - changed_snapshot_count

    lines = [
        "### Summary",
        "",
        "| Type | Count |",
        "|------|-------|",
        f"| Changed | {len(changed_existing)} |",
        f"| Related Models | {related_model_count} |",
        f"| Related Snapshots | {related_snapshot_count} |",
        f"| Sources | {source_count} |",
    ]

    if changed_deleted:
        lines.append(f"| Deleted | {len(changed_deleted)} |")

    lines.extend(["", "**Changed:**"])

    for model in sorted(changed_existing):
        lines.append(f"- `{model}`")

    if changed_deleted:
        lines.extend(["", "**Deleted:**"])
        for model in sorted(changed_deleted):
            lines.append(f"- `{model}`")

    lines.append("")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Generate Mermaid DAG from dbt manifest.json"
    )
    parser.add_argument(
        "--manifest",
        required=True,
        help="Path to manifest.json",
    )
    parser.add_argument(
        "--models",
        default="",
        help="Space-separated list of model names",
    )
    parser.add_argument(
        "--changed-files",
        default="",
        help="Space-separated list of changed file paths",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output file path",
    )
    parser.add_argument(
        "--depth-upstream",
        type=int,
        default=2,
        help="Depth of upstream lineage to include (default: 2, use -1 for all)",
    )
    parser.add_argument(
        "--depth-downstream",
        type=int,
        default=2,
        help="Depth of downstream lineage to include (default: 2, use -1 for all)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Show all related models (equivalent to --depth-upstream -1 --depth-downstream -1)",
    )

    args = parser.parse_args()

    # manifest.json を読み込み
    try:
        with open(args.manifest) as f:
            manifest = json.load(f)
    except FileNotFoundError:
        print(f"Error: manifest.json not found at {args.manifest}", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in manifest.json: {e}", file=sys.stderr)
        sys.exit(1)

    # 対象モデルを特定
    target_models: set[str] = set()

    # --models オプションから
    if args.models:
        for model in args.models.split():
            model = model.strip()
            if model:
                target_models.add(model)

    # --changed-files オプションから
    if args.changed_files:
        for file_path in args.changed_files.split():
            file_path = file_path.strip()
            if file_path:
                model_name = extract_model_name_from_path(file_path)
                if model_name:
                    target_models.add(model_name)

    if not target_models:
        # 変更がない場合
        output_content = "No dbt model changes detected in this PR."
        with open(args.output, "w") as f:
            f.write(output_content)
        print(output_content)
        sys.exit(0)

    # --all オプションの処理
    depth_upstream = args.depth_upstream
    depth_downstream = args.depth_downstream
    if args.all:
        depth_upstream = -1
        depth_downstream = -1

    # リネージを収集
    relevant_nodes, edges = collect_lineage(
        manifest,
        target_models,
        depth_upstream=depth_upstream,
        depth_downstream=depth_downstream,
    )

    if not relevant_nodes:
        output_content = f"Models specified ({', '.join(target_models)}) not found in manifest.json"
        with open(args.output, "w") as f:
            f.write(output_content)
        print(output_content, file=sys.stderr)
        sys.exit(1)

    # 出力を生成
    summary = generate_summary(manifest, target_models, relevant_nodes)
    mermaid = generate_mermaid(manifest, relevant_nodes, edges, target_models)

    output_content = f"{summary}\n{mermaid}"

    with open(args.output, "w") as f:
        f.write(output_content)

    print(f"DAG generated successfully: {args.output}")
    print(f"  - Changed models: {len(target_models)}")
    print(f"  - Total nodes in DAG: {len(relevant_nodes)}")
    print(f"  - Total edges: {len(set(edges))}")


if __name__ == "__main__":
    main()

