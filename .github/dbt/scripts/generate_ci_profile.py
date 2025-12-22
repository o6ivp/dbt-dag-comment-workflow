#!/usr/bin/env python3
"""
dbt_project.yml からプロファイル名を取得し、CI用の profiles.yml を生成するスクリプト

Usage:
    python generate_ci_profile.py --project-dir dbt_core --output ~/.dbt/profiles.yml
"""

import argparse
import sys
from pathlib import Path

try:
    import yaml
except ImportError:
    # PyYAMLがない場合は簡易パーサーを使用
    yaml = None


def parse_profile_name_simple(content: str) -> str:
    """簡易的にprofile名を抽出（PyYAMLなし）"""
    for line in content.splitlines():
        line = line.strip()
        if line.startswith("profile:"):
            # profile: 'name' or profile: "name" or profile: name
            value = line.split(":", 1)[1].strip()
            return value.strip("'\"")
    return None


def get_profile_name(project_dir: Path) -> str:
    """dbt_project.yml からプロファイル名を取得"""
    project_file = project_dir / "dbt_project.yml"

    if not project_file.exists():
        raise FileNotFoundError(f"dbt_project.yml not found at {project_file}")

    content = project_file.read_text()

    if yaml:
        data = yaml.safe_load(content)
        return data.get("profile")
    else:
        return parse_profile_name_simple(content)


def get_adapter_type(project_dir: Path, profile_name: str) -> str:
    """profiles.yml からアダプタータイプを取得"""
    profiles_file = project_dir / "profiles.yml"

    if not profiles_file.exists():
        return None

    content = profiles_file.read_text()

    if yaml:
        data = yaml.safe_load(content)
        profile = data.get(profile_name, {})
        outputs = profile.get("outputs", {})
        # 最初のoutputからtypeを取得
        for output in outputs.values():
            if isinstance(output, dict) and "type" in output:
                return output["type"]
    else:
        # 簡易パーサー
        for line in content.splitlines():
            line = line.strip()
            if line.startswith("type:"):
                return line.split(":", 1)[1].strip()

    return None


def generate_ci_profile(profile_name: str, adapter: str = "redshift") -> str:
    """CI用のprofiles.ymlを生成"""

    # アダプター別のダミー設定
    adapter_configs = {
        "redshift": {
            "type": "redshift",
            "host": "localhost",
            "port": 5439,
            "user": "ci",
            "password": "ci",
            "dbname": "ci",
            "schema": "public",
            "threads": 1,
        },
        "postgres": {
            "type": "postgres",
            "host": "localhost",
            "port": 5432,
            "user": "ci",
            "password": "ci",
            "dbname": "ci",
            "schema": "public",
            "threads": 1,
        },
        "snowflake": {
            "type": "snowflake",
            "account": "ci",
            "user": "ci",
            "password": "ci",
            "database": "ci",
            "warehouse": "ci",
            "schema": "public",
            "threads": 1,
        },
        "bigquery": {
            "type": "bigquery",
            "method": "oauth",
            "project": "ci",
            "dataset": "ci",
            "threads": 1,
        },
    }

    config = adapter_configs.get(adapter, adapter_configs["postgres"])

    # YAML形式で出力
    lines = [
        f"{profile_name}:",
        "  target: ci",
        "  outputs:",
        "    ci:",
    ]

    for key, value in config.items():
        lines.append(f"      {key}: {value}")

    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(
        description="Generate CI profiles.yml from dbt_project.yml"
    )
    parser.add_argument(
        "--project-dir",
        required=True,
        help="Path to dbt project directory containing dbt_project.yml",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output path for profiles.yml",
    )
    parser.add_argument(
        "--adapter",
        default=None,
        choices=["redshift", "postgres", "snowflake", "bigquery"],
        help="dbt adapter type (auto-detected from profiles.yml if not specified)",
    )

    args = parser.parse_args()

    project_dir = Path(args.project_dir)
    output_path = Path(args.output)

    try:
        profile_name = get_profile_name(project_dir)
        if not profile_name:
            print("Error: Could not find 'profile' in dbt_project.yml", file=sys.stderr)
            sys.exit(1)

        # アダプタータイプを自動検出または引数から取得
        adapter = args.adapter
        if not adapter:
            adapter = get_adapter_type(project_dir, profile_name)
        if not adapter:
            print("Error: Could not detect adapter type. Specify --adapter", file=sys.stderr)
            sys.exit(1)

        print(f"Detected: profile={profile_name}, adapter={adapter}")
        profile_content = generate_ci_profile(profile_name, adapter)

        # 出力ディレクトリを作成
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(profile_content)

        print(f"Generated CI profile for '{profile_name}' at {output_path}")

    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

