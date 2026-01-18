#!/usr/bin/env python3
import argparse
import shlex
import subprocess
import sys
from pathlib import Path


def prompt_yes_no(message, default=False):
    suffix = " [Y/n] " if default else " [y/N] "
    while True:
        response = input(message + suffix).strip().lower()
        if not response:
            return default
        if response in ("y", "yes"):
            return True
        if response in ("n", "no"):
            return False
        print("Please answer with y or n.")


def prompt_fk_mode():
    print("\nSelect FK mode for schema steps:")
    print("  1) normal")
    print("  2) skip-fk")
    print("  3) fk-not-valid")
    response = input("Choose [1/2/3] (default: 1): ").strip()
    if response in ("2", "skip", "skip-fk"):
        return ["--skip-fk"]
    if response in ("3", "not-valid", "fk-not-valid"):
        return ["--fk-not-valid"]
    return []


def run_step(title, cmd, cwd):
    cmd_display = " ".join(shlex.quote(part) for part in cmd)
    print(f"\n== {title} ==")
    print(f"Running: {cmd_display}")
    result = subprocess.run(cmd, cwd=cwd)
    if result.returncode != 0:
        print(f"Step failed (exit code {result.returncode}). Stopping.")
        return False
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Run pg-schema-sync step-by-step with prompts.",
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to config YAML (default: config.yaml).",
    )
    parser.add_argument(
        "--install-extensions",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable extension auto-install (default: enabled).",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parent
    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = (repo_root / config_path).resolve()
    if not config_path.exists():
        print(f"Config file not found: {config_path}")
        sys.exit(1)

    base_cmd = [
        sys.executable,
        "-m",
        "src.pg_schema_sync",
        "--config",
        str(config_path),
    ]
    if not args.install_extensions:
        base_cmd.append("--no-install-extensions")

    print("pg-schema-sync stepwise runner")
    print(f"Config: {config_path}")

    fk_args = prompt_fk_mode()

    if prompt_yes_no("Step 1: verify schema differences?", default=True):
        if not run_step("Verify", base_cmd + ["--verify"], cwd=repo_root):
            sys.exit(1)

    if prompt_yes_no("Step 2: generate migration SQL (--no-commit)?", default=True):
        if not run_step("Generate SQL", base_cmd + ["--no-commit"] + fk_args, cwd=repo_root):
            sys.exit(1)

    commit_ok = False
    if prompt_yes_no("Step 3: apply schema migration (--commit)?", default=False):
        commit_ok = run_step("Apply migration", base_cmd + ["--commit"] + fk_args, cwd=repo_root)
        if not commit_ok:
            sys.exit(1)
        if "--fk-not-valid" in fk_args:
            print("FKs were added as NOT VALID. Check history/validate_fks.*.sql to validate later.")

    if commit_ok and prompt_yes_no("Step 4: post-check (--no-commit) after commit?", default=True):
        if not run_step("Post-check", base_cmd + ["--no-commit"] + fk_args, cwd=repo_root):
            sys.exit(1)

    if commit_ok and prompt_yes_no("Optional: run data migration (--with-data)?", default=False):
        if not run_step("Data migration", base_cmd + ["--with-data"] + fk_args, cwd=repo_root):
            sys.exit(1)

    print("\nDone.")


if __name__ == "__main__":
    main()
