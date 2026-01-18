#!/usr/bin/env python3
import argparse
import datetime
import json
import os
import re
import shlex
import subprocess
import sys
from pathlib import Path
from urllib import request, error

import yaml

LOG_HANDLE = None


def prompt_yes_no(message, default=False):
    suffix = " [Y/n] " if default else " [y/N] "
    while True:
        response = input(message + suffix).strip()
        log_only(f"[input] {message} -> {response if response else '<default>'}")
        response = response.lower()
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
    print("     EN: Apply FKs immediately (may fail if target has orphaned rows).")
    print("     KO: FK를 즉시 적용합니다(고아 데이터가 있으면 실패할 수 있습니다).")
    print("  2) skip-fk")
    print("     EN: Apply schema without FKs (add FKs later).")
    print("     KO: FK를 제외하고 스키마만 적용합니다(나중에 FK 추가).")
    print("  3) fk-not-valid")
    print("     EN: Add FKs as NOT VALID, then validate after cleanup.")
    print("     KO: FK를 NOT VALID로 추가하고 정리 후 검증합니다.")
    response = input("Choose [1/2/3] (default: 1): ").strip()
    log_only(f"[input] FK mode -> {response if response else '<default>'}")
    if response in ("2", "skip", "skip-fk"):
        return ["--skip-fk"]
    if response in ("3", "not-valid", "fk-not-valid"):
        return ["--fk-not-valid"]
    return []


def prompt_gemini_choice():
    print("\nNeed Assistant with Gemini?")
    print("  S) Yes with Summary")
    print("     EN: Send error summary only (recommended).")
    print("     KO: 오류 요약만 전송합니다(권장).")
    print("  L) Yes with Log")
    print("     EN: Send error summary and log tail.")
    print("     KO: 오류 요약과 로그 끝부분을 전송합니다.")
    print("  N) Exit")
    print("     EN: Skip Gemini analysis.")
    print("     KO: Gemini 분석을 건너뜁니다.")
    response = input("Choose [S/L/N] (default: S): ").strip()
    log_only(f"[input] Gemini choice -> {response if response else '<default>'}")
    normalized = response.lower()
    if normalized in ("", "s"):
        return "summary"
    if normalized == "l":
        return "summary_tail"
    if normalized == "n":
        return None
    return "summary"

def prompt_remediation_choice():
    print("\nGenerate remediation script with Gemini?")
    print("  S) Save script only")
    print("     EN: Save a Python helper script; do not run.")
    print("     KO: 파이썬 헬퍼 스크립트를 저장만 합니다.")
    print("  R) Save and run")
    print("     EN: Save and execute the script now.")
    print("     KO: 저장 후 즉시 실행합니다.")
    print("  N) Skip")
    print("     EN: Skip script generation.")
    print("     KO: 스크립트 생성을 건너뜁니다.")
    response = input("Choose [S/R/N] (default: N): ").strip()
    log_only(f"[input] Gemini remediation choice -> {response if response else '<default>'}")
    normalized = response.lower()
    if normalized == "s":
        return "save"
    if normalized == "r":
        return "run"
    return None


def run_step(title, cmd, cwd, allowed_returncodes=None):
    cmd_display = " ".join(shlex.quote(part) for part in cmd)
    print(f"\n== {title} ==")
    print(f"Running: {cmd_display}")
    if allowed_returncodes is None:
        allowed_returncodes = {0}
    try:
        process = subprocess.Popen(
            cmd,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        assert process.stdout is not None
        for line in process.stdout:
            print(line, end="")
        returncode = process.wait()
    except KeyboardInterrupt:
        process.terminate()
        process.wait()
        print("\nAborted.")
        return False, 130
    if returncode not in allowed_returncodes:
        print(f"Step failed (exit code {returncode}). Stopping.")
        return False, returncode
    if returncode != 0:
        print(f"Step completed with warnings (exit code {returncode}).")
    return True, returncode


def load_config(config_path):
    try:
        with open(config_path, "r", encoding="utf-8") as handle:
            config = yaml.safe_load(handle)
            if not config:
                print(f"Error: {config_path} is empty or invalid.")
                return None
            return config
    except FileNotFoundError:
        print(f"Error: {config_path} not found.")
    except yaml.YAMLError as exc:
        print(f"Error parsing {config_path}: {exc}")
    except Exception as exc:
        print(f"Unexpected error reading {config_path}: {exc}")
    return None


def normalize_conn_config(config):
    normalized = dict(config)
    if "db" in normalized:
        normalized["dbname"] = normalized.pop("db")
    if "username" in normalized:
        normalized["user"] = normalized.pop("username")
    return normalized


class TeeStream:
    def __init__(self, primary, secondary):
        self.primary = primary
        self.secondary = secondary

    def write(self, data):
        self.primary.write(data)
        self.secondary.write(data)
        self.flush()

    def flush(self):
        self.primary.flush()
        self.secondary.flush()


def setup_logging(log_file):
    global LOG_HANDLE
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_handle = open(log_path, "a", encoding="utf-8")
    LOG_HANDLE = log_handle
    original_stdout = sys.stdout
    original_stderr = sys.stderr
    sys.stdout = TeeStream(sys.stdout, log_handle)
    sys.stderr = TeeStream(sys.stderr, log_handle)
    return log_path, log_handle, original_stdout, original_stderr


def log_only(message):
    if LOG_HANDLE is None:
        return
    LOG_HANDLE.write(message + "\n")
    LOG_HANDLE.flush()

def flush_log():
    if LOG_HANDLE is None:
        return
    LOG_HANDLE.flush()


def pick_validate_file(history_dir):
    candidates = sorted(
        Path(history_dir).glob("validate_fks.*.sql"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    default_path = candidates[0] if candidates else None
    if default_path:
        response = input(f"Validate SQL path (default: {default_path}): ").strip()
        path = Path(response) if response else default_path
    else:
        response = input("Validate SQL path: ").strip()
        path = Path(response) if response else None
    if not path:
        print("No validate SQL path provided.")
        return None
    if not path.exists():
        print(f"Validate SQL not found: {path}")
        return None
    return path


def find_latest_validate_file(history_dir):
    candidates = sorted(
        Path(history_dir).glob("validate_fks.*.sql"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def run_validate_fks(target_config, validate_path):
    try:
        import psycopg2
    except ImportError:
        print("psycopg2 is required to validate FKs. Install dependencies first.")
        return False

    statements = []
    with open(validate_path, "r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped or stripped.startswith("--"):
                continue
            statements.append(stripped)
    if not statements:
        print("No FK validation statements found.")
        return True

    try:
        conn = psycopg2.connect(**target_config)
        with conn:
            with conn.cursor() as cur:
                for stmt in statements:
                    cur.execute(stmt)
        print("FK validation completed.")
        return True
    except psycopg2.Error as exc:
        print(f"FK validation failed: {exc}")
        return False
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_table_names(conn):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """)
        return [row[0] for row in cur.fetchall()]


def run_schema_integrity_check(target_config):
    try:
        import psycopg2
    except ImportError:
        print("psycopg2 is required to check schema integrity. Install dependencies first.")
        return False

    try:
        conn = psycopg2.connect(**target_config)
        with conn.cursor() as cur:
            cur.execute("""
            SELECT c.conname, t.relname, c.contype
            FROM pg_constraint c
            JOIN pg_class t ON c.conrelid = t.oid
            JOIN pg_namespace n ON t.relnamespace = n.oid
            WHERE n.nspname = 'public' AND c.convalidated = false
            ORDER BY t.relname, c.conname
            """)
            invalid = cur.fetchall()
        if invalid:
            print("Invalid (NOT VALID) constraints:")
            for name, table, contype in invalid:
                print(f"  - {table}.{name} ({contype})")
        else:
            print("No NOT VALID constraints found.")
        return True
    except psycopg2.Error as exc:
        print(f"Schema integrity check failed: {exc}")
        return False
    finally:
        try:
            conn.close()
        except Exception:
            pass


def run_data_integrity_check(source_config, target_config):
    try:
        import psycopg2
        from psycopg2 import sql
    except ImportError:
        print("psycopg2 is required to check data integrity. Install dependencies first.")
        return False

    try:
        src_conn = psycopg2.connect(**source_config)
        tgt_conn = psycopg2.connect(**target_config)
        src_tables = set(fetch_table_names(src_conn))
        tgt_tables = set(fetch_table_names(tgt_conn))
        common_tables = sorted(src_tables & tgt_tables)
        if not common_tables:
            print("No common tables found for row count comparison.")
            return True
        diffs = {}
        with src_conn.cursor() as src_cur, tgt_conn.cursor() as tgt_cur:
            for table in common_tables:
                src_cur.execute(
                    sql.SQL("SELECT COUNT(*) FROM {}").format(
                        sql.Identifier("public", table)
                    )
                )
                src_count = src_cur.fetchone()[0]
                tgt_cur.execute(
                    sql.SQL("SELECT COUNT(*) FROM {}").format(
                        sql.Identifier("public", table)
                    )
                )
                tgt_count = tgt_cur.fetchone()[0]
                if src_count != tgt_count:
                    diffs[table] = (src_count, tgt_count)
        if diffs:
            print("Row count differences (source vs target):")
            for table, (src_count, tgt_count) in diffs.items():
                print(f"  - {table}: {src_count} vs {tgt_count}")
        else:
            print("Row counts match for all compared tables.")
        return True
    except psycopg2.Error as exc:
        print(f"Data integrity check failed: {exc}")
        return False
    finally:
        try:
            src_conn.close()
            tgt_conn.close()
        except Exception:
            pass


def load_env_file(env_path):
    if not env_path.exists():
        return
    try:
        with open(env_path, "r", encoding="utf-8") as handle:
            for line in handle:
                stripped = line.strip()
                if not stripped or stripped.startswith("#") or "=" not in stripped:
                    continue
                key, value = stripped.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = value
    except Exception as exc:
        print(f"Warning: failed to load {env_path}: {exc}")


def extract_log_context(log_path, max_tail_lines=300, max_error_lines=200):
    try:
        lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception as exc:
        return "", f"Failed to read log file: {exc}"
    tail = lines[-max_tail_lines:]
    keywords = (
        "Error:", "ERROR", "Traceback", "failed", "FAILED", "❌", "Undefined",
        "UniqueViolation", "violates", "aborted", "InFailedSqlTransaction",
    )
    error_lines = [line for line in lines if any(k in line for k in keywords)]
    error_lines = error_lines[-max_error_lines:]
    summary = "\n".join(error_lines)
    context = "\n".join(tail)
    return summary, context


def call_gemini(prompt, api_key, model="gemini-2.5-flash"):
    url = (
        f"https://generativelanguage.googleapis.com/v1beta/models/"
        f"{model}:generateContent?key={api_key}"
    )
    payload = {
        "contents": [
            {
                "parts": [
                    {"text": prompt}
                ]
            }
        ]
    }
    data = json.dumps(payload).encode("utf-8")
    req = request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=60) as resp:
            body = resp.read().decode("utf-8")
        parsed = json.loads(body)
    except error.HTTPError as exc:
        return None, f"HTTP error from Gemini: {exc}"
    except Exception as exc:
        return None, f"Failed to call Gemini: {exc}"

    candidates = parsed.get("candidates") or []
    if not candidates:
        return None, "Gemini response had no candidates."
    parts = candidates[0].get("content", {}).get("parts", [])
    text = "".join(part.get("text", "") for part in parts)
    return text.strip(), None


def run_gemini_log_analysis(log_path, env_path, scope, status_summary=None, pending_checks=None):
    load_env_file(env_path)
    api_key = os.environ.get("GEMINI_KEY") or os.environ.get("GEMINI_API_KEY")
    if not api_key:
        print("Gemini key not found. Set GEMINI_KEY in .env or environment.")
        return False

    summary, context = extract_log_context(log_path)
    if context.startswith("Failed to read log file"):
        print(context)
        return False

    summary_block = summary or "(no error lines detected)"
    status_block = status_summary or "(no status summary provided)"
    pending_block = "\n".join(f"- {item}" for item in (pending_checks or [])) or "(none)"
    tool_usage_block = """Tool usage context:
- This runner is interactive; steps run only when the user answers yes.
- "commit schema: skipped" usually means Step 3 was declined, not an error.
- "post-check" runs --no-commit after commit; "schema integrity" runs --verify + NOT VALID listing.
- "data integrity" compares row counts only; it does not validate FK consistency.
- Data migration may exit with code 2 for partial failures; the runner continues and marks it failed.
- FK modes: normal / skip-fk / fk-not-valid (NOT VALID requires later validation).
"""

    prompt = f"""You are a PostgreSQL migration assistant.
Analyze the log summary and context, identify root causes, and propose concrete fixes.
Respond in Korean with clear bullet points, and include next-step commands if applicable.
Return TWO sections in this order:
1) Tool-specific actions for this runner/pg-schema-sync (exact step choices or commands).
2) General migration advice that applies regardless of tool.

{tool_usage_block}

Status summary (from the runner):
{status_block}

Pending verification steps (from the runner):
{pending_block}

Log summary (errors only):
{summary_block}
"""
    if scope == "summary_tail":
        prompt += f"""
Log tail:
{context}
"""

    print("\n== Gemini Log Analysis ==")
    response, err = call_gemini(prompt, api_key)
    if err:
        print(err)
        return False
    print(response)
    return True


def extract_json_payload(text):
    fence = "```json"
    if fence in text:
        start = text.index(fence) + len(fence)
        end = text.find("```", start)
        if end != -1:
            return text[start:end].strip()
    return text.strip()

def try_parse_json_payload(text):
    decoder = json.JSONDecoder()
    for idx, ch in enumerate(text):
        if ch not in "{[":
            continue
        try:
            payload, _ = decoder.raw_decode(text[idx:])
            return payload
        except json.JSONDecodeError:
            continue
    return None

def extract_python_code_block(text):
    for fence in ("```python", "```py"):
        start = text.find(fence)
        if start == -1:
            continue
        start += len(fence)
        end = text.find("```", start)
        if end == -1:
            continue
        return text[start:end].strip()
    return None

def normalize_text_field(value):
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return "\n".join(str(item) for item in value)
    try:
        return json.dumps(value, ensure_ascii=False)
    except Exception:
        return str(value)


def normalize_list_field(value):
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, str):
        return [value]
    return []


def print_script_preview(script_path, max_lines=120, max_chars=4000):
    try:
        lines = script_path.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception as exc:
        print(f"Failed to read remediation script: {exc}")
        return
    if not lines:
        return
    preview_lines = []
    total_chars = 0
    truncated = False
    for line in lines:
        line_len = len(line) + 1
        if len(preview_lines) >= max_lines or (total_chars + line_len) > max_chars:
            truncated = True
            break
        preview_lines.append(line)
        total_chars += line_len
    print("\nRemediation script preview:")
    print("```python")
    print("\n".join(preview_lines))
    if truncated:
        print("# ... truncated ...")
    print("```")

def print_text_preview(label, text, max_lines=120, max_chars=4000):
    lines = text.splitlines()
    preview_lines = []
    total_chars = 0
    truncated = False
    for line in lines:
        line_len = len(line) + 1
        if len(preview_lines) >= max_lines or (total_chars + line_len) > max_chars:
            truncated = True
            break
        preview_lines.append(line)
        total_chars += line_len
    print(f"\n{label}:")
    for line in preview_lines:
        print(line)
    if truncated:
        print("... (truncated) ...")


def run_gemini_remediation(
    log_path,
    env_path,
    scope,
    config_path,
    status_summary=None,
    pending_checks=None,
):
    load_env_file(env_path)
    api_key = os.environ.get("GEMINI_KEY") or os.environ.get("GEMINI_API_KEY")
    if not api_key:
        print("Gemini key not found. Set GEMINI_KEY in .env or environment.")
        return None, []

    summary, context = extract_log_context(log_path)
    if context.startswith("Failed to read log file"):
        print(context)
        return None, []

    summary_block = summary or "(no error lines detected)"
    status_block = status_summary or "(no status summary provided)"
    pending_block = "\n".join(f"- {item}" for item in (pending_checks or [])) or "(none)"
    tool_usage_block = """Tool usage context:
- This runner is interactive; steps run only when the user answers yes.
- "commit schema: skipped" usually means Step 3 was declined, not an error.
- "post-check" runs --no-commit after commit; "schema integrity" runs --verify + NOT VALID listing.
- "data integrity" compares row counts only; it does not validate FK consistency.
- Data migration may exit with code 2 for partial failures; the runner continues and marks it failed.
- FK modes: normal / skip-fk / fk-not-valid (NOT VALID requires later validation).
"""

    prompt = f"""You are a PostgreSQL migration assistant.
Generate a remediation helper for this pg-schema-sync runner.
Return ONLY JSON with keys: "python_script", "shell_commands", "notes".

Rules:
- "python_script" must be a complete Python 3 script as a single string.
- The script must be safe by default (print actions, ask before executing commands).
- Use this config path when calling the tool: {config_path}
- "shell_commands" must be a JSON array of command strings (alternatives).
- "notes" must be short Korean instructions (1-3 bullets).
- Do not include Markdown or code fences.

{tool_usage_block}

Status summary (from the runner):
{status_block}

Pending verification steps (from the runner):
{pending_block}

Log summary (errors only):
{summary_block}
"""
    if scope == "summary_tail":
        prompt += f"""
Log tail:
{context}
"""

    print("\n== Gemini Remediation Script ==")
    response, err = call_gemini(prompt, api_key)
    if err:
        print(err)
        return None, []

    payload_text = extract_json_payload(response)
    payload = None
    try:
        payload = json.loads(payload_text)
    except json.JSONDecodeError:
        payload = try_parse_json_payload(response)

    if not isinstance(payload, dict):
        python_script = extract_python_code_block(response)
        if python_script:
            script_path = log_path.parent / f"remediation.{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.py"
            script_path.write_text(python_script + "\n", encoding="utf-8")
            print("Failed to parse JSON. Extracted Python code block instead.")
            print(f"Remediation script saved to {script_path}")
            print_script_preview(script_path)
            return script_path, []
        fallback_path = log_path.parent / f"remediation.{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.txt"
        fallback_path.write_text(response, encoding="utf-8")
        print(f"Failed to parse JSON. Raw response saved to {fallback_path}")
        print_text_preview("Raw Gemini response preview", response)
        return None, []

    python_script = normalize_text_field(payload.get("python_script")).strip()
    shell_commands = normalize_list_field(payload.get("shell_commands"))
    notes_raw = payload.get("notes")
    if isinstance(notes_raw, list):
        notes = "\n".join(f"- {item}" for item in notes_raw)
    else:
        notes = normalize_text_field(notes_raw).strip()

    script_path = None
    if python_script:
        script_path = log_path.parent / f"remediation.{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.py"
        script_path.write_text(python_script + "\n", encoding="utf-8")
        print(f"Remediation script saved to {script_path}")
        print_script_preview(script_path)
    if notes:
        print("Notes:")
        print(notes)
    if shell_commands:
        print("Shell alternatives:")
        for cmd in shell_commands:
            print(f"  - {cmd}")
    return script_path, shell_commands


def fk_mode_from_args(fk_args):
    if "--skip-fk" in fk_args:
        return "skip-fk"
    if "--fk-not-valid" in fk_args:
        return "fk-not-valid"
    return "normal"

def build_status_lines(step_status, fk_args, validated_fks_now, data_migration_failed):
    if data_migration_failed:
        data_migration_state = "failed"
    elif step_status["data_migration"]:
        data_migration_state = "done"
    else:
        data_migration_state = "skipped"

    return [
        f"verify: {'done' if step_status['verify'] else 'skipped'}",
        f"generate SQL: {'done' if step_status['generate_sql'] else 'skipped'}",
        f"commit schema: {'done' if step_status['commit'] else 'skipped'}",
        f"post-check: {'done' if step_status['post_check'] else 'skipped'}",
        f"data migration: {data_migration_state}",
        f"schema integrity: {'done' if step_status['schema_integrity'] else 'skipped'}",
        f"data integrity: {'done' if step_status['data_integrity'] else 'skipped'}",
        f"FK mode: {fk_mode_from_args(fk_args)}",
        f"FK validated: {'yes' if validated_fks_now else 'no'}",
    ]


def build_pending_checks(commit_ok, step_status, fk_args, added_fks_now, validated_fks_now, data_migration_failed):
    pending_checks = []
    if commit_ok and not step_status["verify"]:
        pending_checks.append("Schema verify (--verify) / 스키마 검증")
    if commit_ok and not step_status["post_check"]:
        pending_checks.append("Post-check (--no-commit) / 사후 검증")
    if fk_mode_from_args(fk_args) == "skip-fk" and not added_fks_now:
        pending_checks.append("Add FKs later (--fk-not-valid) / FK 추가")
    if fk_mode_from_args(fk_args) == "fk-not-valid" and not validated_fks_now:
        pending_checks.append("Validate FKs (validate_fks.*.sql) / FK 검증")
    if commit_ok and not step_status["schema_integrity"]:
        pending_checks.append("Schema integrity check / 스키마 무결성 체크")
    if commit_ok and not step_status["data_integrity"]:
        pending_checks.append("Data integrity check / 데이터 무결성 체크")
    if data_migration_failed:
        pending_checks.append("Resolve data migration failures (--with-data) / 데이터 마이그레이션 실패 해결")
    return pending_checks


def maybe_run_gemini_on_failure(
    step_label,
    returncode,
    args,
    log_path,
    repo_root,
    config_path,
    step_status,
    fk_args,
    commit_ok,
    added_fks_now,
    validated_fks_now,
    data_migration_failed,
):
    if returncode == 130:
        return
    choice = args.gemini_scope or prompt_gemini_choice()
    if not choice:
        return
    flush_log()
    status_lines = build_status_lines(step_status, fk_args, validated_fks_now, data_migration_failed)
    status_lines.insert(0, f"failed step: {step_label}")
    pending_checks = build_pending_checks(
        commit_ok,
        step_status,
        fk_args,
        added_fks_now,
        validated_fks_now,
        data_migration_failed,
    )
    if not run_gemini_log_analysis(
        log_path,
        repo_root / ".env",
        choice,
        status_summary="\n".join(status_lines),
        pending_checks=pending_checks,
    ):
        return
    remediation_choice = prompt_remediation_choice()
    if not remediation_choice:
        return
    script_path, _ = run_gemini_remediation(
        log_path,
        repo_root / ".env",
        choice,
        config_path,
        status_summary="\n".join(status_lines),
        pending_checks=pending_checks,
    )
    if remediation_choice == "run":
        if not script_path:
            print("No remediation script generated.")
            return
        print("EN: The script was generated by Gemini. Review before running.")
        print("KO: Gemini가 생성한 스크립트입니다. 실행 전 검토하세요.")
        if prompt_yes_no("Run the remediation script now?", default=False):
            ok, _ = run_step(
                "Remediation script",
                [sys.executable, str(script_path)],
                cwd=repo_root,
            )
            if not ok:
                print("Remediation script failed. Check logs for details.")


def recover_with_fk_mode(base_cmd, cwd, on_failure=None):
    print("\nMigration failed. You can retry with a safer FK mode.")
    print("EN: --fk-not-valid adds FKs without checking existing rows immediately.")
    print("KO: --fk-not-valid는 기존 데이터 검증을 나중에 수행합니다.")
    if prompt_yes_no("Retry with --fk-not-valid? (recommended)", default=True):
        fk_args = ["--fk-not-valid"]
        ok, code = run_step("Regenerate SQL", base_cmd + ["--no-commit"] + fk_args, cwd=cwd)
        if not ok:
            if on_failure:
                on_failure("Regenerate SQL (recovery)", code)
            return False, fk_args
        ok, code = run_step("Apply migration", base_cmd + ["--commit"] + fk_args, cwd=cwd)
        if not ok:
            if on_failure:
                on_failure("Apply migration (recovery)", code)
            return False, fk_args
        print("FKs were added as NOT VALID. Check history/validate_fks.*.sql to validate later.")
        return True, fk_args
    print("EN: --skip-fk applies schema without FKs; you must add them later.")
    print("KO: --skip-fk는 FK 없이 적용하며, 나중에 FK를 추가해야 합니다.")
    if prompt_yes_no("Retry with --skip-fk?", default=False):
        fk_args = ["--skip-fk"]
        ok, code = run_step("Regenerate SQL", base_cmd + ["--no-commit"] + fk_args, cwd=cwd)
        if not ok:
            if on_failure:
                on_failure("Regenerate SQL (recovery)", code)
            return False, fk_args
        ok, code = run_step("Apply migration", base_cmd + ["--commit"] + fk_args, cwd=cwd)
        if not ok:
            if on_failure:
                on_failure("Apply migration (recovery)", code)
            return False, fk_args
        print("FKs were skipped. You can add them later with --fk-not-valid.")
        return True, fk_args
    return False, []


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
    parser.add_argument(
        "--log-file",
        default=None,
        help="Log file path (default: logs/migrate_stepwise.<timestamp>.log).",
    )
    parser.add_argument(
        "--gemini-scope",
        choices=["summary", "summary_tail"],
        default=None,
        help="Gemini input scope: summary or summary_tail (default: prompt).",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parent
    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = (repo_root / config_path).resolve()
    if not config_path.exists():
        print(f"Config file not found: {config_path}")
        sys.exit(1)

    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    log_file = args.log_file
    if not log_file:
        log_file = repo_root / "logs" / f"migrate_stepwise.{timestamp}.log"
    log_path, log_handle, original_stdout, original_stderr = setup_logging(log_file)

    try:
        config = load_config(config_path)
        if not config:
            sys.exit(1)
        if "targets" not in config or "gcp_test" not in config["targets"]:
            print("Error: config must include targets.gcp_test.")
            sys.exit(1)
        target_config = normalize_conn_config(config["targets"]["gcp_test"])
        if "source" not in config:
            print("Error: config must include source.")
            sys.exit(1)
        source_config = normalize_conn_config(config["source"])

        base_cmd = [
            sys.executable,
            "-m",
            "src.pg_schema_sync",
            "--config",
            str(config_path),
        ]
        if not args.install_extensions:
            base_cmd.append("--no-install-extensions")

        print(f"Log file: {log_path}")
        print("pg-schema-sync stepwise runner")
        print(f"Config: {config_path}")

        fk_args = prompt_fk_mode()
        added_fks_now = False
        validated_fks_now = False
        step_status = {
            "verify": False,
            "generate_sql": False,
            "commit": False,
            "post_check": False,
            "data_migration": False,
            "schema_integrity": False,
            "data_integrity": False,
        }
        data_migration_failed = False
        commit_ok = False

        def handle_failure(step_label, returncode):
            maybe_run_gemini_on_failure(
                step_label,
                returncode,
                args,
                log_path,
                repo_root,
                config_path,
                step_status,
                fk_args,
                commit_ok,
                added_fks_now,
                validated_fks_now,
                data_migration_failed,
            )

        if prompt_yes_no("Step 1: verify schema differences?", default=True):
            ok, code = run_step("Verify", base_cmd + ["--verify"], cwd=repo_root)
            if not ok:
                handle_failure("Verify", code)
                sys.exit(1)
            step_status["verify"] = True

        if prompt_yes_no("Step 2: generate migration SQL (--no-commit)?", default=True):
            ok, code = run_step("Generate SQL", base_cmd + ["--no-commit"] + fk_args, cwd=repo_root)
            if not ok:
                handle_failure("Generate SQL", code)
                sys.exit(1)
            step_status["generate_sql"] = True

        if prompt_yes_no("Step 3: apply schema migration (--commit)?", default=False):
            commit_ok, code = run_step("Apply migration", base_cmd + ["--commit"] + fk_args, cwd=repo_root)
            if not commit_ok:
                handle_failure("Apply migration", code)
                commit_ok, recovered_fk_args = recover_with_fk_mode(
                    base_cmd,
                    repo_root,
                    on_failure=handle_failure,
                )
                if not commit_ok:
                    sys.exit(1)
                if recovered_fk_args:
                    fk_args = recovered_fk_args
            if "--fk-not-valid" in fk_args:
                print("FKs were added as NOT VALID. Check history/validate_fks.*.sql to validate later.")
            if commit_ok:
                step_status["commit"] = True

        if commit_ok and "--skip-fk" in fk_args:
            print("EN: You can add FKs as NOT VALID now and validate after cleanup.")
            print("KO: 지금 FK를 NOT VALID로 추가하고, 정리 후 검증할 수 있습니다.")
            if prompt_yes_no("Step 4: add FKs as NOT VALID now?", default=False):
                fk_args = ["--fk-not-valid"]
                ok, code = run_step("Generate SQL (FKs)", base_cmd + ["--no-commit"] + fk_args, cwd=repo_root)
                if not ok:
                    handle_failure("Generate SQL (FKs)", code)
                    sys.exit(1)
                ok, code = run_step("Apply FKs", base_cmd + ["--commit"] + fk_args, cwd=repo_root)
                if not ok:
                    handle_failure("Apply FKs", code)
                    sys.exit(1)
                print("FKs were added as NOT VALID. Check history/validate_fks.*.sql to validate later.")
                added_fks_now = True

        if commit_ok and "--fk-not-valid" in fk_args:
            print("EN: FK validation will fail if orphaned rows still exist.")
            print("KO: 고아 데이터가 남아있으면 FK 검증이 실패합니다.")
            if prompt_yes_no("Step 5: validate FKs now? (requires cleaned data)", default=False):
                validate_path = pick_validate_file(repo_root / "history")
                if not validate_path:
                    sys.exit(1)
                if not run_validate_fks(target_config, validate_path):
                    handle_failure("Validate FKs", 1)
                    sys.exit(1)
                validated_fks_now = True

        if commit_ok and prompt_yes_no("Step 6: post-check (--no-commit) after commit?", default=True):
            ok, code = run_step("Post-check", base_cmd + ["--no-commit"] + fk_args, cwd=repo_root)
            if not ok:
                handle_failure("Post-check", code)
                sys.exit(1)
            step_status["post_check"] = True

        data_done = False
        if commit_ok and prompt_yes_no("Optional: run data migration (--with-data)?", default=False):
            ok, code = run_step(
                "Data migration",
                base_cmd + ["--with-data"] + fk_args,
                cwd=repo_root,
                allowed_returncodes={0, 2},
            )
            if not ok:
                handle_failure("Data migration", code)
                sys.exit(1)
            if code == 0:
                data_done = True
                step_status["data_migration"] = True
            else:
                data_migration_failed = True
                print("EN: Data migration reported failures. Review logs and fix before re-running.")
                print("KO: 데이터 마이그레이션 실패가 보고되었습니다. 로그 확인 후 수정하고 재실행하세요.")

        if commit_ok and prompt_yes_no("Final: run schema integrity check?", default=False):
            print("EN: Runs schema verify and lists NOT VALID constraints.")
            print("KO: 스키마 검증을 실행하고 NOT VALID 제약을 표시합니다.")
            ok, code = run_step("Schema verify", base_cmd + ["--verify"], cwd=repo_root)
            if not ok:
                handle_failure("Schema verify", code)
                sys.exit(1)
            if not run_schema_integrity_check(target_config):
                handle_failure("Schema integrity check", 1)
                sys.exit(1)
            step_status["schema_integrity"] = True

        if commit_ok and prompt_yes_no("Final: run data integrity check (row counts)?", default=False):
            if not data_done:
                if data_migration_failed:
                    print("EN: Data migration failed; row counts may differ.")
                    print("KO: 데이터 마이그레이션 실패로 결과가 다를 수 있습니다.")
                else:
                    print("EN: Data migration was not run; row counts may differ.")
                    print("KO: 데이터 마이그레이션을 실행하지 않았으므로 결과가 다를 수 있습니다.")
            if not run_data_integrity_check(source_config, target_config):
                handle_failure("Data integrity check", 1)
                sys.exit(1)
            step_status["data_integrity"] = True

        if commit_ok:
            final_fk_mode = fk_mode_from_args(fk_args)
            if final_fk_mode == "skip-fk" and not added_fks_now:
                print("\nNext steps for skipped FKs:")
                print(f"EN: Add FKs later with --fk-not-valid using {config_path}.")
                print("EN: Example:")
                print(f"    python -m src.pg_schema_sync --config {config_path} --no-commit --fk-not-valid")
                print(f"    python -m src.pg_schema_sync --config {config_path} --commit --fk-not-valid")
                print("KO: FK를 나중에 추가하려면 --fk-not-valid로 다시 실행하세요.")
                print("KO: 예시:")
                print(f"    python -m src.pg_schema_sync --config {config_path} --no-commit --fk-not-valid")
                print(f"    python -m src.pg_schema_sync --config {config_path} --commit --fk-not-valid")

            if final_fk_mode == "fk-not-valid" and not validated_fks_now:
                latest_validate = find_latest_validate_file(repo_root / "history")
                print("\nNext steps for FK validation:")
                print("EN: Validate FKs after cleaning orphaned rows.")
                print("KO: 고아 데이터를 정리한 뒤 FK 검증을 실행하세요.")
                if latest_validate:
                    print(f"EN: Latest validate file: {latest_validate}")
                    print(f"KO: 최신 validate 파일: {latest_validate}")
                print("EN: You can run migrate_stepwise again and choose FK validation.")
                print("KO: migrate_stepwise를 다시 실행해 FK 검증 단계를 선택할 수 있습니다.")

        status_lines = build_status_lines(step_status, fk_args, validated_fks_now, data_migration_failed)
        print("\nStatus summary / 현재 상태:")
        for line in status_lines:
            print(f"  - {line}")

        pending_checks = build_pending_checks(
            commit_ok,
            step_status,
            fk_args,
            added_fks_now,
            validated_fks_now,
            data_migration_failed,
        )

        if pending_checks:
            print("\nPending verification steps / 미실행 검증 단계:")
            for item in pending_checks:
                print(f"  - {item}")

        choice = args.gemini_scope
        if not choice:
            choice = prompt_gemini_choice()
        if choice:
            status_summary = "\n".join(status_lines)
            run_gemini_log_analysis(
                log_path,
                repo_root / ".env",
                choice,
                status_summary=status_summary,
                pending_checks=pending_checks,
            )

        print("\nDone.")
    except KeyboardInterrupt:
        print("\nAborted.")
        sys.exit(130)
    finally:
        log_handle.close()
        sys.stdout = original_stdout
        sys.stderr = original_stderr


if __name__ == "__main__":
    main()
