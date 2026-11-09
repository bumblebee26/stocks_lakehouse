"""Check every dashboard dataset and every statement in sql/*.sql against the Databricks SQL
warehouse with EXPLAIN (nothing is executed or written). Runs in the Terraform plan job.

  DATABRICKS_HOST=https://... DATABRICKS_TOKEN=... python scripts/check_sql.py

Fails on syntax errors and unknown columns or functions. A table that doesn't exist yet only
warns: it may be created by this very change (the pipeline creates it after the merge).
"""
import json
import os
import re
import sys
import time
import urllib.request

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
HEREDOC = re.compile(r"^\s*(\w+)\s*=\s*<<-SQL\n(.*?)^\s*SQL\s*$", re.S | re.M)
CTAS = re.compile(r"^\s*CREATE\s+OR\s+REPLACE\s+TABLE\s+\S+\s*(?:COMMENT\s+'[^']*'\s*)?AS\s*", re.I | re.S)
NOT_FOUND = ("TABLE_OR_VIEW_NOT_FOUND",)


def statements():
    out = []
    tf = open(os.path.join(ROOT, "infra", "databricks", "dashboard.tf")).read()
    for name, sql in HEREDOC.findall(tf):
        out.append((f"dashboard dataset {name}", sql.strip()))
    sql_dir = os.path.join(ROOT, "sql")
    for fname in sorted(os.listdir(sql_dir)) if os.path.isdir(sql_dir) else []:
        if not fname.endswith(".sql"):
            continue
        text = "\n".join(line for line in open(os.path.join(sql_dir, fname)).read().splitlines()
                         if not line.strip().startswith("--"))
        for i, stmt in enumerate(s for s in re.split(r";\s*(?:\n|$)", text) if s.strip()):
            out.append((f"{fname} statement {i + 1}", CTAS.sub("", stmt.strip(), count=1)))
    return out


def api(method, path, body=None):
    host = os.environ["DATABRICKS_HOST"].rstrip("/")
    req = urllib.request.Request(host + path, method=method, data=json.dumps(body).encode() if body else None,
                                 headers={"Authorization": f"Bearer {os.environ['DATABRICKS_TOKEN']}",
                                          "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())


def explain(warehouse, sql):
    resp = api("POST", "/api/2.0/sql/statements", {"warehouse_id": warehouse, "statement": "EXPLAIN " + sql,
                                                   "wait_timeout": "50s", "on_wait_timeout": "CONTINUE"})
    deadline = time.time() + 300
    while resp["status"]["state"] in ("PENDING", "RUNNING") and time.time() < deadline:
        time.sleep(3)
        resp = api("GET", f"/api/2.0/sql/statements/{resp['statement_id']}")
    if resp["status"]["state"] != "SUCCEEDED":
        return resp["status"].get("error", {}).get("message", resp["status"]["state"])
    # EXPLAIN reports analysis errors as its result text instead of failing
    rows = (resp.get("result") or {}).get("data_array") or []
    text = " ".join(str(c) for r in rows for c in r)
    m = re.search(r"(\[[A-Z_.]+\].*?)(?:SQLSTATE|$)", text, re.S)
    return m.group(1).strip()[:400] if ("Error occurred during query planning" in text or "AnalysisException" in text) and m else None


def main():
    if not (os.environ.get("DATABRICKS_HOST") and os.environ.get("DATABRICKS_TOKEN")):
        print("DATABRICKS_HOST/TOKEN not set; skipping")
        return 0
    warehouses = api("GET", "/api/2.0/sql/warehouses").get("warehouses", [])
    if not warehouses:
        print("No SQL warehouse found")
        return 1
    wh = warehouses[0]["id"]
    errors = warnings = 0
    for label, sql in statements():
        err = explain(wh, sql)
        if not err:
            print(f"OK    {label}")
        elif any(k in err for k in NOT_FOUND):
            warnings += 1
            print(f"WARN  {label}: {err}  (fine if this change creates it)")
        else:
            errors += 1
            print(f"FAIL  {label}: {err}")
    print(f"\n{errors} error(s), {warnings} warning(s)")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
