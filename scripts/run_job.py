"""Run Databricks jobs by name, one after another, and wait for each to finish.

    python scripts/run_job.py stocks-market-weekend stocks-trading

Needs DATABRICKS_HOST (the workspace URL) and DATABRICKS_TOKEN. Prints each task's progress (including retries) and
exits non-zero if a job is missing, a run fails, or a run takes longer than TIMEOUT_MINUTES. A job that fails stops
the jobs after it. Used by .github/workflows/run-job.yml, for example to rebuild every table after a pipeline change.
"""
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request

POLL_SECONDS = 30
TIMEOUT_MINUTES = 150
DONE = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}


def call(method, path, body=None):
    host = os.environ["DATABRICKS_HOST"].rstrip("/")
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(host + path, data=data, method=method,
                                 headers={"Authorization": f"Bearer {os.environ['DATABRICKS_TOKEN']}",
                                          "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read() or b"{}")


def job_id(name):
    jobs = call("GET", "/api/2.1/jobs/list?" + urllib.parse.urlencode({"name": name})).get("jobs", [])
    if len(jobs) != 1:
        raise SystemExit(f"expected one job named {name!r}, found {len(jobs)}")
    return jobs[0]["job_id"]


def _state(s):
    s = s or {}
    return f"{s.get('life_cycle_state', '?')}" + (f" {s['result_state']}" if s.get("result_state") else "")


def run(name):
    run_id = call("POST", "/api/2.1/jobs/run-now", {"job_id": job_id(name)})["run_id"]
    print(f"{name}: run {run_id} started", flush=True)
    seen = {}
    deadline = time.time() + TIMEOUT_MINUTES * 60
    while time.time() < deadline:
        r = call("GET", f"/api/2.1/jobs/runs/get?run_id={run_id}")
        for t in r.get("tasks", []):
            now = f"{_state(t.get('state'))}" + (f" (attempt {t['attempt_number'] + 1})" if t.get("attempt_number") else "")
            if seen.get(t["task_key"]) != now:
                seen[t["task_key"]] = now
                print(f"  {t['task_key']}: {now}", flush=True)
        state = r.get("state", {})
        if state.get("life_cycle_state") in DONE:
            ok = state.get("result_state") == "SUCCESS"
            print(f"{name}: {_state(state)}. {state.get('state_message', '')}".rstrip(), flush=True)
            return ok
        time.sleep(POLL_SECONDS)
    print(f"{name}: still running after {TIMEOUT_MINUTES} minutes (run {run_id}); check it in Databricks", flush=True)
    return False


def main(names):
    if not names or any(not re.fullmatch(r"[a-z0-9][a-z0-9_-]*", n) for n in names):
        raise SystemExit("usage: run_job.py JOB_NAME [JOB_NAME ...]  (lower-case names like stocks-trading)")
    for name in names:
        if not run(name):
            raise SystemExit(f"{name} did not succeed; the jobs after it were not started")
    print("all done")


if __name__ == "__main__":
    main(sys.argv[1:])
