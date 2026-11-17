"""Load the Lakeflow pipelines' files against a mock Spark and check table wiring.

  python scripts/check_pipeline.py pipeline

pipeline/<name>/{bronze,silver,gold}.py are three pipelines: market, research, trading. Catches: decorator misuse,
NameErrors/typos inside table functions, a table defined twice (in one pipeline or across them), reads of tables no
pipeline defines, and reads across pipelines that break the run order: a pipeline may only read tables of pipelines
that run before it (market before research; trading reads market, which refreshes on its own schedule) .
Does NOT check Spark semantics (column existence, types) -- that needs a real pipeline run.
"""
import builtins
import pathlib
import sys
import types

root = pathlib.Path(sys.argv[1])
defined, reads, expectations = {}, [], {}
PIPELINES = ["market", "research", "trading"]
MAY_READ = {"market": set(), "research": {"market"}, "trading": {"market"}}   # what each may read from others
current = {"pipeline": None}
owner = {}


def make_decorator(kind):
    def decorator(*dargs, **dkwargs):
        def wrap(fn):
            name = dkwargs.get("name") or fn.__name__
            if name in defined:
                raise SystemExit(f"duplicate table name: {name} (in {owner[name]} and {current['pipeline']})")
            defined[name] = (kind, fn)
            owner[name] = current["pipeline"]
            return fn
        if dargs and callable(dargs[0]) and not dkwargs:
            return wrap(dargs[0])
        return wrap
    return decorator


def expect_decorator(*args, **kwargs):
    def wrap(fn):
        expectations.setdefault(fn.__name__, []).append(args or kwargs)
        return fn
    return wrap


class Anything:
    """Stand-in for Spark Columns/DataFrames: every attribute, call and operator returns Anything."""
    def __getattr__(self, name):
        if name.startswith("__") and name.endswith("__"):
            raise AttributeError(name)
        return Anything()
    def __call__(self, *a, **k):
        return Anything()
    def __iter__(self):
        return iter([])
    def __bool__(self):
        raise TypeError("Spark Column used in a Python boolean context (use & | ~, not and/or/not)")


for _op in ["lt", "le", "gt", "ge", "eq", "ne", "add", "radd", "sub", "rsub", "mul", "rmul",
            "truediv", "rtruediv", "mod", "and", "rand", "or", "ror", "invert", "neg", "getitem", "pow"]:
    setattr(Anything, f"__{_op}__", lambda self, *a: Anything())
Anything.__hash__ = object.__hash__

dp = types.ModuleType("pyspark.pipelines")
dp.table = make_decorator("table")
dp.materialized_view = make_decorator("materialized_view")
for n in ["expect", "expect_or_drop", "expect_or_fail", "expect_all", "expect_all_or_drop", "expect_all_or_fail"]:
    setattr(dp, n, expect_decorator)

pyspark = types.ModuleType("pyspark")
pyspark.pipelines = dp
sql = types.ModuleType("pyspark.sql")
sql.functions = Anything()
sql.Window = Anything()
pyspark.sql = sql
sys.modules.update({"pyspark": pyspark, "pyspark.pipelines": dp, "pyspark.sql": sql,
                    "pyspark.sql.functions": sql.functions})


class FakeConf:
    def get(self, key, default=None):
        return default


class FakeRead:
    def table(self, name):
        reads.append((current["pipeline"], name))
        return Anything()


class FakeSpark(Anything):
    conf = FakeConf()
    read = FakeRead()


spark = FakeSpark()
builtins.spark = spark

problems = []
for pipe in PIPELINES:
    current["pipeline"] = pipe
    files = [f for f in ["bronze", "silver", "gold"] if (root / pipe / f"{f}.py").exists()]
    if not files:
        problems.append(f"pipeline {pipe} has no files")
    for f in files:
        code = (root / pipe / f"{f}.py").read_text()
        exec(compile(code, f"{pipe}/{f}.py", "exec"), {"__name__": f"{pipe}_{f}"})
    for name, (kind, fn) in [(n, v) for n, v in defined.items() if owner[n] == pipe]:
        try:
            fn()
        except Exception as e:  # noqa: BLE001
            problems.append(f"{name}: {type(e).__name__}: {e}")

missing = sorted({t for _, t in reads} - set(defined))
cross = sorted({(p, t, owner[t]) for p, t in reads if t in owner and owner[t] != p})
wrong = [(p, t, o) for p, t, o in cross if o not in MAY_READ[p]]
for pipe in PIPELINES:
    names = [n for n in defined if owner[n] == pipe]
    print(f"Pipeline {pipe}: {len(names)} tables")
    for name in names:
        kind, fn = defined[name]
        exp = expectations.get(fn.__name__)
        print(f"  {kind:18} {name}" + (f"   [expectations: {sorted(exp[0][0])}]" if exp else ""))
print("Cross-pipeline reads (plain table reads; the other pipeline refreshes first or on its own schedule):")
for p, t, o in cross:
    print(f"  {p:9} reads {t} (from {o})")
if missing:
    problems.append(f"reads undefined tables: {missing}")
if wrong:
    problems.append(f"reads against the run order: {wrong}")
print("RESULT:", "OK" if not problems else "PROBLEMS")
for p in problems:
    print("  -", p)
sys.exit(1 if problems else 0)
