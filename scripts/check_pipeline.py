"""Load the Lakeflow pipeline files against a mock Spark and check table wiring.

Catches: decorator misuse, NameErrors/typos inside table functions, duplicate
table names, and reads of tables the pipeline never defines. Does NOT check Spark
semantics (column existence, types) -- that needs a real pipeline run.
"""
import builtins
import pathlib
import sys
import types
from unittest import mock

root = pathlib.Path(sys.argv[1])
defined, reads, expectations = {}, [], {}


def make_decorator(kind):
    def decorator(*dargs, **dkwargs):
        def wrap(fn):
            name = dkwargs.get("name") or fn.__name__
            if name in defined:
                raise SystemExit(f"duplicate table name: {name}")
            defined[name] = (kind, fn)
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
        reads.append(name)
        return Anything()


class FakeSpark(Anything):
    conf = FakeConf()
    read = FakeRead()


spark = FakeSpark()
builtins.spark = spark

for f in ["bronze", "silver", "gold"]:
    code = (root / f"{f}.py").read_text()
    exec(compile(code, f"{f}.py", "exec"), {"__name__": f})

problems = []
for name, (kind, fn) in defined.items():
    try:
        fn()
    except Exception as e:  # noqa: BLE001
        problems.append(f"{name}: {type(e).__name__}: {e}")

missing = sorted(set(reads) - set(defined))
print(f"Defined {len(defined)} tables:")
for name, (kind, fn) in defined.items():
    exp = expectations.get(fn.__name__)
    print(f"  {kind:18} {name}" + (f"   [expectations: {sorted(exp[0][0])}]" if exp else ""))
print(f"Tables read via spark.read.table: {sorted(set(reads))}")
if missing:
    problems.append(f"reads undefined tables: {missing}")
print("RESULT:", "OK" if not problems else "PROBLEMS")
for p in problems:
    print("  -", p)
sys.exit(1 if problems else 0)
