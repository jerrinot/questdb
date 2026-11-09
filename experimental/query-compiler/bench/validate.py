#!/usr/bin/env python3
"""Validate every response from bench/fixtures against oracle.cpp, then retain one."""
import hashlib
import json
import math
from pathlib import Path
import re
import sys


def validate(output, oracle, shape=None):
    identity = json.loads((output / "identity.json").read_text())
    options = identity["options"]
    name = Path(options["query"]).stem
    query = shape or name
    if query not in ("high", "small", "all"):
        raise ValueError("This oracle covers only bench/fixtures: " + query)
    expected = [{}, {}]
    for line in oracle.read_text().splitlines():
        mode, key, count, total, minimum, maximum, sum_v = map(int, line.split())
        expected[mode][key] = (count, total, minimum, maximum, sum_v)
    source = (Path(__file__).parent / "fixtures/load.sql").read_text()
    long_name = re.search("'(long-.*?)'", source).group(1)
    names = ["\\N", "", "é中", long_name]
    small_keys = {name: index for index, name in enumerate(names)}
    small_keys.update({"name-" + str(index): 4 + index for index in range(127)})
    if query == "all":
        groups = list(expected[1].values())
        count, total, sum_v = (sum(row[index] for row in groups) for index in (0, 1, 4))
        wanted_all = [str(count), str(total), str(min(row[2] for row in groups)),
                      str(max(row[3] for row in groups)), str(total / count), str(sum_v / 8)]
    results = list((output / name).glob("result-*.jsonl"))
    assert len(results) == options["warmups"] + options["measured"], "Missing response files"
    hashes = {}
    for result in results:
        seen = set()
        for line in result.open():
            row = json.loads(line)
            if query == "all":
                key, wanted, floats = 0, wanted_all, (4, 5)
            else:
                key = int(row[0]) if query == "high" else small_keys[row[0]]
                count, total, minimum, maximum, sum_v = expected[query == "high"][key]
                if query == "high":
                    group_name = names[key % 19] if key % 19 < 4 else "name-" + str(key % 127)
                    wanted, floats = [str(key), group_name], (6, 7)
                else:
                    group_name = names[key] if key < 4 else "name-" + str(key - 4)
                    wanted, floats = [group_name], (5, 6)
                wanted += [str(count), str(total), str(minimum), str(maximum), str(total / count),
                           str(sum_v / 8), str(0 if group_name == "\\N" else count)]
            assert key not in seen, (result, "duplicate", key)
            seen.add(key)
            assert len(row) == len(wanted) and all(
                x == y or index in floats and math.isclose(float(x), float(y), rel_tol=1e-12, abs_tol=1e-9)
                for index, (x, y) in enumerate(zip(row, wanted))), (result, key, row, wanted)
        assert len(seen) == {"high": 262144, "small": 131, "all": 1}[query], (result, len(seen))
        hashes[result.name] = hashlib.sha256(result.read_bytes()).hexdigest()
    rows = [int(x) for x in re.findall(r"^RESULT .*native_rows=(\d+)", (output / "run.log").read_text(), re.M)]
    native = options["mode"] == "llvm" and not options["java_join"]
    assert len(rows) == len(results) and all(row == (500000000 if native else 0) for row in rows), rows
    report = {"oracle_sha256": hashlib.sha256(oracle.read_bytes()).hexdigest(), "shape": query, "input_rows": 500000000,
              "groups": {"high": 262144, "small": 131, "all": 1}[query], "native_probe": native, "validated": hashes}
    (output / "validation.json").write_text(json.dumps(report, indent=2))
    for result in results:
        if result.name != "result-0.jsonl":
            result.unlink()
    print("VALIDATED", output.name, len(results), "complete responses", flush=True)


if __name__ == "__main__":
    validate(Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3] if len(sys.argv) > 3 else None)
