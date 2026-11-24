"""Compare completed same-platform benchmark receipts and their raw measurements."""
import argparse
import collections
import json
import math
import statistics
from pathlib import Path


def distribution(values):
    values=sorted(values)
    return {"n":len(values),"median":statistics.median(values),"p95":values[math.ceil(.95*len(values))-1]}


def load(directory):
    receipt=json.loads((directory/"receipt.json").read_text(encoding="utf-8"))
    if receipt["status"]!="PASS" or receipt["profile"]!="formal":
        raise ValueError(f"{directory} lacks a completed formal run")
    data=collections.defaultdict(list)
    for line in (directory/"samples.jsonl").read_text(encoding="utf-8").splitlines():
        row=json.loads(line)
        data[row["rows"],row["case"]].append(row)
    return receipt,data


def summarize(samples):
    summary={"latency_ms":distribution([s["elapsed_ms"] for s in samples])}
    for column in ("tps","rows_per_second","commits_per_wal_sync","growth_bytes"):
        if all(s.get(column) is not None for s in samples):
            summary[column]=distribution([s[column] for s in samples])
    if "commit_ms" in samples[0]:
        summary["transaction_commit_ms"]=distribution([m for s in samples for client in s["commit_ms"] for m in client])
    for key in ("heap_page_reads","index_page_reads","wal_bytes","dirty_pages"):
        measurements=[]
        for s in samples:
            records=s.get("batches",[s])
            if any((entry.get("pages") or {}).get(key) is None for entry in records):break
            measurements.append(sum(entry["pages"][key] for entry in records))
        if len(measurements)==len(samples):summary[key]=distribution(measurements)
    for unit in ("sync_calls","sync_ns","write_bytes"):
        measurements=[]
        for s in samples:
            records=s.get("batches",[s])
            if "wal_os_io" in s:measurements.append(s["wal_os_io"][unit])
            elif all(unit in item.get("os_io",{}).get("wal",{}) for item in records):
                measurements.append(sum(item["os_io"]["wal"][unit] for item in records))
            else:break
        if len(measurements)==len(samples):summary["wal_"+unit]=distribution(measurements)
    return summary


def main():
    cli=argparse.ArgumentParser(description=__doc__)
    cli.add_argument("baseline",type=Path);cli.add_argument("candidate",type=Path)
    cli.add_argument("--output",type=Path,required=True);args=cli.parse_args()
    old,a=load(args.baseline);new,b=load(args.candidate)
    for key in ("platform","machine","logical_cpus","filesystem","reads","writes","sizes","batch",
                "operator_bytes","buffer_frames","seed","buckets_by_id_mod_10","bench_sha256"):
        if old[key]!=new[key]:raise ValueError(f"Different {key}: {old[key]} vs {new[key]}")
    if "group-commit" in (old["suite"],new["suite"]):
        a={k:v for k,v in a.items() if k[1].startswith("concurrent_")}
        b={k:v for k,v in b.items() if k[1].startswith("concurrent_")}
    if a.keys()!=b.keys():raise ValueError("Workloads differ")
    output={"baseline":old["binary_sha256"],"candidate":new["binary_sha256"],
            "baseline_modes":[old["joint_statistics"],old["server_args"]],
            "candidate_modes":[new["joint_statistics"],new["server_args"]],"workloads":[]}
    for (rows,name),samples in a.items():
        other=b[rows,name]
        expected=old["reads"] if name.endswith(("_warm","_reopened")) else old["writes"]
        if len(samples)!=expected or len(other)!=expected:
            raise ValueError(f"Wrong number of independent samples: {rows}/{name}")
        for key in ("aggregate_sha256","result_sha256"):
            if key in samples[0] and {s[key] for s in samples}!={s[key] for s in other}:
                raise ValueError(f"Result differs: {rows}/{name}")
        x,y=summarize(samples),summarize(other)
        output["workloads"].append(dict(rows=rows,case=name,baseline=x,candidate=y,
            median_latency_ratio_candidate_to_baseline=y["latency_ms"]["median"]/x["latency_ms"]["median"]))
    with args.output.open("x",encoding="utf-8") as f:json.dump(output,f,ensure_ascii=False,indent=2)


if __name__=="__main__":
    main()
