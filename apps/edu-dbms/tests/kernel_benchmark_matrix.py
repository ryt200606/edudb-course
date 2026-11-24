"""Run an isolated native control/candidate matrix in series on the current OS."""
import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

from kernel_benchmark import exclusive_output, save_receipt, sha_file


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline",type=Path,required=True)
    parser.add_argument("--candidate",type=Path,required=True)
    parser.add_argument("--baseline-label",required=True)
    parser.add_argument("--output",type=Path,required=True)
    parser.add_argument("--web",type=Path,required=True)
    parser.add_argument("--sizes",default="1000,10000,100000")
    parser.add_argument("--profile",choices=("smoke","formal"),default="formal")
    args=parser.parse_args()
    args.output.mkdir(parents=True,exist_ok=True)
    folder=Path(__file__).resolve().parent
    benchmark=folder/"kernel_benchmark.py"
    compare=folder/"compare_kernel_benchmarks.py"
    stages=[("control",args.baseline,args.baseline_label,[]),
            ("group8",args.candidate,"candidate-mcv-group8",["--joint","--wal-group-size","8"]),
            ("single",args.candidate,"candidate-mcv-individual",["--joint","--wal-group-size","1","--suite","group-commit"])]
    manifest=dict(status="RUNNING",sizes=list(map(int,args.sizes.split(","))),
                  baseline_sha256=sha_file(args.baseline),candidate_sha256=sha_file(args.candidate),
                  harness_sha256=sha_file(benchmark),results=[])
    receipt=args.output/"matrix.json"
    with exclusive_output(args.output/"matrix.lock"):
        save_receipt(receipt,manifest)
        try:
            for n in manifest["sizes"]:
                for name,exe,label,options in stages:
                    path=args.output/f"{n}-{name}"
                    command=[sys.executable,str(benchmark),"--exe",str(exe),"--web",str(args.web),
                             "--profile",args.profile,"--sizes",str(n),"--label",label,"--output",str(path),*options]
                    if (path/"receipt.json").exists():command.append("--resume")
                    print("Running",n,name,flush=True)
                    # Each HTTP request also has a bounded timeout. Larger
                    # combinations can legitimately require several hours.
                    result=subprocess.run(command,timeout=36*3600)
                    if result.returncode:raise RuntimeError(f"{n}/{name} exited {result.returncode}; inspect its receipt")
                    saved=json.loads((path/"receipt.json").read_text(encoding="utf-8"))
                    if saved.get("status")!="PASS":raise RuntimeError(f"{n}/{name} incomplete")
                    manifest["results"].append({"rows":n,"stage":name,"samples_sha256":sha_file(path/"samples.jsonl")})
                    save_receipt(receipt,manifest)
                if args.profile!="formal":continue
                for left,right in [("control","group8"),("single","group8")]:
                    final=args.output/f"{n}-{left}-vs-{right}.json"
                    temp=final.with_suffix(f".{os.getpid()}.json")
                    subprocess.run([sys.executable,str(compare),str(args.output/f"{n}-{left}"),
                                   str(args.output/f"{n}-{right}"),"--output",str(temp)],check=True,timeout=180)
                    os.replace(temp,final)
            manifest["status"]="PASS"
        except BaseException as error:
            manifest.update(status="FAIL",error=repr(error))
            raise
        finally:save_receipt(receipt,manifest)


if __name__=="__main__":
    main()
