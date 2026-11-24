"""Reconstruct the tested 4.0.0 Linux comparison source from its public Git ancestor."""
import argparse
import hashlib
import io
import json
from pathlib import Path
import subprocess
import tarfile


def sha(data):
    return hashlib.sha256(data).hexdigest()


def prepare(repo, output):
    recipe = Path(__file__).resolve().parent
    control = json.loads((recipe / "control.json").read_bytes())
    patch = (recipe / "linux-4.0.patch").read_bytes()
    if sha(patch) != control["patch_sha256"]:
        raise ValueError("Portable patch digest mismatch")
    tree = control["baseline_revision"] + ":apps/edu-dbms"
    archive = subprocess.check_output(["git", "-C", str(repo), "archive", tree], timeout=60)
    # Make accidental reuse of a working checkout or database impossible.
    output.mkdir(exist_ok=False)
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:") as stream:
        for entry in stream:
            if not entry.isfile():
                continue
            target = (output / entry.name).resolve()
            target.relative_to(output)
            target.parent.mkdir(parents=True, exist_ok=True)
            data = stream.extractfile(entry).read()
            if entry.name in control["edited_files"]:
                data = data.replace(b"\r\n", b"\n")
            target.write_bytes(data)
    subprocess.run(["patch", "--batch", "--fuzz=0", "-p1"], cwd=output, input=patch,
                   check=True, timeout=30)
    for name, expected in control["normalized_input_sha256"].items():
        data = (output / name).read_bytes().replace(b"\r\n", b"\n")
        if sha(data) != expected:
            raise ValueError("Verified comparison source differs in " + name)
    print("Verified", len(control["normalized_input_sha256"]), "inputs in", output)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", type=Path, default=Path.cwd())
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    prepare(args.repository.resolve(strict=True), args.output.resolve())
