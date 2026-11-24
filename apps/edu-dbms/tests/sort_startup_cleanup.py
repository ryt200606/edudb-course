"""Exercise real Windows startup cleanup with isolated, typed sort-run fixtures.

Fixtures use the actual newly created database UUID and the documented v3
64-byte header. They are constructed test inputs, not claimed crash remnants.
Only owned child processes and a fresh --output directory are used.
"""
from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import struct
import sys
import time
import uuid
import zlib

from acceptance import Server, require

APP = Path(__file__).resolve().parents[1]


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def header(database_uuid: str, run: int, version: int = 3) -> bytes:
    require(len(database_uuid) == 32, 'Fixture database UUID length')
    data = bytearray(64)
    data[:7] = b'EDUSRT3'
    data[8:40] = database_uuid.encode('ascii')
    struct.pack_into('<QII', data, 40, run, version, 64)
    struct.pack_into('<I', data, 60, zlib.crc32(data) & 0xffffffff)
    return bytes(data)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--exe', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--web', type=Path, default=APP / 'web')
    args = parser.parse_args()
    require(os.name == 'nt', 'This acceptance is Windows-only by user request')
    exe, out, web = args.exe.resolve(), args.output.resolve(), args.web.resolve()
    require(exe.is_file() and (web / 'index.html').is_file(), 'Executable/web missing')
    out.mkdir(parents=True, exist_ok=False)
    data = out / '中文 启动清理隔离库'
    result = {
        'schema': 'edudb-sort-startup-cleanup.v1', 'status': 'IN_PROGRESS',
        'started_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'platform': 'Windows native', 'linux': 'NOT_RUN_USER_REQUEST',
        'binary_sha256': sha(exe), 'script_sha256': sha(Path(__file__)),
        'scope': 'Constructed on-disk fixtures processed by the real native startup; no physical power loss claim',
        'checks': [],
    }

    def save() -> None:
        (out / 'result.json').write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf8')

    def check(name: str, ok: bool, **details) -> None:
        result['checks'].append({'name': name, 'status': 'PASS' if ok else 'FAIL', **details})
        save()
        require(ok, name)

    save()
    started = time.monotonic()
    try:
        with Server(exe, data, web, frames=4) as db:
            session = db.session()
            db.sql(session, 'CREATE TABLE sentinel(id INTEGER PRIMARY KEY,label VARCHAR(40))')
            db.sql(session, "INSERT INTO sentinel VALUES(1,'原有中文记录')")
            db.close(session)
            require(db.api('/checkpoint', 'POST', {}).get('status') == 'PASS', 'Initial checkpoint failed')

        database_uuid = json.loads((data / 'control.json').read_text(encoding='utf8'))['uuid']
        foreign_uuid = uuid.uuid4().hex
        require(foreign_uuid != database_uuid, 'Foreign fixture UUID must differ')
        result['database_uuid'] = database_uuid
        result['foreign_fixture_uuid'] = foreign_uuid
        protected = {p.name: sha(p) for p in data.iterdir() if p.is_file()}
        fixtures = {}

        def fixture(name: str, run: int, contents: bytes, *, delete: bool, token: str | None = None,
                    filename: str | None = None) -> Path:
            token = token or uuid.uuid4().hex
            path = data / (filename or f'edudb-sort-{token}-{run}.tmp')
            require(path.parent.resolve() == data.resolve() and not path.exists(), 'Fresh fixture boundary')
            path.write_bytes(contents)
            fixtures[name] = {'path': path.name, 'sha256': sha(path), 'bytes': len(contents), 'expected_deleted': delete}
            return path

        fixture('own_valid_empty', 1, header(database_uuid, 1), delete=True)
        fixture('own_valid_partial_body', 2, header(database_uuid, 2) + b'incomplete-workspace-body', delete=True)
        fixture('foreign_uuid', 3, header(foreign_uuid, 3) + b'foreign-workspace', delete=False)
        corrupt = bytearray(header(database_uuid, 4)); corrupt[60] ^= 1
        fixture('bad_header_crc', 4, bytes(corrupt), delete=False)
        fixture('non_reserved_user_file', 5, header(database_uuid, 5) + b'preserve-user-bytes',
                delete=False, filename='personal-sort-notes.tmp')
        fixture('invalid_filename_token', 6, header(database_uuid, 6), delete=False, token='g' * 32)
        fixture('filename_header_run_mismatch', 7, header(database_uuid, 700), delete=False)
        fixture('wrong_header_version', 8, header(database_uuid, 8, version=2), delete=False)
        fixture('short_header', 9, b'EDUSRT3', delete=False)
        reserved_directory = data / f'edudb-sort-{uuid.uuid4().hex}-10.tmp'
        reserved_directory.mkdir()
        marker = reserved_directory / 'must-survive.txt'; marker.write_bytes(b'not-a-regular-sort-run')
        marker_sha = sha(marker)
        result['fixtures'] = fixtures
        result['reserved_directory'] = {'path': reserved_directory.name, 'marker_sha256': marker_sha}
        save()

        for restart in range(2):
            with Server(exe, data, web, frames=2) as db:
                session = db.session()
                rows = db.sql(session, 'SELECT id,label FROM sentinel ORDER BY id')['rows']
                diagnostics = db.api('/diagnostics')['storage']
                db.close(session)
                check(f'restart_{restart}_database_unchanged', rows == [['1', '原有中文记录']], rows=rows)
                expected_removed = 2 if restart == 0 else 0
                check(f'restart_{restart}_native_cleanup_counters',
                      diagnostics['stale_sort_files_removed'] == expected_removed
                      and diagnostics['unrecognized_sort_files_retained'] == 7,
                      removed=diagnostics['stale_sort_files_removed'],
                      retained=diagnostics['unrecognized_sort_files_retained'])
            for name, item in fixtures.items():
                path = data / item['path']
                check(f'restart_{restart}_{name}',
                      not path.exists() if item['expected_deleted'] else path.is_file() and sha(path) == item['sha256'])
            check(f'restart_{restart}_reserved_directory_preserved', marker.is_file() and sha(marker) == marker_sha)
            check(f'restart_{restart}_persistent_database_bytes_preserved',
                  all((data / name).is_file() and sha(data / name) == digest for name, digest in protected.items()),
                  protected_sha256=protected)

        check('binary_and_script_unchanged', sha(exe) == result['binary_sha256']
              and sha(Path(__file__)) == result['script_sha256'])
        result['status'] = 'PASS'
        result['seconds'] = round(time.monotonic() - started, 3)
        save()
        print(json.dumps({'status': 'PASS', 'checks': len(result['checks']), 'seconds': result['seconds'],
                          'binary_sha256': result['binary_sha256']}, ensure_ascii=False), flush=True)
        return 0
    except Exception as error:
        result['status'] = 'FAIL'; result['error'] = repr(error)
        result['seconds'] = round(time.monotonic() - started, 3)
        save()
        print(json.dumps({'status': 'FAIL', 'error': repr(error)}, ensure_ascii=False), flush=True)
        return 1


if __name__ == '__main__':
    sys.stdout.reconfigure(encoding='utf8')
    raise SystemExit(main())
