#!/usr/bin/env python3
"""Machine-readable scan and upload checkpoints for ops/prepare-run.sh."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any


SCHEMA_VERSION = 1


def fail(message: str) -> None:
    raise SystemExit(f"prepare: {message}")


def utc_now() -> str:
    return dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def canonical_json_bytes(value: Any) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()


def atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(path.name + ".partial")
    with partial.open("wb") as stream:
        stream.write(canonical_json_bytes(value))
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(partial, path)
    directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def load_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        fail(f"{label} does not exist: {path}")
    except json.JSONDecodeError as error:
        fail(f"invalid {label} {path}: {error}")
    if not isinstance(value, dict):
        fail(f"{label} must be a JSON object: {path}")
    return value


def resolve_scan_dir(walk_root: Path) -> Path:
    direct = sorted(walk_root.glob("*.parquet"))
    if direct:
        return walk_root.resolve()
    scans_root = walk_root / "scans"
    candidates = []
    if scans_root.is_dir():
        candidates = sorted(
            path for path in scans_root.iterdir()
            if path.is_dir() and any(path.glob("*.parquet"))
        )
    if len(candidates) != 1:
        fail(
            f"expected exactly one completed scans/<scan_id> below {walk_root}; "
            f"found {len(candidates)}"
        )
    return candidates[0].resolve()


def scan_inventory(scan_dir: Path) -> list[dict[str, Any]]:
    try:
        import pyarrow.parquet as parquet
    except ImportError:
        fail("pyarrow is required to validate walker Parquet metadata")
    shards = []
    for path in sorted(scan_dir.glob("*.parquet")):
        try:
            rows = parquet.ParquetFile(path).metadata.num_rows
        except Exception as error:  # pyarrow exceptions vary by version
            fail(f"cannot read Parquet metadata from {path}: {error}")
        stat = path.stat()
        shards.append(
            {
                "name": path.name,
                "bytes": stat.st_size,
                "modified_unix_ns": stat.st_mtime_ns,
                "rows": rows,
                "sha256": sha256_file(path),
            }
        )
    if not shards:
        fail(f"no Parquet shards found in {scan_dir}")
    return shards


def command_scan_report(args: argparse.Namespace) -> None:
    walk_root = Path(args.walk_root).resolve()
    walker = Path(args.walker).resolve()
    if sha256_file(walker) != args.walker_sha256:
        fail("nfs-walker digest changed before scan checkpoint creation")
    scan_dir = resolve_scan_dir(walk_root)
    shards = scan_inventory(scan_dir)
    value = {
        "schema_version": SCHEMA_VERSION,
        "stage": "scan",
        "complete": True,
        "completed_utc": utc_now(),
        "run_id": args.run_id,
        "run_env_sha256": args.run_env_sha256,
        "source": {
            "url": args.source_url,
            "root": args.source_root,
            "scan_url": args.scan_url,
        },
        "walker": {
            "path": str(walker),
            "sha256": args.walker_sha256,
            "version": args.walker_version,
        },
        "walk_root": str(walk_root),
        "scan_dir": str(scan_dir),
        "total_shards": len(shards),
        "total_rows": sum(shard["rows"] for shard in shards),
        "total_bytes": sum(shard["bytes"] for shard in shards),
        "shards": shards,
    }
    atomic_json(Path(args.report), value)
    print(scan_dir)


def validate_scan_report(
    report_path: Path,
    run_id: str,
    run_env_sha256: str,
    walker_sha256: str,
) -> tuple[dict[str, Any], Path]:
    report = load_json(report_path, "scan report")
    expected = {
        "schema_version": SCHEMA_VERSION,
        "stage": "scan",
        "complete": True,
        "run_id": run_id,
        "run_env_sha256": run_env_sha256,
    }
    for key, value in expected.items():
        if report.get(key) != value:
            fail(f"scan report {key} mismatch: expected {value!r}, got {report.get(key)!r}")
    if report.get("walker", {}).get("sha256") != walker_sha256:
        fail("scan report was created by a different nfs-walker binary")
    scan_dir = Path(report.get("scan_dir", ""))
    current = scan_inventory(scan_dir)
    if current != report.get("shards"):
        fail("walker shard inventory changed after the completed scan checkpoint")
    return report, scan_dir


def command_verify_scan(args: argparse.Namespace) -> None:
    _, scan_dir = validate_scan_report(
        Path(args.report), args.run_id, args.run_env_sha256, args.walker_sha256
    )
    print(scan_dir)


def rewrite_identity(report: dict[str, Any]) -> str:
    stable = {
        "schema_version": report.get("schema_version"),
        "input_dir": report.get("input_dir"),
        "source_root": report.get("source_root"),
        "walker_version": report.get("walker_version"),
        "complete": report.get("complete"),
        "shards": report.get("shards"),
    }
    return hashlib.sha256(canonical_json_bytes(stable)).hexdigest()


class AwsCli:
    def __init__(self, endpoint: str, region: str, profile: str, verify_tls: bool):
        self.options = [
            "--endpoint-url", endpoint,
            "--region", region,
            "--profile", profile,
        ]
        if not verify_tls:
            self.options.append("--no-verify-ssl")

    def run(
        self,
        service: str,
        arguments: list[str],
        *,
        check: bool = True,
        capture: bool = False,
    ) -> subprocess.CompletedProcess[bytes]:
        command = ["aws", service, *self.options, *arguments]
        result = subprocess.run(
            command,
            check=False,
            stdout=subprocess.PIPE if capture else None,
            stderr=subprocess.PIPE if capture else None,
        )
        if check and result.returncode != 0:
            detail = (result.stderr or b"").decode(errors="replace").strip()
            fail(f"AWS command failed ({' '.join(command[:2])}): {detail}")
        return result

    @staticmethod
    def is_missing(result: subprocess.CompletedProcess[bytes]) -> bool:
        detail = (result.stderr or b"").decode(errors="replace")
        return any(token in detail for token in ("404", "NoSuchKey", "Not Found"))

    def head(self, bucket: str, key: str) -> dict[str, Any] | None:
        result = self.run(
            "s3api",
            ["head-object", "--bucket", bucket, "--key", key, "--output", "json"],
            check=False,
            capture=True,
        )
        if result.returncode == 0:
            try:
                return json.loads(result.stdout)
            except json.JSONDecodeError as error:
                fail(f"invalid head-object JSON for {key}: {error}")
        if self.is_missing(result):
            return None
        detail = (result.stderr or b"").decode(errors="replace").strip()
        fail(f"head-object failed for {key}: {detail}")

    def get_bytes(self, bucket: str, key: str) -> bytes | None:
        result = self.run(
            "s3",
            ["cp", f"s3://{bucket}/{key}", "-", "--only-show-errors"],
            check=False,
            capture=True,
        )
        if result.returncode == 0:
            return result.stdout
        if self.is_missing(result):
            return None
        detail = (result.stderr or b"").decode(errors="replace").strip()
        fail(f"GET failed for {key}: {detail}")


def remote_matches(
    head: dict[str, Any] | None,
    *,
    size: int,
    run_id: str,
    digest: str,
) -> bool:
    if not head or head.get("ContentLength") != size:
        return False
    metadata = {str(key).lower(): value for key, value in head.get("Metadata", {}).items()}
    return (
        metadata.get("vamoose-run-id") == run_id
        and metadata.get("canonical-sha256") == digest
    )


def load_rewrite_plan(path: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    report = load_json(path, "rewrite report")
    if report.get("schema_version") != 1 or report.get("complete") is not True:
        fail("rewrite report is not a completed schema-version-1 checkpoint")
    output_dir = Path(report.get("output_dir", ""))
    plans = []
    for checkpoint in report.get("shards", []):
        output = output_dir / checkpoint["output_name"]
        if not output.is_file() or output.stat().st_size != checkpoint["output_bytes"]:
            fail(f"canonical shard does not match rewrite checkpoint: {output}")
        digest = sha256_file(output)
        if digest != checkpoint.get("output_sha256"):
            fail(f"canonical shard SHA256 does not match rewrite checkpoint: {output}")
        plans.append(
            {
                "name": checkpoint["output_name"],
                "path": output,
                "rows": checkpoint["rows"],
                "bytes": checkpoint["output_bytes"],
                "sha256": digest,
            }
        )
    plans.sort(key=lambda plan: plan["name"])
    if not plans:
        fail("rewrite report contains no shards")
    return report, plans


def upload_context(args: argparse.Namespace, rewrite_report: dict[str, Any]) -> dict[str, Any]:
    return {
        "run_id": args.run_id,
        "run_env_sha256": args.run_env_sha256,
        "bucket": args.bucket,
        "endpoint": args.endpoint,
        "rewrite_identity_sha256": rewrite_identity(rewrite_report),
        "source": {"kind": "nfs", "url": args.source_url, "root": args.source_root},
        "dest": {"kind": "nfs", "url": args.dest_url, "root": args.dest_root},
    }


def command_upload(args: argparse.Namespace) -> None:
    rewrite_report, plans = load_rewrite_plan(Path(args.rewrite_report))
    context = upload_context(args, rewrite_report)
    report_path = Path(args.report)
    if report_path.exists():
        report = load_json(report_path, "upload report")
        if report.get("schema_version") != SCHEMA_VERSION or report.get("context") != context:
            fail("upload report context does not match this run and rewrite checkpoint")
    else:
        report = {
            "schema_version": SCHEMA_VERSION,
            "stage": "upload",
            "complete": False,
            "started_utc": utc_now(),
            "updated_utc": utc_now(),
            "context": context,
            "manifest_created_utc": utc_now(),
            "shards": [],
        }
        atomic_json(report_path, report)

    aws = AwsCli(args.endpoint, args.region, args.profile, args.verify_tls)
    manifest_before = aws.get_bytes(args.bucket, "manifest.json")
    completed = {item["name"]: item for item in report.get("shards", [])}
    for plan in plans:
        key = f"index/{plan['name']}"
        head = aws.head(args.bucket, key)
        if not remote_matches(
            head,
            size=plan["bytes"],
            run_id=args.run_id,
            digest=plan["sha256"],
        ):
            if manifest_before is not None:
                fail(f"manifest already exists but immutable shard {key} does not match")
            print(f"uploading {key} ({plan['bytes']} bytes)", file=sys.stderr)
            aws.run(
                "s3",
                [
                    "cp", str(plan["path"]), f"s3://{args.bucket}/{key}",
                    "--only-show-errors",
                    "--metadata",
                    f"vamoose-run-id={args.run_id},canonical-sha256={plan['sha256']}",
                ],
            )
            head = aws.head(args.bucket, key)
            if not remote_matches(
                head,
                size=plan["bytes"],
                run_id=args.run_id,
                digest=plan["sha256"],
            ):
                fail(f"uploaded shard failed authoritative HEAD validation: {key}")
        etag = str(head.get("ETag", "")).strip('"') if head else ""
        if not etag:
            fail(f"S3 returned an empty ETag for {key}")
        completed[plan["name"]] = {
            "name": plan["name"],
            "key": key,
            "rows": plan["rows"],
            "bytes": plan["bytes"],
            "sha256": plan["sha256"],
            "etag": etag,
        }
        report["shards"] = sorted(completed.values(), key=lambda item: item["name"])
        report["updated_utc"] = utc_now()
        report["complete"] = False
        atomic_json(report_path, report)

    manifest = {
        "format_version": 1,
        "run_id": args.run_id,
        "created_utc": report["manifest_created_utc"],
        "shards": [
            {
                "key": item["key"],
                "rows": item["rows"],
                "bytes": item["bytes"],
                "etag": item["etag"],
            }
            for item in sorted(completed.values(), key=lambda item: item["key"])
        ],
        "total_rows": sum(item["rows"] for item in completed.values()),
        "source": context["source"],
        "dest": context["dest"],
        "options": {
            "preserve_owner": args.preserve_owner,
            "preserve_mode": args.preserve_mode,
            "preserve_times": args.preserve_times,
            "preserve_xattr": args.preserve_xattr,
            "server_side_copy": "off",
        },
    }
    manifest_bytes = canonical_json_bytes(manifest)
    manifest_path = Path(args.manifest)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_bytes(manifest_bytes)
    if manifest_before is None:
        result = aws.run(
            "s3api",
            [
                "put-object", "--bucket", args.bucket, "--key", "manifest.json",
                "--body", str(manifest_path), "--if-none-match", "*",
            ],
            check=False,
            capture=True,
        )
        if result.returncode != 0:
            # A concurrent retry may have won the conditional create.
            manifest_before = aws.get_bytes(args.bucket, "manifest.json")
            if manifest_before is None:
                detail = (result.stderr or b"").decode(errors="replace").strip()
                fail(f"conditional manifest upload failed: {detail}")
        else:
            manifest_before = manifest_bytes
    if manifest_before != manifest_bytes:
        fail("existing manifest.json differs from the preparation checkpoint")

    report["complete"] = True
    report["completed_utc"] = utc_now()
    report["updated_utc"] = report["completed_utc"]
    report["manifest_sha256"] = hashlib.sha256(manifest_bytes).hexdigest()
    report["total_rows"] = manifest["total_rows"]
    atomic_json(report_path, report)
    print(
        f"upload complete: {len(plans)} shards, {manifest['total_rows']} rows, "
        f"s3://{args.bucket}/manifest.json"
    )


def bool_argument(value: str) -> bool:
    if value == "true":
        return True
    if value == "false":
        return False
    raise argparse.ArgumentTypeError("expected true or false")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    scan = subparsers.add_parser("scan-report")
    for name in ("report", "walk-root", "run-id", "run-env-sha256", "source-url", "source-root", "scan-url", "walker", "walker-sha256", "walker-version"):
        scan.add_argument(f"--{name}", required=True)
    scan.set_defaults(handler=command_scan_report)

    verify = subparsers.add_parser("verify-scan")
    for name in ("report", "run-id", "run-env-sha256", "walker-sha256"):
        verify.add_argument(f"--{name}", required=True)
    verify.set_defaults(handler=command_verify_scan)

    upload = subparsers.add_parser("upload")
    for name in (
        "rewrite-report", "report", "manifest", "run-id", "run-env-sha256",
        "endpoint", "bucket", "region", "profile", "source-url", "source-root",
        "dest-url", "dest-root",
    ):
        upload.add_argument(f"--{name}", required=True)
    for name in ("verify-tls", "preserve-owner", "preserve-mode", "preserve-times", "preserve-xattr"):
        upload.add_argument(f"--{name}", required=True, type=bool_argument)
    upload.set_defaults(handler=command_upload)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    args.handler(args)


if __name__ == "__main__":
    main()
