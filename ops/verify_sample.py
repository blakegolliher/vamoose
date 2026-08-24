#!/usr/bin/env python3
"""Build and check deterministic migration verification samples."""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import hashlib
import json
import math
import os
from pathlib import Path
import random
import stat
import sys
from typing import Any


SCHEMA_VERSION = 1
FILE_TYPE_NAMES = {
    1: "regular",
    2: "directory",
    3: "symlink",
    4: "fifo",
    5: "socket",
    6: "block_device",
    7: "char_device",
}


def fail(message: str) -> None:
    raise SystemExit(f"verify-sample: {message}")


def utc_now() -> str:
    return dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z")


def load_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        fail(f"cannot read {label} {path}: {error}")
    if not isinstance(value, dict):
        fail(f"{label} must be a JSON object")
    return value


def atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(path.name + ".partial")
    data = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()
    with partial.open("wb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(partial, path)


def column_value(table: Any, name: str, index: int) -> Any:
    indices = table.schema.get_all_field_indices(name)
    if not indices:
        fail(f"sampled table is missing canonical column {name}")
    return table.column(indices[0])[index].as_py()


def row_from_table(table: Any, index: int, shard_name: str) -> dict[str, Any]:
    raw_path = column_value(table, "path", index)
    if not isinstance(raw_path, bytes):
        fail(f"canonical path is not binary in {shard_name}")
    file_type = int(column_value(table, "file_type", index))
    if file_type not in FILE_TYPE_NAMES:
        fail(f"invalid canonical file_type {file_type} in {shard_name}")
    return {
        "row_id": int(column_value(table, "row_id", index)),
        "shard": shard_name,
        "path_b64": base64.b64encode(raw_path).decode("ascii"),
        "file_type": file_type,
        "size": int(column_value(table, "size", index)),
        "mode": int(column_value(table, "mode", index)),
        "uid": column_value(table, "uid", index),
        "gid": column_value(table, "gid", index),
        "mtime_sec": column_value(table, "mtime_sec", index),
        "mtime_nsec": column_value(table, "mtime_nsec", index),
    }


def command_sample(args: argparse.Namespace) -> None:
    try:
        import pyarrow.parquet as parquet
    except ImportError:
        fail("pyarrow is required to sample canonical Parquet shards")

    report = load_json(Path(args.rewrite_report), "rewrite report")
    if report.get("schema_version") != 1 or report.get("complete") is not True:
        fail("rewrite report is not a completed schema-version-1 checkpoint")
    output_dir = Path(report.get("output_dir", ""))
    shard_paths = [output_dir / shard["output_name"] for shard in report.get("shards", [])]
    if not shard_paths:
        fail("rewrite report contains no canonical shards")

    row_groups: list[tuple[Path, int, int]] = []
    for path in shard_paths:
        if not path.is_file():
            fail(f"canonical shard is missing: {path}")
        parquet_file = parquet.ParquetFile(path)
        for group_index in range(parquet_file.metadata.num_row_groups):
            rows = parquet_file.metadata.row_group(group_index).num_rows
            if rows:
                row_groups.append((path, group_index, rows))
    if not row_groups:
        fail("canonical shards contain no rows")

    rng = random.Random(args.seed)
    # Read a bounded, broadly distributed set of row groups. This keeps a
    # 600M-row verification sample from materializing the entire index while
    # still spreading selections across independently written Parquet groups.
    groups_to_read = min(len(row_groups), max(1, min(128, math.ceil(args.count / 8))))
    shuffled_groups = row_groups.copy()
    rng.shuffle(shuffled_groups)
    selected_groups = shuffled_groups[:groups_to_read]
    selected_rows = sum(group[2] for group in selected_groups)
    while selected_rows < args.count and len(selected_groups) < len(shuffled_groups):
        group = shuffled_groups[len(selected_groups)]
        selected_groups.append(group)
        selected_rows += group[2]
    groups_to_read = len(selected_groups)
    columns = [
        "row_id", "path", "size", "mode", "file_type", "uid", "gid",
        "mtime_sec", "mtime_nsec",
    ]
    candidates: dict[int, dict[str, Any]] = {}
    symlinks: dict[int, dict[str, Any]] = {}
    per_group = max(8, math.ceil(args.count * 2 / groups_to_read))
    for path, group_index, _rows in selected_groups:
        table = parquet.ParquetFile(path).read_row_group(group_index, columns=columns)
        picks = range(table.num_rows)
        if table.num_rows > per_group:
            picks = rng.sample(range(table.num_rows), per_group)
        for index in picks:
            row = row_from_table(table, index, path.name)
            candidates[row["row_id"]] = row
            if row["file_type"] == 3:
                symlinks[row["row_id"]] = row

    # If the broad sample did not encounter a symlink, use file_type column
    # statistics to locate a bounded number of additional candidate groups.
    if not symlinks:
        extra_groups = 0
        for path, group_index, _rows in row_groups:
            parquet_file = parquet.ParquetFile(path)
            metadata = parquet_file.metadata.row_group(group_index)
            column_index = parquet_file.schema_arrow.get_field_index("file_type")
            statistics = metadata.column(column_index).statistics
            if not statistics or not statistics.has_min_max:
                continue
            if not (int(statistics.min) <= 3 <= int(statistics.max)):
                continue
            extra_groups += 1
            table = parquet_file.read_row_group(group_index, columns=columns)
            for index in range(table.num_rows):
                if int(column_value(table, "file_type", index)) == 3:
                    row = row_from_table(table, index, path.name)
                    symlinks[row["row_id"]] = row
                    candidates[row["row_id"]] = row
                    if len(symlinks) >= min(32, args.count):
                        break
            if symlinks or extra_groups >= 32:
                break

    reserved = list(symlinks.values())[: min(32, args.count)]
    reserved_ids = {row["row_id"] for row in reserved}
    remaining = [row for row_id, row in candidates.items() if row_id not in reserved_ids]
    wanted = args.count - len(reserved)
    chosen = reserved + rng.sample(remaining, min(wanted, len(remaining)))
    rng.shuffle(chosen)
    if len(chosen) < min(args.count, sum(group[2] for group in row_groups)):
        fail(
            f"sampling plan produced only {len(chosen)} unique rows; "
            "increase row-group coverage"
        )

    value = {
        "schema_version": SCHEMA_VERSION,
        "created_utc": utc_now(),
        "run_id": args.run_id,
        "seed": args.seed,
        "requested_count": args.count,
        "sample_count": len(chosen),
        "symlink_count": sum(row["file_type"] == 3 for row in chosen),
        "items": chosen,
    }
    atomic_json(Path(args.output), value)
    print(
        f"sampled {len(chosen)} rows from {groups_to_read} row groups "
        f"({value['symlink_count']} symlinks)"
    )


def safe_join(root: bytes, relative: bytes) -> bytes:
    if b"\0" in relative or not relative.startswith(b"/"):
        raise ValueError("canonical path is not an absolute export-relative path")
    components = relative.split(b"/")[1:]
    if any(component in (b"", b".", b"..") for component in components):
        if relative != b"/":
            raise ValueError("canonical path contains an empty, dot, or dot-dot component")
    return root.rstrip(b"/") + (b"/" if relative == b"/" else b"/" + b"/".join(components))


def path_is_beneath(root: bytes, candidate: bytes, relative: bytes) -> bool:
    root_real = os.path.realpath(root)
    check_path = candidate if relative == b"/" else os.path.dirname(candidate)
    parent_real = os.path.realpath(check_path)
    try:
        return os.path.commonpath((root_real, parent_real)) == root_real
    except ValueError:
        return False


def file_type(mode: int) -> int:
    if stat.S_ISREG(mode):
        return 1
    if stat.S_ISDIR(mode):
        return 2
    if stat.S_ISLNK(mode):
        return 3
    if stat.S_ISFIFO(mode):
        return 4
    if stat.S_ISSOCK(mode):
        return 5
    if stat.S_ISBLK(mode):
        return 6
    if stat.S_ISCHR(mode):
        return 7
    return 0


def content_sha256(path: bytes) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def add_mismatch(
    mismatches: list[dict[str, Any]],
    row: dict[str, Any],
    category: str,
    detail: str,
) -> None:
    mismatches.append(
        {
            "row_id": row["row_id"],
            "path_b64": row["path_b64"],
            "category": category,
            "detail": detail,
        }
    )


def verify_one(
    row: dict[str, Any],
    source_root: bytes,
    dest_root: bytes,
    max_content_bytes: int,
    mismatches: list[dict[str, Any]],
) -> None:
    try:
        relative = base64.b64decode(row["path_b64"], validate=True)
        source = safe_join(source_root, relative)
        dest = safe_join(dest_root, relative)
        if not path_is_beneath(source_root, source, relative) or not path_is_beneath(
            dest_root, dest, relative
        ):
            raise ValueError("a parent component resolves outside its verification root")
    except (ValueError, TypeError) as error:
        add_mismatch(mismatches, row, "unsafe_path", str(error))
        return
    try:
        source_stat = os.lstat(source)
    except OSError as error:
        add_mismatch(mismatches, row, "source_missing", str(error))
        return
    try:
        dest_stat = os.lstat(dest)
    except OSError as error:
        add_mismatch(mismatches, row, "destination_missing", str(error))
        return

    expected_type = row["file_type"]
    source_type = file_type(source_stat.st_mode)
    dest_type = file_type(dest_stat.st_mode)
    if source_type != expected_type:
        add_mismatch(
            mismatches, row, "source_changed",
            f"file type scan={FILE_TYPE_NAMES.get(expected_type)} current={FILE_TYPE_NAMES.get(source_type)}",
        )
        return
    if dest_type != expected_type:
        add_mismatch(
            mismatches, row, "file_type",
            f"source={FILE_TYPE_NAMES.get(expected_type)} destination={FILE_TYPE_NAMES.get(dest_type)}",
        )
        return

    expected_permissions = row["mode"] & 0o7777
    source_permissions = stat.S_IMODE(source_stat.st_mode)
    dest_permissions = stat.S_IMODE(dest_stat.st_mode)
    if source_permissions != expected_permissions:
        add_mismatch(
            mismatches, row, "source_changed",
            f"mode scan={expected_permissions:o} current={source_permissions:o}",
        )
    if dest_permissions != expected_permissions:
        add_mismatch(
            mismatches, row, "mode",
            f"expected={expected_permissions:o} destination={dest_permissions:o}",
        )
    for field, source_value, dest_value in (
        ("uid", source_stat.st_uid, dest_stat.st_uid),
        ("gid", source_stat.st_gid, dest_stat.st_gid),
    ):
        expected = row.get(field)
        if expected is not None and source_value != expected:
            add_mismatch(mismatches, row, "source_changed", f"{field} scan={expected} current={source_value}")
        if expected is not None and dest_value != expected:
            add_mismatch(mismatches, row, field, f"expected={expected} destination={dest_value}")
    if row.get("mtime_sec") is not None and row.get("mtime_nsec") is not None:
        expected_mtime_ns = row["mtime_sec"] * 1_000_000_000 + row["mtime_nsec"]
        if source_stat.st_mtime_ns != expected_mtime_ns:
            add_mismatch(
                mismatches, row, "source_changed",
                f"mtime_ns scan={expected_mtime_ns} current={source_stat.st_mtime_ns}",
            )
        if dest_stat.st_mtime_ns != expected_mtime_ns:
            add_mismatch(
                mismatches, row, "mtime",
                f"expected_ns={expected_mtime_ns} destination_ns={dest_stat.st_mtime_ns}",
            )

    if expected_type == 1:
        if source_stat.st_size != row["size"]:
            add_mismatch(
                mismatches, row, "source_changed",
                f"size scan={row['size']} current={source_stat.st_size}",
            )
        if dest_stat.st_size != row["size"]:
            add_mismatch(
                mismatches, row, "size",
                f"expected={row['size']} destination={dest_stat.st_size}",
            )
        if row["size"] <= max_content_bytes:
            try:
                source_digest = content_sha256(source)
                dest_digest = content_sha256(dest)
            except OSError as error:
                add_mismatch(mismatches, row, "content_read", str(error))
            else:
                if source_digest != dest_digest:
                    add_mismatch(
                        mismatches, row, "content",
                        f"source_sha256={source_digest} destination_sha256={dest_digest}",
                    )
    elif expected_type == 3:
        try:
            source_target = os.readlink(source)
            dest_target = os.readlink(dest)
        except OSError as error:
            add_mismatch(mismatches, row, "symlink_read", str(error))
        else:
            if source_target != dest_target:
                add_mismatch(
                    mismatches, row, "symlink_target",
                    "source_b64={} destination_b64={}".format(
                        base64.b64encode(source_target).decode(),
                        base64.b64encode(dest_target).decode(),
                    ),
                )


def command_check(args: argparse.Namespace) -> None:
    sample = load_json(Path(args.sample), "sample")
    if sample.get("schema_version") != SCHEMA_VERSION or sample.get("run_id") != args.run_id:
        fail("sample schema or run ID does not match")
    source_root = os.fsencode(args.source_base)
    dest_root = os.fsencode(args.dest_base)
    if not os.path.isdir(source_root) or not os.path.isdir(dest_root):
        fail("source and destination verification roots must both be directories")

    mismatches: list[dict[str, Any]] = []
    for row in sample.get("items", []):
        verify_one(row, source_root, dest_root, args.max_content_bytes, mismatches)
    categories: dict[str, int] = {}
    for mismatch in mismatches:
        categories[mismatch["category"]] = categories.get(mismatch["category"], 0) + 1
    result = {
        "schema_version": SCHEMA_VERSION,
        "checked_utc": utc_now(),
        "run_id": args.run_id,
        "sample_count": len(sample.get("items", [])),
        "passed": len(mismatches) == 0,
        "mismatch_count": len(mismatches),
        "categories": categories,
        "mismatches": mismatches[: args.max_reported_mismatches],
        "mismatches_truncated": max(0, len(mismatches) - args.max_reported_mismatches),
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    if mismatches:
        raise SystemExit(1)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    sample = subparsers.add_parser("sample")
    sample.add_argument("--rewrite-report", required=True)
    sample.add_argument("--output", required=True)
    sample.add_argument("--run-id", required=True)
    sample.add_argument("--count", type=int, required=True)
    sample.add_argument("--seed", type=int, required=True)
    sample.set_defaults(handler=command_sample)

    check = subparsers.add_parser("check")
    check.add_argument("--sample", required=True)
    check.add_argument("--run-id", required=True)
    check.add_argument("--source-base", required=True)
    check.add_argument("--dest-base", required=True)
    check.add_argument("--max-content-bytes", type=int, required=True)
    check.add_argument("--max-reported-mismatches", type=int, default=100)
    check.set_defaults(handler=command_check)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    if getattr(args, "count", 1) < 1:
        fail("sample count must be positive")
    args.handler(args)


if __name__ == "__main__":
    main()
