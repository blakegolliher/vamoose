import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock


MODULE_PATH = Path(__file__).parents[1] / "prepare.py"
SPEC = importlib.util.spec_from_file_location("vamoose_ops_prepare", MODULE_PATH)
prepare = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(prepare)


class FakeAws:
    objects = {}
    uploads = 0

    def __init__(self, endpoint, region, profile, verify_tls):
        self.endpoint = endpoint

    def get_bytes(self, bucket, key):
        value = self.objects.get((bucket, key))
        return None if value is None else value["body"]

    def head(self, bucket, key):
        value = self.objects.get((bucket, key))
        if value is None:
            return None
        return {
            "ContentLength": len(value["body"]),
            "Metadata": value["metadata"],
            "ETag": f'"{value["etag"]}"',
        }

    def run(self, service, arguments, *, check=True, capture=False):
        if service == "s3" and arguments[0] == "cp":
            source = Path(arguments[1])
            bucket, key = arguments[2].removeprefix("s3://").split("/", 1)
            metadata_text = arguments[arguments.index("--metadata") + 1]
            metadata = dict(item.split("=", 1) for item in metadata_text.split(","))
            body = source.read_bytes()
            self.objects[(bucket, key)] = {
                "body": body,
                "metadata": metadata,
                "etag": hashlib.md5(body).hexdigest(),  # nosec - fake S3 ETag
            }
            type(self).uploads += 1
            return subprocess.CompletedProcess([], 0, b"", b"")
        if service == "s3api" and arguments[0] == "put-object":
            bucket = arguments[arguments.index("--bucket") + 1]
            key = arguments[arguments.index("--key") + 1]
            body = Path(arguments[arguments.index("--body") + 1]).read_bytes()
            if (bucket, key) in self.objects:
                return subprocess.CompletedProcess([], 1, b"", b"412")
            self.objects[(bucket, key)] = {
                "body": body,
                "metadata": {},
                "etag": hashlib.md5(body).hexdigest(),  # nosec - fake S3 ETag
            }
            return subprocess.CompletedProcess([], 0, b"{}", b"")
        raise AssertionError((service, arguments))


class UploadCheckpointTests(unittest.TestCase):
    def setUp(self):
        FakeAws.objects = {}
        FakeAws.uploads = 0
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def args(self):
        canonical = self.root / "canonical"
        canonical.mkdir()
        shard = canonical / "part-r00-00000.parquet"
        shard.write_bytes(b"canonical-parquet-fixture")
        digest = prepare.sha256_file(shard)
        rewrite = {
            "schema_version": 1,
            "input_dir": "/scan",
            "output_dir": str(canonical),
            "source_root": "/dataset",
            "walker_version": "test",
            "complete": True,
            "shards": [
                {
                    "input_name": shard.name,
                    "shard_index": 0,
                    "input_bytes": 123,
                    "input_modified_unix_ns": 456,
                    "output_name": shard.name,
                    "output_bytes": shard.stat().st_size,
                    "output_sha256": digest,
                    "rows": 9,
                }
            ],
        }
        rewrite_path = self.root / "rewrite.json"
        rewrite_path.write_text(json.dumps(rewrite), encoding="utf-8")
        return argparse.Namespace(
            rewrite_report=str(rewrite_path),
            report=str(self.root / "upload.json"),
            manifest=str(self.root / "manifest.json"),
            run_id="run-001",
            run_env_sha256="a" * 64,
            endpoint="https://s3.example",
            bucket="run-001-bucket",
            region="us-east-1",
            profile="test",
            verify_tls=True,
            source_url="nfs://source/export",
            source_root="/dataset",
            dest_url="nfs://dest/export",
            dest_root="/copy",
            preserve_owner=True,
            preserve_mode=True,
            preserve_times=True,
            preserve_xattr=True,
        )

    def test_upload_is_checkpointed_and_idempotent(self):
        args = self.args()
        with mock.patch.object(prepare, "AwsCli", FakeAws):
            prepare.command_upload(args)
            prepare.command_upload(args)

        report = json.loads(Path(args.report).read_text(encoding="utf-8"))
        manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
        self.assertTrue(report["complete"])
        self.assertEqual(report["total_rows"], 9)
        self.assertEqual(manifest["run_id"], "run-001")
        self.assertEqual(manifest["shards"][0]["rows"], 9)
        self.assertEqual(FakeAws.uploads, 1, "second invocation must HEAD-and-skip")


if __name__ == "__main__":
    unittest.main()
