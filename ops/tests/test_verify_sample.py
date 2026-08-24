import base64
import importlib.util
import os
from pathlib import Path
import tempfile
import unittest


MODULE_PATH = Path(__file__).parents[1] / "verify_sample.py"
SPEC = importlib.util.spec_from_file_location("vamoose_verify_sample", MODULE_PATH)
verify = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(verify)


def sample_row(path: bytes, filesystem_path: bytes, row_id: int):
    value = os.lstat(filesystem_path)
    return {
        "row_id": row_id,
        "shard": "fixture.parquet",
        "path_b64": base64.b64encode(path).decode(),
        "file_type": verify.file_type(value.st_mode),
        "size": value.st_size,
        "mode": value.st_mode,
        "uid": value.st_uid,
        "gid": value.st_gid,
        "mtime_sec": value.st_mtime_ns // 1_000_000_000,
        "mtime_nsec": value.st_mtime_ns % 1_000_000_000,
    }


class VerificationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.source = root / "source"
        self.dest = root / "dest"
        self.source.mkdir()
        self.dest.mkdir()

        for parent in (self.source, self.dest):
            file_path = parent / "file.bin"
            file_path.write_bytes(b"same content")
            file_path.chmod(0o640)
            os.utime(file_path, ns=(1_700_000_000_123_000_000,) * 2)
            os.symlink(b"file.bin", os.fsencode(parent / "link"))
            os.utime(
                parent / "link",
                ns=(1_700_000_001_456_000_000,) * 2,
                follow_symlinks=False,
            )

        self.file_row = sample_row(b"/file.bin", os.fsencode(self.source / "file.bin"), 1)
        self.link_row = sample_row(b"/link", os.fsencode(self.source / "link"), 2)

    def tearDown(self):
        self.temp.cleanup()

    def verify(self):
        mismatches = []
        for row in (self.file_row, self.link_row):
            verify.verify_one(
                row,
                os.fsencode(self.source),
                os.fsencode(self.dest),
                1024,
                mismatches,
            )
        return mismatches

    def test_content_metadata_and_symlink_target_match(self):
        self.assertEqual(self.verify(), [])

    def test_content_and_symlink_mismatches_are_distinct(self):
        (self.dest / "file.bin").write_bytes(b"xxxx content")
        os.utime(self.dest / "file.bin", ns=(1_700_000_000_123_000_000,) * 2)
        (self.dest / "link").unlink()
        os.symlink(b"wrong-target", os.fsencode(self.dest / "link"))
        os.utime(
            self.dest / "link",
            ns=(1_700_000_001_456_000_000,) * 2,
            follow_symlinks=False,
        )
        categories = {item["category"] for item in self.verify()}
        self.assertIn("content", categories)
        self.assertIn("symlink_target", categories)

    def test_path_traversal_is_rejected(self):
        with self.assertRaises(ValueError):
            verify.safe_join(os.fsencode(self.source), b"/../escape")


if __name__ == "__main__":
    unittest.main()
