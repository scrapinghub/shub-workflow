import os
import shutil
from tempfile import mkdtemp, mktemp
from unittest import TestCase

from shub_workflow.utils.futils import FSHelper


class FUtilsTest(TestCase):
    def test_fshelper(self) -> None:
        helper = FSHelper()
        test_file_1 = mktemp()
        test_file_2 = mktemp()
        helper.touch(test_file_1)
        helper.mv_file(test_file_1, test_file_2)
        self.assertTrue(helper.exists(test_file_2))
        helper.rm_file(test_file_2)

    def test_list_path_local_prefix(self) -> None:
        helper = FSHelper()
        folder = mkdtemp()
        for name in ("slot1_a.jl.gz", "slot1_b.jl.gz", "slot2_a.jl.gz"):
            helper.touch(os.path.join(folder, name))
        self.assertEqual(len(list(helper.list_path(folder))), 3)
        self.assertEqual(len(list(helper.list_path(folder + "/"))), 3)
        self.assertEqual(
            sorted(helper.list_path(os.path.join(folder, "slot1_"))),
            [os.path.join(folder, "slot1_a.jl.gz"), os.path.join(folder, "slot1_b.jl.gz")],
        )
        self.assertEqual(list(helper.list_path(os.path.join(folder, "slot3_"))), [])
        self.assertEqual(list(helper.list_path(os.path.join(folder, "missing", "slot1_"))), [])
        shutil.rmtree(folder)
