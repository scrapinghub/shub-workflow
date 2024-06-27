from tempfile import mktemp
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
