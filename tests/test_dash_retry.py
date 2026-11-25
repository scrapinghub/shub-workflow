from typing import Callable, List
from unittest import TestCase
from unittest.mock import patch

from scrapinghub.client.exceptions import NotFound, ServerError, Unauthorized

from shub_workflow.utils import dash_retry_decorator


def _flaky(errors: List[Exception]) -> Callable[[], str]:
    """
    A callable that raises the given errors in order, one per call, and then returns "ok".
    Its `calls` list records each attempt.
    """
    calls: List[int] = []

    def func() -> str:
        calls.append(1)
        if errors:
            raise errors.pop(0)
        return "ok"

    func.calls = calls  # type: ignore[attr-defined]
    return func


class DashRetryTest(TestCase):
    def setUp(self) -> None:
        # don't wait between attempts
        patcher = patch("tenacity.nap.time.sleep")
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_temporary_exception_retried(self) -> None:
        func = _flaky([ServerError("blip")] * 10)
        self.assertEqual(dash_retry_decorator(func)(), "ok")
        self.assertEqual(len(func.calls), 11)  # type: ignore[attr-defined]

    @patch("shub_workflow.utils.DASH_AMBIGUOUS_RETRY_MAX", 5)
    def test_ambiguous_exception_recovers(self) -> None:
        func = _flaky([Unauthorized("blip")] * 5)
        self.assertEqual(dash_retry_decorator(func)(), "ok")
        self.assertEqual(len(func.calls), 6)  # type: ignore[attr-defined]

    @patch("shub_workflow.utils.DASH_AMBIGUOUS_RETRY_MAX", 5)
    def test_ambiguous_exception_retried_limited_times(self) -> None:
        func = _flaky([Unauthorized("no access")] * 100)
        with self.assertRaises(Unauthorized):
            dash_retry_decorator(func)()
        self.assertEqual(len(func.calls), 6)  # type: ignore[attr-defined]

    @patch("shub_workflow.utils.DASH_AMBIGUOUS_RETRY_MAX", 2)
    def test_temporary_exceptions_dont_consume_ambiguous_retries(self) -> None:
        func = _flaky([ServerError("blip")] * 10 + [Unauthorized("blip")] * 2)
        self.assertEqual(dash_retry_decorator(func)(), "ok")
        self.assertEqual(len(func.calls), 13)  # type: ignore[attr-defined]

    @patch("shub_workflow.utils.DASH_AMBIGUOUS_RETRY_MAX", 2)
    def test_ambiguous_retries_counted_per_call(self) -> None:
        errors: List[Exception] = [Unauthorized("blip")] * 2
        func = _flaky(errors)
        decorated = dash_retry_decorator(func)
        self.assertEqual(decorated(), "ok")
        errors.extend([Unauthorized("blip")] * 2)
        self.assertEqual(decorated(), "ok")
        self.assertEqual(len(func.calls), 6)  # type: ignore[attr-defined]

    @patch("shub_workflow.utils.DASH_RETRY_MAX", 3)
    def test_total_attempts_limited(self) -> None:
        func = _flaky([ServerError("blip")] * 10)
        with self.assertRaises(ServerError):
            dash_retry_decorator(func)()
        self.assertEqual(len(func.calls), 3)  # type: ignore[attr-defined]

    def test_other_exception_not_retried(self) -> None:
        func = _flaky([NotFound("missing")])
        with self.assertRaises(NotFound):
            dash_retry_decorator(func)()
        self.assertEqual(len(func.calls), 1)  # type: ignore[attr-defined]
