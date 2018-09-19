import sys
from contextlib import contextmanager


@contextmanager
def script_args(argv):
    """Capture cmdline args, for easily test scripts"""
    old_argv = sys.argv
    try:
        sys.argv = ['fake_script.py'] + argv
        yield sys.argv
    finally:
        sys.argv = old_argv
