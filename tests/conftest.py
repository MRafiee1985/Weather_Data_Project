"""Pytest configuration for the test suite.

The tests import modules from the repository without installing the project as
an editable package.  When the ``pytest`` executable is used directly the
repository root may not be on ``sys.path`` which leads to
``ModuleNotFoundError`` for local packages such as :mod:`api_request`.

Adding the repository root to ``sys.path`` here ensures consistent imports for
all test runs.
"""

import os
import sys


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

