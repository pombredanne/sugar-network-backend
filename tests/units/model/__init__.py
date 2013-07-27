# sugar-lint: disable

import sys
from os.path import dirname, join, abspath

src_root = abspath(join(dirname(__file__), '..', '..', '..'))
sys.path.insert(0, src_root)

import tests
