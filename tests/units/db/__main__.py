# sugar-lint: disable

from __init__ import tests

from commands import *
from document import *
from env import *
from index import *
from metadata import *
from migrate import *
from router import *
from storage import *
from volume import *

if __name__ == '__main__':
    tests.main()
