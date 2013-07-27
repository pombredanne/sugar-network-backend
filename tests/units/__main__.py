# sugar-lint: disable

from __init__ import tests

from toolkit.__main__ import *
from db.__main__ import *
from node.__main__ import *
from model.__main__ import *
from client.__main__ import *

if __name__ == '__main__':
    tests.main()
