# sugar-lint: disable

from __init__ import tests

from journal import *
from solver import *
from routes import *
from offline_routes import *
from online_routes import *
from server_routes import *
from cache import *
from releases import *

if __name__ == '__main__':
    tests.main()
