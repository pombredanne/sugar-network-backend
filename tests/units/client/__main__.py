# sugar-lint: disable

from __init__ import tests

from journal import *
from routes import *
from offline_routes import *
from online_routes import *
from server_routes import *
from injector import *
from packagekit import *

if __name__ == '__main__':
    tests.main()
