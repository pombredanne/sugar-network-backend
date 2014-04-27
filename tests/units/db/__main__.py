# sugar-lint: disable

from __init__ import tests

from metadata import *
from storage import *
from index import *
from resource import *
from db_routes import *
from blobs import *
from volume import *
#from migrate import *

if __name__ == '__main__':
    tests.main()
