# sugar-lint: disable

from __init__ import tests

from downloads import *
from files import *
from master import *
from node import *
from obs import *
from stats_node import *
from stats_user import *
from sync import *
from sync_master import *
from sync_offline import *
from sync_online import *
from volume import *

if __name__ == '__main__':
    tests.main()
