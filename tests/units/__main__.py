# sugar-lint: disable

from __init__ import tests

from collection import *
from spec import *
from volume import *
from local import *
from node import *
from dbus_client import *
from sneakernet import *
from router import *
from files_sync import *
from sync_node import *
from sync_master import *
from mounts_monitor import *
from activities import *
from home_mount import *
from remote_mount import *
#from node_mount import *
from injector import *
from mountset import *
from auth import *
from context import *
from implementation import *
from obs import *

tests.main()
