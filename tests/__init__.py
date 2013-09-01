# sugar-lint: disable

import os
import sys
import json
import signal
import shutil
import hashlib
import logging
import zipfile
import unittest
import tempfile
import subprocess
from os.path import dirname, join, exists, abspath, isfile

from M2Crypto import DSA
from gevent import monkey

from sugar_network.toolkit import coroutine, http, mountpoints, Option, gbus
from sugar_network.toolkit.router import Router
from sugar_network.client import IPCConnection, journal, routes as client_routes
from sugar_network.client.routes import ClientRoutes
from sugar_network import db, client, node, toolkit, model
from sugar_network.client import solver
from sugar_network.model.user import User
from sugar_network.model.context import Context
from sugar_network.model.implementation import Implementation
from sugar_network.node.master import MasterRoutes
from sugar_network.node import stats_user, stats_node, obs, slave, downloads
from requests import adapters


root = abspath(dirname(__file__))
# Assume that /tmp is tmpfs
#tmproot = join(root, '.tmp')
tmproot = '/tmp/sugar_network.tests'
tmpdir = None

monkey.patch_socket()
monkey.patch_select()
monkey.patch_ssl()
monkey.patch_time()


def main():
    shutil.rmtree(tmproot, ignore_errors=True)
    unittest.main()


class Test(unittest.TestCase):

    def setUp(self, fork_num=0, tmp_root=None):
        self.maxDiff = None
        self._overriden = []

        os.environ['LANG'] = 'en_US'
        toolkit._default_lang = 'en-us'

        global tmpdir
        tmpdir = join(tmp_root or tmproot, '.'.join(self.id().split('.')[1:]))
        shutil.rmtree(tmpdir, ignore_errors=True)
        os.makedirs(tmpdir)
        os.chdir(tmpdir)

        self._setup_logging(fork_num)

        os.environ['XDG_DATA_HOME'] = tmpdir + '/share'
        os.environ['SUGAR_LOGGER_LEVEL'] = 'all'
        os.environ['HOME'] = tmpdir
        os.environ['LC_ALL'] = 'en_US.UTF-8'
        profile_dir = join(tmpdir, '.sugar', 'default')
        os.makedirs(profile_dir)
        shutil.copy(join(root, 'data', 'owner.key'), join(profile_dir, 'owner.key'))
        shutil.copy(join(root, 'data', 'owner.key.pub'), profile_dir)

        adapters.DEFAULT_RETRIES = 5
        Option.items = {}
        Option.config_files = []
        Option.config = None
        Option._parser = None
        Option._config_to_save = None
        db.index_flush_timeout.value = 0
        db.index_flush_threshold.value = 1
        node.find_limit.value = 1024
        node.data_root.value = tmpdir
        node.files_root.value = None
        node.sync_layers.value = None
        db.index_write_queue.value = 10
        client.local_root.value = tmpdir
        client.api_url.value = 'http://127.0.0.1:8888'
        client.mounts_root.value = None
        client.ipc_port.value = 5555
        client.layers.value = None
        client.cache_limit.value = 0
        client.cache_limit_percent.value = 0
        client.cache_lifetime.value = 0
        client_routes._RECONNECT_TIMEOUT = 0
        mountpoints._connects.clear()
        mountpoints._found.clear()
        mountpoints._COMPLETE_MOUNT_TIMEOUT = .1
        node.stats_root.value = tmpdir + '/stats'
        stats_node.stats_node.value = False
        stats_node.stats_node_step.value = 0
        stats_user.stats_user.value = False
        stats_user.stats_user_step.value = 1
        stats_user._user_cache.clear()
        obs._client = None
        obs._repos = {'base': [], 'presolve': []}
        http._RECONNECTION_NUMBER = 0
        toolkit.cachedir.value = tmpdir + '/tmp'
        journal._ds_root = tmpdir + '/datastore'
        solver.nodeps = False
        solver._stability = None
        solver._conn = None
        downloads._POOL_SIZE = 256
        gbus.join()

        db.Volume.model = [
                'sugar_network.model.user',
                'sugar_network.model.context',
                'sugar_network.model.artifact',
                'sugar_network.model.implementation',
                'sugar_network.model.report',
                ]

        if tmp_root is None:
            self.override(client, 'sugar_profile', lambda: {
                'name': 'test',
                'color': '#000000,#000000',
                'machine_sn': '',
                'machine_uuid': '',
                'pubkey': PUBKEY,
                })

        os.makedirs('tmp')

        self.node = None
        self.client = None

        self.forks = []
        self.fork_num = fork_num

    def tearDown(self):
        self.stop_nodes()
        while db.Volume._flush_pool:
            db.Volume._flush_pool.pop().close()
        while self._overriden:
            mod, name, old_handler = self._overriden.pop()
            setattr(mod, name, old_handler)
        sys.stdout.flush()

    def stop_nodes(self):
        if self.client is not None:
            self.client.close()
        if self.node is not None:
            self.node.stop()
        while self.forks:
            pid = self.forks.pop()
            self.assertEqual(0, self.waitpid(pid))
        coroutine.shutdown()

    def waitpid(self, pid, sig=signal.SIGTERM, ignore_status=False):
        if pid in self.forks:
            self.forks.remove(pid)
        if sig:
            try:
                os.kill(pid, sig)
            except Exception, e:
                pass
        try:
            __, status = os.waitpid(pid, 0)
            if ignore_status:
                return 0
            return os.WEXITSTATUS(status)
        except Exception:
            return 0

    def override(self, mod, name, new_handler):
        self._overriden.append((mod, name, getattr(mod, name)))
        setattr(mod, name, new_handler)

    def touch(self, *files):
        utime = None
        for i in files:
            if isinstance(i, basestring):
                if i.endswith(os.sep):
                    i = i + '.stamp'
                path = i
                if exists(path):
                    content = file(path).read()
                else:
                    content = i
            else:
                if len(i) == 2:
                    path, content = i
                else:
                    path, content, utime = i
                if isinstance(content, list):
                    content = '\n'.join(content)
            path = join(tmpdir, path)

            if not exists(dirname(path)):
                os.makedirs(dirname(path))
            if exists(path):
                os.unlink(path)

            f = file(path, 'w')
            f.write(str(content))
            f.close()

            if utime:
                os.utime(path, (utime, utime))

    def utime(self, path, ts):
        if isfile(path):
            os.utime(path, (ts, ts))
        else:
            for root, __, files in os.walk(path):
                for i in files:
                    os.utime(join(root, i), (ts, ts))

    def zips(self, *items):
        with toolkit.NamedTemporaryFile() as f:
            bundle = zipfile.ZipFile(f.name, 'w')
            for i in items:
                if isinstance(i, basestring):
                    arcname = data = i
                else:
                    arcname, data = i
                if not isinstance(data, basestring):
                    data = '\n'.join(data)
                bundle.writestr(arcname, data)
            bundle.close()
            return file(f.name, 'rb').read()

    def fork(self, cb, *args):
        pid = os.fork()
        if pid:
            self.forks.append(pid)
            coroutine.sleep(.1)
            return pid

        self.fork_num += 1
        self._setup_logging(self.fork_num)
        coroutine.shutdown()
        try:
            cb(*args)
            result = 0
        except Exception:
            logging.exception('Child failed')
            result = 1
        os._exit(result)

    def popen(self, *args, **kwargs):
        self.fork_num += 1
        logfile = file('%s-%s.log' % (tmpdir, self.fork_num), 'w')
        child = subprocess.Popen(*args, stdout=logfile, stderr=logfile, **kwargs)
        self.forks.append(child.pid)
        coroutine.sleep(1)
        return child.pid

    def create_mountset(self, classes=None):
        self.start_server(classes, root=False)

    def start_master(self, classes=None):
        if classes is None:
            classes = [User, Context, Implementation]
        self.node_volume = db.Volume('master', classes)
        cp = MasterRoutes('guid', self.node_volume)
        r = Router(cp)
        self.node = coroutine.WSGIServer(('127.0.0.1', 8888), Router(cp))
        coroutine.spawn(self.node.serve_forever)
        coroutine.dispatch(.1)
        return self.node_volume

    def fork_master(self, classes=None, routes=MasterRoutes):
        if classes is None:
            classes = [User, Context, Implementation]

        def node():
            volume = db.Volume('master', classes)
            cp = routes('guid', volume)
            node = coroutine.WSGIServer(('127.0.0.1', 8888), Router(cp))
            node.serve_forever()

        pid = self.fork(node)
        coroutine.sleep(.1)
        return pid

    def start_client(self, classes=None, routes=ClientRoutes):
        if classes is None:
            classes = [User, Context, Implementation]
        volume = db.Volume('client', classes)
        commands = routes(volume, client.api_url.value)
        self.client = coroutine.WSGIServer(
                ('127.0.0.1', client.ipc_port.value), Router(commands))
        coroutine.spawn(self.client.serve_forever)
        coroutine.dispatch()
        return volume

    def start_online_client(self, classes=None):
        if classes is None:
            classes = [User, Context, Implementation]
        self.start_master(classes)
        volume = db.Volume('client', classes)
        self.client_routes = ClientRoutes(volume, client.api_url.value)
        self.wait_for_events(self.client_routes, event='inline', state='online').wait()
        self.client = coroutine.WSGIServer(
                ('127.0.0.1', client.ipc_port.value), Router(self.client_routes))
        coroutine.spawn(self.client.serve_forever)
        coroutine.dispatch()
        return volume

    def start_offline_client(self, resources=None):
        self.home_volume = db.Volume('db', resources or model.RESOURCES)
        commands = ClientRoutes(self.home_volume)
        server = coroutine.WSGIServer(('127.0.0.1', client.ipc_port.value), Router(commands))
        coroutine.spawn(server.serve_forever)
        coroutine.dispatch()
        return IPCConnection()

    def restful_server(self, classes=None):
        if not exists('remote'):
            os.makedirs('remote')

        logfile = file('remote/log', 'a')
        sys.stdout = sys.stderr = logfile

        for handler in logging.getLogger().handlers:
            logging.getLogger().removeHandler(handler)
        logging.basicConfig(level=logging.DEBUG)

        db.index_flush_timeout.value = 0
        db.index_flush_threshold.value = 1
        node.find_limit.value = 1024
        db.index_write_queue.value = 10

        volume = db.Volume('remote', classes or [User, Context, Implementation])
        cp = MasterRoutes('guid', volume)
        httpd = coroutine.WSGIServer(('127.0.0.1', 8888), Router(cp))
        try:
            coroutine.joinall([
                coroutine.spawn(httpd.serve_forever),
                ])
        finally:
            httpd.stop()
            volume.close()

    def wait_for_events(self, cp, **condition):
        trigger = coroutine.AsyncResult()

        def waiter(trigger):
            for event in cp.subscribe():
                if isinstance(event, basestring) and event.startswith('data: '):
                    event = json.loads(event[6:])
                for key, value in condition.items():
                    if isinstance(value, basestring) and value.startswith('!'):
                        if event.get(key) == value[1:]:
                            break
                    else:
                        if event.get(key) != value:
                            break
                else:
                    trigger.set(event)
                    break

        coroutine.spawn(waiter, trigger)
        coroutine.dispatch()
        return trigger

    def _setup_logging(self, fork_num):
        toolkit.init_logging(10, filename=join(tmpdir, '%s.log' % fork_num))

        sys.stdout.flush()
        sys.stderr.flush()
        outfile = file(join(tmpdir, '%s.out' % fork_num), 'w')
        if fork_num > 0:
            os.dup2(outfile.fileno(), 1)
            os.dup2(outfile.fileno(), 2)
        sys.stdout = sys.stderr = outfile


def sign(privkey, data):
    with tempfile.NamedTemporaryFile() as tmp_privkey:
        tmp_privkey.file.write(privkey)
        tmp_privkey.file.flush()
        key = DSA.load_key(tmp_privkey.name)
        return key.sign_asn1(hashlib.sha1(data).digest()).encode('hex')


PUBKEY = """\
ssh-dss AAAAB3NzaC1kc3MAAACBANuYoFH3uvJGoQFMeW6M3CCJQlPrSv6sqd9dGQlwnnNxLBrq6KgY63e10ULtyYzq9UjiIUowqbtheGrtPCtL5w7qmFcCnq1cFzAk6Xxfe6ytJDx1fql5Y1wKqa+zxOKF6SGNnglyxvf78mZXt2G6wx22AjW+1fEhAOr+g8kRiUbBAAAAFQDA/W3LfD5NBB4vlZFcT10jU4B8QwAAAIBHh1U2B71memu/TsatwOo9+CyUyvF0FHHsXwQDkeRjqY3dcfeV38YoU/EbOZtHIQgdfGrzy7m5osnpBwUtHLunZJuwCt5tBNrpU8CAF7nEXOJ4n2FnoNiWO1IsbWdhkh9Hd7+TBM9hLGmOqlqTIx3TmUG0e4F2X33VVJ8UsrJ3mwAAAIEAm29WVw9zkRbv6CTFhPlLjJ71l/2GE9XFbdznJFRmPNBBWF2J452okRWywzeDMIIoi/z0wmNSr2B6P9wduxSxp8eIWQhKVQa4V4lJyqX/A2tE5SQtFULtw3yiYOUaCjvB2s46ZM6/9K3r8o7FSKHDpYlqAbBKURNCot5zDAu6RgE=
"""
INVALID_PUBKEY = """\
ssh-dss ____B3NzaC1kc3MAAACBANuYoFH3uvJGoQFMeW6M3CCJQlPrSv6sqd9dGQlwnnNxLBrq6KgY63e10ULtyYzq9UjiIUowqbtheGrtPCtL5w7qmFcCnq1cFzAk6Xxfe6ytJDx1fql5Y1wKqa+zxOKF6SGNnglyxvf78mZXt2G6wx22AjW+1fEhAOr+g8kRiUbBAAAAFQDA/W3LfD5NBB4vlZFcT10jU4B8QwAAAIBHh1U2B71memu/TsatwOo9+CyUyvF0FHHsXwQDkeRjqY3dcfeV38YoU/EbOZtHIQgdfGrzy7m5osnpBwUtHLunZJuwCt5tBNrpU8CAF7nEXOJ4n2FnoNiWO1IsbWdhkh9Hd7+TBM9hLGmOqlqTIx3TmUG0e4F2X33VVJ8UsrJ3mwAAAIEAm29WVw9zkRbv6CTFhPlLjJ71l/2GE9XFbdznJFRmPNBBWF2J452okRWywzeDMIIoi/z0wmNSr2B6P9wduxSxp8eIWQhKVQa4V4lJyqX/A2tE5SQtFULtw3yiYOUaCjvB2s46ZM6/9K3r8o7FSKHDpYlqAbBKURNCot5zDAu6RgE=
"""
PRIVKEY = """\
-----BEGIN DSA PRIVATE KEY-----
MIIBvAIBAAKBgQDbmKBR97ryRqEBTHlujNwgiUJT60r+rKnfXRkJcJ5zcSwa6uio
GOt3tdFC7cmM6vVI4iFKMKm7YXhq7TwrS+cO6phXAp6tXBcwJOl8X3usrSQ8dX6p
eWNcCqmvs8TihekhjZ4Jcsb3+/JmV7dhusMdtgI1vtXxIQDq/oPJEYlGwQIVAMD9
bct8Pk0EHi+VkVxPXSNTgHxDAoGAR4dVNge9Znprv07GrcDqPfgslMrxdBRx7F8E
A5HkY6mN3XH3ld/GKFPxGzmbRyEIHXxq88u5uaLJ6QcFLRy7p2SbsArebQTa6VPA
gBe5xFzieJ9hZ6DYljtSLG1nYZIfR3e/kwTPYSxpjqpakyMd05lBtHuBdl991VSf
FLKyd5sCgYEAm29WVw9zkRbv6CTFhPlLjJ71l/2GE9XFbdznJFRmPNBBWF2J452o
kRWywzeDMIIoi/z0wmNSr2B6P9wduxSxp8eIWQhKVQa4V4lJyqX/A2tE5SQtFULt
w3yiYOUaCjvB2s46ZM6/9K3r8o7FSKHDpYlqAbBKURNCot5zDAu6RgECFQC6wU/U
6uUSSSw8Apr+eJQlSFhA+Q==
-----END DSA PRIVATE KEY-----
"""
UID = '25c081e29242cf7a19ae893a420ab3de56e9e989'

PUBKEY2 = """\
ssh-dss AAAAB3NzaC1kc3MAAACBAOTS+oSz5nmXlxGLhnadTHwZDf9H124rRLqIxmLhHZy/I93LPHfG1T/hSF9n46DEKwfpLZ8EMNl2VNATvPhbst0ckcsdaB6FSblYVNMFu9C+SAwiX1+JYw8e9koFq8tIKyBz+V1zzr3VUJoUozYvT4MehIFq2YlYR4AdlnfbwQG/AAAAFQDa4fpL/eMJBgp2azVvcHPXoAN1dQAAAIAM41xtZbZ2GvOyiMB49gPFta/SWsie84agasvDVaUljj4RLgIHAOe75V3vh8Myjz7WxBMqS09IRKO8EM9Xv/BeRdLQfXRFvOY3kG4C5EJPIoZykDKCag9fEtw3PMSSf50wvnO0zz1FlJOKsf0tNYfeO98KY3fUNyxoI4p7HbLAoQAAAIEAxHnjr34jnPHGL8n4lhALJDbBUJOP5SwubArF94wodPmFtDI0ia6lWV1o3aHtwpTKRIiyUocJRaTJzxArdSh3jfutxaoIs+KqPgGa3rO5jbHv07b40bpueH8nnb6Mc5Qas/NaCLwWqWoVs5F7w28v70LB88PcmGxxjP1bXxLlDKE= 
"""
PRIVKEY2 = """\
-----BEGIN DSA PRIVATE KEY-----
MIIBuwIBAAKBgQDk0vqEs+Z5l5cRi4Z2nUx8GQ3/R9duK0S6iMZi4R2cvyPdyzx3
xtU/4UhfZ+OgxCsH6S2fBDDZdlTQE7z4W7LdHJHLHWgehUm5WFTTBbvQvkgMIl9f
iWMPHvZKBavLSCsgc/ldc8691VCaFKM2L0+DHoSBatmJWEeAHZZ328EBvwIVANrh
+kv94wkGCnZrNW9wc9egA3V1AoGADONcbWW2dhrzsojAePYDxbWv0lrInvOGoGrL
w1WlJY4+ES4CBwDnu+Vd74fDMo8+1sQTKktPSESjvBDPV7/wXkXS0H10RbzmN5Bu
AuRCTyKGcpAygmoPXxLcNzzEkn+dML5ztM89RZSTirH9LTWH3jvfCmN31DcsaCOK
ex2ywKECgYEAxHnjr34jnPHGL8n4lhALJDbBUJOP5SwubArF94wodPmFtDI0ia6l
WV1o3aHtwpTKRIiyUocJRaTJzxArdSh3jfutxaoIs+KqPgGa3rO5jbHv07b40bpu
eH8nnb6Mc5Qas/NaCLwWqWoVs5F7w28v70LB88PcmGxxjP1bXxLlDKECFFHbJZ6Y
D+YxdWZ851uNEXjVIvza
-----END DSA PRIVATE KEY-----
"""
UID2 = 'd87dc9fde73fa1cf86c1e7ce86129eaf88985828'
