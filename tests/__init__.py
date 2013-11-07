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
from sugar_network.client.routes import ClientRoutes, _Auth
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
        self.node_routes = None
        self.node_volume = None

        os.environ['LANG'] = 'en_US'
        os.environ['LANGUAGE'] = 'en_US'
        toolkit._default_langs = None

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
        node.stats_root.value = tmpdir + '/stats'
        node.port.value = 8888
        db.index_write_queue.value = 10
        client.local_root.value = tmpdir
        client.api_url.value = 'http://127.0.0.1:8888'
        client.mounts_root.value = None
        client.ipc_port.value = 5555
        client.layers.value = None
        client.cache_limit.value = 0
        client.cache_limit_percent.value = 0
        client.cache_lifetime.value = 0
        client.keyfile.value = join(root, 'data', UID)
        client_routes._RECONNECT_TIMEOUT = 0
        mountpoints._connects.clear()
        mountpoints._found.clear()
        mountpoints._COMPLETE_MOUNT_TIMEOUT = .1
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
            self.override(_Auth, 'profile', lambda self: {
                'name': 'test',
                'color': '#000000,#000000',
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
        if self.node_volume is not None:
            self.node_volume.close()
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

    def start_master(self, classes=None, routes=MasterRoutes):
        if classes is None:
            classes = [User, Context, Implementation]
        self.node_volume = db.Volume('master', classes)
        self.node_routes = routes('guid', self.node_volume)
        self.node = coroutine.WSGIServer(('127.0.0.1', 8888), Router(self.node_routes))
        coroutine.spawn(self.node.serve_forever)
        coroutine.dispatch(.1)
        return self.node_volume

    def fork_master(self, classes=None, routes=MasterRoutes):
        if classes is None:
            classes = [User, Context, Implementation]

        def node():
            volume = db.Volume('master', classes)
            self.node_routes = routes('guid', volume)
            node = coroutine.WSGIServer(('127.0.0.1', 8888), Router(self.node_routes))
            node.serve_forever()

        pid = self.fork(node)
        coroutine.sleep(.1)
        return pid

    def start_client(self, classes=None, routes=ClientRoutes):
        if classes is None:
            classes = [User, Context, Implementation]
        volume = db.Volume('client', classes)
        self.client_routes = routes(volume, client.api_url.value)
        self.client = coroutine.WSGIServer(
                ('127.0.0.1', client.ipc_port.value), Router(self.client_routes))
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
        self.client_routes = ClientRoutes(self.home_volume)
        server = coroutine.WSGIServer(('127.0.0.1', client.ipc_port.value), Router(self.client_routes))
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
        self.node_routes = MasterRoutes('guid', volume)
        httpd = coroutine.WSGIServer(('127.0.0.1', 8888), Router(self.node_routes))
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
-----BEGIN PUBLIC KEY-----
MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQC9VocIcz6dSUj64ErftV13lne0
er++oFy17pQXlViwnHIRi4pQutJcJchezLnLxAtDBLE3CsXdQ5RJlMuW7tb9Jt72
gaN7JMte6f4sKJRBW5rafVewwxzLAw0pFKXqYxQaWEdzOWP2YBbJYuLF2ZB/ZddP
MseM2sIevEeOLXznuwIDAQAB
-----END PUBLIC KEY-----
"""
UID = 'f470db873b6a35903aca1f2492188e1c4b9ffc42'

PUBKEY2 = """\
-----BEGIN PUBLIC KEY-----
MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDQ6AWYEKkp5wjPLXd5d024JWPf
ZJ3F9VuFIWNlLMNvGv5XOIAA/VK/tc98Bt6WxI7QZoLEWKb8S4aqkD1KSqjQIpO7
n9WC2r5B15uTNa1Ry3eq0Z3KGeeC6q0466ETDUhqV03K1quLzR//dGdnBgb+hznL
oLqnwHwnk4DFkdO7ZwIDAQAB
-----END PUBLIC KEY-----
"""
UID2 = 'd820a3405d6aadf2cf207f6817db2a79f8fa07aa'
