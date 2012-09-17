# sugar-lint: disable

import os
import sys
import json
import signal
import shutil
import hashlib
import logging
import unittest
import tempfile
import subprocess
from os.path import dirname, join, exists, abspath, isfile

import requests
from M2Crypto import DSA

import active_document as ad
from active_toolkit import coroutine
from sugar_network.toolkit import sugar, http, sneakernet, mounts_monitor
from sugar_network.toolkit.router import Router
from sugar_network.local.ipc_client import Router as IPCRouter
from sugar_network.local.mounts import HomeMount, RemoteMount
from sugar_network.local.mountset import Mountset
from sugar_network import local, node
from sugar_network.resources.user import User
from sugar_network.resources.context import Context
from sugar_network.node.commands import NodeCommands
from sugar_network.node import stats
from sugar_network.resources.volume import Volume


root = abspath(dirname(__file__))
tmproot = join(root, '.tmp')
tmpdir = None


def main():
    shutil.rmtree(tmproot, ignore_errors=True)
    unittest.main()


class Test(unittest.TestCase):

    def setUp(self, fork_num=0):
        self._overriden = []

        os.environ['LANG'] = 'en_US'

        global tmpdir
        tmpdir = join(tmproot, '.'.join(self.id().split('.')[1:]))
        shutil.rmtree(tmpdir, ignore_errors=True)
        os.makedirs(tmpdir)
        os.chdir(tmpdir)

        if fork_num:
            self.logfile = tmpdir + '-%s.log' % fork_num
        else:
            self.logfile = tmpdir + '.log'
        if exists(self.logfile):
            os.unlink(self.logfile)

        os.environ['XDG_DATA_HOME'] = tmpdir + '/share'
        os.environ['SUGAR_LOGGER_LEVEL'] = 'all'
        os.environ['HOME'] = tmpdir
        profile_dir = join(tmpdir, '.sugar', 'default')
        os.makedirs(profile_dir)
        shutil.copy(join(root, 'data', 'owner.key'), profile_dir)
        shutil.copy(join(root, 'data', 'owner.key.pub'), profile_dir)

        ad.index_flush_timeout.value = 0
        ad.index_flush_threshold.value = 1
        node.find_limit.value = 1024
        node.tmpdir.value = tmpdir + '/tmp'
        node.data_root.value = tmpdir
        node.sync_dirs.value = []
        ad.index_write_queue.value = 10
        local.local_root.value = tmpdir
        local.activity_dirs.value = [tmpdir + '/Activities']
        local.api_url.value = 'http://localhost:8800'
        local.server_mode.value = False
        local.mounts_root.value = None
        local.ipc_port.value = 5101
        mounts_monitor.stop()
        mounts_monitor._COMPLETE_MOUNT_TIMEOUT = .1
        stats.stats_root.value = tmpdir + '/stats'
        stats.stats_step.value = 1
        stats.stats_rras.value = ['RRA:AVERAGE:0.5:1:100']
        stats._cache.clear()

        Volume.RESOURCES = [
                'sugar_network.resources.user',
                'sugar_network.resources.context',
                'sugar_network.resources.report',
                ]

        sugar.nickname = lambda: 'test'
        sugar.color = lambda: '#000000,#000000'

        sneakernet._RESERVED_SIZE = 0
        sneakernet._PACKET_COMPRESS_MODE = ''
        sneakernet.TMPDIR = tmpdir + '/tmp'
        os.makedirs('tmp')

        self._logfile = file(self.logfile + '.out', 'a')
        sys.stdout = sys.stderr = self._logfile

        for handler in logging.getLogger().handlers:
            logging.getLogger().removeHandler(handler)
        logging.basicConfig(level=logging.DEBUG, filename=self.logfile)

        self.server = None
        self.mounts = None

        self.forks = []
        self.fork_num = fork_num

    def tearDown(self):
        self.stop_servers()
        while self._overriden:
            mod, name, old_handler = self._overriden.pop()
            setattr(mod, name, old_handler)
        sys.stdout.flush()

    def stop_servers(self):
        if self.mounts is not None:
            self.mounts.close()
        if self.server is not None:
            self.server.stop()
        while self.forks:
            pid = self.forks.pop()
            self.assertEqual(0, self.waitpid(pid))
        coroutine.shutdown()

    def waitpid(self, pid, sig=signal.SIGTERM):
        if pid in self.forks:
            self.forks.remove(pid)
        if sig:
            try:
                os.kill(pid, sig)
            except Exception, e:
                pass
        try:
            __, status = os.waitpid(pid, 0)
            return os.WEXITSTATUS(status)
        except OSError:
            return 0

    def override(self, mod, name, new_handler):
        self._overriden.append((mod, name, getattr(mod, name)))
        setattr(mod, name, new_handler)

    def touch(self, *files):
        for i in files:
            if isinstance(i, str):
                if i.endswith(os.sep):
                    i = i + '.stamp'
                path = i
                if exists(path):
                    content = file(path).read()
                else:
                    content = i
            else:
                path, content = i
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

    def utime(self, path, ts):
        if isfile(path):
            os.utime(path, (ts, ts))
        else:
            for root, __, files in os.walk(path):
                for i in files:
                    os.utime(join(root, i), (ts, ts))

    def fork(self, cb, *args):
        pid = os.fork()
        if pid:
            self.forks.append(pid)
            coroutine.sleep(2)
            return pid

        self.fork_num += 1
        for handler in logging.getLogger().handlers:
            logging.getLogger().removeHandler(handler)
        logging.basicConfig(level=logging.DEBUG,
                filename='%s-%s.log' % (tmpdir, self.fork_num))

        coroutine.shutdown()
        try:
            cb(*args)
            result = 0
        except Exception:
            logging.exception('Child failed')
            result = 1
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(result)

    def popen(self, *args, **kwargs):
        self.fork_num += 1
        logfile = file('%s-%s.log' % (tmpdir, self.fork_num), 'w')
        child = subprocess.Popen(*args, stdout=logfile, stderr=logfile, **kwargs)
        self.forks.append(child.pid)
        coroutine.sleep(1)
        return child.pid

    def start_server(self, classes=None, root=True):
        if classes is None:
            classes = [User, Context]
        volume = Volume('local', classes)
        self.mounts = Mountset(volume)
        self.mounts['~'] = HomeMount(volume)
        if root:
            self.mounts['/'] = RemoteMount(volume)
        self.server = coroutine.WSGIServer(
                ('localhost', local.ipc_port.value), IPCRouter(self.mounts))
        coroutine.spawn(self.server.serve_forever)
        self.mounts.open()
        self.mounts.opened.wait()
        coroutine.dispatch()

    def create_mountset(self, classes=None):
        self.start_server(classes, root=False)

    def start_ipc_and_restful_server(self, classes=None, **kwargs):
        pid = self.fork(self.restful_server, classes)
        self.start_server(classes)
        self.mounts['/'].mounted.wait()
        return pid

    def restful_server(self, classes=None):
        if not exists('remote'):
            os.makedirs('remote')

        logfile = file('remote/log', 'a')
        sys.stdout = sys.stderr = logfile

        for handler in logging.getLogger().handlers:
            logging.getLogger().removeHandler(handler)
        logging.basicConfig(level=logging.DEBUG)

        ad.index_flush_timeout.value = 0
        ad.index_flush_threshold.value = 1
        node.find_limit.value = 1024
        ad.index_write_queue.value = 10

        volume = Volume('remote', classes or [User, Context])
        cp = NodeCommands(volume)
        httpd = coroutine.WSGIServer(('localhost', 8800), Router(cp))
        try:
            coroutine.joinall([
                coroutine.spawn(httpd.serve_forever),
                ])
        finally:
            httpd.stop()
            volume.close()


class Request(object):

    def __init__(self, url, uid=None, privkey=None, pubkey=None):
        self.url = url
        self.uid = uid or UID
        self.privkey = privkey or PRIVKEY

        #self.post('/user', {'uid': self.uid, 'pubkey': pubkey or PUBKEY})
        self.post('/user', {'pubkey': pubkey or PUBKEY})

    def get(self, path, **kwargs):
        return self._request('GET', path, **kwargs)

    def put(self, path, data, **kwargs):
        return self._request('PUT', path, data, **kwargs)

    def post(self, path, data, **kwargs):
        return self._request('POST', path, data, **kwargs)

    def delete(self, path, **kwargs):
        return self._request('DELETE', path, **kwargs)

    def _request(self, method, path, data=None, headers=None, **kwargs):
        if not headers:
            if data:
                headers = {'Content-Type': 'application/json'}
                data = json.dumps(data)
            else:
                headers = {}
        headers['SUGAR_USER'] = self.uid
        headers['SUGAR_USER_SIGNATURE'] = sign(self.privkey, self.uid)

        response = requests.request(method, self.url + path, data=data,
                headers=headers, config={'keep_alive': True}, params=kwargs)
        reply = response.content

        if response.status_code != 200:
            if reply:
                raise RuntimeError(reply)
            else:
                response.raise_for_status()

        if response.headers.get('Content-Type') == 'application/json':
            return json.loads(reply)
        else:
            return reply


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
