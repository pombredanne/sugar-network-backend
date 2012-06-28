# sugar-lint: disable

import os
import sys
import signal
import shutil
import logging
import unittest
import subprocess
from os.path import dirname, join, exists, abspath

import active_document as ad
import restful_document as rd
from active_toolkit import coroutine
from sugar_network.client import bus
from sugar_network.toolkit import sugar, http
from sugar_network.local.bus import IPCServer
from sugar_network.local.mounts import Mounts
from sugar_network import local, node
from sugar_network.node.mounts import NodeCommands
from sugar_network.resources.user import User
from sugar_network.resources.context import Context
from sugar_network.node import sneakernet


root = abspath(dirname(__file__))
tmproot = join(root, '.tmp')
tmpdir = None


def main():
    shutil.rmtree(tmproot, ignore_errors=True)
    unittest.main()


class Test(unittest.TestCase):

    def setUp(self):
        self._overriden = []

        os.environ['LANG'] = 'en_US'

        global tmpdir
        tmpdir = join(tmproot, '.'.join(self.id().split('.')[1:]))
        shutil.rmtree(tmpdir, ignore_errors=True)
        os.makedirs(tmpdir)
        os.chdir(tmpdir)

        self.logfile = tmpdir + '.log'
        if exists(self.logfile):
            os.unlink(self.logfile)

        os.environ['SUGAR_LOGGER_LEVEL'] = 'all'
        os.environ['HOME'] = tmpdir
        profile_dir = join(tmpdir, '.sugar', 'default')
        os.makedirs(profile_dir)
        shutil.copy(join(root, 'data', 'owner.key'), profile_dir)
        shutil.copy(join(root, 'data', 'owner.key.pub'), profile_dir)

        ad.index_flush_timeout.value = 0
        ad.index_flush_threshold.value = 1
        ad.find_limit.value = 1024
        ad.index_write_queue.value = 10
        local.local_root.value = tmpdir
        local.activities.value = [tmpdir + '/Activities']
        local.api_url.value = 'http://localhost:8800'
        local.server_mode.value = False

        node.privkey.value = join(profile_dir, 'owner.key')

        sugar.nickname = lambda: 'test'
        sugar.color = lambda: '#000000,#000000'

        sneakernet._RESERVED_SIZE = 0
        sneakernet._PACKET_COMPRESS_MODE = ''

        self._logfile = file(self.logfile + '.out', 'a')
        sys.stdout = sys.stderr = self._logfile

        bus._CONNECTION_POOL = 1
        bus.Client.close()

        http.reset()

        for handler in logging.getLogger().handlers:
            logging.getLogger().removeHandler(handler)
        logging.basicConfig(level=logging.DEBUG, filename=self.logfile)

        self.server = None
        self.mounts = None

        self.forks = []
        self.fork_num = 0

    def tearDown(self):
        self.stop_servers()
        while self._overriden:
            mod, name, old_handler = self._overriden.pop()
            setattr(mod, name, old_handler)
        coroutine.shutdown()
        sys.stdout.flush()

    def stop_servers(self):
        if self.mounts is not None:
            self.mounts.close()
        if bus.Client.connection is not None:
            bus.Client.connection.close()
        bus.Client.close()
        if self.server is not None:
            self.server.stop()
        while self.forks:
            pid = self.forks.pop()
            self.assertEqual(0, self.waitpid(pid))

    def waitpid(self, pid, sig=signal.SIGTERM):
        if pid in self.forks:
            self.forks.remove(pid)
        try:
            os.kill(pid, sig)
        except Exception:
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

    def fork(self, cb, *args):
        pid = os.fork()
        if not pid:
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
        else:
            self.forks.append(pid)
            coroutine.sleep(1)
            return pid

    def popen(self, *args):
        self.fork_num += 1
        logfile = file('%s-%s.log' % (tmpdir, self.fork_num), 'w')
        child = subprocess.Popen(*args, stdout=logfile, stderr=logfile)
        self.forks.append(child.pid)
        coroutine.sleep(1)
        return child.pid

    def start_server(self, classes=None, open=True):

        def server():
            self.server.serve_forever()

        if classes is None:
            classes = [User, Context]
        volume = ad.SingleVolume('local', classes)
        self.mounts = Mounts(volume)
        self.server = IPCServer(self.mounts)
        coroutine.spawn(server)
        if open:
            self.mounts.open()
        coroutine.dispatch()

    def start_ipc_and_restful_server(self, classes=None):
        pid = self.fork(self.restful_server, classes)
        self.start_server(classes, open=False)

        def wait_connect(event):
            if event['event'] == 'connect':
                connected.set()

        connected = coroutine.Event()
        bus.Client.connect(wait_connect)
        coroutine.dispatch()
        self.mounts.open()
        connected.wait()

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
        ad.find_limit.value = 1024
        ad.index_write_queue.value = 10

        volume = ad.SingleVolume('remote', classes or [User, Context])
        cp = Commands(volume)
        httpd = coroutine.WSGIServer(('localhost', 8800), rd.Router(cp))
        subscriber = rd.SubscribeSocket(volume, 'localhost', 8801)
        try:
            coroutine.joinall([
                coroutine.spawn(httpd.serve_forever),
                coroutine.spawn(subscriber.serve_forever),
                ])
        finally:
            httpd.stop()
            subscriber.stop()
            volume.close()


class Commands(NodeCommands):

    def __init__(self, volume):
        NodeCommands.__init__(self, volume)


PUBKEY = """\
ssh-dss AAAAB3NzaC1kc3MAAACBANuYoFH3uvJGoQFMeW6M3CCJQlPrSv6sqd9dGQlwnnNxLBrq6KgY63e10ULtyYzq9UjiIUowqbtheGrtPCtL5w7qmFcCnq1cFzAk6Xxfe6ytJDx1fql5Y1wKqa+zxOKF6SGNnglyxvf78mZXt2G6wx22AjW+1fEhAOr+g8kRiUbBAAAAFQDA/W3LfD5NBB4vlZFcT10jU4B8QwAAAIBHh1U2B71memu/TsatwOo9+CyUyvF0FHHsXwQDkeRjqY3dcfeV38YoU/EbOZtHIQgdfGrzy7m5osnpBwUtHLunZJuwCt5tBNrpU8CAF7nEXOJ4n2FnoNiWO1IsbWdhkh9Hd7+TBM9hLGmOqlqTIx3TmUG0e4F2X33VVJ8UsrJ3mwAAAIEAm29WVw9zkRbv6CTFhPlLjJ71l/2GE9XFbdznJFRmPNBBWF2J452okRWywzeDMIIoi/z0wmNSr2B6P9wduxSxp8eIWQhKVQa4V4lJyqX/A2tE5SQtFULtw3yiYOUaCjvB2s46ZM6/9K3r8o7FSKHDpYlqAbBKURNCot5zDAu6RgE=
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
