# sugar-lint: disable

import os
import sys
import time
import json
import shutil
import signal
import hashlib
import logging
import tempfile
import unittest
from os.path import dirname, join, exists, abspath

import gevent
import requests
from M2Crypto import DSA
from dbus.mainloop.glib import DBusGMainLoop

import active_document as ad
from active_toolkit import coroutine, util
from active_document import env as _env, index_queue as _index_queue
from active_document import storage as _storage
from active_document import directory as _directory
from restful_document.router import Router
from restful_document.subscribe_socket import SubscribeSocket


root = abspath(dirname(__file__))
tmproot = join(root, '.tmp')
tmpdir = None

DBusGMainLoop(set_as_default=True)


def main():
    shutil.rmtree(tmproot, ignore_errors=True)
    unittest.main()


class Test(unittest.TestCase):

    httpd_pids = {}

    def setUp(self):
        self._overriden = []

        global tmpdir
        tmpdir = join(tmproot, '.'.join(self.id().split('.')[1:]))
        shutil.rmtree(tmpdir, ignore_errors=True)
        os.makedirs(tmpdir)
        os.chdir(tmpdir)

        logfile = tmpdir + '.log'
        if exists(logfile):
            os.unlink(logfile)

        self._logfile = file(logfile + '.out', 'a')
        sys.stdout = sys.stderr = self._logfile

        for handler in logging.getLogger().handlers:
            logging.getLogger().removeHandler(handler)
        logging.basicConfig(level=logging.DEBUG, filename=logfile)

        _env.index_flush_timeout.value = 0
        _env.index_flush_threshold.value = 1
        _env.find_limit.value = 1024
        _env.index_lazy_open.value = False
        _directory._DIFF_PAGE_SIZE = 256

        _index_queue.errnum = 0

        _storage._ensure_path_locker = _FakeLocker()

        self.httpd_seqno = 0

    def tearDown(self):
        while Test.httpd_pids:
            self.httpdown(Test.httpd_pids.keys()[0])
        self.assertEqual(0, _index_queue.errnum)
        while self._overriden:
            mod, name, old_handler = self._overriden.pop()
            setattr(mod, name, old_handler)
        _index_queue.stop()
        sys.stdout.flush()

    def override(self, mod, name, new_handler):
        self._overriden.append((mod, name, getattr(mod, name)))
        setattr(mod, name, new_handler)

    def fork(self, cb):
        pid = os.fork()
        if not pid:
            try:
                cb()
                result = 0
            except Exception:
                logging.exception('Child failed')
                result = 1
            sys.stdout.flush()
            sys.stderr.flush()
            os._exit(0)
        else:
            __, status = os.waitpid(pid, 0)
            self.assertEqual(0, os.WEXITSTATUS(status))

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

    def httpd(self, port, classes):
        if port in Test.httpd_pids:
            self.httpdown(port)

        self.httpd_seqno += 1

        child_pid = os.fork()
        if child_pid:
            time.sleep(1)
            Test.httpd_pids[port] = child_pid
            return

        for handler in logging.getLogger().handlers:
            logging.getLogger().removeHandler(handler)
        logging.basicConfig(level=logging.DEBUG, filename=tmpdir + '-%s.http.log' % self.httpd_seqno)

        volume = ad.SingleVolume(tmpdir + '/db', classes)
        cp = ad.VolumeCommands(volume)
        httpd = coroutine.WSGIServer(('localhost', port), Router(cp))
        subscriber = SubscribeSocket(volume, 'localhost', port + 1)
        try:
            gevent.joinall([
                gevent.spawn(httpd.serve_forever),
                gevent.spawn(subscriber.serve_forever),
                ])
        finally:
            httpd.stop()
            subscriber.stop()
            volume.close()

    def httpdown(self, port):
        pid = Test.httpd_pids[port]
        del Test.httpd_pids[port]
        os.kill(pid, signal.SIGINT)
        sys.stdout.flush()
        os.waitpid(pid, 0)


class Resource(object):

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


class User(ad.Document):

    @ad.active_property(ad.StoredProperty)
    def pubkey(self, value):
        return value

    @classmethod
    def before_create(cls, props):
        ssh_pubkey = props['pubkey'].split()[1]
        props['guid'] = str(hashlib.sha1(ssh_pubkey).hexdigest())

        with tempfile.NamedTemporaryFile() as tmp_pubkey:
            tmp_pubkey.file.write(props['pubkey'])
            tmp_pubkey.file.flush()

            pubkey_pkcs8 = util.assert_call(
                    ['ssh-keygen', '-f', tmp_pubkey.name, '-e', '-m', 'PKCS8'])
            props['pubkey'] = pubkey_pkcs8

        super(User, cls).before_create(props)


def sign(privkey, data):
    with tempfile.NamedTemporaryFile() as tmp_privkey:
        tmp_privkey.file.write(privkey)
        tmp_privkey.file.flush()
        key = DSA.load_key(tmp_privkey.name)
        return key.sign_asn1(hashlib.sha1(data).digest()).encode('hex')


class _FakeLocker(object):

    def __enter__(self, *args):
        return self

    def __exit__(self, *args):
        pass


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
