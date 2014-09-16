# Copyright (C) 2011-2014 Aleksey Lim
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import re
import logging

import xapian

from sugar_network import toolkit
from sugar_network.toolkit.router import ACL, File
from sugar_network.toolkit.coroutine import this
from sugar_network.toolkit import pylru, i18n, http, enforce


#: Xapian term prefix for GUID value
GUID_PREFIX = 'I'

_HL_STEM_CACHE = 1024
_HL_PREFIX = '<mark>'
_HL_POSTFIX = '</mark>'
_HL_WORDS_RE = re.compile(r'[\w\']+|\s+|[^\w\'\s]+')

_logger = logging.getLogger('db.metadata')


def stored_property(klass=None, *args, **kwargs):

    def getter(func, self):
        value = self[func.__name__]
        return func(self, value)

    def decorate_setter(func, attr):
        # pylint: disable-msg=W0212
        attr.prop.setter = lambda self, value: \
                self._set(attr.name, func(self, value))
        attr.prop.on_set = func
        return attr

    def decorate_getter(func):
        enforce(func.__name__ != 'guid',
                "Active property should not have 'guid' name")
        attr = lambda self: getter(func, self)
        attr.setter = lambda func: decorate_setter(func, attr)
        # pylint: disable-msg=W0212
        attr._is_db_property = True
        attr.name = func.__name__
        attr.prop = (klass or Property)(*args, name=attr.name, **kwargs)
        attr.prop.on_get = func
        return attr

    return decorate_getter


def indexed_property(klass=None, *args, **kwargs):
    enforce('slot' in kwargs or 'prefix' in kwargs or 'full_text' in kwargs,
            "None of 'slot', 'prefix' or 'full_text' was specified "
            'for indexed property')
    return stored_property(klass, *args, **kwargs)


class IndexableText(str):
    pass


class Metadata(dict):
    """Structure to describe the document.

    Dictionary derived class that contains `Property` objects.

    """

    def __init__(self, cls):
        """
        :param cls:
            class inherited from `db.Resource`

        """
        self._name = cls.__name__.lower()

        slots = {}
        prefixes = {}

        for attr in [getattr(cls, i) for i in dir(cls)]:
            if not hasattr(attr, '_is_db_property'):
                continue
            prop = attr.prop

            if hasattr(prop, 'slot'):
                enforce(prop.slot is None or prop.slot not in slots,
                        'Property %r has a slot already defined for %r in %r',
                        prop.name, slots.get(prop.slot), self.name)
                slots[prop.slot] = prop.name

            if hasattr(prop, 'prefix'):
                enforce(not prop.prefix or prop.prefix not in prefixes,
                        'Property %r has a prefix already defined for %r',
                        prop.name, prefixes.get(prop.prefix))
                prefixes[prop.prefix] = prop.name

            if prop.setter is not None:
                setattr(cls, attr.name, property(attr, prop.setter))
            else:
                setattr(cls, attr.name, property(attr))

            self[prop.name] = prop

        self._keys = dict.keys(self)
        self._keys.sort()

    @property
    def name(self):
        """Resource type name."""
        return self._name

    def keys(self):
        return self._keys

    def __getitem__(self, prop_name):
        enforce(prop_name in self, http.NotFound,
                'There is no %r property in %r', prop_name, self.name)
        return dict.__getitem__(self, prop_name)


class Property(object):
    """Collect information about document properties."""

    def __init__(self, name=None,
            slot=None, prefix=None, full_text=False, boolean=False,
            acl=ACL.PUBLIC, default=None):
        """
        :param name:
            property name;
        :param acl:
            access to the property,
            might be an ORed composition of `db.ACCESS_*` constants;
        :param default:
            default property value;
        :param slot:
            Xapian document's slot number to add property value to;
        :param prefix:
            Xapian serach term prefix, if `None`, property is not a term;
        :param full_text:
            the property takes part in full-text search;
        :param boolean:
            Xapian will use boolean search for this property;

        """
        enforce(name == 'guid' or slot != 0,
                "Slot '0' is reserved for internal needs in %r",
                name)
        enforce(name == 'guid' or prefix != GUID_PREFIX,
                'Prefix %r is reserved for internal needs in %r',
                GUID_PREFIX, name)
        enforce(acl ^ ACL.AUTHOR or acl & ACL.AUTH,
                'ACL.AUTHOR without ACL.AUTH')

        self.setter = None
        self.on_get = lambda self, x: x
        self.on_set = None
        self.name = name
        self.acl = acl
        self.default = default
        self.indexed = slot is not None or prefix is not None or full_text
        self.slot = slot
        self.prefix = prefix
        self.full_text = full_text
        self.boolean = boolean

    def typecast(self, value):
        """Convert input values to types stored in the system."""
        return value

    def reprcast(self, value):
        """Convert output values before returning out of the system."""
        return self.default if value is None else value

    def encode(self, value):
        """Convert stored value to strings capable for indexing."""
        yield toolkit.ascii(value)

    def decode(self, value):
        """Make input string capable for indexing."""
        return toolkit.ascii(value)

    def slotting(self, value):
        """Convert stored value to xapian.NumberValueRangeProcessor values."""
        return next(self.encode(value))

    def teardown(self, value):
        """Cleanup property value on resetting."""
        pass

    def assert_access(self, mode, value=None):
        """Is access to the property permitted.

        If there are no permissions, function should raise
        `http.Forbidden` exception.

        :param mode:
            one of `db.ACCESS_*` constants
            to specify the access mode

        """
        enforce(mode & self.acl, http.Forbidden,
                '%s access is disabled for %r property',
                ACL.NAMES[mode], self.name)


class Reference(Property):
    pass


class Boolean(Property):

    def typecast(self, value):
        if isinstance(value, basestring):
            return value.lower() in ('true', '1', 'on', 'yes')
        return bool(value)

    def encode(self, value):
        yield '1' if value else '0'

    def decode(self, value):
        return '1' if self.typecast(value) else '0'

    def slotting(self, value):
        return xapian.sortable_serialise(value)


class Numeric(Property):

    def typecast(self, value):
        return int(value)

    def encode(self, value):
        yield str(value)

    def decode(self, value):
        return str(int(value))

    def slotting(self, value):
        return xapian.sortable_serialise(value)


class List(Property):

    def __init__(self, subtype=None, **kwargs):
        Property.__init__(self, **kwargs)
        self._subtype = subtype or Property()

    def typecast(self, value):
        if value is None:
            return []
        if type(value) not in (list, tuple):
            return [self._subtype.typecast(value)]
        return [self._subtype.typecast(i) for i in value]

    def encode(self, value):
        for i in value:
            for j in self._subtype.encode(i):
                yield j

    def decode(self, value):
        return self._subtype.decode(value)


class Dict(Property):

    def __init__(self, subtype=None, **kwargs):
        Property.__init__(self, **kwargs)
        self._subtype = subtype or Property()

    def typecast(self, value):
        for key, value_ in value.items():
            value[key] = self._subtype.typecast(value_)
        return value

    def encode(self, items):
        for i in items.values():
            for j in self._subtype.encode(i):
                yield j


class Enum(Property):

    def __init__(self, items, **kwargs):
        enforce(items, 'Enum should not be empty')
        Property.__init__(self, **kwargs)
        self._items = items
        if type(next(iter(items))) in (int, long):
            self._subtype = Numeric()
        else:
            self._subtype = Property()

    def typecast(self, value):
        value = self._subtype.typecast(value)
        enforce(value in self._items, ValueError,
                "Value %r is not in '%s' enum",
                value, ', '.join([str(i) for i in self._items]))
        return value

    def slotting(self, value):
        return self._subtype.slotting(value)


class Blob(Property):

    def __init__(self, mime_type='application/octet-stream', default='',
            **kwargs):
        Property.__init__(self, default=default, **kwargs)
        self.mime_type = mime_type

    def typecast(self, value):
        mime_type = None

        if value is None:
            return
        elif isinstance(value, File):
            return value.digest
        elif isinstance(value, File.Digest):
            return value
        elif isinstance(value, dict):
            mime_type = value.get('content-type')
            value = toolkit.tobytes(value.get('content'))
        elif isinstance(value, basestring):
            value = toolkit.tobytes(value)
        elif not hasattr(value, 'read'):
            raise http.BadRequest('Inappropriate blob value')

        if not mime_type and this.request.prop == self.name:
            mime_type = this.request.content_type
        if not mime_type:
            mime_type = self.mime_type

        return this.volume.blobs.post(toolkit.tobytes(value), mime_type).digest

    def reprcast(self, value):
        if not value:
            return File.AWAY
        return this.volume.blobs.get(value)

    def teardown(self, value):
        if value:
            this.volume.blobs.delete(value)

    def assert_access(self, mode, value=None):
        if mode == ACL.WRITE and not value:
            mode = ACL.CREATE
        Property.assert_access(self, mode, value)


class Composite(Property):
    pass


class Localized(Composite):

    def typecast(self, value):
        if isinstance(value, dict):
            return value
        return {this.request.accept_language[0]: value}

    def reprcast(self, value):
        if value is None:
            value = self.default
        else:
            value = i18n.decode(value, this.request.accept_language)
        if 'highlight' in this.request and this.query:
            value = _highlight(value)
        return value

    def encode(self, value):
        for i in value.values():
            yield toolkit.ascii(i)

    def slotting(self, value):
        # TODO Multilingual sorting
        return i18n.decode(value) or ''


class Aggregated(Composite):

    def __init__(self, subtype=None,
            acl=ACL.CREATE | ACL.READ | ACL.INSERT | ACL.REMOVE, **kwargs):
        enforce(not (acl & ACL.WRITE),
                'ACL.WRITE not allowed for aggregated properties')
        Property.__init__(self, acl=acl, default={}, **kwargs)
        self._subtype = subtype or Property()

    def subtypecast(self, value, aggid=None):
        value = self._subtype.typecast(value)
        if type(value) is tuple:
            aggid_, value = value
            enforce(not aggid or aggid == aggid_, http.BadRequest,
                    'Wrong aggregated id')
            aggid = aggid_
        elif isinstance(value, File.Digest):
            aggid = value
        elif not aggid:
            aggid = toolkit.uuid()
        return aggid, value

    def subreprcast(self, value):
        return self._subtype.reprcast(value)

    def subteardown(self, value):
        self._subtype.teardown(value)

    def typecast(self, value):
        enforce(type(value) is list, http.BadRequest,
                'Aggregated value should be a list')
        result = {}
        for aggvalue in value:
            aggid, aggvalue = self.subtypecast(aggvalue)
            result[aggid] = {'value': aggvalue}
        return result

    def reprcast(self, value):
        result = []
        for key, aggvalue in value.items():
            if 'value' not in aggvalue:
                continue
            aggvalue['key'] = key
            aggvalue['value'] = self.subreprcast(aggvalue['value'])
            if 'author' in aggvalue:
                Author.format(aggvalue['author'])
            result.append(aggvalue)
        return result

    def encode(self, items):
        for agg in items.values():
            if 'value' in agg:
                for j in self._subtype.encode(agg['value']):
                    yield j


class Guid(Property):

    def __init__(self):
        Property.__init__(self, name='guid', slot=0, prefix=GUID_PREFIX,
                acl=ACL.CREATE | ACL.READ)


class Author(Dict):

    INSYSTEM = 1 << 0
    ORIGINAL = 1 << 16

    @staticmethod
    def format(authors):
        for guid, user in authors.items():
            avatar = None
            if 'name' not in user:
                db_user = this.volume['user'][guid]
                if db_user.exists:
                    avatar = db_user.repr('avatar')
                    user['name'] = db_user['name']
                    user['role'] |= Author.INSYSTEM
            if 'avatar' not in user:
                if not avatar:
                    avatar = File(digest='assets/missing-avatar.png')
                user['avatar'] = avatar

    def encode(self, value):
        for guid, props in value.items():
            if 'name' in props:
                yield toolkit.ascii(props['name'])
            else:
                user = this.volume['user'][guid]
                if user.exists:
                    yield toolkit.ascii(user['name'])
            yield guid


class _Stemmer(object):

    _pool = {}

    @staticmethod
    def get(lang):
        lang = lang.split('-')[0]
        stemmer = _Stemmer._pool.get(lang)
        if stemmer is None:
            stemmer = _Stemmer._pool[lang] = _Stemmer(lang)
        return stemmer

    def __init__(self, lang):
        try:
            self._stemmer = xapian.Stem(lang)
            self._cache = pylru.lrucache(_HL_STEM_CACHE)
        except Exception:
            _logger.warn('Failed to create %r stemmer, ignore stemming', lang)
            self._stemmer = None

    def __call__(self, word):
        if self._stemmer is None:
            return word
        if word in self._cache:
            return self._cache[word]
        else:
            stem = self._cache[word] = self._stemmer(word)
            return stem


def _is_term(word):
    for term in this.query:
        if word.startswith(term):
            return True


def _highlight(text):
    snippet_size = int(this.request['highlight'] or 0)
    stemmer = _Stemmer.get(this.request.accept_language[0])
    words = _HL_WORDS_RE.findall(text)
    snippet_start = 0
    snippet_stop = 0
    snippet_len = 0
    found = False

    for index, word in enumerate(words):
        normalized_word = toolkit.ascii(word.lower())
        if _is_term(normalized_word) or _is_term(stemmer(normalized_word)):
            words[index] = _HL_PREFIX + word + _HL_POSTFIX
            found = True
        if not snippet_size:
            continue
        if word.istitle():
            if found:
                snippet_stop = index
            else:
                snippet_start = index
                snippet_len = len(word)
        elif '.' in word or '\n' in word:
            if found:
                snippet_stop = index + 1
            else:
                snippet_start = index + 1
                snippet_len = 0
        else:
            snippet_len += len(word)
            if found and snippet_len >= snippet_size:
                if not snippet_stop:
                    snippet_stop = index + 1
                break

    if snippet_stop:
        return ''.join(words[snippet_start:snippet_stop + 1]).strip()
    return ''.join(words)
