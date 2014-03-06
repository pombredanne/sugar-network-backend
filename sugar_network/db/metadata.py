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

import xapian

from sugar_network import toolkit
from sugar_network.toolkit.router import ACL, File
from sugar_network.toolkit.coroutine import this
from sugar_network.toolkit import i18n, http, enforce


#: Xapian term prefix for GUID value
GUID_PREFIX = 'I'


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

    @property
    def name(self):
        """Resource type name."""
        return self._name

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
        if isinstance(value, File):
            return value.digest
        if isinstance(value, File.Digest):
            return value

        enforce(value is None or isinstance(value, basestring) or
                hasattr(value, 'read'),
                http.BadRequest, 'Inappropriate blob value')

        if not value:
            return ''

        mime_type = None
        if this.request.prop == self.name:
            mime_type = this.request.content_type
        if not mime_type:
            mime_type = self.mime_type
        return this.volume.blobs.post(value, mime_type).digest

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
            return self.default
        return i18n.decode(value, this.request.accept_language)

    def encode(self, value):
        for i in value.values():
            yield toolkit.ascii(i)

    def slotting(self, value):
        # TODO Multilingual sorting
        return i18n.decode(value) or ''


class Aggregated(Composite):

    def __init__(self, subtype=None, acl=ACL.READ | ACL.INSERT | ACL.REMOVE,
            **kwargs):
        enforce(not (acl & (ACL.CREATE | ACL.WRITE)),
                'ACL.CREATE|ACL.WRITE not allowed for aggregated properties')
        Property.__init__(self, acl=acl, default={}, **kwargs)
        self._subtype = subtype or Property()

    def subtypecast(self, value):
        return self._subtype.typecast(value)

    def subteardown(self, value):
        self._subtype.teardown(value)

    def typecast(self, value):
        return dict(value)

    def encode(self, items):
        for agg in items.values():
            if 'value' in agg:
                for j in self._subtype.encode(agg['value']):
                    yield j


class Guid(Property):

    def __init__(self):
        Property.__init__(self, name='guid', slot=0, prefix=GUID_PREFIX,
                acl=ACL.CREATE | ACL.READ)


class Authors(Dict):

    def typecast(self, value):
        if type(value) not in (list, tuple):
            return dict(value)
        result = {}
        for order, author in enumerate(value):
            user = author.pop('guid')
            author['order'] = order
            result[user] = author
        return result

    def reprcast(self, value):
        result = []
        for guid, props in sorted(value.items(),
                cmp=lambda x, y: cmp(x[1]['order'], y[1]['order'])):
            if 'name' in props:
                result.append({
                    'guid': guid,
                    'name': props['name'],
                    'role': props['role'],
                    })
            else:
                result.append({
                    'name': guid,
                    'role': props['role'],
                    })
        return result

    def encode(self, value):
        for guid, props in value.items():
            if 'name' in props:
                yield props['name']
            if not (props['role'] & ACL.INSYSTEM):
                yield guid
