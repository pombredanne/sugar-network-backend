# Copyright (C) 2011-2013 Aleksey Lim
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

# pylint: disable-msg=C0301
# pep8: ignore=E501

"""Data storing backend for the Sugar Network.

This module defines how data will be stored on Sugar Network nodes.
The implementation is based on `Xapian`_ index with keeping data in
the files system (decision might be transparently changed in the future).
The design is tailored by the original requirements for the Sugar Network,
such as:

* Powerful full-text search to let users effectively browse
  Sugar Network content;

* Implementation should run on restricted systems like OLPC's `XO`_ laptops;

* Sugar Network should run in unmaintained manner as long as possible in
  environments, like rural schools, where there is no skilled IT administrative
  personnel to support servers (remote maintaining might be impossible if
  there is no Internet connectivity).

To start using :mod:`sugar_network.db`, you need to declare
:ref:`Data Classes <db-scheme>`, optionally implement extra
:ref:`Commands <db-commands>`, collect all :ref:`Data Classes <db-scheme>`
in a :ref:`Volume <db-volume>`, and, start calling :ref:`Commands <db-usage>`
to manipulate the data.

.. _db-scheme:

Data scheme
-----------

The :mod:`sugar_network..db` design is similar to `Active Record`_ pattern, i.e.,
you need to declare data classes with properties that relate to data "fields".
Classes should inherit the :class:`.Document`.

To declare data fields, use the following decorators:

* :data:`sugar_network.db.indexed_property <IndexedProperty>`,
  stored and indexed properties;
* :data:`sugar_network.db.stored_property <StoredProperty>`,
  only stored properties;
* :data:`sugar_network.db.blob_property <BlobProperty>`,
  only stored large binary properties, e.g., files.

For example::

    from sugar_network import db

    class MyDocyment(db.Document):

        @db.indexed_property(slot=1)
        def indexed_property(self, value):
            return value

        @db.stored_property()
        def stored_property(self, value):
            return value

        @db.blob_property()
        def blob_property(self, value):
            return value

The ``value`` argument might be changed in decorated functions to proceed
any post-processing functionality. The
:data:`sugar_network.db.blob_property <BlobProperty>` decorator accepts
``value`` in form of :class:`sugar_network.db.PropertyMetadata` object.

Besides, it is also possible to hook post-setting phase for data properties::

    from sugar_network import db

    class MyDocyment(db.Document):

        @db.indexed_property(slot=1)
        def prop(self, value):
            return value

        @prop.setter
        def prop(self, value):
            return value

Like for getters, setters might change property value, passed via ``value``
argument, before storing them in the database.

.. _db-commands:

Manipulating Data
-----------------

In contrast to `Active Record`_ pattern, :class:`sugar_network.db.Document`
values are being manipulated from special classes, command processors,
inherited from the :class:`sugar_network.db.CommandsProcessor`. There is
a special class, :class:`sugar_network.db.VolumeCommands`, which provides
standard commands to work with data, e.g., create new items, delete existing,
make searches, etc.

It is all time possible to add custom commands with the following decorators:

* :data:`sugar_network.db.volume_command`,
  commands to process the entire volume;
* :data:`sugar_network.db.directory_command`,
  commands to work with particular :ref:`Data Class <db-scheme>`;
* :data:`sugar_network.db.document_command`,
  get access to particular object of a :ref:`Data Class <db-scheme>`;
* :data:`sugar_network.db.property_command`,
  get access to a certain property of a particular object of
  a :ref:`Data Class <db-scheme>`.

Decorated functions accept arguments associated with request parameters passed
by a command caller. Missed arguments don't break commands, you just
don't have access to the corresponding parameters (but see for
:class:`request <Request>` argument). Also, there is a couple of optional
arguments:

* :class:`request <Request>` to get access to the original request object;
* :class:`response <Response>` to manipulate reply object.

Another difference between different level decorators is that they
are expecting different request parameters:

* :data:`volume_command`,
  no expectations;
* :data:`directory_command`,
  assumes passing ``document`` parameter with :ref:`Data Class <db-scheme>`
  name;
* :data:`document_command`,
  assumes passing ``document`` and ``guid`` parameters to identify
  a :ref:`Data Class <db-scheme>` object;
* :data:`property_command`,
  assumes passing ``document``, ``guid`` and ``prop`` parameters to identify
  the certain property of a :ref:`Data Class <db-scheme>` object.

What decorated functions return is the result of corresponding command.

.. _db-volume:

Data Volumes
------------

The :class:`sugar_network.db.Volume` is intended to keep the actual data.
It is a dictionary of :class:`sugar_network.db.Directory` items that represent
corresponding :ref:`Data Class <db-scheme>` objects.

Initiate :class:`Volume` object with files system path to keep database files
and a list of :ref:`Data Classes <db-scheme>`::

    from sugar_network import db

    volume = db.Volume('path/to/store/db', [MyDocyment])

To start populating data, construct
:class:`sugar_network.db.VolumeCommands` object::

    cp = db.VolumeCommands(volume)

After processing all data manipulations, close the volume::

    volume.close()

.. _db-usage:

Usage
-----

It is possible to call :class:`VolumeCommands`'s functions directly, but,
the regular way is using commands interface::

    from sugar_network import db

    volume = db.Volume('path/to/store/db', [MyDocyment])
    cp = db.VolumeCommands(volume)

    request = db.Request()
    response = db.Response()
    cp.call(request, response)

The :class:`sugar_network.db.Request` object specifies all request parameters
that should be passed to the specified command. A proper parameters set depends
on particular command but includes the following common parameters:

* ```method```, if omitted, ``GET`` will be used;
* ``cmd``, optional extra command.

The combination of these parameters defines the unique command that
should be called.

The example code uses all mentioned above features::

    from sugar_network import db

    class MyDocyment(db.Document):

        @db.indexed_property(slot=1)
        def prop1(self, value):
            return value

        @db.stored_property()
        def prop2(self, value):
            return value

        @db.blob_property()
        def blob_property(self, value):
            return value

    class MyCommands(db.VolumeCommands):

        @db.volume_command(method='GET')
        def ping(self, answer):
            return answer

        @db.directory_command(method='GET', cmd='count')
        def count(self, document):
            items, total = self.volume[document].find()
            return total

        @db.document_command(method='POST', cmd='clone')
        def clone(self, document, guid):
            item = self.volume[document].get(guid)
            return self.volume[document].create(item.properties(['prop1', 'prop2']))

        @db.property_command(method='PUT', cmd='mutate')
        def mutate(self, document, guid, prop, request):
            self.volume[document].update(guid, {prop: request.content})

    volume = db.Volume('db', [MyDocyment])
    cp = MyCommands(volume)

    # Create new document
    request = db.Request(method='POST', document='mydocyment')
    request.content = {'prop1': '1', 'prop2': '1'}
    guid = cp.call(request)

    # List newly created document
    request = db.Request(method='GET', document='mydocyment', reply=['prop1', 'prop2'])
    cp.call(request)
    # Output: {'total': 1L, 'result': [{'prop1': '1', 'prop2': '1'}]}

    # Update created document
    request = db.Request(method='PUT', document='mydocyment', guid=guid)
    request.content = {'prop1': '2', 'prop2': '2'}
    cp.call(request)

    # List updated document
    request = db.Request(method='GET', document='mydocyment', reply=['prop1', 'prop2'])
    cp.call(request)
    # Output: {'total': 1L, 'result': [{'prop1': '2', 'prop2': '2'}]}

    # Call custom volume command ``ping``
    request = db.Request(method='GET', answer='pong')
    cp.call(request)
    # Output: 'pong'

    # Call custom directory command ``count``
    request = db.Request(method='GET', cmd='count', document='mydocyment')
    cp.call(request)
    # Output: 1

    # Call custom document command ``clone``
    request = db.Request(method='POST', cmd='clone', document='mydocyment', guid=guid)
    guid2 = cp.call(request)

    # Call custom property command ``mutate`` for the 2nd document
    request = db.Request(method='PUT', cmd='mutate', document='mydocyment', guid=guid2, prop='prop2')
    request.content = '3'
    cp.call(request)

    # List two documents
    request = db.Request(method='GET', document='mydocyment', reply=['prop1', 'prop2'])
    cp.call(request)
    # Output: {'total': 2L, 'result': [{'prop1': '2', 'prop2': '3'}, {'prop1': '2', 'prop2': '2'}]}

    # Delete the first one
    request = db.Request(method='DELETE', document='mydocyment', guid=guid)
    cp.call(request)

    # List the 2nd document
    request = db.Request(method='GET', document='mydocyment', reply=['prop1', 'prop2'])
    cp.call(request)
    # Output: {'total': 1L, 'result': [{'prop1': '2', 'prop2': '3'}]}

    volume.close()

Declarations
------------

Properties
^^^^^^^^^^
.. autoclass:: sugar_network.db.Property
    :members:

.. autoclass:: sugar_network.db.StoredProperty
    :members:

.. autoclass:: sugar_network.db.IndexedProperty
    :members:

.. autoclass:: sugar_network.db.BlobProperty
    :members:

.. autoclass:: sugar_network.db.PropertyMetadata
    :members:

Document
^^^^^^^^
.. autoclass:: sugar_network.db.Document
    :members:

Commands
^^^^^^^^
.. autoclass:: sugar_network.db.CommandsProcessor
    :members:

.. autoclass:: sugar_network.db.VolumeCommands
    :members:

.. autoclass:: sugar_network.db.Request
    :members:

.. autoclass:: sugar_network.db.Response
    :members:

Volume
^^^^^^
.. autoclass:: sugar_network.db.Directory
    :members:

.. autoclass:: sugar_network.db.Volume
    :members:

.. _Xapian: http://xapian.org/
.. _XO: http://en.wikipedia.org/wiki/OLPC_XO-1
.. _Active Record: http://en.wikipedia.org/wiki/Active_record

"""

from sugar_network.db.env import \
        ACCESS_CREATE, ACCESS_WRITE, ACCESS_READ, ACCESS_DELETE, \
        ACCESS_AUTHOR, ACCESS_AUTH, ACCESS_PUBLIC, ACCESS_LEVELS, \
        ACCESS_SYSTEM, ACCESS_LOCAL, ACCESS_REMOTE, MAX_LIMIT, \
        index_flush_timeout, index_flush_threshold, index_write_queue, \
        BadRequest, NotFound, Forbidden, CommandNotFound, \
        uuid, default_lang, gettext

from sugar_network.db.metadata import \
        indexed_property, stored_property, blob_property, \
        Property, StoredProperty, BlobProperty, IndexedProperty, \
        PropertyMetadata

from sugar_network.db.commands import \
        volume_command, volume_command_pre, volume_command_post, \
        directory_command, directory_command_pre, directory_command_post, \
        document_command, document_command_pre, document_command_post, \
        property_command, property_command_pre, property_command_post, \
        to_int, to_list, Request, Response, CommandsProcessor

from sugar_network.db.document import Document

from sugar_network.db.directory import Directory

from sugar_network.db.volume import Volume, VolumeCommands
