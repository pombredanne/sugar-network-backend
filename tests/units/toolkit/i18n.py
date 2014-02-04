#!/usr/bin/env python
# sugar-lint: disable

import gettext

from __init__ import tests

from sugar_network.toolkit import i18n


class I18nTest(tests.Test):

    def test_decode(self):
        # Fallback to default lang
        i18n._default_langs = ['default']
        self.assertEqual('foo', i18n.decode({'lang': 'foo', 'default': 'bar'}, 'lang'))
        self.assertEqual('bar', i18n.decode({'lang': 'foo', 'default': 'bar'}, 'fake'))

        # Exact accept_language
        self.assertEqual('', i18n.decode(None, 'lang'))
        self.assertEqual('foo', i18n.decode('foo', 'lang'))
        self.assertEqual('foo', i18n.decode({'lang': 'foo', 'fake': 'bar', 'default': 'default'}, 'lang'))
        self.assertEqual('foo', i18n.decode({'lang': 'foo', 'fake': 'bar', 'default': 'default'}, ['lang', 'fake']))
        self.assertEqual('bar', i18n.decode({'lang': 'foo', 'fake': 'bar', 'default': 'default'}, ['fake', 'lang']))

        # Last resort
        self.assertEqual('foo', i18n.decode({'1': 'foo', '2': 'bar'}, 'fake'))

        # Primed accept_language
        self.assertEqual('foo', i18n.decode({'1': 'foo', '2': 'bar', 'default': 'default'}, '1-a'))

        # Primed i18n value
        self.assertEqual('bar', i18n.decode({'1-a': 'foo', '1': 'bar', 'default': 'default'}, '1-b'))
        self.assertEqual('foo', i18n.decode({'1-a': 'foo', '2': 'bar', 'default': 'default'}, '1-b'))

    def test_decode_EnAsTheLastResort(self):
        i18n._default_langs = ['en-us']
        self.assertEqual('right', i18n.decode({'a': 'wrong', 'en': 'right'}, 'probe'))
        self.assertEqual('exact', i18n.decode({'a': 'wrong', 'en': 'right', 'probe': 'exact'}, 'probe'))

    def test_encode(self):
        self.assertEqual({
            'en': 'Delete Log File',
            'es': 'Borrar el archivo de registro',
            'fr': 'Supprimer le fichier log',
            }, i18n.encode('Delete Log File'))

        self.assertEqual({
            'en': "Error: Can't open file 'probe'\n",
            'es': "Error: No se puede abrir el archivo 'probe'\n",
            'fr': "Erreur : Ouverture du fichier 'probe' impossible\n",
            }, i18n.encode("Error: Can't open file '%s'\n", 'probe'))

        self.assertEqual({
            'en': "Error: Can't open file '1'\n",
            'es': "Error: No se puede abrir el archivo '2'\n",
            'fr': "Erreur : Ouverture du fichier '3' impossible\n",
            }, i18n.encode("Error: Can't open file '%s'\n", {'en': 1, 'es': 2, 'fr': 3}))

        self.assertEqual({
            'en': '1 when deleting 5',
            'es': '2 borrando 6',
            'fr': '3 lors de la suppression de 7',
            }, i18n.encode('%(error)s when deleting %(file)s', error={'en': 1, 'es': 2, 'fr': 3}, file={'en': 5, 'es': 6, 'fr': 7}))


if __name__ == '__main__':
    tests.main()
