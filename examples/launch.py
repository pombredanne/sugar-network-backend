#!/usr/bin/env python

import logging

import sugar_network as client
from sugar_network import sweets


logging.getLogger().setLevel(logging.DEBUG)
print '-- See %s for logs' % client.profile_path('logs')

client.api_url.value = 'http://localhost:8000'

context = client.Context(implement='com.ywwg.CartoonBuilderActivity')
sweets.launch(context['guid'])
