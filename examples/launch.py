#!/usr/bin/env python

import sugar_network as client


client.api_url.value = 'http://localhost:8000'

print '-- See %s for logs' % client.profile_path('logs')

print '-- Launch by one of "implement" values, e.g., here bundle_id'
client.launch('com.ywwg.CartoonBuilderActivity')

print '-- Launch directly by context GUID and avoid "implement" resolving'
context = client.Context(implement='com.ywwg.CartoonBuilderActivity')
client.launch(context['guid'])
