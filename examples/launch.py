#!/usr/bin/env python

import sugar_network as client


client.api_url.value = 'http://localhost:8000'

print '-- See %s for logs' % client.profile_path('logs')

print '-- Launch by GUID (here bundle_id, GUID created by aslo_sync.py)'
client.launch('com.ywwg.CartoonBuilderActivity')
