#!/usr/bin/env python

import sugar_network as client


client.api_url.value = 'http://localhost:8000'

print '-- Launch by specifying name context should implement'
print '-- See %s for logs' % client.profile_path('logs')
client.launch('com.ywwg.CartoonBuilderActivity')
