#!/usr/bin/env python

import sugar_network


def main():
    client = sugar_network.Client('/')
    client.checkin('com.ywwg.CartoonBuilderActivity')


if __name__ == '__main__':
    import os
    from sugar_network import local

    os.system('sugar-network-service -DD start ' \
              '--local-root=tmp ' \
              '--activities-root=tmp/Activities ' \
              '--api-url=http://localhost:8000')
    try:
        local.local_root.value = 'tmp'
        local.api_url.value = 'http://localhost:8000'
        local.activities_root.value = 'tmp/Activities'
        main()
    finally:
        os.system('sugar-network-service --local-root=tmp stop')
