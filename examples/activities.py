#!/usr/bin/env python

import sugar_network


def main():
    client = sugar_network.Client('/')
    client.checkin('com.ywwg.CartoonBuilderActivity')


if __name__ == '__main__':
    import os
    from local_document import env

    os.system('sugar-network-service -DD start ' \
              '--local-root=tmp ' \
              '--activities-root=tmp/Activities ' \
              '--api-url=http://localhost:8000')
    try:
        env.local_root.value = 'tmp'
        env.api_url.value = 'http://localhost:8000'
        env.activities_root.value = 'tmp/Activities'
        main()
    finally:
        os.system('sugar-network-service --local-root=tmp stop')
