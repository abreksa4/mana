import asyncio
import sys
from argparse import ArgumentParser

from mada import Database
from maga import Maga
from mala import get_metadata

try:
    import uvloop

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass

import asyncio_redis

from asyncio_redis.encoders import BytesEncoder

ih2bytes = lambda x: bytes(bytearray.fromhex(x))


class Crawler(Maga):
    def __init__(self, database, loop=None, active_tcp_limit=1000, max_per_session=2500):
        super().__init__(loop)
        self.database = database
        self.seen_ct = 0
        self.active = 0
        self.threshold = active_tcp_limit
        self.max = max_per_session
        self.connection = None
        asyncio.get_event_loop().run_until_complete(self.connect_redis())

    async def connect_redis(self):
        self.connection = await asyncio_redis.Connection.create(
            'localhost', 6379, encoder=BytesEncoder())

    async def handler(self, infohash, addr, peer_addr=None):
        exists = await self.connection.exists(ih2bytes(infohash))
        if self.running and (self.active < self.threshold) and (
            self.seen_ct < self.max) and not exists:
            await self.connection.set(
                ih2bytes(infohash), b'', pexpire=int(6e8))  # expires in 1wk
            self.seen_ct += 1
            if peer_addr is None:
                peer_addr = addr
            self.active += 1
            metainfo = await get_metadata(
                infohash, peer_addr[0], peer_addr[1], loop=self.loop
            )
            self.active -= 1
            await self.log(metainfo, infohash)
        if self.seen_ct >= self.max:
            self.stop()

    async def log(self, metainfo, infohash):
        if metainfo not in [False, None]:
            try:
                self.database.add_metadata(infohash, metainfo)
                out = infohash + ' ' + metainfo[b'name'].decode(
                    'utf-8').replace('\n', '\\n') + '\n'
                sys.stdout.write(out)
                sys.stdout.flush()
            except UnicodeDecodeError:
                print(infohash + '    (not rendered)')

    async def handle_announce_peer(self, infohash, addr, peer_addr):
        await self.handler(infohash, addr, peer_addr)


if __name__ == '__main__':
    argparse = ArgumentParser()

    # crawler
    argparse.add_argument('-l', '--listen', type=str, default='0.0.0.0:6881',
                          help='Bind to this address (default: 0.0.0.0:6881)')

    # database
    argparse.add_argument('-e', '--elasticsearch-hosts', type=str, required=True,
                          help='Elasticsearch host addresses (comma-separated)')

    args = argparse.parse_args()

    try:
        host, port = args.listen.split(':')
        port = int(port)
    except ValueError:
        try:
            host = '0.0.0.0'
            port = int(args.listen)
        except ValueError:
            host = args.listen
            port = 6881

    db = Database(args.elasticsearch_hosts.split(','))

    crawler = Crawler(db)
    crawler.run(host, port, False)

    sys.exit(0)

    """

    if len(sys.argv) > 2 and sys.argv[2] == "--forever":
        while True:
            new_crawler = Crawler()
            new_crawler.seen = crawler.seen
            del crawler
            crawler = new_crawler
            time.sleep(5)
            new_crawler.run(args.host, args.port, False)
            print('>>> crawler round done', crawler.loop, new_crawler.loop)

    """