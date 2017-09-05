import logging
import socket
from datetime import datetime

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elasticsearch_dsl import DocType, Date, Long, \
    Nested, Keyword, Text

logging.getLogger('elasticsearch').setLevel(logging.ERROR)

PENDING_INFO_HASHES = 1


class Torrent(DocType):
    name = Text(fields={'keyword': Keyword(ignore_above=4096)})
    size = Long()
    found_at = Date()
    found_by = Keyword()

    files = Nested()
    files.field('path', 'text', fields={'keyword': Keyword(ignore_above=4096)})
    files.field('size', 'long')

    class Meta:
        index = 'torrents'
        doc_type = 'torrent'


class Database:
    def __init__(self, hosts) -> None:
        logging.info("elasticsearch via {}".format(', '.join(hosts)))
        self.elastic = Elasticsearch(hosts=hosts, timeout=15)
        Torrent.init(using=self.elastic)

        self.pending = []

    def add_metadata(self, info_hash: bytes, metadata: bytes) -> bool:
        torrent = Torrent()
        torrent.meta.id = info_hash.lower()
        torrent.found_at = datetime.utcnow()
        torrent.found_by = socket.gethostname()
        torrent.size = 0
        try:
            assert b'/' not in metadata[b'name']
            torrent.name = metadata[b'name'].decode("utf-8")

            if b'files' in metadata:  # Multiple File torrent:
                for file in metadata[b'files']:
                    assert type(file[b'length']) is int
                    # Refuse trailing slash in any of the path items
                    assert not any(b"/" in item for item in file[b'path'])
                    path = '/'.join(i.decode('utf-8') for i in file[b'path'])
                    torrent.files.append(
                        {'size': file[b'length'], 'path': path})
                    torrent.size += file[b'length']
            else:  # Single File torrent:
                assert type(metadata[b'length']) is int
                torrent.files.append(
                    {'size': metadata[b'length'], 'path': torrent.name})
                torrent.size = metadata[b'length']
                # TODO: Make sure this catches ALL, AND ONLY operational errors
            assert (torrent.size != 0)
        except (
                AssertionError, KeyError, AttributeError,
                UnicodeDecodeError, TypeError) as ex:
            logging.exception("Error during metadata decoding")
            return False

        self.pending.append(torrent)
        logging.info("Added: `%s` (%s)", torrent.name, torrent.meta.id)

        if len(self.pending) >= PENDING_INFO_HASHES:
            self.commit()

        return True

    def commit(self):
        logging.info("Committing %d torrents" % len(self.pending))
        bulk(self.elastic, (torrent.to_dict(True) for torrent in self.pending))
        self.pending.clear()

    def close(self) -> None:
        if len(self.pending) > 0:
            self.commit()
