from __future__ import annotations

from redis.client.base import BaseRedis, PipelineMixin
from redis.io import sio


class SyncIORedis(BaseRedis[sio.SyncIORedisConnectionPool]):

    def make_pool(self) -> sio.SyncIORedisConnectionPool:
        return sio.SyncIORedisConnectionPool(protocol=self.protocol)

    def connect(self):
        if self.single_connection_client:
            self.connection.connect()

        elif self.protocol.pool_info.pre_fill:
            self.connection_pool.fill()

    def disconnect(self, *, inuse: bool = True):
        if self.connection:
            self.connection.disconnect()
        if self.auto_close_connection_pool:
            self.connection_pool.disconnect(inuse=inuse)

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect(inuse=True)

    def pipeline(self, *, transaction: bool = True):
        return SyncIOPipeline(
            connection_pool=self.connection_pool, transaction=transaction
        )


class SyncIOPipeline(PipelineMixin, SyncIORedis):

    def _do_watch(self, *names: str | bytes):
        if not self.connection:
            self.connection = self.connection_pool.acquire()
        return self.connection.execute_command("WATCH", *names)

    def _do_release_connection(self, conn: sio.SyncIORedisConnectionPool):
        try:
            conn.execute_command("UNWATCH")
        except ConnectionError:
            conn.disconnect()
        self.connection_pool.release(conn)
