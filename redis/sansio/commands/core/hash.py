import warnings
from typing import List, Optional

from redis.sansio.commands.base import CommandsProtocol
from redis.sansio.commands.normalize import iterkeysargs
from redis.sansio.exceptions import DataError


class HashCommands(CommandsProtocol):
    """
    Redis commands for Hash data type.
    see: https://redis.io/topics/data-types-intro#redis-hashes
    """

    def hdel(self, name: str, *keys: List) -> int:
        """
        Delete ``keys`` from hash ``name``

        For more information check https://redis.io/commands/hdel
        """
        return self.execute_command("HDEL", name, *keys)

    def hexists(self, name: str, key: str) -> bool:
        """
        Returns a boolean indicating if ``key`` exists within hash ``name``

        For more information check https://redis.io/commands/hexists
        """
        return self.execute_command("HEXISTS", name, key)

    def hget(self, name: str, key: str) -> Optional[str]:
        """
        Return the value of ``key`` within the hash ``name``

        For more information check https://redis.io/commands/hget
        """
        return self.execute_command("HGET", name, key)

    def hgetall(self, name: str) -> dict:
        """
        Return a Python dict of the hash's name/value pairs

        For more information check https://redis.io/commands/hgetall
        """
        return self.execute_command("HGETALL", name)

    def hincrby(self, name: str, key: str, amount: int = 1) -> int:
        """
        Increment the value of ``key`` in hash ``name`` by ``amount``

        For more information check https://redis.io/commands/hincrby
        """
        return self.execute_command("HINCRBY", name, key, amount)

    def hincrbyfloat(self, name: str, key: str, amount: float = 1.0) -> float:
        """
        Increment the value of ``key`` in hash ``name`` by floating ``amount``

        For more information check https://redis.io/commands/hincrbyfloat
        """
        return self.execute_command("HINCRBYFLOAT", name, key, amount)

    def hkeys(self, name: str) -> List:
        """
        Return the list of keys within hash ``name``

        For more information check https://redis.io/commands/hkeys
        """
        return self.execute_command("HKEYS", name)

    def hlen(self, name: str) -> int:
        """
        Return the number of elements in hash ``name``

        For more information check https://redis.io/commands/hlen
        """
        return self.execute_command("HLEN", name)

    def hset(
        self,
        name: str,
        key: Optional[str] = None,
        value: Optional[str] = None,
        mapping: Optional[dict] = None,
    ) -> int:
        """
        Set ``key`` to ``value`` within hash ``name``,
        ``mapping`` accepts a dict of key/value pairs that will be
        added to hash ``name``.
        Returns the number of fields that were added.

        For more information check https://redis.io/commands/hset
        """
        if key is None and not mapping:
            raise DataError("'hset' with no key value pairs")
        items = []
        if key is not None:
            items.extend((key, value))
        if mapping:
            for pair in mapping.items():
                items.extend(pair)

        return self.execute_command("HSET", name, *items)

    def hsetnx(self, name: str, key: str, value: str) -> bool:
        """
        Set ``key`` to ``value`` within hash ``name`` if ``key`` does not
        exist.  Returns 1 if HSETNX created a field, otherwise 0.

        For more information check https://redis.io/commands/hsetnx
        """
        return self.execute_command("HSETNX", name, key, value)

    def hmset(self, name: str, mapping: dict) -> str:
        """
        Set key to value within hash ``name`` for each corresponding
        key and value from the ``mapping`` dict.

        For more information check https://redis.io/commands/hmset
        """
        warnings.warn(
            f"{self.__class__.__name__}.hmset() is deprecated. "
            f"Use {self.__class__.__name__}.hset() instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        if not mapping:
            raise DataError("'hmset' with 'mapping' of length 0")
        items = []
        for pair in mapping.items():
            items.extend(pair)
        return self.execute_command("HMSET", name, *items)

    def hmget(self, name: str, keys: List, *args: List) -> List:
        """
        Returns a list of values ordered identically to ``keys``

        For more information check https://redis.io/commands/hmget
        """
        args = iterkeysargs(keys, args)
        return self.execute_command("HMGET", name, *args)

    def hvals(self, name: str) -> List:
        """
        Return the list of values within hash ``name``

        For more information check https://redis.io/commands/hvals
        """
        return self.execute_command("HVALS", name)

    def hstrlen(self, name: str, key: str) -> int:
        """
        Return the number of bytes stored in the value of ``key``
        within hash ``name``

        For more information check https://redis.io/commands/hstrlen
        """
        return self.execute_command("HSTRLEN", name, key)
