from typing import List, Optional, Union

from sansredis.sansio.commands.base import CommandsProtocol
from sansredis.sansio.commands.normalize import iterkeysargs


class SetCommands(CommandsProtocol):
    """
    Redis commands for Set data type.
    see: https://redis.io/topics/data-types#sets
    """

    def sadd(self, name: str, *values: List):
        """
        Add ``value(s)`` to set ``name``

        For more information check https://redis.io/commands/sadd
        """
        return self.execute_command("SADD", name, *values)

    def scard(self, name: str):
        """
        Return the number of elements in set ``name``

        For more information check https://redis.io/commands/scard
        """
        return self.execute_command("SCARD", name)

    def sdiff(self, keys: List, *args: List) -> List:
        """
        Return the difference of sets specified by ``keys``

        For more information check https://redis.io/commands/sdiff
        """
        args = iterkeysargs(keys, args)
        return self.execute_command("SDIFF", *args)

    def sdiffstore(self, dest: str, keys: List, *args: List):
        """
        Store the difference of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.

        For more information check https://redis.io/commands/sdiffstore
        """
        args = iterkeysargs(keys, args)
        return self.execute_command("SDIFFSTORE", dest, *args)

    def sinter(self, keys: List, *args: List) -> List:
        """
        Return the intersection of sets specified by ``keys``

        For more information check https://redis.io/commands/sinter
        """
        args = iterkeysargs(keys, args)
        return self.execute_command("SINTER", *args)

    def sintercard(self, numkeys: int, keys: List[str], limit: int = 0):
        """
        Return the cardinality of the intersect of multiple sets specified by ``keys`.

        When LIMIT provided (defaults to 0 and means unlimited), if the intersection
        cardinality reaches limit partway through the computation, the algorithm will
        exit and yield limit as the cardinality

        For more information check https://redis.io/commands/sintercard
        """
        args = [numkeys, *keys, "LIMIT", limit]
        return self.execute_command("SINTERCARD", *args)

    def sinterstore(self, dest: str, keys: List, *args: List):
        """
        Store the intersection of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.

        For more information check https://redis.io/commands/sinterstore
        """
        args = iterkeysargs(keys, args)
        return self.execute_command("SINTERSTORE", dest, *args)

    def sismember(self, name: str, value: str) -> bool:
        """
        Return a boolean indicating if ``value`` is a member of set ``name``

        For more information check https://redis.io/commands/sismember
        """
        return self.execute_command("SISMEMBER", name, value)

    def smembers(self, name: str) -> List:
        """
        Return all members of the set ``name``

        For more information check https://redis.io/commands/smembers
        """
        return self.execute_command("SMEMBERS", name)

    def smismember(self, name: str, values: List, *args: List) -> List[bool]:
        """
        Return whether each value in ``values`` is a member of the set ``name``
        as a list of ``bool`` in the order of ``values``

        For more information check https://redis.io/commands/smismember
        """
        args = iterkeysargs(values, args)
        return self.execute_command("SMISMEMBER", name, *args)

    def smove(self, src: str, dst: str, value: str) -> bool:
        """
        Move ``value`` from set ``src`` to set ``dst`` atomically

        For more information check https://redis.io/commands/smove
        """
        return self.execute_command("SMOVE", src, dst, value)

    def spop(self, name: str, count: Optional[int] = None) -> Union[str, List, None]:
        """
        Remove and return a random member of set ``name``

        For more information check https://redis.io/commands/spop
        """
        args = (count is not None) and [count] or []
        return self.execute_command("SPOP", name, *args)

    def srandmember(
        self,
        name: str,
        number: Optional[int] = None,
    ) -> Union[str, List, None]:
        """
        If ``number`` is None, returns a random member of set ``name``.

        If ``number`` is supplied, returns a list of ``number`` random
        members of set ``name``. Note this is only available when running
        Redis 2.6+.

        For more information check https://redis.io/commands/srandmember
        """
        args = (number is not None) and [number] or []
        return self.execute_command("SRANDMEMBER", name, *args)

    def srem(self, name: str, *values: List):
        """
        Remove ``values`` from set ``name``

        For more information check https://redis.io/commands/srem
        """
        return self.execute_command("SREM", name, *values)

    def sunion(self, keys: List, *args: List) -> List:
        """
        Return the union of sets specified by ``keys``

        For more information check https://redis.io/commands/sunion
        """
        args = iterkeysargs(keys, args)
        return self.execute_command("SUNION", *args)

    def sunionstore(self, dest: str, keys: List, *args: List):
        """
        Store the union of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.

        For more information check https://redis.io/commands/sunionstore
        """
        args = iterkeysargs(keys, args)
        return self.execute_command("SUNIONSTORE", dest, *args)
