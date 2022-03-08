from typing import List, Optional, Union

from redis.sansio.commands.base import CommandsProtocol
from redis.sansio.commands.normalize import iterkeysargs
from redis.sansio.exceptions import DataError


class ListCommands(CommandsProtocol):
    """
    Redis commands for List data type.
    see: https://redis.io/topics/data-types#lists
    """

    def blpop(self, keys: List, timeout: Optional[int] = 0) -> List:
        """
        LPOP a value off of the first non-empty list
        named in the ``keys`` list.

        If none of the lists in ``keys`` has a value to LPOP, then block
        for ``timeout`` seconds, or until a value gets pushed on to one
        of the lists.

        If timeout is 0, then block indefinitely.

        For more information check https://redis.io/commands/blpop
        """
        if timeout is None:
            timeout = 0
        keys = iterkeysargs(keys, [timeout])
        return self.execute_command("BLPOP", *keys)

    def brpop(self, keys: List, timeout: Optional[int] = 0) -> List:
        """
        RPOP a value off of the first non-empty list
        named in the ``keys`` list.

        If none of the lists in ``keys`` has a value to RPOP, then block
        for ``timeout`` seconds, or until a value gets pushed on to one
        of the lists.

        If timeout is 0, then block indefinitely.

        For more information check https://redis.io/commands/brpop
        """
        if timeout is None:
            timeout = 0
        keys = iterkeysargs(keys, [timeout])
        return self.execute_command("BRPOP", *keys)

    def brpoplpush(
        self, src: str, dst: str, timeout: Optional[int] = 0
    ) -> Optional[str]:
        """
        Pop a value off the tail of ``src``, push it on the head of ``dst``
        and then return it.

        This command blocks until a value is in ``src`` or until ``timeout``
        seconds elapse, whichever is first. A ``timeout`` value of 0 blocks
        forever.

        For more information check https://redis.io/commands/brpoplpush
        """
        if timeout is None:
            timeout = 0
        return self.execute_command("BRPOPLPUSH", src, dst, timeout)

    def blmpop(
        self,
        timeout: float,
        numkeys: int,
        *args: List[str],
        direction: str,
        count: Optional[int] = 1,
    ) -> Optional[list]:
        """
        Pop ``count`` values (default 1) from first non-empty in the list
        of provided key names.

        When all lists are empty this command blocks the operator until another
        client pushes to it or until the timeout, timeout of 0 blocks indefinitely

        For more information check https://redis.io/commands/blmpop
        """
        args = [timeout, numkeys, *args, direction, "COUNT", count]

        return self.execute_command("BLMPOP", *args)

    def lmpop(
        self,
        num_keys: int,
        *args: List[str],
        direction: str = None,
        count: Optional[int] = 1,
    ) -> List:
        """
        Pop ``count`` values (default 1) first non-empty list key from the list
        of args provided key names.

        For more information check https://redis.io/commands/lmpop
        """
        args = [num_keys] + list(args) + [direction]
        if count != 1:
            args.extend(["COUNT", count])

        return self.execute_command("LMPOP", *args)

    def lindex(self, name: str, index: int) -> Optional[str]:
        """
        Return the item from list ``name`` at position ``index``

        Negative indexes are supported and will return an item at the
        end of the list

        For more information check https://redis.io/commands/lindex
        """
        return self.execute_command("LINDEX", name, index)

    def linsert(self, name: str, where: str, refvalue: str, value: str) -> int:
        """
        Insert ``value`` in list ``name`` either immediately before or after
        [``where``] ``refvalue``

        Returns the new length of the list on success or -1 if ``refvalue``
        is not in the list.

        For more information check https://redis.io/commands/linsert
        """
        return self.execute_command("LINSERT", name, where, refvalue, value)

    def llen(self, name: str) -> int:
        """
        Return the length of the list ``name``

        For more information check https://redis.io/commands/llen
        """
        return self.execute_command("LLEN", name)

    def lpop(self, name: str, count: Optional[int] = None) -> Union[str, List, None]:
        """
        Removes and returns the first elements of the list ``name``.

        By default, the command pops a single element from the beginning of
        the list. When provided with the optional ``count`` argument, the reply
        will consist of up to count elements, depending on the list's length.

        For more information check https://redis.io/commands/lpop
        """
        if count is not None:
            return self.execute_command("LPOP", name, count)
        else:
            return self.execute_command("LPOP", name)

    def lpush(self, name: str, *values: List) -> int:
        """
        Push ``values`` onto the head of the list ``name``

        For more information check https://redis.io/commands/lpush
        """
        return self.execute_command("LPUSH", name, *values)

    def lpushx(self, name: str, *values: List) -> int:
        """
        Push ``value`` onto the head of the list ``name`` if ``name`` exists

        For more information check https://redis.io/commands/lpushx
        """
        return self.execute_command("LPUSHX", name, *values)

    def lrange(self, name: str, start: int, end: int) -> List:
        """
        Return a slice of the list ``name`` between
        position ``start`` and ``end``

        ``start`` and ``end`` can be negative numbers just like
        Python slicing notation

        For more information check https://redis.io/commands/lrange
        """
        return self.execute_command("LRANGE", name, start, end)

    def lrem(self, name: str, count: int, value: str) -> int:
        """
        Remove the first ``count`` occurrences of elements equal to ``value``
        from the list stored at ``name``.

        The count argument influences the operation in the following ways:
            count > 0: Remove elements equal to value moving from head to tail.
            count < 0: Remove elements equal to value moving from tail to head.
            count = 0: Remove all elements equal to value.

            For more information check https://redis.io/commands/lrem
        """
        return self.execute_command("LREM", name, count, value)

    def lset(self, name: str, index: int, value: str) -> str:
        """
        Set element at ``index`` of list ``name`` to ``value``

        For more information check https://redis.io/commands/lset
        """
        return self.execute_command("LSET", name, index, value)

    def ltrim(self, name: str, start: int, end: int) -> str:
        """
        Trim the list ``name``, removing all values not within the slice
        between ``start`` and ``end``

        ``start`` and ``end`` can be negative numbers just like
        Python slicing notation

        For more information check https://redis.io/commands/ltrim
        """
        return self.execute_command("LTRIM", name, start, end)

    def rpop(self, name: str, count: Optional[int] = None) -> Union[str, List, None]:
        """
        Removes and returns the last elements of the list ``name``.

        By default, the command pops a single element from the end of the list.
        When provided with the optional ``count`` argument, the reply will
        consist of up to count elements, depending on the list's length.

        For more information check https://redis.io/commands/rpop
        """
        if count is not None:
            return self.execute_command("RPOP", name, count)
        else:
            return self.execute_command("RPOP", name)

    def rpoplpush(self, src: str, dst: str) -> str:
        """
        RPOP a value off of the ``src`` list and atomically LPUSH it
        on to the ``dst`` list.  Returns the value.

        For more information check https://redis.io/commands/rpoplpush
        """
        return self.execute_command("RPOPLPUSH", src, dst)

    def rpush(self, name: str, *values: List) -> int:
        """
        Push ``values`` onto the tail of the list ``name``

        For more information check https://redis.io/commands/rpush
        """
        return self.execute_command("RPUSH", name, *values)

    def rpushx(self, name: str, value: str) -> int:
        """
        Push ``value`` onto the tail of the list ``name`` if ``name`` exists

        For more information check https://redis.io/commands/rpushx
        """
        return self.execute_command("RPUSHX", name, value)

    def lpos(
        self,
        name: str,
        value: str,
        rank: Optional[int] = None,
        count: Optional[int] = None,
        maxlen: Optional[int] = None,
    ) -> Union[str, List, None]:
        """
        Get position of ``value`` within the list ``name``

         If specified, ``rank`` indicates the "rank" of the first element to
         return in case there are multiple copies of ``value`` in the list.
         By default, LPOS returns the position of the first occurrence of
         ``value`` in the list. When ``rank`` 2, LPOS returns the position of
         the second ``value`` in the list. If ``rank`` is negative, LPOS
         searches the list in reverse. For example, -1 would return the
         position of the last occurrence of ``value`` and -2 would return the
         position of the next to last occurrence of ``value``.

         If specified, ``count`` indicates that LPOS should return a list of
         up to ``count`` positions. A ``count`` of 2 would return a list of
         up to 2 positions. A ``count`` of 0 returns a list of all positions
         matching ``value``. When ``count`` is specified and but ``value``
         does not exist in the list, an empty list is returned.

         If specified, ``maxlen`` indicates the maximum number of list
         elements to scan. A ``maxlen`` of 1000 will only return the
         position(s) of items within the first 1000 entries in the list.
         A ``maxlen`` of 0 (the default) will scan the entire list.

         For more information check https://redis.io/commands/lpos
        """
        pieces = [name, value]
        if rank is not None:
            pieces.extend(["RANK", rank])

        if count is not None:
            pieces.extend(["COUNT", count])

        if maxlen is not None:
            pieces.extend(["MAXLEN", maxlen])

        return self.execute_command("LPOS", *pieces)

    def sort(
        self,
        name: str,
        start: Optional[int] = None,
        num: Optional[int] = None,
        by: Optional[str] = None,
        get: Optional[List[str]] = None,
        desc: bool = False,
        alpha: bool = False,
        store: Optional[str] = None,
        groups: Optional[bool] = False,
    ) -> Union[List, int]:
        """
        Sort and return the list, set or sorted set at ``name``.

        ``start`` and ``num`` allow for paging through the sorted data

        ``by`` allows using an external key to weight and sort the items.
            Use an "*" to indicate where in the key the item value is located

        ``get`` allows for returning items from external keys rather than the
            sorted data itself.  Use an "*" to indicate where in the key
            the item value is located

        ``desc`` allows for reversing the sort

        ``alpha`` allows for sorting lexicographically rather than numerically

        ``store`` allows for storing the result of the sort into
            the key ``store``

        ``groups`` if set to True and if ``get`` contains at least two
            elements, sort will return a list of tuples, each containing the
            values fetched from the arguments to ``get``.

        For more information check https://redis.io/commands/sort
        """
        if (start is not None and num is None) or (num is not None and start is None):
            raise DataError("``start`` and ``num`` must both be specified")

        pieces = [name]
        if by is not None:
            pieces.extend([b"BY", by])
        if start is not None and num is not None:
            pieces.extend([b"LIMIT", start, num])
        if get is not None:
            # If get is a string assume we want to get a single value.
            # Otherwise assume it's an interable and we want to get multiple
            # values. We can't just iterate blindly because strings are
            # iterable.
            if isinstance(get, (bytes, str)):
                pieces.extend([b"GET", get])
            else:
                for g in get:
                    pieces.extend([b"GET", g])
        if desc:
            pieces.append(b"DESC")
        if alpha:
            pieces.append(b"ALPHA")
        if store is not None:
            pieces.extend([b"STORE", store])
        if groups:
            if not get or isinstance(get, (bytes, str)) or len(get) < 2:
                raise DataError(
                    'when using "groups" the "get" argument '
                    "must be specified and contain at least "
                    "two keys"
                )

        options = {"groups": len(get) if groups else None}
        return self.execute_command("SORT", *pieces, **options)
