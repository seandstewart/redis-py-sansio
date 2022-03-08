import datetime
import time
import warnings
from typing import Optional, Union

from redis.sansio.commands.base import CommandsProtocol
from redis.sansio.commands.core.bitfield import BitFieldOperation
from redis.sansio.commands.normalize import iterkeysargs
from redis.sansio.exceptions import DataError


class BasicKeyCommands(CommandsProtocol):
    """
    Redis basic key-based commands
    """

    def append(self, key, value):
        """
        Appends the string ``value`` to the value at ``key``. If ``key``
        doesn't already exist, create it with a value of ``value``.
        Returns the new length of the value at ``key``.

        For more information check https://redis.io/commands/append
        """
        return self.execute_command("APPEND", key, value)

    def bitcount(self, key, start=None, end=None):
        """
        Returns the count of set bits in the value of ``key``.  Optional
        ``start`` and ``end`` parameters indicate which bytes to consider

        For more information check https://redis.io/commands/bitcount
        """
        params = [key]
        if start is not None and end is not None:
            params.append(start)
            params.append(end)
        elif (start is not None and end is None) or (end is not None and start is None):
            raise DataError("Both start and end must be specified")
        return self.execute_command("BITCOUNT", *params)

    def bitfield(self, key, default_overflow=None):
        """
        Return a BitFieldOperation instance to conveniently construct one or
        more bitfield operations on ``key``.

        For more information check https://redis.io/commands/bitfield
        """
        return BitFieldOperation(self, key, default_overflow=default_overflow)

    def bitop(self, operation, dest, *keys):
        """
        Perform a bitwise operation using ``operation`` between ``keys`` and
        store the result in ``dest``.

        For more information check https://redis.io/commands/bitop
        """
        return self.execute_command("BITOP", operation, dest, *keys)

    def bitpos(self, key, bit, start=None, end=None):
        """
        Return the position of the first bit set to 1 or 0 in a string.
        ``start`` and ``end`` defines search range. The range is interpreted
        as a range of bytes and not a range of bits, so start=0 and end=2
        means to look at the first three bytes.

        For more information check https://redis.io/commands/bitpos
        """
        if bit not in (0, 1):
            raise DataError("bit must be 0 or 1")
        params = [key, bit]

        start is not None and params.append(start)

        if start is not None and end is not None:
            params.append(end)
        elif start is None and end is not None:
            raise DataError("start argument is not set, " "when end is specified")
        return self.execute_command("BITPOS", *params)

    def copy(self, source, destination, destination_db=None, replace=False):
        """
        Copy the value stored in the ``source`` key to the ``destination`` key.

        ``destination_db`` an alternative destination database. By default,
        the ``destination`` key is created in the source Redis database.

        ``replace`` whether the ``destination`` key should be removed before
        copying the value to it. By default, the value is not copied if
        the ``destination`` key already exists.

        For more information check https://redis.io/commands/copy
        """
        params = [source, destination]
        if destination_db is not None:
            params.extend(["DB", destination_db])
        if replace:
            params.append("REPLACE")
        return self.execute_command("COPY", *params)

    def decrby(self, name, amount=1):
        """
        Decrements the value of ``key`` by ``amount``.  If no key exists,
        the value will be initialized as 0 - ``amount``

        For more information check https://redis.io/commands/decrby
        """
        return self.execute_command("DECRBY", name, amount)

    decr = decrby

    def delete(self, *names):
        """
        Delete one or more keys specified by ``names``
        """
        return self.execute_command("DEL", *names)

    def __delitem__(self, name):
        self.delete(name)

    def dump(self, name):
        """
        Return a serialized version of the value stored at the specified key.
        If key does not exist a nil bulk reply is returned.

        For more information check https://redis.io/commands/dump
        """
        options = {}
        return self.execute_command("DUMP", name, never_decode=True)

    def exists(self, *names):
        """
        Returns the number of ``names`` that exist

        For more information check https://redis.io/commands/exists
        """
        return self.execute_command("EXISTS", *names)

    __contains__ = exists

    def expire(self, name, time):
        """
        Set an expire flag on key ``name`` for ``time`` seconds. ``time``
        can be represented by an integer or a Python timedelta object.

        For more information check https://redis.io/commands/expire
        """
        if isinstance(time, datetime.timedelta):
            time = int(time.total_seconds())
        return self.execute_command("EXPIRE", name, time)

    def expireat(self, name, when):
        """
        Set an expire flag on key ``name``. ``when`` can be represented
        as an integer indicating unix time or a Python datetime object.

        For more information check https://redis.io/commands/expireat
        """
        if isinstance(when, datetime.datetime):
            when = int(time.mktime(when.timetuple()))
        return self.execute_command("EXPIREAT", name, when)

    def get(self, name):
        """
        Return the value at key ``name``, or None if the key doesn't exist

        For more information check https://redis.io/commands/get
        """
        return self.execute_command("GET", name)

    def getdel(self, name):
        """
        Get the value at key ``name`` and delete the key. This command
        is similar to GET, except for the fact that it also deletes
        the key on success (if and only if the key's value type
        is a string).

        For more information check https://redis.io/commands/getdel
        """
        return self.execute_command("GETDEL", name)

    def getex(self, name, ex=None, px=None, exat=None, pxat=None, persist=False):
        """
        Get the value of key and optionally set its expiration.
        GETEX is similar to GET, but is a write command with
        additional options. All time parameters can be given as
        datetime.timedelta or integers.

        ``ex`` sets an expire flag on key ``name`` for ``ex`` seconds.

        ``px`` sets an expire flag on key ``name`` for ``px`` milliseconds.

        ``exat`` sets an expire flag on key ``name`` for ``ex`` seconds,
        specified in unix time.

        ``pxat`` sets an expire flag on key ``name`` for ``ex`` milliseconds,
        specified in unix time.

        ``persist`` remove the time to live associated with ``name``.

        For more information check https://redis.io/commands/getex
        """

        opset = {ex, px, exat, pxat}
        if len(opset) > 2 or len(opset) > 1 and persist:
            raise DataError(
                "``ex``, ``px``, ``exat``, ``pxat``, "
                "and ``persist`` are mutually exclusive."
            )

        pieces = []
        # similar to set command
        if ex is not None:
            pieces.append("EX")
            if isinstance(ex, datetime.timedelta):
                ex = int(ex.total_seconds())
            pieces.append(ex)
        if px is not None:
            pieces.append("PX")
            if isinstance(px, datetime.timedelta):
                px = int(px.total_seconds() * 1000)
            pieces.append(px)
        # similar to pexpireat command
        if exat is not None:
            pieces.append("EXAT")
            if isinstance(exat, datetime.datetime):
                s = int(exat.microsecond / 1000000)
                exat = int(time.mktime(exat.timetuple())) + s
            pieces.append(exat)
        if pxat is not None:
            pieces.append("PXAT")
            if isinstance(pxat, datetime.datetime):
                ms = int(pxat.microsecond / 1000)
                pxat = int(time.mktime(pxat.timetuple())) * 1000 + ms
            pieces.append(pxat)
        if persist:
            pieces.append("PERSIST")

        return self.execute_command("GETEX", name, *pieces)

    def __getitem__(self, name):
        """
        Return the value at key ``name``, raises a KeyError if the key
        doesn't exist.
        """
        value = self.get(name)
        if value is not None:
            return value
        raise KeyError(name)

    def getbit(self, name, offset):
        """
        Returns a boolean indicating the value of ``offset`` in ``name``

        For more information check https://redis.io/commands/getbit
        """
        return self.execute_command("GETBIT", name, offset)

    def getrange(self, key, start, end):
        """
        Returns the substring of the string value stored at ``key``,
        determined by the offsets ``start`` and ``end`` (both are inclusive)

        For more information check https://redis.io/commands/getrange
        """
        return self.execute_command("GETRANGE", key, start, end)

    def getset(self, name, value):
        """
        Sets the value at key ``name`` to ``value``
        and returns the old value at key ``name`` atomically.

        As per Redis 6.2, GETSET is considered deprecated.
        Please use SET with GET parameter in new code.

        For more information check https://redis.io/commands/getset
        """
        return self.execute_command("GETSET", name, value)

    def incrby(self, name, amount=1):
        """
        Increments the value of ``key`` by ``amount``.  If no key exists,
        the value will be initialized as ``amount``

        For more information check https://redis.io/commands/incrby
        """
        return self.execute_command("INCRBY", name, amount)

    incr = incrby

    def incrbyfloat(self, name, amount=1.0):
        """
        Increments the value at key ``name`` by floating ``amount``.
        If no key exists, the value will be initialized as ``amount``

        For more information check https://redis.io/commands/incrbyfloat
        """
        return self.execute_command("INCRBYFLOAT", name, amount)

    def keys(self, pattern="*", **kwargs):
        """
        Returns a list of keys matching ``pattern``

        For more information check https://redis.io/commands/keys
        """
        return self.execute_command("KEYS", pattern, **kwargs)

    def lmove(self, first_list, second_list, src="LEFT", dest="RIGHT"):
        """
        Atomically returns and removes the first/last element of a list,
        pushing it as the first/last element on the destination list.
        Returns the element being popped and pushed.

        For more information check https://redis.io/commands/lmove
        """
        params = [first_list, second_list, src, dest]
        return self.execute_command("LMOVE", *params)

    def blmove(self, first_list, second_list, timeout, src="LEFT", dest="RIGHT"):
        """
        Blocking version of lmove.

        For more information check https://redis.io/commands/blmove
        """
        params = [first_list, second_list, src, dest, timeout]
        return self.execute_command("BLMOVE", *params)

    def mget(self, keys, *args):
        """
        Returns a list of values ordered identically to ``keys``

        For more information check https://redis.io/commands/mget
        """
        args = iterkeysargs(keys, args)
        empty_response = not args
        return self.execute_command("MGET", *args, empty_response=empty_response)

    def mset(self, mapping):
        """
        Sets key/values based on a mapping. Mapping is a dictionary of
        key/value pairs. Both keys and values should be strings or types that
        can be cast to a string via str().

        For more information check https://redis.io/commands/mset
        """
        items = []
        for pair in mapping.items():
            items.extend(pair)
        return self.execute_command("MSET", *items)

    def msetnx(self, mapping):
        """
        Sets key/values based on a mapping if none of the keys are already set.
        Mapping is a dictionary of key/value pairs. Both keys and values
        should be strings or types that can be cast to a string via str().
        Returns a boolean indicating if the operation was successful.

        For more information check https://redis.io/commands/msetnx
        """
        items = []
        for pair in mapping.items():
            items.extend(pair)
        return self.execute_command("MSETNX", *items)

    def move(self, name, db):
        """
        Moves the key ``name`` to a different Redis database ``db``

        For more information check https://redis.io/commands/move
        """
        return self.execute_command("MOVE", name, db)

    def persist(self, name):
        """
        Removes an expiration on ``name``

        For more information check https://redis.io/commands/persist
        """
        return self.execute_command("PERSIST", name)

    def pexpire(self, name, time):
        """
        Set an expire flag on key ``name`` for ``time`` milliseconds.
        ``time`` can be represented by an integer or a Python timedelta
        object.

        For more information check https://redis.io/commands/pexpire
        """
        if isinstance(time, datetime.timedelta):
            time = int(time.total_seconds() * 1000)
        return self.execute_command("PEXPIRE", name, time)

    def pexpireat(self, name, when):
        """
        Set an expire flag on key ``name``. ``when`` can be represented
        as an integer representing unix time in milliseconds (unix time * 1000)
        or a Python datetime object.

        For more information check https://redis.io/commands/pexpireat
        """
        if isinstance(when, datetime.datetime):
            ms = int(when.microsecond / 1000)
            when = int(time.mktime(when.timetuple())) * 1000 + ms
        return self.execute_command("PEXPIREAT", name, when)

    def psetex(self, name, time_ms, value):
        """
        Set the value of key ``name`` to ``value`` that expires in ``time_ms``
        milliseconds. ``time_ms`` can be represented by an integer or a Python
        timedelta object

        For more information check https://redis.io/commands/psetex
        """
        if isinstance(time_ms, datetime.timedelta):
            time_ms = int(time_ms.total_seconds() * 1000)
        return self.execute_command("PSETEX", name, time_ms, value)

    def pttl(self, name):
        """
        Returns the number of milliseconds until the key ``name`` will expire

        For more information check https://redis.io/commands/pttl
        """
        return self.execute_command("PTTL", name)

    def hrandfield(self, key, count=None, withvalues=False):
        """
        Return a random field from the hash value stored at key.

        count: if the argument is positive, return an array of distinct fields.
        If called with a negative count, the behavior changes and the command
        is allowed to return the same field multiple times. In this case,
        the number of returned fields is the absolute value of the
        specified count.
        withvalues: The optional WITHVALUES modifier changes the reply so it
        includes the respective values of the randomly selected hash fields.

        For more information check https://redis.io/commands/hrandfield
        """
        params = []
        if count is not None:
            params.append(count)
        if withvalues:
            params.append("WITHVALUES")

        return self.execute_command("HRANDFIELD", key, *params)

    def randomkey(self, **kwargs):
        """
        Returns the name of a random key

        For more information check https://redis.io/commands/randomkey
        """
        return self.execute_command("RANDOMKEY", **kwargs)

    def rename(self, src, dst):
        """
        Rename key ``src`` to ``dst``

        For more information check https://redis.io/commands/rename
        """
        return self.execute_command("RENAME", src, dst)

    def renamenx(self, src, dst):
        """
        Rename key ``src`` to ``dst`` if ``dst`` doesn't already exist

        For more information check https://redis.io/commands/renamenx
        """
        return self.execute_command("RENAMENX", src, dst)

    def restore(
        self,
        name,
        ttl,
        value,
        replace=False,
        absttl=False,
        idletime=None,
        frequency=None,
    ):
        """
        Create a key using the provided serialized value, previously obtained
        using DUMP.

        ``replace`` allows an existing key on ``name`` to be overridden. If
        it's not specified an error is raised on collision.

        ``absttl`` if True, specified ``ttl`` should represent an absolute Unix
        timestamp in milliseconds in which the key will expire. (Redis 5.0 or
        greater).

        ``idletime`` Used for eviction, this is the number of seconds the
        key must be idle, prior to execution.

        ``frequency`` Used for eviction, this is the frequency counter of
        the object stored at the key, prior to execution.

        For more information check https://redis.io/commands/restore
        """
        params = [name, ttl, value]
        if replace:
            params.append("REPLACE")
        if absttl:
            params.append("ABSTTL")
        if idletime is not None:
            params.append("IDLETIME")
            try:
                params.append(int(idletime))
            except ValueError:
                raise DataError("idletimemust be an integer")

        if frequency is not None:
            params.append("FREQ")
            try:
                params.append(int(frequency))
            except ValueError:
                raise DataError("frequency must be an integer")

        return self.execute_command("RESTORE", *params)

    def set(
        self,
        name,
        value,
        ex=None,
        px=None,
        nx=False,
        xx=False,
        keepttl=False,
        get=False,
        exat=None,
        pxat=None,
    ):
        """
        Set the value at key ``name`` to ``value``

        ``ex`` sets an expire flag on key ``name`` for ``ex`` seconds.

        ``px`` sets an expire flag on key ``name`` for ``px`` milliseconds.

        ``nx`` if set to True, set the value at key ``name`` to ``value`` only
            if it does not exist.

        ``xx`` if set to True, set the value at key ``name`` to ``value`` only
            if it already exists.

        ``keepttl`` if True, retain the time to live associated with the key.
            (Available since Redis 6.0)

        ``get`` if True, set the value at key ``name`` to ``value`` and return
            the old value stored at key, or None if the key did not exist.
            (Available since Redis 6.2)

        ``exat`` sets an expire flag on key ``name`` for ``ex`` seconds,
            specified in unix time.

        ``pxat`` sets an expire flag on key ``name`` for ``ex`` milliseconds,
            specified in unix time.

        For more information check https://redis.io/commands/set
        """
        pieces = [name, value]
        options = {}
        if ex is not None:
            pieces.append("EX")
            if isinstance(ex, datetime.timedelta):
                pieces.append(int(ex.total_seconds()))
            elif isinstance(ex, int):
                pieces.append(ex)
            else:
                raise DataError("ex must be datetime.timedelta or int")
        if px is not None:
            pieces.append("PX")
            if isinstance(px, datetime.timedelta):
                pieces.append(int(px.total_seconds() * 1000))
            elif isinstance(px, int):
                pieces.append(px)
            else:
                raise DataError("px must be datetime.timedelta or int")
        if exat is not None:
            pieces.append("EXAT")
            if isinstance(exat, datetime.datetime):
                s = int(exat.microsecond / 1000000)
                exat = int(time.mktime(exat.timetuple())) + s
            pieces.append(exat)
        if pxat is not None:
            pieces.append("PXAT")
            if isinstance(pxat, datetime.datetime):
                ms = int(pxat.microsecond / 1000)
                pxat = int(time.mktime(pxat.timetuple())) * 1000 + ms
            pieces.append(pxat)
        if keepttl:
            pieces.append("KEEPTTL")

        if nx:
            pieces.append("NX")
        if xx:
            pieces.append("XX")

        if get:
            pieces.append("GET")
            options["get"] = True

        return self.execute_command("SET", *pieces, **options)

    def __setitem__(self, name, value):
        self.set(name, value)

    def setbit(self, name, offset, value):
        """
        Flag the ``offset`` in ``name`` as ``value``. Returns a boolean
        indicating the previous value of ``offset``.

        For more information check https://redis.io/commands/setbit
        """
        value = value and 1 or 0
        return self.execute_command("SETBIT", name, offset, value)

    def setex(self, name, time, value):
        """
        Set the value of key ``name`` to ``value`` that expires in ``time``
        seconds. ``time`` can be represented by an integer or a Python
        timedelta object.

        For more information check https://redis.io/commands/setex
        """
        if isinstance(time, datetime.timedelta):
            time = int(time.total_seconds())
        return self.execute_command("SETEX", name, time, value)

    def setnx(self, name, value):
        """
        Set the value of key ``name`` to ``value`` if key doesn't exist

        For more information check https://redis.io/commands/setnx
        """
        return self.execute_command("SETNX", name, value)

    def setrange(self, name, offset, value):
        """
        Overwrite bytes in the value of ``name`` starting at ``offset`` with
        ``value``. If ``offset`` plus the length of ``value`` exceeds the
        length of the original value, the new value will be larger than before.
        If ``offset`` exceeds the length of the original value, null bytes
        will be used to pad between the end of the previous value and the start
        of what's being injected.

        Returns the length of the new string.

        For more information check https://redis.io/commands/setrange
        """
        return self.execute_command("SETRANGE", name, offset, value)

    def stralgo(
        self,
        algo,
        value1,
        value2,
        specific_argument="strings",
        len=False,
        idx=False,
        minmatchlen=None,
        withmatchlen=False,
        **kwargs,
    ):
        """
        Implements complex algorithms that operate on strings.
        Right now the only algorithm implemented is the LCS algorithm
        (longest common substring). However new algorithms could be
        implemented in the future.

        ``algo`` Right now must be LCS
        ``value1`` and ``value2`` Can be two strings or two keys
        ``specific_argument`` Specifying if the arguments to the algorithm
        will be keys or strings. strings is the default.
        ``len`` Returns just the len of the match.
        ``idx`` Returns the match positions in each string.
        ``minmatchlen`` Restrict the list of matches to the ones of a given
        minimal length. Can be provided only when ``idx`` set to True.
        ``withmatchlen`` Returns the matches with the len of the match.
        Can be provided only when ``idx`` set to True.

        For more information check https://redis.io/commands/stralgo
        """
        # check validity
        supported_algo = ["LCS"]
        if algo not in supported_algo:
            supported_algos_str = ", ".join(supported_algo)
            raise DataError(f"The supported algorithms are: {supported_algos_str}")
        if specific_argument not in ["keys", "strings"]:
            raise DataError("specific_argument can be only keys or strings")
        if len and idx:
            raise DataError("len and idx cannot be provided together.")

        pieces = [algo, specific_argument.upper(), value1, value2]
        if len:
            pieces.append(b"LEN")
        if idx:
            pieces.append(b"IDX")
        try:
            int(minmatchlen)
            pieces.extend([b"MINMATCHLEN", minmatchlen])
        except TypeError:
            pass
        if withmatchlen:
            pieces.append(b"WITHMATCHLEN")

        return self.execute_command(
            "STRALGO",
            *pieces,
            len=len,
            idx=idx,
            minmatchlen=minmatchlen,
            withmatchlen=withmatchlen,
            **kwargs,
        )

    def strlen(self, name):
        """
        Return the number of bytes stored in the value of ``name``

        For more information check https://redis.io/commands/strlen
        """
        return self.execute_command("STRLEN", name)

    def substr(self, name, start, end=-1):
        """
        Return a substring of the string at key ``name``. ``start`` and ``end``
        are 0-based integers specifying the portion of the string to return.
        """
        return self.execute_command("SUBSTR", name, start, end)

    def touch(self, *args):
        """
        Alters the last access time of a key(s) ``*args``. A key is ignored
        if it does not exist.

        For more information check https://redis.io/commands/touch
        """
        return self.execute_command("TOUCH", *args)

    def ttl(self, name):
        """
        Returns the number of seconds until the key ``name`` will expire

        For more information check https://redis.io/commands/ttl
        """
        return self.execute_command("TTL", name)

    def type(self, name):
        """
        Returns the type of key ``name``

        For more information check https://redis.io/commands/type
        """
        return self.execute_command("TYPE", name)

    def watch(self, *names):
        """
        Watches the values at keys ``names``, or None if the key doesn't exist

        For more information check https://redis.io/commands/watch
        """
        warnings.warn(DeprecationWarning("Call WATCH from a Pipeline object"))

    def unwatch(self):
        """
        Unwatches the value at key ``name``, or None of the key doesn't exist

        For more information check https://redis.io/commands/unwatch
        """
        warnings.warn(DeprecationWarning("Call UNWATCH from a Pipeline object"))

    def unlink(self, *names):
        """
        Unlink one or more keys specified by ``names``

        For more information check https://redis.io/commands/unlink
        """
        return self.execute_command("UNLINK", *names)

    def lcs(
        self,
        key1: str,
        key2: str,
        len: Optional[bool] = False,
        idx: Optional[bool] = False,
        minmatchlen: Optional[int] = 0,
        withmatchlen: Optional[bool] = False,
    ) -> Union[str, int, list]:
        """
        Find the longest common subsequence between ``key1`` and ``key2``.
        If ``len`` is true the length of the match will will be returned.
        If ``idx`` is true the match position in each strings will be returned.
        ``minmatchlen`` restrict the list of matches to the ones of
        the given ``minmatchlen``.
        If ``withmatchlen`` the length of the match also will be returned.
        For more information check https://redis.io/commands/lcs
        """
        pieces = [key1, key2]
        if len:
            pieces.append("LEN")
        if idx:
            pieces.append("IDX")
        if minmatchlen != 0:
            pieces.extend(["MINMATCHLEN", minmatchlen])
        if withmatchlen:
            pieces.append("WITHMATCHLEN")
        return self.execute_command("LCS", *pieces)
