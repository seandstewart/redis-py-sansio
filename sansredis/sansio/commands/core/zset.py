from typing import List, Optional

from sansredis.sansio.commands.base import CommandsProtocol
from sansredis.sansio.exceptions import DataError


class SortedSetCommands(CommandsProtocol):
    """
    Redis commands for Sorted Sets data type.
    see: https://redis.io/topics/data-types-intro#redis-sorted-sets
    """

    def zadd(
        self, name, mapping, nx=False, xx=False, ch=False, incr=False, gt=None, lt=None
    ):
        """
        Set any number of element-name, score pairs to the key ``name``. Pairs
        are specified as a dict of element-names keys to score values.

        ``nx`` forces ZADD to only create new elements and not to update
        scores for elements that already exist.

        ``xx`` forces ZADD to only update scores of elements that already
        exist. New elements will not be added.

        ``ch`` modifies the return value to be the numbers of elements changed.
        Changed elements include new elements that were added and elements
        whose scores changed.

        ``incr`` modifies ZADD to behave like ZINCRBY. In this mode only a
        single element/score pair can be specified and the score is the amount
        the existing score will be incremented by. When using this mode the
        return value of ZADD will be the new score of the element.

        ``LT`` Only update existing elements if the new score is less than
        the current score. This flag doesn't prevent adding new elements.

        ``GT`` Only update existing elements if the new score is greater than
        the current score. This flag doesn't prevent adding new elements.

        The return value of ZADD varies based on the mode specified. With no
        options, ZADD returns the number of new elements added to the sorted
        set.

        ``NX``, ``LT``, and ``GT`` are mutually exclusive options.

        See: https://redis.io/commands/ZADD
        """
        if not mapping:
            raise DataError("ZADD requires at least one element/score pair")
        if nx and xx:
            raise DataError("ZADD allows either 'nx' or 'xx', not both")
        if incr and len(mapping) != 1:
            raise DataError(
                "ZADD option 'incr' only works when passing a "
                "single element/score pair"
            )
        if nx is True and (gt is not None or lt is not None):
            raise DataError("Only one of 'nx', 'lt', or 'gr' may be defined.")

        pieces = []
        options = {}
        if nx:
            pieces.append(b"NX")
        if xx:
            pieces.append(b"XX")
        if ch:
            pieces.append(b"CH")
        if incr:
            pieces.append(b"INCR")
            options["as_score"] = True
        if gt:
            pieces.append(b"GT")
        if lt:
            pieces.append(b"LT")
        for pair in mapping.items():
            pieces.append(pair[1])
            pieces.append(pair[0])
        return self.execute_command("ZADD", name, *pieces, **options)

    def zcard(self, name):
        """
        Return the number of elements in the sorted set ``name``

        For more information check https://redis.io/commands/zcard
        """
        return self.execute_command("ZCARD", name)

    def zcount(self, name, min, max):
        """
        Returns the number of elements in the sorted set at key ``name`` with
        a score between ``min`` and ``max``.

        For more information check https://redis.io/commands/zcount
        """
        return self.execute_command("ZCOUNT", name, min, max)

    def zdiff(self, keys, withscores=False):
        """
        Returns the difference between the first and all successive input
        sorted sets provided in ``keys``.

        For more information check https://redis.io/commands/zdiff
        """
        pieces = [len(keys), *keys]
        if withscores:
            pieces.append("WITHSCORES")
        return self.execute_command("ZDIFF", *pieces)

    def zdiffstore(self, dest, keys):
        """
        Computes the difference between the first and all successive input
        sorted sets provided in ``keys`` and stores the result in ``dest``.

        For more information check https://redis.io/commands/zdiffstore
        """
        pieces = [len(keys), *keys]
        return self.execute_command("ZDIFFSTORE", dest, *pieces)

    def zincrby(self, name, amount, value):
        """
        Increment the score of ``value`` in sorted set ``name`` by ``amount``

        For more information check https://redis.io/commands/zincrby
        """
        return self.execute_command("ZINCRBY", name, amount, value)

    def zinter(self, keys, aggregate=None, withscores=False):
        """
        Return the intersect of multiple sorted sets specified by ``keys``.
        With the ``aggregate`` option, it is possible to specify how the
        results of the union are aggregated. This option defaults to SUM,
        where the score of an element is summed across the inputs where it
        exists. When this option is set to either MIN or MAX, the resulting
        set will contain the minimum or maximum score of an element across
        the inputs where it exists.

        For more information check https://redis.io/commands/zinter
        """
        return self._zaggregate("ZINTER", None, keys, aggregate, withscores=withscores)

    def zinterstore(self, dest, keys, aggregate=None):
        """
        Intersect multiple sorted sets specified by ``keys`` into a new
        sorted set, ``dest``. Scores in the destination will be aggregated
        based on the ``aggregate``. This option defaults to SUM, where the
        score of an element is summed across the inputs where it exists.
        When this option is set to either MIN or MAX, the resulting set will
        contain the minimum or maximum score of an element across the inputs
        where it exists.

        For more information check https://redis.io/commands/zinterstore
        """
        return self._zaggregate("ZINTERSTORE", dest, keys, aggregate)

    def zintercard(self, numkeys: int, keys: List[str], limit: int = 0) -> int:
        """
        Return the cardinality of the intersect of multiple sorted sets
        specified by ``keys`.
        When LIMIT provided (defaults to 0 and means unlimited), if the intersection
        cardinality reaches limit partway through the computation, the algorithm will
        exit and yield limit as the cardinality

        For more information check https://redis.io/commands/zintercard
        """
        args = [numkeys, *keys, "LIMIT", limit]
        return self.execute_command("ZINTERCARD", *args)

    def zlexcount(self, name, min, max):
        """
        Return the number of items in the sorted set ``name`` between the
        lexicographical range ``min`` and ``max``.

        For more information check https://redis.io/commands/zlexcount
        """
        return self.execute_command("ZLEXCOUNT", name, min, max)

    def zpopmax(self, name, count=None):
        """
        Remove and return up to ``count`` members with the highest scores
        from the sorted set ``name``.

        For more information check https://redis.io/commands/zpopmax
        """
        args = (count is not None) and [count] or []
        options = {"withscores": True}
        return self.execute_command("ZPOPMAX", name, *args, **options)

    def zpopmin(self, name, count=None):
        """
        Remove and return up to ``count`` members with the lowest scores
        from the sorted set ``name``.

        For more information check https://redis.io/commands/zpopmin
        """
        args = (count is not None) and [count] or []
        options = {"withscores": True}
        return self.execute_command("ZPOPMIN", name, *args, **options)

    def zrandmember(self, key, count=None, withscores=False):
        """
        Return a random element from the sorted set value stored at key.

        ``count`` if the argument is positive, return an array of distinct
        fields. If called with a negative count, the behavior changes and
        the command is allowed to return the same field multiple times.
        In this case, the number of returned fields is the absolute value
        of the specified count.

        ``withscores`` The optional WITHSCORES modifier changes the reply so it
        includes the respective scores of the randomly selected elements from
        the sorted set.

        For more information check https://redis.io/commands/zrandmember
        """
        params = []
        if count is not None:
            params.append(count)
        if withscores:
            params.append("WITHSCORES")

        return self.execute_command("ZRANDMEMBER", key, *params)

    def bzpopmax(self, keys, timeout=0):
        """
        ZPOPMAX a value off of the first non-empty sorted set
        named in the ``keys`` list.

        If none of the sorted sets in ``keys`` has a value to ZPOPMAX,
        then block for ``timeout`` seconds, or until a member gets added
        to one of the sorted sets.

        If timeout is 0, then block indefinitely.

        For more information check https://redis.io/commands/bzpopmax
        """
        if timeout is None:
            timeout = 0
        keys = list_or_args(keys, None)
        keys.append(timeout)
        return self.execute_command("BZPOPMAX", *keys)

    def bzpopmin(self, keys, timeout=0):
        """
        ZPOPMIN a value off of the first non-empty sorted set
        named in the ``keys`` list.

        If none of the sorted sets in ``keys`` has a value to ZPOPMIN,
        then block for ``timeout`` seconds, or until a member gets added
        to one of the sorted sets.

        If timeout is 0, then block indefinitely.

        For more information check https://redis.io/commands/bzpopmin
        """
        if timeout is None:
            timeout = 0
        keys = list_or_args(keys, None)
        keys.append(timeout)
        return self.execute_command("BZPOPMIN", *keys)

    def zmpop(
        self,
        num_keys: int,
        keys: List[str],
        min: Optional[bool] = False,
        max: Optional[bool] = False,
        count: Optional[int] = 1,
    ) -> list:
        """
        Pop ``count`` values (default 1) off of the first non-empty sorted set
        named in the ``keys`` list.
        For more information check https://redis.io/commands/zmpop
        """
        args = [num_keys] + keys
        if (min and max) or (not min and not max):
            raise DataError
        elif min:
            args.append("MIN")
        else:
            args.append("MAX")
        if count != 1:
            args.extend(["COUNT", count])

        return self.execute_command("ZMPOP", *args)

    def bzmpop(
        self,
        timeout: float,
        numkeys: int,
        keys: List[str],
        min: Optional[bool] = False,
        max: Optional[bool] = False,
        count: Optional[int] = 1,
    ) -> Optional[list]:
        """
        Pop ``count`` values (default 1) off of the first non-empty sorted set
        named in the ``keys`` list.

        If none of the sorted sets in ``keys`` has a value to pop,
        then block for ``timeout`` seconds, or until a member gets added
        to one of the sorted sets.

        If timeout is 0, then block indefinitely.

        For more information check https://redis.io/commands/bzmpop
        """
        args = [timeout, numkeys, *keys]
        if (min and max) or (not min and not max):
            raise DataError("Either min or max, but not both must be set")
        elif min:
            args.append("MIN")
        else:
            args.append("MAX")
        args.extend(["COUNT", count])

        return self.execute_command("BZMPOP", *args)

    def _zrange(
        self,
        command,
        dest,
        name,
        start,
        end,
        desc=False,
        byscore=False,
        bylex=False,
        withscores=False,
        score_cast_func=float,
        offset=None,
        num=None,
    ):
        if byscore and bylex:
            raise DataError(
                "``byscore`` and ``bylex`` can not be " "specified together."
            )
        if (offset is not None and num is None) or (num is not None and offset is None):
            raise DataError("``offset`` and ``num`` must both be specified.")
        if bylex and withscores:
            raise DataError(
                "``withscores`` not supported in combination " "with ``bylex``."
            )
        pieces = [command]
        if dest:
            pieces.append(dest)
        pieces.extend([name, start, end])
        if byscore:
            pieces.append("BYSCORE")
        if bylex:
            pieces.append("BYLEX")
        if desc:
            pieces.append("REV")
        if offset is not None and num is not None:
            pieces.extend(["LIMIT", offset, num])
        if withscores:
            pieces.append("WITHSCORES")
        options = {"withscores": withscores, "score_cast_func": score_cast_func}
        return self.execute_command(*pieces, **options)

    def zrange(
        self,
        name,
        start,
        end,
        desc=False,
        withscores=False,
        score_cast_func=float,
        byscore=False,
        bylex=False,
        offset=None,
        num=None,
    ):
        """
        Return a range of values from sorted set ``name`` between
        ``start`` and ``end`` sorted in ascending order.

        ``start`` and ``end`` can be negative, indicating the end of the range.

        ``desc`` a boolean indicating whether to sort the results in reversed
        order.

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs.

        ``score_cast_func`` a callable used to cast the score return value.

        ``byscore`` when set to True, returns the range of elements from the
        sorted set having scores equal or between ``start`` and ``end``.

        ``bylex`` when set to True, returns the range of elements from the
        sorted set between the ``start`` and ``end`` lexicographical closed
        range intervals.
        Valid ``start`` and ``end`` must start with ( or [, in order to specify
        whether the range interval is exclusive or inclusive, respectively.

        ``offset`` and ``num`` are specified, then return a slice of the range.
        Can't be provided when using ``bylex``.

        For more information check https://redis.io/commands/zrange
        """
        # Need to support ``desc`` also when using old redis version
        # because it was supported in 3.5.3 (of redis-py)
        if not byscore and not bylex and (offset is None and num is None) and desc:
            return self.zrevrange(name, start, end, withscores, score_cast_func)

        return self._zrange(
            "ZRANGE",
            None,
            name,
            start,
            end,
            desc,
            byscore,
            bylex,
            withscores,
            score_cast_func,
            offset,
            num,
        )

    def zrevrange(self, name, start, end, withscores=False, score_cast_func=float):
        """
        Return a range of values from sorted set ``name`` between
        ``start`` and ``end`` sorted in descending order.

        ``start`` and ``end`` can be negative, indicating the end of the range.

        ``withscores`` indicates to return the scores along with the values
        The return type is a list of (value, score) pairs

        ``score_cast_func`` a callable used to cast the score return value

        For more information check https://redis.io/commands/zrevrange
        """
        pieces = ["ZREVRANGE", name, start, end]
        if withscores:
            pieces.append(b"WITHSCORES")
        options = {"withscores": withscores, "score_cast_func": score_cast_func}
        return self.execute_command(*pieces, **options)

    def zrangestore(
        self,
        dest,
        name,
        start,
        end,
        byscore=False,
        bylex=False,
        desc=False,
        offset=None,
        num=None,
    ):
        """
        Stores in ``dest`` the result of a range of values from sorted set
        ``name`` between ``start`` and ``end`` sorted in ascending order.

        ``start`` and ``end`` can be negative, indicating the end of the range.

        ``byscore`` when set to True, returns the range of elements from the
        sorted set having scores equal or between ``start`` and ``end``.

        ``bylex`` when set to True, returns the range of elements from the
        sorted set between the ``start`` and ``end`` lexicographical closed
        range intervals.
        Valid ``start`` and ``end`` must start with ( or [, in order to specify
        whether the range interval is exclusive or inclusive, respectively.

        ``desc`` a boolean indicating whether to sort the results in reversed
        order.

        ``offset`` and ``num`` are specified, then return a slice of the range.
        Can't be provided when using ``bylex``.

        For more information check https://redis.io/commands/zrangestore
        """
        return self._zrange(
            "ZRANGESTORE",
            dest,
            name,
            start,
            end,
            desc,
            byscore,
            bylex,
            False,
            None,
            offset,
            num,
        )

    def zrangebylex(self, name, min, max, start=None, num=None):
        """
        Return the lexicographical range of values from sorted set ``name``
        between ``min`` and ``max``.

        If ``start`` and ``num`` are specified, then return a slice of the
        range.

        For more information check https://redis.io/commands/zrangebylex
        """
        if (start is not None and num is None) or (num is not None and start is None):
            raise DataError("``start`` and ``num`` must both be specified")
        pieces = ["ZRANGEBYLEX", name, min, max]
        if start is not None and num is not None:
            pieces.extend([b"LIMIT", start, num])
        return self.execute_command(*pieces)

    def zrevrangebylex(self, name, max, min, start=None, num=None):
        """
        Return the reversed lexicographical range of values from sorted set
        ``name`` between ``max`` and ``min``.

        If ``start`` and ``num`` are specified, then return a slice of the
        range.

        For more information check https://redis.io/commands/zrevrangebylex
        """
        if (start is not None and num is None) or (num is not None and start is None):
            raise DataError("``start`` and ``num`` must both be specified")
        pieces = ["ZREVRANGEBYLEX", name, max, min]
        if start is not None and num is not None:
            pieces.extend(["LIMIT", start, num])
        return self.execute_command(*pieces)

    def zrangebyscore(
        self,
        name,
        min,
        max,
        start=None,
        num=None,
        withscores=False,
        score_cast_func=float,
    ):
        """
        Return a range of values from the sorted set ``name`` with scores
        between ``min`` and ``max``.

        If ``start`` and ``num`` are specified, then return a slice
        of the range.

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs

        `score_cast_func`` a callable used to cast the score return value

        For more information check https://redis.io/commands/zrangebyscore
        """
        if (start is not None and num is None) or (num is not None and start is None):
            raise DataError("``start`` and ``num`` must both be specified")
        pieces = ["ZRANGEBYSCORE", name, min, max]
        if start is not None and num is not None:
            pieces.extend(["LIMIT", start, num])
        if withscores:
            pieces.append("WITHSCORES")
        options = {"withscores": withscores, "score_cast_func": score_cast_func}
        return self.execute_command(*pieces, **options)

    def zrevrangebyscore(
        self,
        name,
        max,
        min,
        start=None,
        num=None,
        withscores=False,
        score_cast_func=float,
    ):
        """
        Return a range of values from the sorted set ``name`` with scores
        between ``min`` and ``max`` in descending order.

        If ``start`` and ``num`` are specified, then return a slice
        of the range.

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs

        ``score_cast_func`` a callable used to cast the score return value

        For more information check https://redis.io/commands/zrevrangebyscore
        """
        if (start is not None and num is None) or (num is not None and start is None):
            raise DataError("``start`` and ``num`` must both be specified")
        pieces = ["ZREVRANGEBYSCORE", name, max, min]
        if start is not None and num is not None:
            pieces.extend(["LIMIT", start, num])
        if withscores:
            pieces.append("WITHSCORES")
        options = {"withscores": withscores, "score_cast_func": score_cast_func}
        return self.execute_command(*pieces, **options)

    def zrank(self, name, value):
        """
        Returns a 0-based value indicating the rank of ``value`` in sorted set
        ``name``

        For more information check https://redis.io/commands/zrank
        """
        return self.execute_command("ZRANK", name, value)

    def zrem(self, name, *values):
        """
        Remove member ``values`` from sorted set ``name``

        For more information check https://redis.io/commands/zrem
        """
        return self.execute_command("ZREM", name, *values)

    def zremrangebylex(self, name, min, max):
        """
        Remove all elements in the sorted set ``name`` between the
        lexicographical range specified by ``min`` and ``max``.

        Returns the number of elements removed.

        For more information check https://redis.io/commands/zremrangebylex
        """
        return self.execute_command("ZREMRANGEBYLEX", name, min, max)

    def zremrangebyrank(self, name, min, max):
        """
        Remove all elements in the sorted set ``name`` with ranks between
        ``min`` and ``max``. Values are 0-based, ordered from smallest score
        to largest. Values can be negative indicating the highest scores.
        Returns the number of elements removed

        For more information check https://redis.io/commands/zremrangebyrank
        """
        return self.execute_command("ZREMRANGEBYRANK", name, min, max)

    def zremrangebyscore(self, name, min, max):
        """
        Remove all elements in the sorted set ``name`` with scores
        between ``min`` and ``max``. Returns the number of elements removed.

        For more information check https://redis.io/commands/zremrangebyscore
        """
        return self.execute_command("ZREMRANGEBYSCORE", name, min, max)

    def zrevrank(self, name, value):
        """
        Returns a 0-based value indicating the descending rank of
        ``value`` in sorted set ``name``

        For more information check https://redis.io/commands/zrevrank
        """
        return self.execute_command("ZREVRANK", name, value)

    def zscore(self, name, value):
        """
        Return the score of element ``value`` in sorted set ``name``

        For more information check https://redis.io/commands/zscore
        """
        return self.execute_command("ZSCORE", name, value)

    def zunion(self, keys, aggregate=None, withscores=False):
        """
        Return the union of multiple sorted sets specified by ``keys``.
        ``keys`` can be provided as dictionary of keys and their weights.
        Scores will be aggregated based on the ``aggregate``, or SUM if
        none is provided.

        For more information check https://redis.io/commands/zunion
        """
        return self._zaggregate("ZUNION", None, keys, aggregate, withscores=withscores)

    def zunionstore(self, dest, keys, aggregate=None):
        """
        Union multiple sorted sets specified by ``keys`` into
        a new sorted set, ``dest``. Scores in the destination will be
        aggregated based on the ``aggregate``, or SUM if none is provided.

        For more information check https://redis.io/commands/zunionstore
        """
        return self._zaggregate("ZUNIONSTORE", dest, keys, aggregate)

    def zmscore(self, key, members):
        """
        Returns the scores associated with the specified members
        in the sorted set stored at key.
        ``members`` should be a list of the member name.
        Return type is a list of score.
        If the member does not exist, a None will be returned
        in corresponding position.

        For more information check https://redis.io/commands/zmscore
        """
        if not members:
            raise DataError("ZMSCORE members must be a non-empty list")
        pieces = [key] + members
        return self.execute_command("ZMSCORE", *pieces)

    def _zaggregate(self, command, dest, keys, aggregate=None, **options):
        pieces = [command]
        if dest is not None:
            pieces.append(dest)
        pieces.append(len(keys))
        if isinstance(keys, dict):
            keys, weights = keys.keys(), keys.values()
        else:
            weights = None
        pieces.extend(keys)
        if weights:
            pieces.append(b"WEIGHTS")
            pieces.extend(weights)
        if aggregate:
            if aggregate.upper() in ["SUM", "MIN", "MAX"]:
                pieces.append(b"AGGREGATE")
                pieces.append(aggregate)
            else:
                raise DataError("aggregate can be sum, min or max.")
        if options.get("withscores", False):
            pieces.append(b"WITHSCORES")
        return self.execute_command(*pieces, **options)
