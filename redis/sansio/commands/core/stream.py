from redis.sansio.commands.base import CommandsProtocol
from redis.sansio.exceptions import DataError


class StreamCommands(CommandsProtocol):
    """
    Redis commands for Stream data type.
    see: https://redis.io/topics/streams-intro
    """

    def xack(self, name, groupname, *ids):
        """
        Acknowledges the successful processing of one or more messages.
        name: name of the stream.
        groupname: name of the consumer group.
        *ids: message ids to acknowledge.

        For more information check https://redis.io/commands/xack
        """
        return self.execute_command("XACK", name, groupname, *ids)

    def xadd(
        self,
        name,
        fields,
        id="*",
        maxlen=None,
        approximate=True,
        nomkstream=False,
        minid=None,
        limit=None,
    ):
        """
        Add to a stream.
        name: name of the stream
        fields: dict of field/value pairs to insert into the stream
        id: Location to insert this record. By default it is appended.
        maxlen: truncate old stream members beyond this size.
        Can't be specified with minid.
        approximate: actual stream length may be slightly more than maxlen
        nomkstream: When set to true, do not make a stream
        minid: the minimum id in the stream to query.
        Can't be specified with maxlen.
        limit: specifies the maximum number of entries to retrieve

        For more information check https://redis.io/commands/xadd
        """
        pieces = []
        if maxlen is not None and minid is not None:
            raise DataError(
                "Only one of ```maxlen``` or ```minid``` " "may be specified"
            )

        if maxlen is not None:
            if not isinstance(maxlen, int) or maxlen < 1:
                raise DataError("XADD maxlen must be a positive integer")
            pieces.append(b"MAXLEN")
            if approximate:
                pieces.append(b"~")
            pieces.append(str(maxlen))
        if minid is not None:
            pieces.append(b"MINID")
            if approximate:
                pieces.append(b"~")
            pieces.append(minid)
        if limit is not None:
            pieces.extend([b"LIMIT", limit])
        if nomkstream:
            pieces.append(b"NOMKSTREAM")
        pieces.append(id)
        if not isinstance(fields, dict) or len(fields) == 0:
            raise DataError("XADD fields must be a non-empty dict")
        for pair in fields.items():
            pieces.extend(pair)
        return self.execute_command("XADD", name, *pieces)

    def xautoclaim(
        self,
        name,
        groupname,
        consumername,
        min_idle_time,
        start_id=0,
        count=None,
        justid=False,
    ):
        """
        Transfers ownership of pending stream entries that match the specified
        criteria. Conceptually, equivalent to calling XPENDING and then XCLAIM,
        but provides a more straightforward way to deal with message delivery
        failures via SCAN-like semantics.
        name: name of the stream.
        groupname: name of the consumer group.
        consumername: name of a consumer that claims the message.
        min_idle_time: filter messages that were idle less than this amount of
        milliseconds.
        start_id: filter messages with equal or greater ID.
        count: optional integer, upper limit of the number of entries that the
        command attempts to claim. Set to 100 by default.
        justid: optional boolean, false by default. Return just an array of IDs
        of messages successfully claimed, without returning the actual message

        For more information check https://redis.io/commands/xautoclaim
        """
        try:
            if int(min_idle_time) < 0:
                raise DataError(
                    "XAUTOCLAIM min_idle_time must be a non" "negative integer"
                )
        except TypeError:
            pass

        kwargs = {}
        pieces = [name, groupname, consumername, min_idle_time, start_id]

        try:
            if int(count) < 0:
                raise DataError("XPENDING count must be a integer >= 0")
            pieces.extend([b"COUNT", count])
        except TypeError:
            pass
        if justid:
            pieces.append(b"JUSTID")
            kwargs["parse_justid"] = True

        return self.execute_command("XAUTOCLAIM", *pieces, **kwargs)

    def xclaim(
        self,
        name,
        groupname,
        consumername,
        min_idle_time,
        message_ids,
        idle=None,
        time=None,
        retrycount=None,
        force=False,
        justid=False,
    ):
        """
        Changes the ownership of a pending message.
        name: name of the stream.
        groupname: name of the consumer group.
        consumername: name of a consumer that claims the message.
        min_idle_time: filter messages that were idle less than this amount of
        milliseconds
        message_ids: non-empty list or tuple of message IDs to claim
        idle: optional. Set the idle time (last time it was delivered) of the
         message in ms
        time: optional integer. This is the same as idle but instead of a
         relative amount of milliseconds, it sets the idle time to a specific
         Unix time (in milliseconds).
        retrycount: optional integer. set the retry counter to the specified
         value. This counter is incremented every time a message is delivered
         again.
        force: optional boolean, false by default. Creates the pending message
         entry in the PEL even if certain specified IDs are not already in the
         PEL assigned to a different client.
        justid: optional boolean, false by default. Return just an array of IDs
         of messages successfully claimed, without returning the actual message

         For more information check https://redis.io/commands/xclaim
        """
        if not isinstance(min_idle_time, int) or min_idle_time < 0:
            raise DataError("XCLAIM min_idle_time must be a non negative " "integer")
        if not isinstance(message_ids, (list, tuple)) or not message_ids:
            raise DataError(
                "XCLAIM message_ids must be a non empty list or "
                "tuple of message IDs to claim"
            )

        kwargs = {}
        pieces = [name, groupname, consumername, str(min_idle_time)]
        pieces.extend(list(message_ids))

        if idle is not None:
            if not isinstance(idle, int):
                raise DataError("XCLAIM idle must be an integer")
            pieces.extend((b"IDLE", str(idle)))
        if time is not None:
            if not isinstance(time, int):
                raise DataError("XCLAIM time must be an integer")
            pieces.extend((b"TIME", str(time)))
        if retrycount is not None:
            if not isinstance(retrycount, int):
                raise DataError("XCLAIM retrycount must be an integer")
            pieces.extend((b"RETRYCOUNT", str(retrycount)))

        if force:
            if not isinstance(force, bool):
                raise DataError("XCLAIM force must be a boolean")
            pieces.append(b"FORCE")
        if justid:
            if not isinstance(justid, bool):
                raise DataError("XCLAIM justid must be a boolean")
            pieces.append(b"JUSTID")
            kwargs["parse_justid"] = True
        return self.execute_command("XCLAIM", *pieces, **kwargs)

    def xdel(self, name, *ids):
        """
        Deletes one or more messages from a stream.
        name: name of the stream.
        *ids: message ids to delete.

        For more information check https://redis.io/commands/xdel
        """
        return self.execute_command("XDEL", name, *ids)

    def xgroup_create(self, name, groupname, id="$", mkstream=False):
        """
        Create a new consumer group associated with a stream.
        name: name of the stream.
        groupname: name of the consumer group.
        id: ID of the last item in the stream to consider already delivered.

        For more information check https://redis.io/commands/xgroup-create
        """
        pieces = ["XGROUP CREATE", name, groupname, id]
        if mkstream:
            pieces.append(b"MKSTREAM")
        return self.execute_command(*pieces)

    def xgroup_delconsumer(self, name, groupname, consumername):
        """
        Remove a specific consumer from a consumer group.
        Returns the number of pending messages that the consumer had before it
        was deleted.
        name: name of the stream.
        groupname: name of the consumer group.
        consumername: name of consumer to delete

        For more information check https://redis.io/commands/xgroup-delconsumer
        """
        return self.execute_command("XGROUP DELCONSUMER", name, groupname, consumername)

    def xgroup_destroy(self, name, groupname):
        """
        Destroy a consumer group.
        name: name of the stream.
        groupname: name of the consumer group.

        For more information check https://redis.io/commands/xgroup-destroy
        """
        return self.execute_command("XGROUP DESTROY", name, groupname)

    def xgroup_createconsumer(self, name, groupname, consumername):
        """
        Consumers in a consumer group are auto-created every time a new
        consumer name is mentioned by some command.
        They can be explicitly created by using this command.
        name: name of the stream.
        groupname: name of the consumer group.
        consumername: name of consumer to create.

        See: https://redis.io/commands/xgroup-createconsumer
        """
        return self.execute_command(
            "XGROUP CREATECONSUMER", name, groupname, consumername
        )

    def xgroup_setid(self, name, groupname, id):
        """
        Set the consumer group last delivered ID to something else.
        name: name of the stream.
        groupname: name of the consumer group.
        id: ID of the last item in the stream to consider already delivered.

        For more information check https://redis.io/commands/xgroup-setid
        """
        return self.execute_command("XGROUP SETID", name, groupname, id)

    def xinfo_consumers(self, name, groupname):
        """
        Returns general information about the consumers in the group.
        name: name of the stream.
        groupname: name of the consumer group.

        For more information check https://redis.io/commands/xinfo-consumers
        """
        return self.execute_command("XINFO CONSUMERS", name, groupname)

    def xinfo_groups(self, name):
        """
        Returns general information about the consumer groups of the stream.
        name: name of the stream.

        For more information check https://redis.io/commands/xinfo-groups
        """
        return self.execute_command("XINFO GROUPS", name)

    def xinfo_stream(self, name, full=False):
        """
        Returns general information about the stream.
        name: name of the stream.
        full: optional boolean, false by default. Return full summary

        For more information check https://redis.io/commands/xinfo-stream
        """
        pieces = [name]
        options = {}
        if full:
            pieces.append(b"FULL")
            options = {"full": full}
        return self.execute_command("XINFO STREAM", *pieces, **options)

    def xlen(self, name):
        """
        Returns the number of elements in a given stream.

        For more information check https://redis.io/commands/xlen
        """
        return self.execute_command("XLEN", name)

    def xpending(self, name, groupname):
        """
        Returns information about pending messages of a group.
        name: name of the stream.
        groupname: name of the consumer group.

        For more information check https://redis.io/commands/xpending
        """
        return self.execute_command("XPENDING", name, groupname)

    def xpending_range(
        self,
        name,
        groupname,
        idle=None,
        min=None,
        max=None,
        count=None,
        consumername=None,
    ):
        """
        Returns information about pending messages, in a range.

        name: name of the stream.
        groupname: name of the consumer group.
        idle: available from  version 6.2. filter entries by their
        idle-time, given in milliseconds (optional).
        min: minimum stream ID.
        max: maximum stream ID.
        count: number of messages to return
        consumername: name of a consumer to filter by (optional).
        """
        if {min, max, count} == {None}:
            if idle is not None or consumername is not None:
                raise DataError(
                    "if XPENDING is provided with idle time"
                    " or consumername, it must be provided"
                    " with min, max and count parameters"
                )
            return self.xpending(name, groupname)

        pieces = [name, groupname]
        if min is None or max is None or count is None:
            raise DataError(
                "XPENDING must be provided with min, max "
                "and count parameters, or none of them."
            )
        # idle
        try:
            if int(idle) < 0:
                raise DataError("XPENDING idle must be a integer >= 0")
            pieces.extend(["IDLE", idle])
        except TypeError:
            pass
        # count
        try:
            if int(count) < 0:
                raise DataError("XPENDING count must be a integer >= 0")
            pieces.extend([min, max, count])
        except TypeError:
            pass
        # consumername
        if consumername:
            pieces.append(consumername)

        return self.execute_command("XPENDING", *pieces, parse_detail=True)

    def xrange(self, name, min="-", max="+", count=None):
        """
        Read stream values within an interval.
        name: name of the stream.
        start: first stream ID. defaults to '-',
               meaning the earliest available.
        finish: last stream ID. defaults to '+',
                meaning the latest available.
        count: if set, only return this many items, beginning with the
               earliest available.

        For more information check https://redis.io/commands/xrange
        """
        pieces = [min, max]
        if count is not None:
            if not isinstance(count, int) or count < 1:
                raise DataError("XRANGE count must be a positive integer")
            pieces.append(b"COUNT")
            pieces.append(str(count))

        return self.execute_command("XRANGE", name, *pieces)

    def xread(self, streams, count=None, block=None):
        """
        Block and monitor multiple streams for new data.
        streams: a dict of stream names to stream IDs, where
                   IDs indicate the last ID already seen.
        count: if set, only return this many items, beginning with the
               earliest available.
        block: number of milliseconds to wait, if nothing already present.

        For more information check https://redis.io/commands/xread
        """
        pieces = []
        if block is not None:
            if not isinstance(block, int) or block < 0:
                raise DataError("XREAD block must be a non-negative integer")
            pieces.append(b"BLOCK")
            pieces.append(str(block))
        if count is not None:
            if not isinstance(count, int) or count < 1:
                raise DataError("XREAD count must be a positive integer")
            pieces.append(b"COUNT")
            pieces.append(str(count))
        if not isinstance(streams, dict) or len(streams) == 0:
            raise DataError("XREAD streams must be a non empty dict")
        pieces.append(b"STREAMS")
        keys, values = zip(*streams.items())
        pieces.extend(keys)
        pieces.extend(values)
        return self.execute_command("XREAD", *pieces)

    def xreadgroup(
        self, groupname, consumername, streams, count=None, block=None, noack=False
    ):
        """
        Read from a stream via a consumer group.
        groupname: name of the consumer group.
        consumername: name of the requesting consumer.
        streams: a dict of stream names to stream IDs, where
               IDs indicate the last ID already seen.
        count: if set, only return this many items, beginning with the
               earliest available.
        block: number of milliseconds to wait, if nothing already present.
        noack: do not add messages to the PEL

        For more information check https://redis.io/commands/xreadgroup
        """
        pieces = [b"GROUP", groupname, consumername]
        if count is not None:
            if not isinstance(count, int) or count < 1:
                raise DataError("XREADGROUP count must be a positive integer")
            pieces.append(b"COUNT")
            pieces.append(str(count))
        if block is not None:
            if not isinstance(block, int) or block < 0:
                raise DataError("XREADGROUP block must be a non-negative " "integer")
            pieces.append(b"BLOCK")
            pieces.append(str(block))
        if noack:
            pieces.append(b"NOACK")
        if not isinstance(streams, dict) or len(streams) == 0:
            raise DataError("XREADGROUP streams must be a non empty dict")
        pieces.append(b"STREAMS")
        pieces.extend(streams.keys())
        pieces.extend(streams.values())
        return self.execute_command("XREADGROUP", *pieces)

    def xrevrange(self, name, max="+", min="-", count=None):
        """
        Read stream values within an interval, in reverse order.
        name: name of the stream
        start: first stream ID. defaults to '+',
               meaning the latest available.
        finish: last stream ID. defaults to '-',
                meaning the earliest available.
        count: if set, only return this many items, beginning with the
               latest available.

        For more information check https://redis.io/commands/xrevrange
        """
        pieces = [max, min]
        if count is not None:
            if not isinstance(count, int) or count < 1:
                raise DataError("XREVRANGE count must be a positive integer")
            pieces.append(b"COUNT")
            pieces.append(str(count))

        return self.execute_command("XREVRANGE", name, *pieces)

    def xtrim(self, name, maxlen=None, approximate=True, minid=None, limit=None):
        """
        Trims old messages from a stream.
        name: name of the stream.
        maxlen: truncate old stream messages beyond this size
        Can't be specified with minid.
        approximate: actual stream length may be slightly more than maxlen
        minid: the minimum id in the stream to query
        Can't be specified with maxlen.
        limit: specifies the maximum number of entries to retrieve

        For more information check https://redis.io/commands/xtrim
        """
        pieces = []
        if maxlen is not None and minid is not None:
            raise DataError("Only one of ``maxlen`` or ``minid`` " "may be specified")

        if maxlen is not None:
            pieces.append(b"MAXLEN")
        if minid is not None:
            pieces.append(b"MINID")
        if approximate:
            pieces.append(b"~")
        if maxlen is not None:
            pieces.append(maxlen)
        if minid is not None:
            pieces.append(minid)
        if limit is not None:
            pieces.append(b"LIMIT")
            pieces.append(limit)

        return self.execute_command("XTRIM", name, *pieces)
