from sansredis.sansio.commands.base import CommandsProtocol
from sansredis.sansio.commands.normalize import iterkeysargs
from sansredis.sansio.exceptions import DataError, RedisConnectionError, RedisError


class ManagementCommands(CommandsProtocol):
    """
    Redis management commands
    """

    def auth(self):
        """
        This function throws a NotImplementedError since it is intentionally
        not supported.
        """
        raise NotImplementedError(
            "AUTH is intentionally not implemented in the client."
        )

    def bgrewriteaof(self, **kwargs):
        """Tell the Redis server to rewrite the AOF file from data in memory.

        For more information check https://redis.io/commands/bgrewriteaof
        """
        return self.execute_command("BGREWRITEAOF", **kwargs)

    def bgsave(self, schedule=True, **kwargs):
        """
        Tell the Redis server to save its data to disk.  Unlike save(),
        this method is asynchronous and returns immediately.

        For more information check https://redis.io/commands/bgsave
        """
        pieces = []
        if schedule:
            pieces.append("SCHEDULE")
        return self.execute_command("BGSAVE", *pieces, **kwargs)

    def role(self):
        """
        Provide information on the role of a Redis instance in
        the context of replication, by returning if the instance
        is currently a master, slave, or sentinel.

        For more information check https://redis.io/commands/role
        """
        return self.execute_command("ROLE")

    def client_kill(self, address, **kwargs):
        """Disconnects the client at ``address`` (ip:port)

        For more information check https://redis.io/commands/client-kill
        """
        return self.execute_command("CLIENT KILL", address, **kwargs)

    def client_kill_filter(
        self,
        _id=None,
        _type=None,
        addr=None,
        skipme=None,
        laddr=None,
        user=None,
        **kwargs,
    ):
        """
        Disconnects client(s) using a variety of filter options
        :param id: Kills a client by its unique ID field
        :param type: Kills a client by type where type is one of 'normal',
        'master', 'slave' or 'pubsub'
        :param addr: Kills a client by its 'address:port'
        :param skipme: If True, then the client calling the command
        will not get killed even if it is identified by one of the filter
        options. If skipme is not provided, the server defaults to skipme=True
        :param laddr: Kills a client by its 'local (bind) address:port'
        :param user: Kills a client for a specific user name
        """
        args = []
        if _type is not None:
            client_types = ("normal", "master", "slave", "pubsub")
            if str(_type).lower() not in client_types:
                raise DataError(f"CLIENT KILL type must be one of {client_types!r}")
            args.extend((b"TYPE", _type))
        if skipme is not None:
            if not isinstance(skipme, bool):
                raise DataError("CLIENT KILL skipme must be a bool")
            if skipme:
                args.extend((b"SKIPME", b"YES"))
            else:
                args.extend((b"SKIPME", b"NO"))
        if _id is not None:
            args.extend((b"ID", _id))
        if addr is not None:
            args.extend((b"ADDR", addr))
        if laddr is not None:
            args.extend((b"LADDR", laddr))
        if user is not None:
            args.extend((b"USER", user))
        if not args:
            raise DataError(
                "CLIENT KILL <filter> <value> ... ... <filter> "
                "<value> must specify at least one filter"
            )
        return self.execute_command("CLIENT KILL", *args, **kwargs)

    def client_info(self, **kwargs):
        """
        Returns information and statistics about the current
        client operator.

        For more information check https://redis.io/commands/client-info
        """
        return self.execute_command("CLIENT INFO", **kwargs)

    def client_list(self, _type=None, client_id=[], **kwargs):
        """
        Returns a list of currently connected clients.
        If type of client specified, only that type will be returned.
        :param _type: optional. one of the client types (normal, master,
         replica, pubsub)
        :param client_id: optional. a list of client ids

        For more information check https://redis.io/commands/client-list
        """
        args = []
        if _type is not None:
            client_types = ("normal", "master", "replica", "pubsub")
            if str(_type).lower() not in client_types:
                raise DataError(f"CLIENT LIST _type must be one of {client_types!r}")
            args.append(b"TYPE")
            args.append(_type)
        if not isinstance(client_id, list):
            raise DataError("client_id must be a list")
        if client_id != []:
            args.append(b"ID")
            args.append(" ".join(client_id))
        return self.execute_command("CLIENT LIST", *args, **kwargs)

    def client_getname(self, **kwargs):
        """
        Returns the current operator name

        For more information check https://redis.io/commands/client-getname
        """
        return self.execute_command("CLIENT GETNAME", **kwargs)

    def client_getredir(self, **kwargs):
        """
        Returns the ID (an integer) of the client to whom we are
        redirecting tracking notifications.

        see: https://redis.io/commands/client-getredir
        """
        return self.execute_command("CLIENT GETREDIR", **kwargs)

    def client_reply(self, reply, **kwargs):
        """
        Enable and disable redis server replies.
        ``reply`` Must be ON OFF or SKIP,
            ON - The default most with server replies to commands
            OFF - Disable server responses to commands
            SKIP - Skip the response of the immediately following command.

        Note: When setting OFF or SKIP replies, you will need a client object
        with a timeout specified in seconds, and will need to catch the
        TimeoutError.
              The test_client_reply unit test illustrates this, and
              conftest.py has a client with a timeout.

        See https://redis.io/commands/client-reply
        """
        replies = ["ON", "OFF", "SKIP"]
        if reply not in replies:
            raise DataError(f"CLIENT REPLY must be one of {replies!r}")
        return self.execute_command("CLIENT REPLY", reply, **kwargs)

    def client_id(self, **kwargs):
        """
        Returns the current operator id

        For more information check https://redis.io/commands/client-id
        """
        return self.execute_command("CLIENT ID", **kwargs)

    def client_tracking_on(
        self,
        clientid=None,
        prefix=[],
        bcast=False,
        optin=False,
        optout=False,
        noloop=False,
    ):
        """
        Turn on the tracking mode.
        For more information about the options look at client_tracking func.

        See https://redis.io/commands/client-tracking
        """
        return self.client_tracking(
            True, clientid, prefix, bcast, optin, optout, noloop
        )

    def client_tracking_off(
        self,
        clientid=None,
        prefix=[],
        bcast=False,
        optin=False,
        optout=False,
        noloop=False,
    ):
        """
        Turn off the tracking mode.
        For more information about the options look at client_tracking func.

        See https://redis.io/commands/client-tracking
        """
        return self.client_tracking(
            False, clientid, prefix, bcast, optin, optout, noloop
        )

    def client_tracking(
        self,
        on=True,
        clientid=None,
        prefix=[],
        bcast=False,
        optin=False,
        optout=False,
        noloop=False,
        **kwargs,
    ):
        """
        Enables the tracking feature of the Redis server, that is used
        for server assisted client side caching.

        ``on`` indicate for tracking on or tracking off. The dafualt is on.

        ``clientid`` send invalidation messages to the operator with
        the specified ID.

        ``bcast`` enable tracking in broadcasting mode. In this mode
        invalidation messages are reported for all the prefixes
        specified, regardless of the keys requested by the operator.

        ``optin``  when broadcasting is NOT active, normally don't track
        keys in read only commands, unless they are called immediately
        after a CLIENT CACHING yes command.

        ``optout`` when broadcasting is NOT active, normally track keys in
        read only commands, unless they are called immediately after a
        CLIENT CACHING no command.

        ``noloop`` don't send notifications about keys modified by this
        operator itself.

        ``prefix``  for broadcasting, register a given key prefix, so that
        notifications will be provided only for keys starting with this string.

        See https://redis.io/commands/client-tracking
        """

        if len(prefix) != 0 and bcast is False:
            raise DataError("Prefix can only be used with bcast")

        pieces = ["ON"] if on else ["OFF"]
        if clientid is not None:
            pieces.extend(["REDIRECT", clientid])
        for p in prefix:
            pieces.extend(["PREFIX", p])
        if bcast:
            pieces.append("BCAST")
        if optin:
            pieces.append("OPTIN")
        if optout:
            pieces.append("OPTOUT")
        if noloop:
            pieces.append("NOLOOP")

        return self.execute_command("CLIENT TRACKING", *pieces)

    def client_trackinginfo(self, **kwargs):
        """
        Returns the information about the current client operator's
        use of the server assisted client side cache.

        See https://redis.io/commands/client-trackinginfo
        """
        return self.execute_command("CLIENT TRACKINGINFO", **kwargs)

    def client_setname(self, name, **kwargs):
        """
        Sets the current operator name

        For more information check https://redis.io/commands/client-setname
        """
        return self.execute_command("CLIENT SETNAME", name, **kwargs)

    def client_unblock(self, client_id, error=False, **kwargs):
        """
        Unblocks a operator by its client id.
        If ``error`` is True, unblocks the client with a special error message.
        If ``error`` is False (default), the client is unblocked using the
        regular timeout mechanism.

        For more information check https://redis.io/commands/client-unblock
        """
        args = ["CLIENT UNBLOCK", int(client_id)]
        if error:
            args.append(b"ERROR")
        return self.execute_command(*args, **kwargs)

    def client_pause(self, timeout, all=True, **kwargs):
        """
        Suspend all the Redis clients for the specified amount of time
        :param timeout: milliseconds to pause clients

        For more information check https://redis.io/commands/client-pause
        :param all: If true (default) all client commands are blocked.
             otherwise, clients are only blocked if they attempt to execute
             a write command.
             For the WRITE mode, some commands have special behavior:
                 EVAL/EVALSHA: Will block client for all scripts.
                 PUBLISH: Will block client.
                 PFCOUNT: Will block client.
                 WAIT: Acknowledgments will be delayed, so this command will
                 appear blocked.
        """
        args = ["CLIENT PAUSE", str(timeout)]
        if not isinstance(timeout, int):
            raise DataError("CLIENT PAUSE timeout must be an integer")
        if not all:
            args.append("WRITE")
        return self.execute_command(*args, **kwargs)

    def client_unpause(self, **kwargs):
        """
        Unpause all redis clients

        For more information check https://redis.io/commands/client-unpause
        """
        return self.execute_command("CLIENT UNPAUSE", **kwargs)

    def client_no_evict(self, mode: str) -> str:
        """
        Sets the client eviction mode for the current operator.

        For more information check https://redis.io/commands/client-no-evict
        """
        return self.execute_command("CLIENT NO-EVICT", mode)

    def command(self, **kwargs):
        """
        Returns dict reply of details about all Redis commands.

        For more information check https://redis.io/commands/command
        """
        return self.execute_command("COMMAND", **kwargs)

    def command_info(self, **kwargs):
        raise NotImplementedError(
            "COMMAND INFO is intentionally not implemented in the client."
        )

    def command_count(self, **kwargs):
        return self.execute_command("COMMAND COUNT", **kwargs)

    def config_get(self, pattern="*", **kwargs):
        """
        Return a dictionary of configuration based on the ``pattern``

        For more information check https://redis.io/commands/config-get
        """
        return self.execute_command("CONFIG GET", pattern, **kwargs)

    def config_set(self, name, value, **kwargs):
        """Set config item ``name`` with ``value``

        For more information check https://redis.io/commands/config-set
        """
        return self.execute_command("CONFIG SET", name, value, **kwargs)

    def config_resetstat(self, **kwargs):
        """
        Reset runtime statistics

        For more information check https://redis.io/commands/config-resetstat
        """
        return self.execute_command("CONFIG RESETSTAT", **kwargs)

    def config_rewrite(self, **kwargs):
        """
        Rewrite config file with the minimal change to reflect running config.

        For more information check https://redis.io/commands/config-rewrite
        """
        return self.execute_command("CONFIG REWRITE", **kwargs)

    def dbsize(self, **kwargs):
        """
        Returns the number of keys in the current database

        For more information check https://redis.io/commands/dbsize
        """
        return self.execute_command("DBSIZE", **kwargs)

    def debug_object(self, key, **kwargs):
        """
        Returns version specific meta information about a given key

        For more information check https://redis.io/commands/debug-object
        """
        return self.execute_command("DEBUG OBJECT", key, **kwargs)

    def debug_segfault(self, **kwargs):
        raise NotImplementedError(
            """
            DEBUG SEGFAULT is intentionally not implemented in the client.

            For more information check https://redis.io/commands/debug-segfault
            """
        )

    def echo(self, value, **kwargs):
        """
        Echo the string back from the server

        For more information check https://redis.io/commands/echo
        """
        return self.execute_command("ECHO", value, **kwargs)

    def flushall(self, asynchronous=False, **kwargs):
        """
        Delete all keys in all databases on the current host.

        ``asynchronous`` indicates whether the operation is
        executed asynchronously by the server.

        For more information check https://redis.io/commands/flushall
        """
        args = []
        if asynchronous:
            args.append(b"ASYNC")
        return self.execute_command("FLUSHALL", *args, **kwargs)

    def flushdb(self, asynchronous=False, **kwargs):
        """
        Delete all keys in the current database.

        ``asynchronous`` indicates whether the operation is
        executed asynchronously by the server.

        For more information check https://redis.io/commands/flushdb
        """
        args = []
        if asynchronous:
            args.append(b"ASYNC")
        return self.execute_command("FLUSHDB", *args, **kwargs)

    def sync(self):
        """
        Initiates a replication stream from the master.

        For more information check https://redis.io/commands/sync
        """
        return self.execute_command("SYNC")

    def psync(self, replicationid, offset):
        """
        Initiates a replication stream from the master.
        Newer version for `sync`.

        For more information check https://redis.io/commands/sync
        """
        return self.execute_command("PSYNC", replicationid, offset)

    def swapdb(self, first, second, **kwargs):
        """
        Swap two databases

        For more information check https://redis.io/commands/swapdb
        """
        return self.execute_command("SWAPDB", first, second, **kwargs)

    def select(self, index, **kwargs):
        """Select the Redis logical database at index.

        See: https://redis.io/commands/select
        """
        return self.execute_command("SELECT", index, **kwargs)

    def info(self, section=None, **kwargs):
        """
        Returns a dictionary containing information about the Redis server

        The ``section`` option can be used to select a specific section
        of information

        The section option is not supported by older versions of Redis Server,
        and will generate ResponseError

        For more information check https://redis.io/commands/info
        """
        if section is None:
            return self.execute_command("INFO", **kwargs)
        else:
            return self.execute_command("INFO", section, **kwargs)

    def lastsave(self, **kwargs):
        """
        Return a Python datetime object representing the last time the
        Redis database was saved to disk

        For more information check https://redis.io/commands/lastsave
        """
        return self.execute_command("LASTSAVE", **kwargs)

    def lolwut(self, *version_numbers, **kwargs):
        """
        Get the Redis version and a piece of generative computer art

        See: https://redis.io/commands/lolwut
        """
        if version_numbers:
            return self.execute_command("LOLWUT VERSION", *version_numbers, **kwargs)
        else:
            return self.execute_command("LOLWUT", **kwargs)

    def reset(self):
        """Perform a full reset on the operator's server side contenxt.

        See: https://redis.io/commands/reset
        """
        return self.execute_command("RESET")

    def migrate(
        self,
        host,
        port,
        keys,
        destination_db,
        timeout,
        copy=False,
        replace=False,
        auth=None,
        **kwargs,
    ):
        """
        Migrate 1 or more keys from the current Redis server to a different
        server specified by the ``host``, ``port`` and ``destination_db``.

        The ``timeout``, specified in milliseconds, indicates the maximum
        time the operator between the two servers can be idle before the
        command is interrupted.

        If ``copy`` is True, the specified ``keys`` are NOT deleted from
        the source server.

        If ``replace`` is True, this operation will overwrite the keys
        on the destination server if they exist.

        If ``auth`` is specified, authenticate to the destination server with
        the password provided.

        For more information check https://redis.io/commands/migrate
        """
        keys = iterkeysargs(keys, [])
        if not keys:
            raise DataError("MIGRATE requires at least one key")
        pieces = []
        if copy:
            pieces.append(b"COPY")
        if replace:
            pieces.append(b"REPLACE")
        if auth:
            pieces.append(b"AUTH")
            pieces.append(auth)
        pieces.append(b"KEYS")
        pieces.extend(keys)
        return self.execute_command(
            "MIGRATE", host, port, "", destination_db, timeout, *pieces, **kwargs
        )

    def object(self, infotype, key, **kwargs):
        """
        Return the encoding, idletime, or refcount about the key
        """
        return self.execute_command(
            "OBJECT", infotype, key, infotype=infotype, **kwargs
        )

    def memory_doctor(self, **kwargs):
        raise NotImplementedError(
            """
            MEMORY DOCTOR is intentionally not implemented in the client.

            For more information check https://redis.io/commands/memory-doctor
            """
        )

    def memory_help(self, **kwargs):
        raise NotImplementedError(
            """
            MEMORY HELP is intentionally not implemented in the client.

            For more information check https://redis.io/commands/memory-help
            """
        )

    def memory_stats(self, **kwargs):
        """
        Return a dictionary of memory stats

        For more information check https://redis.io/commands/memory-stats
        """
        return self.execute_command("MEMORY STATS", **kwargs)

    def memory_malloc_stats(self, **kwargs):
        """
        Return an internal statistics report from the memory allocator.

        See: https://redis.io/commands/memory-malloc-stats
        """
        return self.execute_command("MEMORY MALLOC-STATS", **kwargs)

    def memory_usage(self, key, samples=None, **kwargs):
        """
        Return the total memory usage for key, its value and associated
        administrative overheads.

        For nested data structures, ``samples`` is the number of elements to
        sample. If left unspecified, the server's default is 5. Use 0 to sample
        all elements.

        For more information check https://redis.io/commands/memory-usage
        """
        args = []
        if isinstance(samples, int):
            args.extend([b"SAMPLES", samples])
        return self.execute_command("MEMORY USAGE", key, *args, **kwargs)

    def memory_purge(self, **kwargs):
        """
        Attempts to purge dirty pages for reclamation by allocator

        For more information check https://redis.io/commands/memory-purge
        """
        return self.execute_command("MEMORY PURGE", **kwargs)

    def ping(self, **kwargs):
        """
        Ping the Redis server

        For more information check https://redis.io/commands/ping
        """
        return self.execute_command("PING", **kwargs)

    def quit(self, **kwargs):
        """
        Ask the server to close the operator.

        For more information check https://redis.io/commands/quit
        """
        return self.execute_command("QUIT", **kwargs)

    def replicaof(self, *args, **kwargs):
        """
        Update the replication settings of a redis replica, on the fly.
        Examples of valid arguments include:
            NO ONE (set no replication)
            host port (set to the host and port of a redis server)

        For more information check  https://redis.io/commands/replicaof
        """
        return self.execute_command("REPLICAOF", *args, **kwargs)

    def save(self, **kwargs):
        """
        Tell the Redis server to save its data to disk,
        blocking until the save is complete

        For more information check https://redis.io/commands/save
        """
        return self.execute_command("SAVE", **kwargs)

    def shutdown(self, save=False, nosave=False, **kwargs):
        """Shutdown the Redis server.  If Redis has persistence configured,
        data will be flushed before shutdown.  If the "save" option is set,
        a data flush will be attempted even if there is no persistence
        configured.  If the "nosave" option is set, no data flush will be
        attempted.  The "save" and "nosave" options cannot both be set.

        For more information check https://redis.io/commands/shutdown
        """
        if save and nosave:
            raise DataError("SHUTDOWN save and nosave cannot both be set")
        args = ["SHUTDOWN"]
        if save:
            args.append("SAVE")
        if nosave:
            args.append("NOSAVE")
        try:
            self.execute_command(*args, **kwargs)
        except RedisConnectionError:
            # a ConnectionError here is expected
            return
        raise RedisError("SHUTDOWN seems to have failed.")

    def slaveof(self, host=None, port=None, **kwargs):
        """
        Set the server to be a replicated slave of the instance identified
        by the ``host`` and ``port``. If called without arguments, the
        instance is promoted to a master instead.

        For more information check https://redis.io/commands/slaveof
        """
        if host is None and port is None:
            return self.execute_command("SLAVEOF", b"NO", b"ONE", **kwargs)
        return self.execute_command("SLAVEOF", host, port, **kwargs)

    def slowlog_get(self, num=None, **kwargs):
        """
        Get the entries from the slowlog. If ``num`` is specified, get the
        most recent ``num`` items.

        For more information check https://redis.io/commands/slowlog-get
        """
        args = ["SLOWLOG GET"]
        if num is not None:
            args.append(num)
        return self.execute_command(*args, **kwargs)

    def slowlog_len(self, **kwargs):
        """
        Get the number of items in the slowlog

        For more information check https://redis.io/commands/slowlog-len
        """
        return self.execute_command("SLOWLOG LEN", **kwargs)

    def slowlog_reset(self, **kwargs):
        """
        Remove all items in the slowlog

        For more information check https://redis.io/commands/slowlog-reset
        """
        return self.execute_command("SLOWLOG RESET", **kwargs)

    def time(self, **kwargs):
        """
        Returns the server time as a 2-item tuple of ints:
        (seconds since epoch, microseconds into this second).

        For more information check https://redis.io/commands/time
        """
        return self.execute_command("TIME", **kwargs)

    def wait(self, num_replicas, timeout, **kwargs):
        """
        Redis synchronous replication
        That returns the number of replicas that processed the query when
        we finally have at least ``num_replicas``, or when the ``timeout`` was
        reached.

        For more information check https://redis.io/commands/wait
        """
        return self.execute_command("WAIT", num_replicas, timeout, **kwargs)

    def hello(self):
        """
        This function throws a NotImplementedError since it is intentionally
        not supported.
        """
        raise NotImplementedError(
            "HELLO is intentionally not implemented in the client."
        )

    def failover(self):
        """
        This function throws a NotImplementedError since it is intentionally
        not supported.
        """
        raise NotImplementedError(
            "FAILOVER is intentionally not implemented in the client."
        )
