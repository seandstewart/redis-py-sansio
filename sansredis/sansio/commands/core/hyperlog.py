from sansredis.sansio.commands.base import CommandsProtocol


class HyperlogCommands(CommandsProtocol):
    """
    Redis commands of HyperLogLogs data type.
    see: https://redis.io/topics/data-types-intro#hyperloglogs
    """

    def pfadd(self, name, *values):
        """
        Adds the specified elements to the specified HyperLogLog.

        For more information check https://redis.io/commands/pfadd
        """
        return self.execute_command("PFADD", name, *values)

    def pfcount(self, *sources):
        """
        Return the approximated cardinality of
        the set observed by the HyperLogLog at key(s).

        For more information check https://redis.io/commands/pfcount
        """
        return self.execute_command("PFCOUNT", *sources)

    def pfmerge(self, dest, *sources):
        """
        Merge N different HyperLogLogs into a single one.

        For more information check https://redis.io/commands/pfmerge
        """
        return self.execute_command("PFMERGE", dest, *sources)
