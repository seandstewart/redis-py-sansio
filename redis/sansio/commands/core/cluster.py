from __future__ import annotations

from redis.sansio.commands.base import CommandsProtocol


class ClusterCommands(CommandsProtocol):
    """
    Class for Redis Cluster commands
    """

    def cluster(self, cluster_arg, *args, **kwargs):
        return self.execute_command(f"CLUSTER {cluster_arg.upper()}", *args, **kwargs)

    def readwrite(self, **kwargs):
        """
        Disables read queries for a operator to a Redis Cluster slave node.

        For more information check https://redis.io/commands/readwrite
        """
        return self.execute_command("READWRITE", **kwargs)

    def readonly(self, **kwargs):
        """
        Enables read queries for a operator to a Redis Cluster replica node.

        For more information check https://redis.io/commands/readonly
        """
        return self.execute_command("READONLY", **kwargs)
