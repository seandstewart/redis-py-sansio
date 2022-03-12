from sansredis.sansio.commands.base import CommandsProtocol


class PubSubCommands(CommandsProtocol):
    """
    Redis PubSub commands.
    see https://redis.io/topics/pubsub
    """

    def publish(self, channel, message, **kwargs):
        """
        Publish ``message`` on ``channel``.
        Returns the number of subscribers the message was delivered to.

        For more information check https://redis.io/commands/publish
        """
        return self.execute_command("PUBLISH", channel, message, **kwargs)

    def pubsub_channels(self, pattern="*", **kwargs):
        """
        Return a list of channels that have at least one subscriber

        For more information check https://redis.io/commands/pubsub-channels
        """
        return self.execute_command("PUBSUB CHANNELS", pattern, **kwargs)

    def pubsub_numpat(self, **kwargs):
        """
        Returns the number of subscriptions to patterns

        For more information check https://redis.io/commands/pubsub-numpat
        """
        return self.execute_command("PUBSUB NUMPAT", **kwargs)

    def pubsub_numsub(self, *args, **kwargs):
        """
        Return a list of (channel, number of subscribers) tuples
        for each channel given in ``*args``

        For more information check https://redis.io/commands/pubsub-numsub
        """
        return self.execute_command("PUBSUB NUMSUB", *args, **kwargs)
