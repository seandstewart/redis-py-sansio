from __future__ import annotations

import hashlib

from sansredis.sansio.commands.base import CommandsProtocol
from sansredis.sansio.exceptions import DataError, NoScriptError


class ScriptCommands(CommandsProtocol):
    """
    Redis Lua script commands. see:
    https://redis.com/ebook/part-3-next-steps/chapter-11-scripting-redis-with-lua/
    """

    def _eval(
        self, command: str, script: str, numkeys: int, *keys_and_args: list
    ) -> str:
        return self.execute_command(command, script, numkeys, *keys_and_args)

    def eval(self, script: str, numkeys: int, *keys_and_args: list) -> str:
        """
        Execute the Lua ``script``, specifying the ``numkeys`` the script
        will touch and the key names and argument values in ``keys_and_args``.
        Returns the result of the script.

        In practice, use the object returned by ``register_script``. This
        function exists purely for Redis API completion.

        For more information check  https://redis.io/commands/eval
        """
        return self._eval("EVAL", script, numkeys, *keys_and_args)

    def eval_ro(self, script: str, numkeys: int, *keys_and_args: list) -> str:
        """
        The read-only variant of the EVAL command

        Execute the read-only Lue ``script`` specifying the ``numkeys`` the script
        will touch and the key names and argument values in ``keys_and_args``.
        Returns the result of the script.

        For more information check  https://redis.io/commands/eval_ro
        """
        return self._eval("EVAL_RO", script, numkeys, *keys_and_args)

    def _evalsha(
        self, command: str, sha: str, numkeys: int, *keys_and_args: list
    ) -> str:
        return self.execute_command(command, sha, numkeys, *keys_and_args)

    def evalsha(self, sha: str, numkeys: int, *keys_and_args: list) -> str:
        """
        Use the ``sha`` to execute a Lua script already registered via EVAL
        or SCRIPT LOAD. Specify the ``numkeys`` the script will touch and the
        key names and argument values in ``keys_and_args``. Returns the result
        of the script.

        In practice, use the object returned by ``register_script``. This
        function exists purely for Redis API completion.

        For more information check  https://redis.io/commands/evalsha
        """
        return self._evalsha("EVALSHA", sha, numkeys, *keys_and_args)

    def evalsha_ro(self, sha: str, numkeys: int, *keys_and_args: list) -> str:
        """
        The read-only variant of the EVALSHA command

        Use the ``sha`` to execute a read-only Lua script already registered via EVAL
        or SCRIPT LOAD. Specify the ``numkeys`` the script will touch and the
        key names and argument values in ``keys_and_args``. Returns the result
        of the script.

        For more information check  https://redis.io/commands/evalsha_ro
        """
        return self._evalsha("EVALSHA_RO", sha, numkeys, *keys_and_args)

    def script_exists(self, *args):
        """
        Check if a script exists in the script cache by specifying the SHAs of
        each script as ``args``. Returns a list of boolean values indicating if
        if each already script exists in the cache.

        For more information check  https://redis.io/commands/script-exists
        """
        return self.execute_command("SCRIPT EXISTS", *args)

    def script_debug(self, *args):
        raise NotImplementedError(
            "SCRIPT DEBUG is intentionally not implemented in the client."
        )

    def script_flush(self, sync_type=None):
        """Flush all scripts from the script cache.
        ``sync_type`` is by default SYNC (synchronous) but it can also be
                      ASYNC.
        For more information check  https://redis.io/commands/script-flush
        """

        # Redis pre 6 had no sync_type.
        if sync_type not in ["SYNC", "ASYNC", None]:
            raise DataError(
                "SCRIPT FLUSH defaults to SYNC in redis > 6.2, or "
                "accepts SYNC/ASYNC. For older versions, "
                "of redis leave as None."
            )
        if sync_type is None:
            pieces = []
        else:
            pieces = [sync_type]
        return self.execute_command("SCRIPT FLUSH", *pieces)

    def script_kill(self):
        """
        Kill the currently executing Lua script

        For more information check https://redis.io/commands/script-kill
        """
        return self.execute_command("SCRIPT KILL")

    def script_load(self, script):
        """
        Load a Lua ``script`` into the script cache. Returns the SHA.

        For more information check https://redis.io/commands/script-load
        """
        return self.execute_command("SCRIPT LOAD", script)

    def register_script(self, script):
        """
        Register a Lua ``script`` specifying the ``keys`` it will touch.
        Returns a Script object that is callable and hides the complexity of
        deal with scripts, keys, and shas. This is the preferred way to work
        with Lua scripts.
        """
        return Script(self, script)


class Script(CommandsProtocol):
    """
    An executable Lua script object returned by ``register_script``
    """

    def __init__(self, registered_client, script):
        self.registered_client = registered_client
        self.script = script
        # Precalculate and store the SHA1 hex digest of the script.

        if isinstance(script, str):
            # We need the encoding from the client in order to generate an
            # accurate byte representation of the script
            encoder = registered_client.get_encoder()
            script = encoder(script)
        self.sha = hashlib.sha1(script).hexdigest()

    def __call__(self, keys=(), args=(), client=None):
        """Execute the script, passing any required ``args``"""
        if client is None:
            client = self.registered_client
        args = tuple(keys) + tuple(args)
        # make sure the Redis server knows about the script
        from sansredis.clients import Pipeline

        if isinstance(client, Pipeline):
            # Make sure the pipeline can register the script before executing.
            client.scripts.add(self)
        try:
            return client.evalsha(self.sha, len(keys), *args)
        except NoScriptError:
            # Maybe the client is pointed to a different server than the client
            # that created this instance?
            # Overwrite the sha just in case there was a discrepancy.
            self.sha = client.script_load(self.script)
            return client.evalsha(self.sha, len(keys), *args)
