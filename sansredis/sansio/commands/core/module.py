from sansredis.sansio.commands.base import CommandsProtocol


class ModuleCommands(CommandsProtocol):
    """
    Redis Module commands.
    see: https://redis.io/topics/modules-intro
    """

    def module_load(self, path, *args):
        """
        Loads the module from ``path``.
        Passes all ``*args`` to the module, during loading.
        Raises ``ModuleError`` if a module is not found at ``path``.

        For more information check https://redis.io/commands/module-load
        """
        return self.execute_command("MODULE LOAD", path, *args)

    def module_unload(self, name):
        """
        Unloads the module ``name``.
        Raises ``ModuleError`` if ``name`` is not in loaded modules.

        For more information check https://redis.io/commands/module-unload
        """
        return self.execute_command("MODULE UNLOAD", name)

    def module_list(self):
        """
        Returns a list of dictionaries containing the name and version of
        all loaded modules.

        For more information check https://redis.io/commands/module-list
        """
        return self.execute_command("MODULE LIST")

    def command_info(self):
        raise NotImplementedError(
            "COMMAND INFO is intentionally not implemented in the client."
        )

    def command_count(self):
        return self.execute_command("COMMAND COUNT")

    def command_getkeys(self, *args):
        return self.execute_command("COMMAND GETKEYS", *args)

    def command(self):
        return self.execute_command("COMMAND")
