from __future__ import annotations

from sansredis.sansio.commands.core.acl import ACLCommands
from sansredis.sansio.commands.core.cluster import ClusterCommands
from sansredis.sansio.commands.core.geo import GeoCommands
from sansredis.sansio.commands.core.hash import HashCommands
from sansredis.sansio.commands.core.hyperlog import HyperlogCommands
from sansredis.sansio.commands.core.key import BasicKeyCommands
from sansredis.sansio.commands.core.list import ListCommands
from sansredis.sansio.commands.core.management import ManagementCommands
from sansredis.sansio.commands.core.module import ModuleCommands
from sansredis.sansio.commands.core.pubsub import PubSubCommands
from sansredis.sansio.commands.core.scan import ScanCommands
from sansredis.sansio.commands.core.script import ScriptCommands
from sansredis.sansio.commands.core.set import SetCommands
from sansredis.sansio.commands.core.stream import StreamCommands
from sansredis.sansio.commands.core.zset import SortedSetCommands


class DataAccessCommands(
    BasicKeyCommands,
    HyperlogCommands,
    HashCommands,
    GeoCommands,
    ListCommands,
    ScanCommands,
    SetCommands,
    StreamCommands,
    SortedSetCommands,
):
    """
    A class containing all of the implemented data access redis commands.
    This class is to be used as a mixin.
    """


class CoreCommands(
    ACLCommands,
    ClusterCommands,
    DataAccessCommands,
    ManagementCommands,
    ModuleCommands,
    PubSubCommands,
    ScriptCommands,
):
    """
    A class containing all of the implemented redis commands. This class is
    to be used as a mixin.
    """
