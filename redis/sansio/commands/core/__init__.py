from __future__ import annotations

from redis.sansio.commands.core.acl import ACLCommands
from redis.sansio.commands.core.cluster import ClusterCommands
from redis.sansio.commands.core.geo import GeoCommands
from redis.sansio.commands.core.hash import HashCommands
from redis.sansio.commands.core.hyperlog import HyperlogCommands
from redis.sansio.commands.core.key import BasicKeyCommands
from redis.sansio.commands.core.list import ListCommands
from redis.sansio.commands.core.management import ManagementCommands
from redis.sansio.commands.core.module import ModuleCommands
from redis.sansio.commands.core.pubsub import PubSubCommands
from redis.sansio.commands.core.scan import ScanCommands
from redis.sansio.commands.core.script import ScriptCommands
from redis.sansio.commands.core.set import SetCommands
from redis.sansio.commands.core.stream import StreamCommands
from redis.sansio.commands.core.zset import SortedSetCommands


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
