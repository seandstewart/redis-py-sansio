from redis.sansio.commands.base import CommandsProtocol
from redis.sansio.exceptions import DataError


class GeoCommands(CommandsProtocol):
    """
    Redis Geospatial commands.
    see: https://redis.com/redis-best-practices/indexing-patterns/geospatial/
    """

    def geoadd(self, name, values, nx=False, xx=False, ch=False):
        """
        Add the specified geospatial items to the specified key identified
        by the ``name`` argument. The Geospatial items are given as ordered
        members of the ``values`` argument, each item or place is formed by
        the triad longitude, latitude and name.

        Note: You can use ZREM to remove elements.

        ``nx`` forces ZADD to only create new elements and not to update
        scores for elements that already exist.

        ``xx`` forces ZADD to only update scores of elements that already
        exist. New elements will not be added.

        ``ch`` modifies the return value to be the numbers of elements changed.
        Changed elements include new elements that were added and elements
        whose scores changed.

        For more information check https://redis.io/commands/geoadd
        """
        if nx and xx:
            raise DataError("GEOADD allows either 'nx' or 'xx', not both")
        if len(values) % 3 != 0:
            raise DataError("GEOADD requires places with lon, lat and name" " values")
        pieces = [name]
        if nx:
            pieces.append("NX")
        if xx:
            pieces.append("XX")
        if ch:
            pieces.append("CH")
        pieces.extend(values)
        return self.execute_command("GEOADD", *pieces)

    def geodist(self, name, place1, place2, unit=None):
        """
        Return the distance between ``place1`` and ``place2`` members of the
        ``name`` key.
        The units must be one of the following : m, km mi, ft. By default
        meters are used.

        For more information check https://redis.io/commands/geodist
        """
        pieces = [name, place1, place2]
        if unit and unit not in ("m", "km", "mi", "ft"):
            raise DataError("GEODIST invalid unit")
        elif unit:
            pieces.append(unit)
        return self.execute_command("GEODIST", *pieces)

    def geohash(self, name, *values):
        """
        Return the geo hash string for each item of ``values`` members of
        the specified key identified by the ``name`` argument.

        For more information check https://redis.io/commands/geohash
        """
        return self.execute_command("GEOHASH", name, *values)

    def geopos(self, name, *values):
        """
        Return the positions of each item of ``values`` as members of
        the specified key identified by the ``name`` argument. Each position
        is represented by the pairs lon and lat.

        For more information check https://redis.io/commands/geopos
        """
        return self.execute_command("GEOPOS", name, *values)

    def georadius(
        self,
        name,
        longitude,
        latitude,
        radius,
        unit=None,
        withdist=False,
        withcoord=False,
        withhash=False,
        count=None,
        sort=None,
        store=None,
        store_dist=None,
        any=False,
    ):
        """
        Return the members of the specified key identified by the
        ``name`` argument which are within the borders of the area specified
        with the ``latitude`` and ``longitude`` location and the maximum
        distance from the center specified by the ``radius`` value.

        The units must be one of the following : m, km mi, ft. By default

        ``withdist`` indicates to return the distances of each place.

        ``withcoord`` indicates to return the latitude and longitude of
        each place.

        ``withhash`` indicates to return the geohash string of each place.

        ``count`` indicates to return the number of elements up to N.

        ``sort`` indicates to return the places in a sorted way, ASC for
        nearest to fairest and DESC for fairest to nearest.

        ``store`` indicates to save the places names in a sorted set named
        with a specific key, each element of the destination sorted set is
        populated with the score got from the original geo sorted set.

        ``store_dist`` indicates to save the places names in a sorted set
        named with a specific key, instead of ``store`` the sorted set
        destination score is set with the distance.

        For more information check https://redis.io/commands/georadius
        """
        return self._georadiusgeneric(
            "GEORADIUS",
            name,
            longitude,
            latitude,
            radius,
            unit=unit,
            withdist=withdist,
            withcoord=withcoord,
            withhash=withhash,
            count=count,
            sort=sort,
            store=store,
            store_dist=store_dist,
            any=any,
        )

    def georadiusbymember(
        self,
        name,
        member,
        radius,
        unit=None,
        withdist=False,
        withcoord=False,
        withhash=False,
        count=None,
        sort=None,
        store=None,
        store_dist=None,
        any=False,
    ):
        """
        This command is exactly like ``georadius`` with the sole difference
        that instead of taking, as the center of the area to query, a longitude
        and latitude value, it takes the name of a member already existing
        inside the geospatial index represented by the sorted set.

        For more information check https://redis.io/commands/georadiusbymember
        """
        return self._georadiusgeneric(
            "GEORADIUSBYMEMBER",
            name,
            member,
            radius,
            unit=unit,
            withdist=withdist,
            withcoord=withcoord,
            withhash=withhash,
            count=count,
            sort=sort,
            store=store,
            store_dist=store_dist,
            any=any,
        )

    def _georadiusgeneric(self, command, *args, **kwargs):
        pieces = list(args)
        if kwargs["unit"] and kwargs["unit"] not in ("m", "km", "mi", "ft"):
            raise DataError("GEORADIUS invalid unit")
        elif kwargs["unit"]:
            pieces.append(kwargs["unit"])
        else:
            pieces.append(
                "m",
            )

        if kwargs["any"] and kwargs["count"] is None:
            raise DataError("``any`` can't be provided without ``count``")

        for arg_name, byte_repr in (
            ("withdist", "WITHDIST"),
            ("withcoord", "WITHCOORD"),
            ("withhash", "WITHHASH"),
        ):
            if kwargs[arg_name]:
                pieces.append(byte_repr)

        if kwargs["count"] is not None:
            pieces.extend(["COUNT", kwargs["count"]])
            if kwargs["any"]:
                pieces.append("ANY")

        if kwargs["sort"]:
            if kwargs["sort"] == "ASC":
                pieces.append("ASC")
            elif kwargs["sort"] == "DESC":
                pieces.append("DESC")
            else:
                raise DataError("GEORADIUS invalid sort")

        if kwargs["store"] and kwargs["store_dist"]:
            raise DataError("GEORADIUS store and store_dist cant be set" " together")

        if kwargs["store"]:
            pieces.extend([b"STORE", kwargs["store"]])

        if kwargs["store_dist"]:
            pieces.extend([b"STOREDIST", kwargs["store_dist"]])

        return self.execute_command(command, *pieces, **kwargs)

    def geosearch(
        self,
        name,
        member=None,
        longitude=None,
        latitude=None,
        unit="m",
        radius=None,
        width=None,
        height=None,
        sort=None,
        count=None,
        any=False,
        withcoord=False,
        withdist=False,
        withhash=False,
    ):
        """
        Return the members of specified key identified by the
        ``name`` argument, which are within the borders of the
        area specified by a given shape. This command extends the
        GEORADIUS command, so in addition to searching within circular
        areas, it supports searching within rectangular areas.
        This command should be used in place of the deprecated
        GEORADIUS and GEORADIUSBYMEMBER commands.
        ``member`` Use the position of the given existing
         member in the sorted set. Can't be given with ``longitude``
         and ``latitude``.
        ``longitude`` and ``latitude`` Use the position given by
        this coordinates. Can't be given with ``member``
        ``radius`` Similar to GEORADIUS, search inside circular
        area according the given radius. Can't be given with
        ``height`` and ``width``.
        ``height`` and ``width`` Search inside an axis-aligned
        rectangle, determined by the given height and width.
        Can't be given with ``radius``
        ``unit`` must be one of the following : m, km, mi, ft.
        `m` for meters (the default value), `km` for kilometers,
        `mi` for miles and `ft` for feet.
        ``sort`` indicates to return the places in a sorted way,
        ASC for nearest to farest and DESC for farest to nearest.
        ``count`` limit the results to the first count matching items.
        ``any`` is set to True, the command will return as soon as
        enough matches are found. Can't be provided without ``count``
        ``withdist`` indicates to return the distances of each place.
        ``withcoord`` indicates to return the latitude and longitude of
        each place.
        ``withhash`` indicates to return the geohash string of each place.

        For more information check https://redis.io/commands/geosearch
        """

        return self._geosearchgeneric(
            "GEOSEARCH",
            name,
            member=member,
            longitude=longitude,
            latitude=latitude,
            unit=unit,
            radius=radius,
            width=width,
            height=height,
            sort=sort,
            count=count,
            any=any,
            withcoord=withcoord,
            withdist=withdist,
            withhash=withhash,
            store=None,
            store_dist=None,
        )

    def geosearchstore(
        self,
        dest,
        name,
        member=None,
        longitude=None,
        latitude=None,
        unit="m",
        radius=None,
        width=None,
        height=None,
        sort=None,
        count=None,
        any=False,
        storedist=False,
    ):
        """
        This command is like GEOSEARCH, but stores the result in
        ``dest``. By default, it stores the results in the destination
        sorted set with their geospatial information.
        if ``store_dist`` set to True, the command will stores the
        items in a sorted set populated with their distance from the
        center of the circle or box, as a floating-point number.

        For more information check https://redis.io/commands/geosearchstore
        """
        return self._geosearchgeneric(
            "GEOSEARCHSTORE",
            dest,
            name,
            member=member,
            longitude=longitude,
            latitude=latitude,
            unit=unit,
            radius=radius,
            width=width,
            height=height,
            sort=sort,
            count=count,
            any=any,
            withcoord=None,
            withdist=None,
            withhash=None,
            store=None,
            store_dist=storedist,
        )

    def _geosearchgeneric(self, command, *args, **kwargs):
        pieces = list(args)

        # FROMMEMBER or FROMLONLAT
        if kwargs["member"] is None:
            if kwargs["longitude"] is None or kwargs["latitude"] is None:
                raise DataError(
                    "GEOSEARCH must have member or" " longitude and latitude"
                )
        if kwargs["member"]:
            if kwargs["longitude"] or kwargs["latitude"]:
                raise DataError(
                    "GEOSEARCH member and longitude or latitude" " cant be set together"
                )
            pieces.extend([b"FROMMEMBER", kwargs["member"]])
        if kwargs["longitude"] and kwargs["latitude"]:
            pieces.extend([b"FROMLONLAT", kwargs["longitude"], kwargs["latitude"]])

        # BYRADIUS or BYBOX
        if kwargs["radius"] is None:
            if kwargs["width"] is None or kwargs["height"] is None:
                raise DataError("GEOSEARCH must have radius or" " width and height")
        if kwargs["unit"] is None:
            raise DataError("GEOSEARCH must have unit")
        if kwargs["unit"].lower() not in ("m", "km", "mi", "ft"):
            raise DataError("GEOSEARCH invalid unit")
        if kwargs["radius"]:
            if kwargs["width"] or kwargs["height"]:
                raise DataError(
                    "GEOSEARCH radius and width or height" " cant be set together"
                )
            pieces.extend([b"BYRADIUS", kwargs["radius"], kwargs["unit"]])
        if kwargs["width"] and kwargs["height"]:
            pieces.extend([b"BYBOX", kwargs["width"], kwargs["height"], kwargs["unit"]])

        # sort
        if kwargs["sort"]:
            if kwargs["sort"].upper() == "ASC":
                pieces.append(b"ASC")
            elif kwargs["sort"].upper() == "DESC":
                pieces.append(b"DESC")
            else:
                raise DataError("GEOSEARCH invalid sort")

        # count any
        if kwargs["count"]:
            pieces.extend([b"COUNT", kwargs["count"]])
            if kwargs["any"]:
                pieces.append(b"ANY")
        elif kwargs["any"]:
            raise DataError("GEOSEARCH ``any`` can't be provided " "without count")

        # other properties
        for arg_name, byte_repr in (
            ("withdist", b"WITHDIST"),
            ("withcoord", b"WITHCOORD"),
            ("withhash", b"WITHHASH"),
            ("store_dist", b"STOREDIST"),
        ):
            if kwargs[arg_name]:
                pieces.append(byte_repr)

        return self.execute_command(command, *pieces, **kwargs)
