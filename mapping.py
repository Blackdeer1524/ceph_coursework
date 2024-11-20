from dataclasses import dataclass, field
from enum import Enum, auto
from typing import (
    Callable,
    Hashable,
    Iterable,
    Iterator,
    NewType,
)
from crush import Tunables, apply
from parser import (
    Bucket,
    OutOfClusterWeight,
    Device,
    DeviceID_T,
    Rule,
    WeightT
)


@dataclass
class Operation:
    class OpType(Enum):
        INSERT = auto()
        UPDATE = auto()
        DELETE = auto()

    object_id: int
    type: OpType


@dataclass
class LogData:
    ops: list[Operation] = field(init=False, default_factory=list)


ObjectID_T = NewType("ObjectID_T", int)


@dataclass(frozen=True)
class EPrimaryRecvSuccess:
    obj: ObjectID_T
    dst: DeviceID_T


@dataclass(frozen=True)
class EPrimaryRecvAcknowledged:
    obj: ObjectID_T
    dst: DeviceID_T


@dataclass(frozen=True)
class EPrimaryRecvFailure:
    id: ObjectID_T
    dst: DeviceID_T


@dataclass(frozen=True)
class EPrimaryReplicationFail:
    obj: ObjectID_T
    dst: DeviceID_T


@dataclass(frozen=True)
class EReplicaRecvAcknowledged:
    obj: ObjectID_T
    replica: DeviceID_T
    primary: DeviceID_T


@dataclass(frozen=True)
class EReplicaRecvSuccess:
    obj: ObjectID_T
    primary: DeviceID_T
    replica: DeviceID_T


@dataclass(frozen=True)
class EReplicaRecvFailure:
    obj: ObjectID_T
    primary: DeviceID_T
    replica: DeviceID_T


@dataclass(frozen=True)
class EPeeringStart:
    id: int
    pg_id: int
    device_ids: list[DeviceID_T]


@dataclass(frozen=True)
class EPeeringSuccess:
    id: int


@dataclass(frozen=True)
class EPeeringFailure:
    id: int


@dataclass()
class EMainloopInteration:
    callback_results: list["Event"]


EventTag = (
    EMainloopInteration
    | EPrimaryRecvSuccess
    | EPrimaryRecvFailure
    | EPrimaryRecvAcknowledged
    | EPrimaryReplicationFail
    | EReplicaRecvSuccess
    | EReplicaRecvFailure
    | EReplicaRecvAcknowledged
    | EPeeringStart
    | EPeeringSuccess
    | EPeeringFailure
)


@dataclass(order=True)
class Event:
    tag: EventTag = field(compare=False)
    time: int
    callback: Callable[[], None] | None = field(compare=False, default=None, repr=False)


def test_proba(p: float, *args: Hashable) -> bool:
    h = hash(args) & 0xFFFF
    cutoff = int(p * 0xFFFF)
    return h >= cutoff


class AliveIntervals:
    def __init__(self, id: int, p_die: float):
        self.id = id
        self.p_die = p_die

    def check_at_time(self, t: int) -> bool:
        return test_proba(self.p_die, self.id, str(t))


@dataclass
class Context:
    current_time: int
    timestep: int
    timesteps_to_peer: int
    # send timeout
    timeout: int
    # how much time it takes to recieve one file from user
    user_conn_speed: dict[DeviceID_T, int]
    # how much time it takes to send one file from one device to another
    conn_speed: dict[tuple[DeviceID_T, DeviceID_T], int]
    failure_proba: dict[DeviceID_T, float]

    alive_intervals_per_device: dict[DeviceID_T, AliveIntervals]

    def do_time_step(self):
        self.current_time += self.timestep


@dataclass
class PlacementGroup:
    id: int
    logs: dict[DeviceID_T, LogData] = field(init=False, default_factory=dict)
    last_sync: int = field(init=False, default=-1)
    _maps: list[list[Device]] = field(init=False, default_factory=list)
    is_peering: bool = field(init=False, default=False)

    @property
    def maps(self) -> list[list[Device]]:
        return self._maps

    def record_mapping(self, m: list[Device]) -> bool:
        if len(self._maps) == 0 or m != self._maps[-1]:
            self._maps.append(m)
            return True
        return False

    def peer(self, context: Context) -> tuple[list[list[Device]], bool]:
        syncing_maps = self._maps[self.last_sync :]
        return syncing_maps, all(
            all(
                any(
                    context.alive_intervals_per_device[d.info.id].check_at_time(
                        context.current_time + j * context.timestep
                    )
                    for d in map
                )
                for j in range(context.timesteps_to_peer)
            )
            for map in syncing_maps
        )

    # UPdate, DELete, inSERT
    def updelsert(
        self, context: Context, obj_id: ObjectID_T, op_type: Operation.OpType
    ) -> list[Event]:
        cur_map = self.maps[-1]

        primary = cur_map[0]
        if not test_proba(
            context.failure_proba[primary.info.id],
            context.current_time,
            obj_id,
            primary.info.id,
        ) or not context.alive_intervals_per_device[primary.info.id].check_at_time(
            context.current_time
        ):
            return [
                Event(
                    EPrimaryRecvFailure(obj_id, primary.info.id),
                    context.current_time + context.timeout,
                )
            ]

        max_time = primary_write_time = (
            context.current_time + context.user_conn_speed[primary.info.id]
        )
        res: list[Event] = [
            Event(
                EPrimaryRecvSuccess(obj_id, primary.info.id),
                primary_write_time,
                lambda: self.logs[primary.info.id].ops.append(
                    Operation(obj_id, op_type)
                ),
            )
        ]

        secondary = cur_map[1:]
        failed = False
        for d in secondary:
            if test_proba(
                context.failure_proba[d.info.id],
                context.current_time,
                obj_id,
                d.info.id,
            ) and context.alive_intervals_per_device[primary.info.id].check_at_time(
                context.current_time
            ):
                res.append(
                    Event(
                        EReplicaRecvSuccess(obj_id, primary.info.id, d.info.id),
                        primary_write_time
                        + context.conn_speed[primary.info.id, d.info.id],
                        (
                            lambda x: (
                                lambda: self.logs[x.info.id].ops.append(
                                    Operation(obj_id, op_type)
                                )
                            )
                        )(d),
                    ),
                )
                res.append(
                    Event(
                        EReplicaRecvAcknowledged(obj_id, primary.info.id, d.info.id),
                        primary_write_time
                        + context.conn_speed[primary.info.id, d.info.id]
                        + 1,
                    )
                )
                max_time = max(
                    max_time,
                    primary_write_time
                    + context.conn_speed[primary.info.id, d.info.id]
                    + 1,
                )
            else:
                failed = True
                res.append(
                    Event(
                        EReplicaRecvFailure(obj_id, primary.info.id, d.info.id),
                        primary_write_time + context.timeout,
                    )
                )
                max_time = max(
                    max_time,
                    primary_write_time + context.timeout,
                )

        if failed:
            res.append(
                Event(EPrimaryReplicationFail(obj_id, primary.info.id), max_time + 1)
            )
        else:
            res.append(
                Event(
                    EPrimaryRecvAcknowledged(obj_id, primary.info.id),
                    max_time + 1,
                )
            )

        return res


@dataclass
class PGInstance:
    log: list[Operation]
    last_completed: int = field(init=False, default=-1)


class PGList:
    def __init__(self, c: list[PlacementGroup]):
        self._col: list[PlacementGroup] = c

    def __iter__(self) -> Iterator[PlacementGroup]:
        return iter(self._col)

    def object_insert(self, context: Context, obj_id: ObjectID_T):
        h = hash(str(obj_id)) % len(self._col)
        self._col[h].updelsert(context, obj_id, Operation.OpType.INSERT)

    def object_update(self, context: Context, obj_id: ObjectID_T):
        h = hash(str(obj_id)) % len(self._col)
        self._col[h].updelsert(context, obj_id, Operation.OpType.UPDATE)

    def object_delete(self, context: Context, obj_id: ObjectID_T):
        h = hash(str(obj_id)) % len(self._col)
        self._col[h].updelsert(context, obj_id, Operation.OpType.DELETE)


@dataclass
class PoolParams:
    size: int  # replicas count
    min_size: int  # minimum allowed number of replicas returned by CRUSH
    pgs: Iterable[PlacementGroup]
    # pg_count: int  # placement groups' count


def map_pg(
    root: Bucket,
    rule: Rule,
    tunables: Tunables,
    cfg: PoolParams,
    context: Context,
) -> list[Event]:
    events: list[Event] = []
    for pg in cfg.pgs:
        res = apply(pg.id, root, rule, cfg.size, tunables)
        assert not isinstance(res, str), res
        if pg.is_peering or (len(pg.maps) > 0 and pg.maps[-1] == res):
            continue

        maps, success = pg.peer(context)
        devices_used_in_peering: set[DeviceID_T] = set()
        for m in maps:
            devices_used_in_peering.update((d.info.id for d in m))

        peering_id = hash((pg.id, context.current_time))
        events.append(
            Event(
                EPeeringStart(
                    peering_id,
                    pg.id,
                    list(devices_used_in_peering),
                ),
                context.current_time,
            )
        )

        def success_wrapper(
            inner_pg: PlacementGroup, ds: list[Device]
        ) -> Callable[[], None]:
            def inner():
                inner_pg.is_peering = False
                inner_pg.last_sync = len(inner_pg.maps)
                inner_pg.maps.append(ds)

            return inner

        def fail_wrapper(inner_pg: PlacementGroup) -> Callable[[], None]:
            def inner():
                inner_pg.is_peering = False

            return inner

        pg.is_peering = True
        if success:
            events.append(
                Event(
                    EPeeringSuccess(peering_id),
                    context.current_time + context.timestep * context.timesteps_to_peer,
                    success_wrapper(pg, res),
                )
            )
        else:
            events.append(
                Event(
                    EPeeringFailure(peering_id),
                    context.current_time + context.timestep * context.timesteps_to_peer,
                    fail_wrapper(pg),
                )
            )
    return events


def get_iteration_event(
    root: Bucket,
    devices: dict[DeviceID_T, Device],
    init_weights: dict[DeviceID_T, WeightT],
    rule: Rule,
    tunables: Tunables,
    cfg: PoolParams,
    context: Context,
) -> Event:
    tag = EMainloopInteration([])

    def callback():
        for d_id, intervals in context.alive_intervals_per_device.items():
            if intervals.check_at_time(context.current_time):
                devices[d_id].update_weight(init_weights[d_id])
            else:
                devices[d_id].update_weight(OutOfClusterWeight)
        tag.callback_results = map_pg(root, rule, tunables, cfg, context)
        context.do_time_step()
        tag.callback_results.append(
            get_iteration_event(root, devices, init_weights, rule, tunables, cfg, context)
        )

    return Event(tag, context.current_time, callback)
