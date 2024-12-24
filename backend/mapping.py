from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum, auto
from hashlib import sha256
from time import time
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
    WeightT,
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
PlacementGroupID_T = NewType("PlacementGroupID_T", int)


@dataclass()
class EMainloopInteration:
    callback_results: list["Event"]


@dataclass(frozen=True)
class ESendFailure:
    obj: ObjectID_T
    reason: str

    def to_json(self):
        return {"type": "send_fail", "objId": self.obj, "reason": self.reason}


@dataclass(frozen=True)
class EPrimaryRecvSuccess:
    operation_id: int
    obj: ObjectID_T
    pg: PlacementGroupID_T
    cur_map: list[DeviceID_T]

    def to_json(self):
        return {
            "type": "primary_recv_success",
            "pg": self.pg,
            "objId": self.obj,
            "map": [f"osd.{i}" for i in self.cur_map],
        }


@dataclass(frozen=True)
class EPrimaryRecvAcknowledged:
    operation_id: int
    obj: ObjectID_T
    pg: PlacementGroupID_T
    osd: DeviceID_T

    def to_json(self):
        return {
            "type": "primary_recv_ack",
            "pg": self.pg,
            "objId": self.obj,
            "osd": f"osd.{self.osd}",
        }


@dataclass(frozen=True)
class EPrimaryRecvFailure:
    obj: ObjectID_T
    pg: PlacementGroupID_T
    osd: DeviceID_T

    def to_json(self):
        return {
            "type": "primary_recv_fail",
            "pg": self.pg,
            "objId": self.obj,
            "osd": f"osd.{self.osd}",
        }


@dataclass(frozen=True)
class EPrimaryReplicationFail:
    operation_id: int
    obj: ObjectID_T
    pg: PlacementGroupID_T
    osd: DeviceID_T

    def to_json(self):
        return {
            "type": "primary_replication_fail",
            "pg": self.pg,
            "objId": self.obj,
            "osd": f"osd.{self.osd}",
        }


@dataclass(frozen=True)
class EReplicaRecvAcknowledged:
    operation_id: int
    obj: ObjectID_T
    pg: PlacementGroupID_T
    osd: DeviceID_T

    def to_json(self):
        return {
            "type": "replica_recv_ack",
            "pg": self.pg,
            "objId": self.obj,
            "osd": f"osd.{self.osd}",
        }


@dataclass(frozen=True)
class EReplicaRecvSuccess:
    operation_id: int
    obj: ObjectID_T
    pg: PlacementGroupID_T
    osd: DeviceID_T

    def to_json(self):
        return {
            "type": "replica_recv_success",
            "pg": self.pg,
            "objId": self.obj,
            "osd": f"osd.{self.osd}",
        }


@dataclass(frozen=True)
class EReplicaRecvFailure:
    operation_id: int
    obj: ObjectID_T
    pg: PlacementGroupID_T
    osd: DeviceID_T

    def to_json(self):
        return {
            "type": "replica_recv_fail",
            "pg": self.pg,
            "objId": self.obj,
            "osd": f"osd.{self.osd}",
        }


@dataclass(frozen=True)
class EPeeringStart:
    peering_id: int
    pg: PlacementGroupID_T
    device_ids: list[DeviceID_T]
    map_candidate: list[DeviceID_T]

    def to_json(self):
        return {
            "type": "peering_start",
            "pg": self.pg,
            "osds": [f"osd.{i}" for i in self.device_ids],
            "new_map_candidate": [f"osd.{i}" for i in self.map_candidate],
            "peering_id": self.peering_id,
        }


@dataclass(frozen=True)
class EPeeringSuccess:
    peering_id: int
    pg: PlacementGroupID_T

    def to_json(self):
        return {
            "type": "peering_success",
            "peering_id": self.peering_id,
        }


@dataclass(frozen=True)
class EPeeringFailure:
    peering_id: int
    pg: PlacementGroupID_T

    def to_json(self):
        return {
            "type": "peering_fail",
            "peering_id": self.peering_id,
        }


@dataclass
class EOSDFailed:
    osd: DeviceID_T

    def to_json(self):
        return {"type": "osd_failed", "osd": f"osd.{self.osd}"}


@dataclass
class EOSDRecovered:
    osd: DeviceID_T

    def to_json(self):
        return {"type": "osd_recovered", "osd": f"osd.{self.osd}"}


EventTag = (
    EMainloopInteration
    | ESendFailure
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
    | EOSDFailed
    | EOSDRecovered
)


@dataclass
class Event:
    tag: EventTag = field(compare=False)
    time: int
    callback: Callable[[], None] | None = field(compare=False, default=None, repr=False)

    def __le__(self, other: "Event"):
        return self < other

    def __lt__(self, other: "Event"):
        if self.time < other.time:
            return True
        elif self.time == other.time:
            if isinstance(self.tag, EPeeringSuccess):
                if isinstance(other.tag, EPeeringSuccess):
                    return False
                else:
                    return True
            return False
        return False


def test_proba(p: float, *args: Hashable) -> bool:
    h = sha256(str(args).encode())
    h = int(h.hexdigest(), 16) & 0xFFFF
    cutoff = int(p * 0xFFFF)
    return h >= cutoff


class AliveIntervals:
    def __init__(self, id: int, p_die: float):
        self.id = id
        self.p_die = p_die

    def check_at_time(self, t: int) -> bool:
        return test_proba(self.p_die, self.id, str(t))

    def update_death_proba(self, p: float):
        self.p_die = p


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

    death_proba: float

    def do_time_step(self):
        self.current_time += self.timestep

    def update_death_proba(self, p: float):
        self.death_proba = p
        for interval in self.alive_intervals_per_device.values():
            interval.update_death_proba(p)


@dataclass
class PlacementGroup:
    id: PlacementGroupID_T
    logs: dict[DeviceID_T, LogData] = field(
        init=False, default_factory=lambda: defaultdict(LogData)
    )
    last_sync: int = field(init=False, default=-1)
    _maps: list[list[DeviceID_T]] = field(init=False, default_factory=list)
    is_peering: bool = field(init=False, default=False)

    def start_peering(self):
        self.is_peering = True

    def stop_peering(self):
        self.is_peering = False

    @property
    def maps(self) -> list[list[DeviceID_T]]:
        return self._maps

    def record_mapping(self, m: list[DeviceID_T]) -> bool:
        if len(self._maps) == 0 or m != self._maps[-1]:
            self._maps.append(m)
            return True
        return False

    def peer(self, context: Context) -> tuple[list[list[DeviceID_T]], bool]:
        syncing_maps = self._maps[self.last_sync :]
        return syncing_maps, all(
            all(
                any(
                    (
                        context.alive_intervals_per_device[id].check_at_time(
                            context.current_time + j * context.timestep
                        )
                        if context.alive_intervals_per_device.get(id) is not None
                        else False
                    )
                    for id in map
                )
                for j in range(context.timesteps_to_peer)
            )
            for map in syncing_maps
        )

    # UPdate, DELete, inSERT
    def updelsert(
        self, context: Context, obj_id: ObjectID_T, op_type: Operation.OpType
    ) -> list[Event]:
        if len(self.maps) == 0 or len(self.maps[-1]) == 0:
            return [Event(ESendFailure(obj_id, "empty map"), context.current_time)]
        cur_map = self.maps[-1]

        primary_id = cur_map[0]
        max_time = primary_write_time = (
            context.current_time + context.user_conn_speed[primary_id]
        )

        operation_id = hash((obj_id, time()))

        if not context.alive_intervals_per_device[primary_id].check_at_time(
            primary_write_time
        ) or not test_proba(
            context.failure_proba[primary_id],
            context.current_time,
            obj_id,
            primary_id,
        ):
            return [
                Event(
                    EPrimaryRecvFailure(obj_id, self.id, primary_id),
                    primary_write_time,
                )
            ]

        res: list[Event] = [
            Event(
                EPrimaryRecvSuccess(
                    operation_id, obj_id, self.id, [d_id for d_id in cur_map]
                ),
                primary_write_time,
                lambda: self.logs[primary_id].ops.append(Operation(obj_id, op_type)),
            )
        ]

        secondary = cur_map[1:]
        failed = False
        for device_id in secondary:
            if context.alive_intervals_per_device[device_id].check_at_time(
                primary_write_time + context.conn_speed[primary_id, device_id]
            ) and test_proba(
                context.failure_proba[device_id],
                context.current_time,
                obj_id,
                device_id,
            ):
                res.append(
                    Event(
                        EReplicaRecvSuccess(operation_id, obj_id, self.id, device_id),
                        primary_write_time + context.conn_speed[primary_id, device_id],
                        (
                            lambda x: (
                                lambda: self.logs[x].ops.append(
                                    Operation(obj_id, op_type)
                                )
                            )
                        )(device_id),
                    ),
                )
                res.append(
                    Event(
                        EReplicaRecvAcknowledged(
                            operation_id, obj_id, self.id, device_id
                        ),
                        primary_write_time
                        + context.conn_speed[primary_id, device_id]
                        + 1,
                    )
                )
                max_time = max(
                    max_time,
                    primary_write_time + context.conn_speed[primary_id, device_id] + 1,
                )
            else:
                failed = True
                res.append(
                    Event(
                        EReplicaRecvFailure(operation_id, obj_id, self.id, device_id),
                        primary_write_time + context.conn_speed[primary_id, device_id],
                    )
                )
                max_time = max(
                    max_time,
                    primary_write_time + context.conn_speed[primary_id, device_id],
                )

        if failed:
            res.append(
                Event(
                    EPrimaryReplicationFail(operation_id, obj_id, self.id, primary_id),
                    max_time + 1,
                )
            )
        else:
            res.append(
                Event(
                    EPrimaryRecvAcknowledged(operation_id, obj_id, self.id, primary_id),
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

    def get(self, id: PlacementGroupID_T) -> PlacementGroup:
        return self._col[id]

    def object_insert(self, context: Context, obj_id: ObjectID_T):
        h = int(sha256(str(obj_id).encode()).hexdigest(), 16) % len(self._col)
        return self._col[h].updelsert(context, obj_id, Operation.OpType.INSERT)

    def object_update(self, context: Context, obj_id: ObjectID_T):
        h = int(sha256(str(obj_id).encode()).hexdigest(), 16) % len(self._col)
        return self._col[h].updelsert(context, obj_id, Operation.OpType.UPDATE)

    def object_delete(self, context: Context, obj_id: ObjectID_T):
        h = int(sha256(str(obj_id).encode()).hexdigest(), 16) % len(self._col)
        return self._col[h].updelsert(context, obj_id, Operation.OpType.DELETE)


@dataclass
class PoolParams:
    size: int  # replicas count
    min_size: int  # minimum allowed number of replicas returned by CRUSH
    pgs: Iterable[PlacementGroup]
    # pg_count: int  # placement groups' count


def map_pg(
    root: Bucket,
    devices: dict[DeviceID_T, Device],
    rule: Rule,
    tunables: Tunables,
    cfg: PoolParams,
    context: Context,
) -> list[Event]:
    events: list[Event] = []
    for pg in cfg.pgs:
        res = apply(pg.id, root, rule, cfg.size, tunables)
        assert not isinstance(res, str), res
        res = [d.info.id for d in res]
        if pg.is_peering or (len(pg.maps) > 0 and pg.maps[-1] == res):
            continue

        prev_maps, success = pg.peer(context)
        devices_used_in_peering: set[DeviceID_T] = set()
        peering_id = hash((pg.id, context.current_time))

        for m in prev_maps:
            devices_used_in_peering.update((d_id for d_id in m if d_id in devices))

        events.append(
            Event(
                EPeeringStart(
                    peering_id,
                    pg.id,
                    list(devices_used_in_peering),
                    res,
                ),
                context.current_time,
                (lambda x: lambda: x.start_peering())(pg),
            )
        )

        def success_wrapper(
            inner_pg: PlacementGroup, ds: list[DeviceID_T]
        ) -> Callable[[], None]:
            def inner():
                inner_pg.stop_peering()
                inner_pg.last_sync = len(inner_pg.maps)
                inner_pg.maps.append(ds)

            return inner

        def fail_wrapper(inner_pg: PlacementGroup) -> Callable[[], None]:
            def inner():
                inner_pg.stop_peering()

            return inner

        if success:
            events.append(
                Event(
                    EPeeringSuccess(peering_id, pg.id),
                    context.current_time + context.timestep * context.timesteps_to_peer,
                    success_wrapper(pg, res),
                )
            )
        else:
            events.append(
                Event(
                    EPeeringFailure(peering_id, pg.id),
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
            device = devices[d_id]
            if init_weights[d_id] == OutOfClusterWeight:
                tag.callback_results.append(
                    Event(EOSDFailed(d_id), context.current_time)
                )
            elif intervals.check_at_time(context.current_time):
                if device.weight != init_weights[d_id]:
                    device.update_weight(init_weights[d_id])
                    tag.callback_results.append(
                        Event(EOSDRecovered(d_id), context.current_time)
                    )
            else:
                if device.weight == init_weights[d_id]:
                    device.update_weight(OutOfClusterWeight)
                    tag.callback_results.append(
                        Event(EOSDFailed(d_id), context.current_time)
                    )

        tag.callback_results.extend(map_pg(root, devices, rule, tunables, cfg, context))

        context.do_time_step()
        tag.callback_results.append(
            get_iteration_event(
                root, devices, init_weights, rule, tunables, cfg, context
            )
        )

    return Event(tag, context.current_time, callback)
