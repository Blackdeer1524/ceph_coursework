from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum, auto
import random
from typing import Callable, Generator, Hashable, NewType
from crush import Tunables, apply
from parser import (
    Bucket,
    OutOfClusterWeight,
    Device,
    DeviceID_T,
    Rule,
)


@dataclass
class Operation:
    class OpType(Enum):
        CREATE = auto()
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


EventTag = (
    EPrimaryRecvSuccess
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


@dataclass
class Event:
    tag: EventTag
    time: int
    callback: Callable[[], None] | None = None


def test_proba(p: float, *args: Hashable) -> bool:
    h = hash(args) & 0xFFFF
    cutoff = int(p * 0xFFFF)
    return h >= cutoff


class AliveIntervals:
    def __init__(self, step: int, time_limit: int, p_die: float, p_resurrect: float):
        self.res: list[tuple[int, int]] = []

        cur_time = 0
        old_start = 0
        while cur_time < time_limit:
            for cur_time in range(cur_time + step, time_limit, step):
                if random.uniform(0, b=1) < p_die:
                    break
            self.res.append((old_start, cur_time))
            for cur_time in range(cur_time + step, time_limit):
                if random.uniform(0, 1) < p_resurrect:
                    break
            old_start = cur_time

    def check_at_time(self, t: int) -> bool:
        for i in self.res:
            if t < i[0]:
                return False
            elif i[0] <= t < i[1]:
                return True
        return False


@dataclass
class Context:
    current_time: int
    timestep: int
    timesteps_to_peer: int
    time_limit: int
    # send timeout
    timeout: int
    # how much time it takes to recieve one file from user
    user_conn_speed: dict[DeviceID_T, int]
    # how much time it takes to send one file from one device to another
    conn_speed: dict[tuple[DeviceID_T, DeviceID_T], int]
    failure_proba: dict[DeviceID_T, float]

    alive_intervals_per_device: dict[DeviceID_T, AliveIntervals]

    def do_time_step(self) -> "Context":
        return Context(
            current_time=self.current_time + self.timestep,
            timestep=self.timestep,
            timesteps_to_peer=self.timesteps_to_peer,
            time_limit=self.time_limit,
            timeout=self.timeout,
            user_conn_speed=self.user_conn_speed,
            conn_speed=self.conn_speed,
            failure_proba=self.failure_proba,
            alive_intervals_per_device=self.alive_intervals_per_device,
        )


@dataclass
class PlacementGroup:
    id: int
    logs: dict[DeviceID_T, LogData] = field(init=False, default_factory=dict)
    _last_sync: int = field(init=False, default=-1)
    _maps: list[list[Device]] = field(init=False, default_factory=list)

    @property
    def last_sync(self) -> int:
        return self._last_sync

    @property
    def maps(self) -> list[list[Device]]:
        return self._maps

    def record_mapping(self, m: list[Device]) -> bool:
        if len(self._maps) == 0 or m != self._maps[-1]:
            self._maps.append(m)
            return True
        return False

    def peer(self, context: Context) -> tuple[list[list[Device]], bool]:
        syncing_maps = self._maps[self._last_sync :]
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
        self, obj_id: ObjectID_T, context: Context, op_type: Operation.OpType
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
                        lambda: self.logs[d.info.id].ops.append(
                            Operation(obj_id, op_type)
                        ),
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


@dataclass
class PoolParams:
    size: int  # replicas count
    min_size: int  # minimum allowed number of replicas returned by CRUSH
    pgs: dict[int, PlacementGroup]
    # pg_count: int  # placement groups' count


def map_pg(
    root: Bucket,
    rule: Rule,
    tunables: Tunables,
    cfg: PoolParams,
    context: Context,
) -> None:
    events: list[Event] = []
    for pg in cfg.pgs.values():
        res = apply(pg.id, root, rule, cfg.size, tunables)
        assert not isinstance(res, str), res

        # WARN: one can record a map ONLY AFTER successfull peering!!!
        # if not pg.record_mapping(res):
        #     continue
        if not (len(pg.maps) > 0 and pg.maps[-1] != res):
            continue

        maps, success = pg.peer(context)
        devices_used_in_peering: set[DeviceID_T] = set()
        for m in maps:
            devices_used_in_peering.update((d.info.id for d in m))

        peer_id = hash((pg.id, context.current_time))
        events.append(
            Event(
                EPeeringStart(
                    peer_id,
                    pg.id,
                    list(devices_used_in_peering),
                ),
                context.current_time,
            )
        )
        if success:
            events.append(
                Event(
                    EPeeringSuccess(peer_id),
                    context.current_time + context.timestep * context.timesteps_to_peer,
                    lambda: pg.maps.append(res),
                )
            )
        else:
            local_context = context
            while True:
                events.append(
                    Event(
                        EPeeringFailure(peer_id),
                        local_context.current_time
                        + local_context.timestep * local_context.timesteps_to_peer,
                    )
                )

                for _ in range(local_context.timesteps_to_peer):
                    local_context = local_context.do_time_step()

                _, success = pg.peer(local_context)
                if success:
                    events.append(
                        Event(
                            EPeeringSuccess(peer_id),
                            local_context.current_time
                            + local_context.timestep * local_context.timesteps_to_peer,
                            lambda: pg.maps.append(res),
                        )
                    )


# между итерациями генератора можно делать операции
def mainloop(
    root: Bucket,
    devices: dict[DeviceID_T, Device],
    rule: Rule,
    tunables: Tunables,
    cfg: PoolParams,
) -> Generator[None, None, None]:
    old_weights = {d.info.id: d.weight for d in devices.values()}

    context = Context(
        current_time=0,
        timestep=20,
        timesteps_to_peer=3,
        time_limit=1000,
        timeout=100,
        user_conn_speed=defaultdict(lambda: 30),
        conn_speed=defaultdict(lambda: 30),
        failure_proba=defaultdict(lambda: 0.05),
        alive_intervals_per_device={},
    )

    # todo: probably should be inside `tunable`
    DEATH_PROBA = 0.05
    RESURRECTION_PROBA = 0.30

    for d in devices.values():
        context.alive_intervals_per_device[d.info.id] = AliveIntervals(
            context.timestep, context.time_limit, DEATH_PROBA, RESURRECTION_PROBA
        )

    while context.current_time < context.time_limit:
        # WARN: these weight updates will probably break `map_pg` :(
        for d_id, intervals in context.alive_intervals_per_device.items():
            if intervals.check_at_time(context.current_time):
                devices[d_id].update_weight(old_weights[d_id])
            else:
                devices[d_id].update_weight(OutOfClusterWeight)
        map_pg(root, rule, tunables, cfg, context)
        yield
        context.do_time_step()
