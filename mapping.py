from dataclasses import dataclass, field
from enum import Enum, auto
import random
from typing import Callable
from crush import Tunables, apply
from parser import (
    Bucket,
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


@dataclass
class Event:
    time: int
    callback: Callable[[], None]


@dataclass
class Context:
    current_time: int
    timeout: int
    user_conn_speed: dict[DeviceID_T, int]
    conn_speed: dict[tuple[DeviceID_T, DeviceID_T], int]
    failure_proba: dict[DeviceID_T, float]


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

    def record_mapping(self, m: list[Device]) -> None:
        self._maps.append(m)

    def insert(self, obj_id: int, context: Context) -> list[Event]:
        cur_map = self.maps[-1]

        primary = cur_map[0]
        p = random.uniform(0, 1)
        if p < context.failure_proba[primary.info.id]:
            return [Event(context.current_time + context.timeout, lambda: None)]

        primary_write_time = (
            context.current_time + context.user_conn_speed[primary.info.id]
        )
        res: list[Event] = [
            Event(
                primary_write_time,
                lambda: self.logs[primary.info.id].ops.append(
                    Operation(obj_id, Operation.OpType.CREATE)
                ),
            )
        ]
        secondary = cur_map[1:]


        
        return res


@dataclass
class PGInstance:
    log: list[Operation]
    last_completed: int = field(init=False, default=-1)


@dataclass
class PoolParams:
    size: int  # replicas count
    min_size: int  # allowed minimum number of replicas returned by CRUSH
    pgs: dict[int, PlacementGroup]
    # pg_count: int  # placement groups' count


def map_pg(
    root: Bucket,
    rule: Rule,
    tunables: Tunables,
    cfg: PoolParams,
) -> None:
    for pg in cfg.pgs.values():
        res = apply(pg.id, root, rule, cfg.size, tunables)
        assert not isinstance(res, str), res
        pg.record_mapping(res)
