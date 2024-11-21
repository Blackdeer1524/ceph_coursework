from collections import defaultdict
from pprint import pprint
import sys
from typing import Generator
import heapq

from crush import Tunables
from mapping import (
    DeviceID_T,
    WeightT,
    AliveIntervals,
    Context,
    EMainloopInteration,
    EPeeringFailure,
    EPeeringStart,
    EPeeringSuccess,
    EPrimaryRecvAcknowledged,
    EPrimaryRecvFailure,
    EPrimaryRecvSuccess,
    EPrimaryReplicationFail,
    EReplicaRecvAcknowledged,
    EReplicaRecvFailure,
    EReplicaRecvSuccess,
    Event,
    PGList,
    PlacementGroup,
    PoolParams,
    get_iteration_event,
)
from parser import Parser


def read_from_stdin_til_eof() -> Generator[str, None, None]:
    while True:
        s = sys.stdin.readline()
        if s == "":
            return
        yield s


def main():
    q = open("./maps/default_map").readlines()
    m = "".join(q)
    # m = "".join(read_from_stdin_til_eof())
    # print(m)

    pgs: list[PlacementGroup] = []
    for i in range(20):
        pgs.append(PlacementGroup(i))

    cfg = PoolParams(size=3, min_size=2, pgs=PGList(pgs))
    tunables = Tunables(5)

    p = Parser(m)
    r = p.parse()

    context = Context(
        current_time=0,
        timestep=20,
        timesteps_to_peer=3,
        timeout=100,
        user_conn_speed=defaultdict(lambda: 30),
        conn_speed=defaultdict(lambda: 30),
        failure_proba=defaultdict(lambda: 0.05),
        alive_intervals_per_device={},
    )

    DEATH_PROBA = 0.05

    init_weights: dict[DeviceID_T, WeightT] = {}
    for d in r.devices.values():
        context.alive_intervals_per_device[d.info.id] = AliveIntervals(
            d.info.id, DEATH_PROBA
        )
        init_weights[d.info.id] = d.weight

    loop: Event = get_iteration_event(
        r.root, r.devices, init_weights, r.rules[0], tunables, cfg, context
    )

    h: list[Event] = []
    heapq.heappush(h, loop)
    while len(h) > 0:
        top = heapq.heappop(h)
        pprint(top)
        if top.callback is not None:
            top.callback()

        match top.tag:
            case EMainloopInteration() as t:
                for e in t.callback_results:
                    heapq.heappush(h, e)
            case EPrimaryRecvSuccess():
                ...
            case EPrimaryRecvFailure():
                ...
            case EPrimaryRecvAcknowledged():
                ...
            case EPrimaryReplicationFail():
                ...
            case EReplicaRecvSuccess():
                ...
            case EReplicaRecvFailure():
                ...
            case EReplicaRecvAcknowledged():
                ...
            case EPeeringStart():
                ...
            case EPeeringSuccess():
                ...
            case EPeeringFailure():
                ...


if __name__ == "__main__":
    main()
