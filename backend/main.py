from collections import defaultdict
from dataclasses import dataclass
import json
import sys
from typing import Any, Generator
from parser import OutOfClusterWeight
import heapq


from parser import Device, ParserResult, ParsingError
from crush import Tunables
from mapping import (
    EOSDFailed,
    EOSDRecovered,
    ESendFailure,
    PlacementGroupID_T,
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


def initQueue(): ...


def process_pending_events(q: list[Event]):
    res: list[dict[str, Any]] = []
    if len(q) == 0:
        return -1, res

    cur_time = q[0].time
    while len(q) > 0 and q[0].time == cur_time:
        top = heapq.heappop(q)
        if top.callback is not None:
            top.callback()

        match top.tag:
            case EMainloopInteration() as t:
                for e in t.callback_results:
                    heapq.heappush(q, e)
            case (
                EPrimaryRecvSuccess()
                | EPrimaryRecvFailure()
                | EPrimaryRecvAcknowledged()
                | EPrimaryReplicationFail()
                | EReplicaRecvSuccess()
                | EReplicaRecvFailure()
                | EReplicaRecvAcknowledged()
                | EPeeringStart()
                | EPeeringSuccess()
                | EPeeringFailure()
                | EOSDFailed()
                | EOSDRecovered()
                | ESendFailure()
            ):
                res.append(top.tag.to_json())
    return cur_time, res


import asyncio
from websockets.asyncio.server import serve


@dataclass
class SetupResult:
    queue: list[Event]
    pgs: PGList
    context: Context
    devices: dict[DeviceID_T, Device]


# info: a lot of params can be made params to this function
def setup_event_queue(r: ParserResult) -> SetupResult:
    context = Context(
        current_time=0,
        timestep=20,
        timesteps_to_peer=2,
        timeout=70,
        user_conn_speed=defaultdict(lambda: 20),
        conn_speed=defaultdict(lambda: 20),
        failure_proba=defaultdict(lambda: 0.05),
        alive_intervals_per_device={},
    )

    DEATH_PROBA = 0.25

    init_weights: dict[DeviceID_T, WeightT] = {}
    for d in r.devices.values():
        context.alive_intervals_per_device[d.info.id] = AliveIntervals(
            d.info.id, DEATH_PROBA
        )
        init_weights[d.info.id] = d.weight

    pgs = PGList(c=[PlacementGroup(PlacementGroupID_T(i)) for i in range(8)])

    cfg = PoolParams(size=3, min_size=2, pgs=pgs)
    tunables = Tunables(5)

    loop: Event = get_iteration_event(
        r.root, r.devices, init_weights, r.rules[0], tunables, cfg, context
    )
    return SetupResult([loop], pgs, context, r.devices)


def adjustMapping(r: ParserResult, setup: SetupResult):
    DEATH_PROBA = 0.25

    context = Context(
        current_time=setup.context.current_time,
        timestep=20,
        timesteps_to_peer=2,
        timeout=70,
        user_conn_speed=defaultdict(lambda: 20),
        conn_speed=defaultdict(lambda: 20),
        failure_proba=defaultdict(lambda: 0.05),
        alive_intervals_per_device={},
    )
    init_weights: dict[DeviceID_T, WeightT] = {}
    for d in r.devices.values():
        init_weights[d.info.id] = d.weight
        context.alive_intervals_per_device[d.info.id] = AliveIntervals(
            d.info.id, DEATH_PROBA
        )

        oldDevice = setup.devices.get(d.info.id)
        if oldDevice is not None and oldDevice.weight == OutOfClusterWeight:
            d.update_weight(oldDevice.weight)

    tunables = Tunables(5)
    cfg = PoolParams(size=3, min_size=2, pgs=setup.pgs)
    new_loop: list[Event] = []
    for e in setup.queue:
        match e.tag:
            case EMainloopInteration():
                heapq.heappush(
                    new_loop,
                    get_iteration_event(
                        r.root,
                        r.devices,
                        init_weights,
                        r.rules[0],
                        tunables,
                        cfg,
                        context,
                    ),
                )
            case EPeeringSuccess() | EPeeringFailure() | EOSDFailed() | EOSDRecovered():
                heapq.heappush(new_loop, e)
            case _:
                ...
    setup.queue = new_loop


async def handler(websocket):
    setup: SetupResult | None = None
    async for message in websocket:
        m = json.loads(message)
        match m["type"]:
            case "adjust_rule":
                assert setup is not None
                try:
                    r = Parser(m["message"]).parse()
                except ParsingError as e:
                    await websocket.send(
                        json.dumps(
                            {
                                "type": "hierarchy_fail",
                                "data": str(e),
                            }
                        )
                    )
                else:
                    hierarchy = r.root.to_json()
                    adjustMapping(r, setup)
                    await websocket.send(
                        json.dumps(
                            {
                                "type": "adjust_hierarchy_success",
                                "data": hierarchy,
                            }
                        )
                    )
            case "rule":
                try:
                    r = Parser(m["message"]).parse()
                except ParsingError as e:
                    await websocket.send(
                        json.dumps(
                            {
                                "type": "hierarchy_fail",
                                "data": str(e),
                            }
                        )
                    )
                else:
                    hierarchy = r.root.to_json()
                    setup = setup_event_queue(r)
                    await websocket.send(
                        json.dumps(
                            {
                                "type": "hierarchy_success",
                                "data": hierarchy,
                            }
                        )
                    )
            case "step":
                assert setup is not None
                time, messages = process_pending_events(setup.queue)
                await websocket.send(
                    json.dumps(
                        {"type": "events", "timestamp": time, "events": messages}
                    )
                )
            case "insert":
                assert setup is not None
                for event in setup.pgs.object_insert(setup.context, m["id"]):
                    heapq.heappush(setup.queue, event)
            case other:
                print(other)

        # await websocket.send(message)


async def test():
    async with serve(handler, "localhost", 8080) as server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(test())

    # main()
