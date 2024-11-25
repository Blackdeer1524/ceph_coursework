from collections import defaultdict
from dataclasses import dataclass
import json
from pprint import pprint
import sys
from typing import Any, Generator
import heapq


from parser import ParserResult
from crush import Tunables
from mapping import (
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


def main():
    q = open("./maps/default_map").readlines()
    m = "".join(q)
    # m = "".join(read_from_stdin_til_eof())
    # print(m)

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

    pgs: list[PlacementGroup] = []
    for i in range(20):
        pgs.append(PlacementGroup(i))

    cfg = PoolParams(size=3, min_size=2, pgs=PGList(c=pgs))
    tunables = Tunables(5)

    p = Parser(m)
    r = p.parse()

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


def process_pending_events(q: list[Event]):
    res: list[dict[str, Any]] = []
    if len(q) == 0:
        return -1, res

    cur_time = q[0].time
    while len(q) > 0 and q[0].time == cur_time:
        top = heapq.heappop(q)
        pprint(top)
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
            ):
                res.append(top.tag.to_json())
    return cur_time, res


import asyncio
from websockets.asyncio.server import serve


@dataclass(frozen=True)
class SetupResult:
    queue: list[Event]
    pgs: PGList
    context: Context


# info: a lot of params can be made params to this function
def setup_event_queue(r: ParserResult) -> SetupResult:
    context = Context(
        current_time=0,
        timestep=20,
        timesteps_to_peer=2,
        timeout=70,
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

    pgs = PGList(c=[PlacementGroup(PlacementGroupID_T(i)) for i in range(20)])

    cfg = PoolParams(size=3, min_size=2, pgs=pgs)
    tunables = Tunables(5)

    loop: Event = get_iteration_event(
        r.root, r.devices, init_weights, r.rules[0], tunables, cfg, context
    )
    return SetupResult([loop], pgs, context)


async def handler(websocket):
    setup: SetupResult | None = None
    async for message in websocket:
        m = json.loads(message)
        match m["type"]:
            case "rule":
                r = Parser(m["message"]).parse()
                hierarchy = r.root.to_json()
                setup = setup_event_queue(r)
                await websocket.send(json.dumps({
                    "type": "hierarchy",
                    "data": hierarchy ,
                }))
            case "step":
                if setup is not None:
                    time, messages = process_pending_events(setup.queue)
                    print(messages)
                    await websocket.send(json.dumps({"type": "events", "timestamp": time, "events": messages}))
            case other:
                print(other)

        # await websocket.send(message)


async def test():
    async with serve(handler, "localhost", 8080) as server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(test())

    # main()
