import heapq
import json
import sys
from collections import defaultdict
from dataclasses import dataclass
from parser import (Device, OutOfClusterWeight, Parser, ParserResult,
                    ParsingError)
from typing import Any, Generator

from crush import Tunables
from mapping import (AliveIntervals, Context, DeviceID_T, EMainloopInteration,
                     EOSDFailed, EOSDRecovered, EPeeringFailure, EPeeringStart,
                     EPeeringSuccess, EPrimaryRecvAcknowledged,
                     EPrimaryRecvFailure, EPrimaryRecvSuccess,
                     EPrimaryReplicationFail, EReplicaRecvAcknowledged,
                     EReplicaRecvFailure, EReplicaRecvSuccess, ESendFailure,
                     Event, PGList, PlacementGroup, PlacementGroupID_T,
                     PoolParams, WeightT, get_iteration_event)


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
def setup_event_queue(r: ParserResult, death_proba: float) -> SetupResult:
    context = Context(
        current_time=0,
        timestep=20,
        timesteps_to_peer=2,
        timeout=70,
        user_conn_speed=defaultdict(lambda: 20),
        conn_speed=defaultdict(lambda: 20),
        failure_proba=defaultdict(lambda: 0.05),
        alive_intervals_per_device={},
        death_proba=death_proba,
    )

    init_weights: dict[DeviceID_T, WeightT] = {}
    for d in r.devices.values():
        init_weights[d.info.id] = d.weight
        context.alive_intervals_per_device[d.info.id] = AliveIntervals(
            d.info.id, context.death_proba, d.weight
        )

    pgs = PGList(c=[PlacementGroup(PlacementGroupID_T(i)) for i in range(8)])

    cfg = PoolParams(size=3, min_size=2, pgs=pgs)
    tunables = Tunables(5)

    loop: Event = get_iteration_event(
        r.root, r.devices, init_weights, r.rules[0], tunables, cfg, context
    )
    return SetupResult([loop], pgs, context, r.devices)


def adjust_mapping(r: ParserResult, setup: SetupResult):
    context = Context(
        current_time=setup.context.current_time,
        timestep=setup.context.timestep,
        timesteps_to_peer=setup.context.timesteps_to_peer,
        timeout=setup.context.timeout,
        user_conn_speed=setup.context.user_conn_speed,
        conn_speed=setup.context.conn_speed,
        failure_proba=setup.context.failure_proba,
        alive_intervals_per_device={},
        death_proba=setup.context.death_proba,
    )

    new_loop: list[Event] = []
    init_weights: dict[DeviceID_T, WeightT] = {}
    for d in r.devices.values():
        init_weights[d.info.id] = d.weight
        context.alive_intervals_per_device[d.info.id] = AliveIntervals(
            d.info.id, context.death_proba, d.weight
        )

        oldDevice = setup.devices.get(d.info.id)
        if oldDevice is not None and oldDevice.weight == OutOfClusterWeight:
            d.update_weight(OutOfClusterWeight)

    tunables = Tunables(5)
    cfg = PoolParams(size=3, min_size=2, pgs=setup.pgs)

    new_peerings: set[int] = set()
    failing_ops: set[int] = set()
    while len(setup.queue) > 0:
        e = heapq.heappop(setup.queue)
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
            case ESendFailure():
                heapq.heappush(new_loop, e)
            case EPrimaryRecvSuccess() as tag:
                primary_osd = tag.cur_map[0]
                if primary_osd not in r.devices:
                    heapq.heappush(
                        new_loop,
                        Event(
                            ESendFailure(tag.obj, f"couldn't find osd.{primary_osd}"),
                            e.time,
                        ),
                    )
                    failing_ops.add(tag.operation_id)
                    continue

                new_map = [primary_osd]
                for i in range(1, len(tag.cur_map)):
                    if tag.cur_map[i] not in r.devices:
                        failing_ops.add(tag.operation_id)
                    else:
                        new_map.append(tag.cur_map[i])

                heapq.heappush(
                    new_loop,
                    Event(
                        EPrimaryRecvSuccess(tag.operation_id, tag.obj, tag.pg, new_map),
                        e.time,
                    ),
                )
            case EPrimaryRecvFailure(osd=osd):
                if osd not in r.devices:
                    continue
                heapq.heappush(new_loop, e)
            case EPrimaryRecvAcknowledged(operation_id=id):
                if e.tag.osd not in r.devices:
                    continue
                if id in failing_ops:
                    heapq.heappush(
                        new_loop,
                        Event(
                            EPrimaryReplicationFail(id, e.tag.obj, e.tag.pg, e.tag.osd),
                            e.time,
                        ),
                    )
                else:
                    heapq.heappush(new_loop, e)
            case EPrimaryReplicationFail(operation_id=id):
                if e.tag.osd not in r.devices:
                    continue
                heapq.heappush(new_loop, e)
            case EReplicaRecvSuccess():
                if e.tag.osd not in r.devices:
                    continue
                heapq.heappush(new_loop, e)
            case EReplicaRecvFailure(operation_id=id):
                if e.tag.osd not in r.devices:
                    continue
                heapq.heappush(new_loop, e)
            case EReplicaRecvAcknowledged():
                if e.tag.osd not in r.devices:
                    continue
                heapq.heappush(new_loop, e)
            case EPeeringStart():
                new_peerings.add(e.tag.peering_id)
            case EPeeringSuccess() as tag:
                if e.tag.peering_id in new_peerings:
                    continue
                heapq.heappush(
                    new_loop,
                    Event(
                        EPeeringFailure(e.tag.peering_id, e.tag.pg),
                        e.time,
                        (lambda x: lambda: setup.pgs.get(x).stop_peering())(tag.pg),
                    ),
                )
            case EPeeringFailure():
                if e.tag.peering_id in new_peerings:
                    continue
                heapq.heappush(new_loop, e)
            case EOSDFailed():
                if e.tag.osd not in r.devices:
                    continue
                heapq.heappush(new_loop, e)
            case EOSDRecovered():
                if e.tag.osd not in r.devices:
                    continue
                heapq.heappush(new_loop, e)
    return SetupResult(new_loop, setup.pgs, context, r.devices)


async def handler(websocket):  # type: ignore
    setup: SetupResult | None = None
    async for message in websocket:  # type: ignore
        m = json.loads(message)  # type: ignore
        message_type = m["type"]
        if message_type ==  "rule":
            try:
                r = Parser(m["message"]).parse()
            except ParsingError as e:
                await websocket.send(  # type: ignore
                    json.dumps(
                        {
                            "type": "hierarchy_fail",
                            "data": str(e),
                        }
                    )
                )
            else:
                hierarchy = r.root.to_json()
                setup = setup_event_queue(
                    r, setup.context.death_proba if setup is not None else 0.25
                )
                await websocket.send(  # type: ignore
                    json.dumps(
                        {
                            "type": "hierarchy_success",
                            "data": hierarchy,
                        }
                    )
                )
        elif message_type ==  "adjust_rule":
            assert setup is not None
            try:
                r = Parser(m["message"]).parse()
            except ParsingError as e:
                await websocket.send(  # type: ignore
                    json.dumps(
                        {
                            "type": "hierarchy_fail",
                            "data": str(e),
                        }
                    )
                )
            else:
                hierarchy = r.root.to_json()
                setup = adjust_mapping(r, setup)
                await websocket.send(  # type: ignore
                    json.dumps(
                        {
                            "type": "adjust_hierarchy_success",
                            "data": hierarchy,
                            "timestamp": setup.context.current_time,
                        }
                    )
                )
        elif message_type == "step":
            assert setup is not None
            time, messages = process_pending_events(setup.queue)
            await websocket.send(  # type: ignore
                json.dumps(
                    {"type": "events", "timestamp": time, "events": messages}
                )
            )
        elif message_type == "insert":
            assert setup is not None
            for event in setup.pgs.object_insert(setup.context, m["id"]):
                heapq.heappush(setup.queue, event)
        elif message_type == "mode":
            assert setup is not None
            new_mode = m["new_mode"]
            if new_mode == "randomized":
                setup.context.update_death_proba(0.25)
            else:
                setup.context.update_death_proba(0)
        else:
            print(m)
        # await websocket.send(message)


async def main():
    async with serve(handler, "localhost", 8080) as server:  # type: ignore
        await server.serve_forever()  # type: ignore


if __name__ == "__main__":
    asyncio.run(main())
