from collections import defaultdict
import copy
from dataclasses import dataclass
from pprint import pprint
import sys
from typing import DefaultDict, Generator
from crush import Tunables, apply
from parser import (
    Bucket,
    Parser,
    Rule,
    OutOfClusterWeight,
    DeviceID_T,
    WeightT,
    Weights,
)


def read_from_stdin_til_eof() -> Generator[str, None, None]:
    while True:
        s = sys.stdin.readline()
        if s == "":
            return
        yield s


@dataclass
class PoolParams:
    size: int  # replicas count
    min_size: int  # allowed minimum number of replicas returned by CRUSH
    pg_count: int  # placement groups' count


def map_pg(
    root: Bucket,
    rule: Rule,
    ws: Weights,
    tunables: Tunables,
    cfg: PoolParams,
) -> DefaultDict[DeviceID_T, list[int]]:
    osd2pg: DefaultDict[DeviceID_T, list[int]] = defaultdict(list)

    for pg_id in range(cfg.pg_count):
        res = apply(pg_id, root, rule, cfg.size, ws, tunables)
        assert not isinstance(res, str)
        for d in res:
            osd2pg[d.id].append(pg_id)

    return osd2pg


def turn_off_and_remap(
    osd2pg: dict[DeviceID_T, list[int]],
    osd_ids: list[DeviceID_T],
    root: Bucket,
    rule: Rule,
    ws: Weights,
    tunables: Tunables,
    cfg: PoolParams,
) -> Weights | str:
    ret: Weights = copy.deepcopy(ws)
    for id in osd_ids:
        ret.devices_ws[id] = OutOfClusterWeight

    for osd_id in osd_ids:
        old = osd2pg.pop(osd_id)
        for pg_id in old:
            new_osds_res = apply(pg_id, root, rule, cfg.size, ret, tunables)
            if isinstance(new_osds_res, str):
                return new_osds_res
            for osd in new_osds_res:
                osd2pg[osd.id].append(pg_id)

    return ret


def main():
    q = open("./maps/default_map").readlines()
    m = "".join(q)
    # m = "".join(read_from_stdin_til_eof())
    # print(m)

    cfg = PoolParams(size=3, min_size=2, pg_count=200)
    tunables = Tunables(5)

    p = Parser(m)
    r = p.parse()

    osd2pg = map_pg(r.root, r.rules[0], r.ws, tunables, cfg)
    # pprint(osd2pg)
    pprint({key: len(value) for key, value in osd2pg.items()})

    res = turn_off_and_remap(
        osd2pg,
        [DeviceID_T(1), DeviceID_T(2), DeviceID_T(3)],
        r.root,
        r.rules[0],
        r.ws,
        tunables,
        cfg,
    )
    assert not isinstance(res, str), res
    r.ws = res
    # pprint(osd2pg)
    pprint({key: len(value) for key, value in osd2pg.items()})

    r.ws.devices_ws[DeviceID_T(1)] = WeightT(3.0)
    osd2pg = map_pg(r.root, r.rules[0], r.ws, tunables, cfg)
    pprint({key: len(value) for key, value in osd2pg.items()})


if __name__ == "__main__":
    main()
