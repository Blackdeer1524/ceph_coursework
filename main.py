from collections import defaultdict
from dataclasses import dataclass
from pprint import pprint
import sys
from typing import DefaultDict, Generator
from crush import Tunables, apply
from parser import Parser, WeightT, DeviceID_T

def read_from_stdin_til_eof() -> Generator[str, None, None]:
    while True:
        s = sys.stdin.readline()
        if s == '':
            return
        yield s
        

@dataclass
class PoolParams:
    size: int  # replicas count
    min_size: int  # allowed minimum number of replicas returned by CRUSH
    pg_count: int  # placement groups' count 


def main():
    q = open("./maps/default_map").readlines()
    m = "".join(q)
    # m = "".join(read_from_stdin_til_eof())
    # print(m)

    cfg = PoolParams(size=3, min_size=2, pg_count=20)
    tunables = Tunables(5)

    p = Parser(m)
    r = p.parse()
    # pprint(r)

    rule = r.rules[0]
    res = apply(0, r.root, rule,  cfg.size, r.ws, tunables)

    osd2pg: DefaultDict[int, list[int]] = defaultdict(list)
    
    teams_list = ["PG", "Active set"]
    row_format ="{:>4} | {:>15}"
    print(row_format.format(*teams_list))
    for pg_id in range(cfg.pg_count): 
        res = apply(pg_id, r.root, rule,  cfg.size, r.ws, tunables)
        assert(not isinstance(res, str))
        for d in res:
            osd2pg[d.id].append(pg_id)
        print(row_format.format(pg_id, str([i.id for i in res])))
    
    
    pprint(osd2pg)
    # PG_id -> OSD
    # OSD -> list[PG_id]
    


if __name__ == "__main__":
    main()
