from pprint import pprint
import sys
from typing import Generator
from crush import Tunables, apply
from parser import Parser, WeightT, DeviceID_T

def read_from_stdin_til_eof() -> Generator[str, None, None]:
    while True:
        s = sys.stdin.readline()
        if s == '':
            return
        yield s


def main():
    q = open("./maps/default_map").readlines()
    m = "".join(q)
    # m = "".join(read_from_stdin_til_eof())
    print(m)

    p = Parser(m)
    r = p.parse()
    # pprint(r)

    res = apply(0, r.root, r.rules[0],  3, r.ws, Tunables(5))
    assert not isinstance(res, str), res
    pprint(res)

    r.ws.devices_ws[res[0].id] = WeightT(0)
    res = apply(0, r.root, r.rules[0],  3, r.ws, Tunables(5))
    pprint(res)

    r.ws.devices_ws[res[1].id] = WeightT(0)
    res = apply(0, r.root, r.rules[0],  3, r.ws, Tunables(5))
    pprint(res)
    
    r.ws.buckets_ws[-2] = WeightT(0)
    res = apply(0, r.root, r.rules[0],  3, r.ws, Tunables(5))
    pprint(res)


if __name__ == "__main__":
    main()
