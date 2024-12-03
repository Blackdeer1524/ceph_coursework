from dataclasses import dataclass
from hashlib import sha256
from typing import Literal
from hashing import crush_hash_2
from parser import (
    Bucket,
    BucketT,
    StepChoose,
    Device,
    DeviceInfo,
    Rule,
    StepEmit,
    StepTake,
    WeightT,
    OutOfClusterWeight,
    UnitWeight,
)


def bfs(h: Bucket | Device, name: str) -> Bucket | Device | None:
    q: list[Bucket | Device] = [h]
    while len(q) > 0:
        s = q.pop()
        match s:
            case Bucket() as b:
                if b.name == name:
                    return b
                for bd in b.children:
                    q.append(bd)
            case Device(DeviceInfo(id=id)) as d:
                if f"osd.{id}" == name:
                    return d
    return None


@dataclass
class Tunables:
    # Tunable. The default value when the
    # CHOOSE_TRIES or CHOOSELEAF_TRIES steps are omitted in a rule.
    choose_total_tries: int


def is_out(weight: WeightT, item: int, x: int) -> bool:
    if weight >= UnitWeight:
        return False
    if weight == OutOfClusterWeight:
        return True
    
    h = int(sha256(str((x, item)).encode()).hexdigest(), 16)
    if (h & 0xFFFF) < weight * 0xFFFF:
        return False
    return True


def is_collision(out: list[Device] | list[Bucket], outpos: int, id: int) -> bool:
    # TODO: figure out how to pattern match on `out` directly
    for o in out[:outpos]:
        match o:
            case Device(DeviceInfo(id=o_id)):
                if o_id == id:
                    return True
            case Bucket(id=o_id):
                if o_id == id:
                    return True
    return False


def choose_firstn(
    x: int,
    cur: Bucket,
    target: BucketT | Literal["osd"],
    num_replicas: int,
    max_replicas: int,
    tries: int,
    recurcive_tries: int,
    recurse_to_leaf: bool,
    out: list[Device] | list[Bucket],
    out2: list[Device],
    outpos: int,
) -> int:
    if num_replicas == 0:
        num_replicas = len(cur.children)
    elif num_replicas < 0:
        num_replicas = max_replicas + num_replicas

    ftotal = 0
    for rep in range(num_replicas):
        skip_rep = False
        while True:
            item = cur
            repeat_descent = False
            r = rep + ftotal
            while True:
                repeat_bucket = False
                bd = item.choose(x, r)
                match bd:
                    case Bucket() as b:
                        if b.type != target:
                            item = b
                            repeat_bucket = True
                            continue

                        if is_collision(out, outpos, b.id):
                            if ftotal >= tries:
                                skip_rep = True
                            else:
                                ftotal += 1
                                repeat_descent = True
                            break

                        if recurse_to_leaf:
                            res = choose_firstn(
                                x,
                                b,
                                "osd",
                                1,
                                0,
                                recurcive_tries,
                                0,
                                False,
                                out2,
                                [],
                                outpos,
                            )
                            if res <= outpos:
                                skip_rep = True
                                break
                        out.append(b)  # type: ignore by invariant
                        outpos += 1
                    case Device() as d:
                        if (
                            target != "osd"
                            or is_collision(out, outpos, d.info.id)
                            or is_out(d.weight, d.info.id, x)
                        ):
                            if ftotal >= tries:
                                skip_rep = True
                            else:
                                ftotal += 1
                                repeat_descent = True
                            break
                        out.append(d)  # type: ignore by invariant
                        outpos += 1
                        if recurse_to_leaf:
                            out2.append(d)
                if not repeat_bucket:
                    break
            if not repeat_descent:
                break

        if skip_rep:
            continue

    return outpos


def apply(
    x: int,
    root: Bucket,
    rule: Rule,
    pool_replicas: int,
    tunables: Tunables,
) -> list[Device] | str:
    i: list[Device | Bucket] = [root]
    o: list[Device] = []

    rules = rule.rules
    new_i: list[Device | Bucket] = []
    for j in range(len(rules)):
        s = rules[j]
        match s:
            case StepTake() as t:
                for item in i:
                    h = bfs(item, t.name)
                    if h is None:
                        continue
                    new_i.append(h)
            case StepChoose() as c:
                if c.is_chooseleaf:
                    for item in i:
                        if isinstance(item, Device):
                            continue

                        out: list[Bucket] | list[Device] = []
                        out2: list[Device] = []
                        choose_firstn(
                            x,
                            item,
                            c.bucket_type,
                            s.n,
                            pool_replicas,
                            tunables.choose_total_tries,
                            tunables.choose_total_tries,
                            True,
                            out,
                            out2,
                            0,
                        )
                        new_i.extend(out2)
                else:
                    for item in i:
                        if isinstance(item, Device):
                            continue

                        out: list[Bucket] | list[Device] = []
                        choose_firstn(
                            x,
                            item,
                            c.bucket_type,
                            s.n,
                            pool_replicas,
                            tunables.choose_total_tries,
                            tunables.choose_total_tries,
                            False,
                            out,
                            [],
                            0,
                        )
                        new_i.extend(out)
            case StepEmit():
                buckets: list[str] = []
                for item in i:
                    if isinstance(item, Bucket):
                        buckets.append(f"[{item.id}] {item.name}")

                if len(buckets) > 0:
                    return "{}th step of crush rule generated buckets: {}".format(
                        ",".join(buckets)
                    )
                o.extend(i)  # type: ignore
                i = []
        i = new_i
        new_i = []

    return o
