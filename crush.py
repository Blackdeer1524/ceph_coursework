from dataclasses import dataclass
from typing import Literal
from hashing import crush_hash_2
from parser import Bucket, BucketT, Device, Rule


def bfs(h: Bucket, name: str) -> Bucket | None:
    q: list[Bucket | Device] = [h]
    while len(q) > 0:
        s = q.pop()
        if not isinstance(s, Bucket):
            continue
        if s.name == name:
            return s
        for c in s.children:
            match c:
                case (Device() as d, _):
                    q.append(d)
                case Bucket() as b:
                    q.append(b)
    return None


@dataclass
class Tunables:
    # Tunable. The default value when the CHOOSE_TRIES or CHOOSELEAF_TRIES steps are omitted in a rule.
    choose_total_tries: int


def is_out(weight: int, item: int, x: int):
    if weight >= 0x10000:
        return False
    if weight == 0:
        return True
    if crush_hash_2(x, item) & 0xFFFF < weight:
        return False
    return True


def is_collision(out: list[Device] | list[Bucket], outpos: int, id: int) -> bool:
    return any(o.id == id for o in out[:outpos])


def choose_firstn(
    x: int,
    cur: Bucket,
    target: BucketT | Literal["osd"],
    num_replicas: int,
    tries: int,
    recurcive_tries: int,
    recurse_to_leaf: bool,
    out: list[Device] | list[Bucket],
    out2: list[Device],
    outpos: int,
) -> int:
    ftotal = 0
    for r in range(num_replicas):
        skip_rep = False

        while True:
            item = cur
            repeat_descent = False
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
                    case (Device() as d, weight):
                        if (
                            target != "osd"
                            or is_collision(out, outpos, d.id)
                            or is_out(weight, d.id, x)
                        ):
                            if ftotal >= tries:
                                skip_rep = True
                            else:
                                ftotal += 1
                                repeat_descent = True
                            break
                        out2.append(d)  # type: ignore by invariant
                if not repeat_bucket:
                    break
            if not repeat_descent:
                break

        if skip_rep:
            continue

        out.append(item)  # type: ignore by invariant
        outpos += 1
    return outpos


def apply(
    x: int, h: Bucket, r: Rule, num_reps: int, tunables: Tunables
) -> list[Device] | list[Bucket] | str:
    start = bfs(h, r.take.bucket_name)
    if start is None:
        return r.take.bucket_name + " not found"

    out: list[Device] | list[Bucket] = []
    out2: list[Device] = []

    if r.choose.is_chooseleaf:
        choose_firstn(
            x,
            start,
            r.choose.bucket_type,
            num_reps,
            tunables.choose_total_tries,
            tunables.choose_total_tries,
            True,
            out,
            out2,
            0,
        )
        out = out2
    return out
