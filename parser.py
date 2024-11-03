"""
device: device osd.{INT} [class STR] 

bucket: [bucket-type] [bucket-name](STR) {
    "id" [a unique negative numeric ID] 
    "weight" [the relative capacity/capability of the item(s)]
    "alg" [the bucket type: uniform | list | tree | straw2 ]  
    "hash" [the hash] 
    "item" [item-name] weight [weight]
}

rule: "rule" <rulename> {
    id <unique number>
    type [replicated | erasure]
    min_size <min-size>
    max_size <max-size>
    step take <bucket-type> [class <class-name>]
    step [choose|chooseleaf] [firstn|indep] <N> <bucket-type>
    step emit
}
"""

import sys
from dataclasses import dataclass, field
from enum import Enum, StrEnum, auto
from typing import Any, Generator, Literal, NewType, NoReturn, Optional, Self

import platform

from hashing import crush_hash32_3, crush_ln

assert platform.system() == "Linux", "Systems other than GNU/Linux are NOT supported"


class BucketT(StrEnum):
    host = auto()
    chassis = auto()
    rack = auto()
    row = auto()
    pdu = auto()
    pod = auto()
    room = auto()
    datacenter = auto()
    region = auto()
    root = auto()

    def __le__(self, value: Any) -> bool:
        return BucketT.BUCKETS_HIERARCHY[self] <= BucketT.BUCKETS_HIERARCHY[value]  # type: ignore

    def __lt__(self, value: Any) -> bool:
        return BucketT.BUCKETS_HIERARCHY[self] < BucketT.BUCKETS_HIERARCHY[value]  # type: ignore

    def __ge__(self, value: Any) -> bool:
        return BucketT.BUCKETS_HIERARCHY[self] >= BucketT.BUCKETS_HIERARCHY[value]  # type: ignore

    def __gt__(self, value: Any) -> bool:
        return BucketT.BUCKETS_HIERARCHY[self] > BucketT.BUCKETS_HIERARCHY[value]  # type: ignore


BucketT.BUCKETS_HIERARCHY: dict[BucketT, int] = {  # type: ignore
    "osd": 0,
    BucketT.host: 1,
    BucketT.chassis: 2,
    BucketT.rack: 3,
    BucketT.row: 4,
    BucketT.pdu: 5,
    BucketT.pod: 6,
    BucketT.room: 7,
    BucketT.datacenter: 8,
    BucketT.region: 9,
    BucketT.root: 10,
}


DeviceID_T = NewType("DeviceID_T", int)  # type invariant: always > 0
BucketID_T = NewType("BucketID_T", int)  # type invariant: always < 0
# WeightT = NewType("WeightT", int)  # 16.16 fixed point float number


class WeightT:
    def __init__(self, w: float):
        self.value = int(w * (1 << 16))

    def __repr__(self) -> str:
        if self.value == 0:
            return "OutOfCluster"
        return str(self.value >> 16)

    def __lt__(self, other: Self) -> bool:
        return self.value < other.value

    def __le__(self, other: Self) -> bool:
        return self.value <= other.value

    def __gt__(self, other: Self) -> bool:
        return self.value > other.value
    
    def __ge__(self, other: Self) -> bool:
        return self.value >= other.value

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, WeightT):
            raise NotImplementedError()
        return self.value == other.value


OutOfClusterWeight = WeightT(0.0)
UnitWeight = WeightT(1.0)


@dataclass
class Device:
    id: DeviceID_T
    device_class: str | None = None
    

class AlgType(Enum):
    uniform = auto()
    list = auto()
    tree = auto()
    straw2 = auto()


@dataclass
class Straw2Arg:
    weights: list[int]
    ids: list[int]


@dataclass
class Weights:
    buckets_ws: dict[BucketID_T, WeightT]
    devices_ws: dict[DeviceID_T, WeightT]


@dataclass
class Bucket:
    name: str
    type: BucketT
    id: BucketID_T
    # NOTE: can implement device class hierarchy using dict[device_class, list[Self | tuple[Device, int]]]
    children: list[Self | Device]
    alg: AlgType  # actually AlgType.straw2
    # hash: int = 0 # will NOT have hash field

    def choose(self, x: int, r: int, ws: Weights) -> tuple[Self | Device, WeightT]:
        match self.alg:
            case AlgType.uniform:
                return self._choose_uniform(x, r, ws)
            case AlgType.straw2:
                return self._choose_straw2(x, r, ws)
            case _:
                raise NotImplementedError()

    def _choose_uniform(
        self, x: int, r: int, ws: Weights
    ) -> tuple[Self | Device, WeightT]:
        s = hash((x, self.id, r))
        c = self.children[s % len(self.children)]
        match c:
            case Bucket() as b:
                return b, ws.buckets_ws[b.id]
            case Device() as d:
                return d, ws.devices_ws[d.id]

    def _choose_straw2(
        self, x: int, r: int, ws: Weights
    ) -> tuple[Self | Device, WeightT]:
        assert self.alg == AlgType.straw2

        S64_MIN = -((1 << 64) - 1)

        high_draw = 0
        high = 0
        for i in range(len(self.children)):
            db = self.children[i]
            match db:
                case Device(id=id):
                    w = ws.devices_ws[id]
                case Bucket(id=id):
                    w = ws.buckets_ws[id]

            if w == OutOfClusterWeight:
                draw = S64_MIN
            else:
                # hash(*, -1, *) == hash(*, -2, *)
                # https://stackoverflow.com/questions/10130454/why-do-1-and-2-both-hash-to-2-in-cpython
                args = (x, abs(db.id), r)
                u = hash(args)
                u &= 0xFFFF
                ln = crush_ln(u) - 0x1000000000000
                draw = int(ln / w.value)

            if i == 0 or high_draw < draw:
                high = i
                high_draw = draw
                rw = w
        return self.children[high], rw  # type: ignore (len(self.children) is always > 0)


@dataclass
class StepTake:
    name: str
    device_class: str | None = None


@dataclass
class StepChoose:
    is_chooseleaf: bool
    n: int
    bucket_type: BucketT | Literal["osd"]


@dataclass
class StepEmit: ...


StepT = StepTake | StepChoose | StepEmit


@dataclass
class Rule:
    name: str
    id: int

    min_size: int
    max_size: int

    rules: list[StepT]


@dataclass
class ParserResult:
    root: Bucket
    ws: Weights
    rules: list[Rule]


class Parser:
    def __init__(self, text: str):
        self.text = text
        self.cursor = 0

        self.last_newline_pos = -1
        self.row = 1
        self.col = 1

    def parse(self) -> ParserResult:
        self.skip_whitespace_lns()

        seen_devices = {"osd." + str(d.id): d for d in self.parse_devices()}

        buckets: list[Bucket] = []
        seen_buckets: set[str] = set()

        root_node: Optional[Bucket] = None

        ws = Weights({}, {})
        for b, b_ws in self.parse_buckets(seen_devices):
            buckets.append(b)
            seen_buckets.add(b.name)

            if b.type == "root":
                if root_node is not None:
                    self.report_error_with_line(
                        f"root node already registered: {root_node.name}"
                    )
                root_node = b

            ws.buckets_ws.update(b_ws.buckets_ws)
            ws.devices_ws.update(b_ws.devices_ws)

        if root_node is None:
            self.report_error_with_line("no root node found")

        seen_buckets_c = {b for b in seen_buckets}
        seen_buckets_c.remove(root_node.name)

        def traverse(cur: Bucket):
            for item in cur.children:
                match item:
                    case Bucket(name=name) as b:
                        seen_buckets_c.remove(name)
                        traverse(b)
                    case Device():
                        ...

        traverse(root_node)
        if len(seen_buckets_c) > 0:
            self.report_error("found disconected nodes: " + ",".join(seen_buckets_c))

        rules = list(self.parse_rules(seen_buckets))
        return ParserResult(root_node, ws, rules)

    def skip_n(self, n: int) -> None:
        self.advance(n)

    def read_num(self) -> str | None:
        new = self.cursor
        while new < len(self.text) and self.text[new].isnumeric():
            new += 1

        if new == self.cursor:
            return None

        if new >= len(self.text) or self.text[new].isspace():
            return self.text[self.cursor : new]
        return None

    def read_float(self) -> str | None:
        new = self.cursor
        while new < len(self.text) and self.text[new].isnumeric():
            new += 1
        nonempty_prefix = new != self.cursor

        if new < len(self.text) and self.text[new] == ".":
            new += 1

        suf_start = new
        while new < len(self.text) and self.text[new].isnumeric():
            new += 1
        nonempty_suffix = suf_start != new

        if (nonempty_prefix or nonempty_suffix) and (
            new >= len(self.text) or self.text[new].isspace()
        ):
            return self.text[self.cursor : new]
        return None

    def match_prefix(self, target: str) -> bool:
        return self.text[self.cursor : self.cursor + len(target)] == target

    def match_substr(self, target: str) -> bool:
        if self.cursor >= len(self.text):
            return False
        if self.cursor + len(target) >= len(self.text):
            return self.text[self.cursor : self.cursor + len(target)] == target
        return (
            self.text[self.cursor + len(target)].isspace()
            and self.text[self.cursor : self.cursor + len(target)] == target
        )

    def read_word(self) -> str | None:
        new = self.cursor
        if new >= len(self.text) or not self.text[new].isalpha():
            return None
        new += 1
        while new < len(self.text) and (
            self.text[new].isalnum() or self.text[new] in "-_."
        ):
            new += 1
        return self.text[self.cursor : new]

    def report_error(self, msg: str) -> NoReturn:
        print(msg, file=sys.stderr)
        exit(1)

    def report_error_with_line(self, msg: str) -> NoReturn:
        if self.cursor < len(self.text):
            right_newline = self.cursor + self.text[self.cursor :].find("\n")
        else:
            right_newline = len(self.text)

        col_prefix = f"{self.row} | "
        print(
            "{}{}\n{}^\n{}{}\n".format(
                col_prefix,
                self.text[self.last_newline_pos + 1 : right_newline],
                " " * (len(col_prefix) + self.col - 1),
                " " * len(col_prefix),
                msg,
            ),
            file=sys.stderr,
        )
        exit(1)

    def read_bucket_type(self) -> BucketT | None:
        for t in BucketT:
            if self.match_substr(t):
                return t
        return None

    def advance(self, n: int = 1) -> None:
        self.cursor += n
        self.col += n

    def skip_whitespace_lns(self) -> None:
        while self.cursor < len(self.text) and self.text[self.cursor].isspace():
            if self.text[self.cursor] == "\n":
                self.last_newline_pos = self.cursor
                self.row += 1
                self.col = 0
            self.advance()

    def skip_whitespace_lns_required(self) -> None:
        found = False
        while self.cursor < len(self.text) and self.text[self.cursor].isspace():
            if self.text[self.cursor] == "\n":
                found = True
                self.last_newline_pos = self.cursor
                self.row += 1
                self.col = 0
            self.advance()
        if not found:
            self.report_error_with_line("new line chars not found")

    def skip_whitespace_to_token_this_line(self) -> None:
        new = self.cursor
        while new < len(self.text) and self.text[new] in (" ", "\t"):
            new += 1
        if (
            new == self.cursor
            and not self.text[self.cursor].isspace()
            and self.text[self.cursor] not in "{}"
        ):
            self.report_error_with_line("expected a blank space")
        self.advance(new - self.cursor)

    def parse_devices(self) -> Generator[Device, None, None]:
        device_nums: set[str] = set()
        seen_ids: set[str] = set()
        while True:
            if not self.match_substr("device"):
                maybe_type = self.read_bucket_type()
                if maybe_type is not None:
                    return
                self.report_error_with_line(
                    """expected "device" or buckets description"""
                )

            self.skip_n(len("device"))
            self.skip_whitespace_to_token_this_line()

            device_num = self.read_num()
            if device_num is None:
                self.report_error_with_line("expected a device number")

            if device_num in device_nums:
                self.report_error_with_line(
                    "device with this number is already defined"
                )
            device_nums.add(device_num)

            self.skip_n(len(device_num))
            self.skip_whitespace_to_token_this_line()

            if not self.match_prefix("osd."):
                self.report_error_with_line("expected osd id declaration")

            self.skip_n(len("osd."))
            osd_id = self.read_num()
            if osd_id is None:
                self.report_error_with_line("bad osd declaration: expected a number")
            if osd_id in seen_ids:
                self.report_error_with_line("osd id already registered")
            seen_ids.add(osd_id)

            self.skip_n(len(osd_id))
            self.skip_whitespace_to_token_this_line()

            if not self.match_substr("class"):
                yield Device(DeviceID_T(int(osd_id)))
                continue

            self.skip_n(len("class"))
            self.skip_whitespace_to_token_this_line()
            class_name = self.read_word()
            if class_name is None:
                self.report_error_with_line("expected a device class")
            self.skip_n(len(class_name))
            yield Device(DeviceID_T(int(osd_id)), class_name)
            self.skip_whitespace_lns_required()

    def parse_buckets(
        self, seen_devices: dict[str, Device]
    ) -> Generator[tuple[Bucket, Weights], None, None]:
        seen_ids: set[str] = set()
        seen_buckets: dict[str, Bucket] = {}
        child2parent: dict[str, str] = {}

        while True:
            bucket_type = self.read_bucket_type()
            if bucket_type is None:
                if self.match_substr("rule"):
                    return
                self.report_error_with_line("expected a bucket type")

            self.skip_n(len(bucket_type))
            self.skip_whitespace_to_token_this_line()

            bucket_name = self.read_word()
            if bucket_name is None:
                self.report_error_with_line("expected a bucket name")

            if bucket_name in seen_buckets:
                self.report_error_with_line(
                    f"bucket with name `{bucket_name}` already exists"
                )

            self.skip_n(len(bucket_name))
            self.skip_whitespace_to_token_this_line()

            b, ws = self.parse_bucket_block(
                bucket_name,
                bucket_type,
                seen_devices,
                seen_buckets,
                child2parent,
                seen_ids,
            )
            seen_buckets[b.name] = b

            yield b, ws
            self.skip_whitespace_lns_required()

    def parse_bucket_block(
        self,
        bucket_name: str,
        bucket_type: BucketT,
        seen_devices: dict[str, Device],
        seen_buckets: dict[str, Bucket],
        child2parent: dict[str, str],
        seen_ids: set[str],
    ) -> tuple[Bucket, Weights]:
        if not self.match_substr("{"):
            self.report_error_with_line("expected a bucket block start")
        self.skip_n(1)
        self.skip_whitespace_lns_required()

        b_id: BucketID_T | None = None
        b_alg: AlgType | None = None
        b_hash: Optional[int] = None

        while True:
            field = self.read_word()
            if field is None:
                if self.match_substr("}"):
                    self.report_error_with_line("found bucket with no children")
                self.report_error_with_line("expected a bucket field")
            if field == "id":
                if b_id is not None:
                    self.report_error_with_line("found double declaration of a field")

                self.skip_n(len(field))
                self.skip_whitespace_to_token_this_line()

                if not self.match_prefix("-"):
                    self.report_error_with_line(
                        "expected a bucket ID (which are always negative)"
                    )
                self.skip_n(1)

                bucket_id = self.read_num()
                if bucket_id is None:
                    self.report_error_with_line(
                        "expected a bucket ID (which are always negative)"
                    )

                if bucket_id in seen_ids:
                    self.report_error_with_line(
                        f"bucket with id `{bucket_id}` already exists"
                    )
                seen_ids.add(bucket_id)

                self.skip_n(len(bucket_id))
                b_id = BucketID_T(-int(bucket_id))
            elif field == "alg":
                if b_alg is not None:
                    self.report_error_with_line("found double declaration of a field")

                self.skip_n(len(field))
                self.skip_whitespace_to_token_this_line()

                alg = self.read_word()
                if alg is None:
                    self.report_error_with_line(
                        "expected algorith types: uniform, straw2"
                    )
                self.skip_n(len(alg))

                if alg == "uniform":
                    b_alg = AlgType.uniform
                elif alg == "straw2":
                    b_alg = AlgType.straw2
                else:
                    self.report_error_with_line(
                        "only uniform & straw2 algs are allowed"
                    )

                # elif alg == "list":
                #     b_alg = AlgType.list
                # elif alg == "tree":
                #     b_alg = AlgType.tree
                # elif alg == "straw2":
                #     b_alg = AlgType.straw2
                # else:
                #     self.report_error_with_line(
                #         "unknown alg type: only uniform, list, tree, straw2 are allowed"
                #     )
            elif field == "hash":
                if b_hash is not None:
                    self.report_error_with_line("found double declaration of a field")

                self.skip_n(len(field))
                self.skip_whitespace_to_token_this_line()

                hash = self.read_num()
                if hash is None:
                    self.report_error_with_line("expected hash")
                if hash != "0":
                    self.report_error_with_line("only `0` hash is supported")

                self.skip_n(len(hash))
                b_hash = 0
            elif field == "item":
                if b_id is None:
                    self.report_error_with_line("expected a bucket to have an ID")
                if b_alg is None:
                    b_alg = AlgType.straw2
                if b_hash is None:
                    b_hash = 0

                items, ws = self.parse_bucket_items(
                    bucket_name, bucket_type, seen_devices, seen_buckets, child2parent
                )

                if not self.match_substr("}"):
                    self.report_error_with_line("expected a bucket block end")
                self.skip_n(1)

                return (
                    Bucket(bucket_name, bucket_type, b_id, items, b_alg),
                    ws,
                )
            else:
                self.report_error_with_line("unknown field")
            self.skip_whitespace_lns_required()

    def parse_bucket_items(
        self,
        parent: str,
        parent_type: BucketT,
        seen_devices: dict[str, Device],
        seen_buckets: dict[str, Bucket],
        child2parent: dict[str, str],
    ) -> tuple[list[Bucket | Device], Weights]:
        res: list[Bucket | Device] = []
        buckets_weights: dict[BucketID_T, WeightT] = {}
        devices_weights: dict[DeviceID_T, WeightT] = {}
        while True:
            item_res = self.parse_bucket_item(
                parent, parent_type, seen_buckets, seen_devices, child2parent
            )
            match item_res:
                case Bucket(id=id), w:
                    buckets_weights[id] = w
                case Device(id=id), w:
                    devices_weights[id] = w
                case None:
                    break
            res.append(item_res[0])
            self.skip_whitespace_lns_required()
        return res, Weights(buckets_weights, devices_weights)

    def parse_bucket_item(
        self,
        parent: str,
        parent_type: BucketT,
        seen_buckets: dict[str, Bucket],
        seen_devices: dict[str, Device],
        child2parent: dict[str, str],
    ) -> tuple[Bucket | Device, WeightT] | None:
        item_decl = self.read_word()
        if item_decl != "item":
            if self.match_substr("}"):
                return None
            self.report_error_with_line("expected an item declaration")
        self.skip_n(len(item_decl))
        self.skip_whitespace_to_token_this_line()

        item_name = self.read_word()
        if item_name is None:
            self.report_error_with_line("expected an item name")

        if (b := seen_buckets.get(item_name)) is not None:
            if b.type >= parent_type:
                self.report_error_with_line(
                    f"hierarchy violation: {item_name}({b.type}) is a child of {parent}({parent_type})"
                )
        elif item_name not in seen_devices:
            self.report_error_with_line("unknown item")

        if (p := child2parent.get(item_name)) is not None:
            self.report_error_with_line(f"item already registered at {p}")
        child2parent[item_name] = parent

        self.skip_n(len(item_name))
        self.skip_whitespace_to_token_this_line()

        weight: WeightT | None = None
        while True:
            key = self.read_word()
            if key is None:
                if self.cursor >= len(self.text):
                    self.report_error_with_line("unexpected EOF")
                if self.text[self.cursor].isspace():  # \n
                    break
                self.report_error_with_line("bad field name")

            if key == "weight":
                self.skip_n(len(key))
                self.skip_whitespace_to_token_this_line()

                w = self.read_float()
                if w is None:
                    self.report_error_with_line("expected a float number")
                weight = WeightT(float(w))

                self.skip_n(len(w))
                self.skip_whitespace_to_token_this_line()
            else:
                self.report_error_with_line("unexpected attribute")

        if weight is None:
            self.report_error_with_line("no weight was declared")

        if (b := seen_buckets.get(item_name)) is not None:
            return b, weight
        return seen_devices[item_name], weight

    def parse_rules(self, seen_buckets: set[str]) -> Generator[Rule, None, None]:
        seen_ids: set[int] = set()
        seen_names: set[str] = set()
        while True:
            if self.cursor >= len(self.text):
                return

            if not self.match_substr(target="rule"):
                self.report_error_with_line("expected a rule declaration")
            self.skip_n(len("rule"))
            self.skip_whitespace_to_token_this_line()

            rule_name = self.read_word()
            if rule_name is None:
                self.report_error_with_line("expected a rule name")
            self.skip_n(len(rule_name))
            self.skip_whitespace_to_token_this_line()

            rule = self.parse_rule_block(rule_name, seen_buckets)
            if rule.id in seen_ids:
                self.report_error_with_line(f"rule with id `{rule.id}` already exists")
            if rule.name in seen_names:
                self.report_error_with_line(
                    f"rule with name `{rule.name}` alread exists"
                )

            seen_ids.add(rule.id)
            seen_names.add(rule.name)

            yield rule
            self.skip_whitespace_lns_required()

    def parse_rule_block(self, name: str, seen_buckets: set[str]) -> Rule:
        if not self.match_substr("{"):
            self.report_error_with_line("expected a rule block")
        self.skip_n(1)
        self.skip_whitespace_lns_required()

        rule_id: int | None = None
        rule_min_size = 1
        rule_max_size = 10
        while True:
            key = self.read_word()
            if key is None:
                self.report_error_with_line("expected a rule attribute")

            if key == "id":
                self.skip_n(len(key))
                self.skip_whitespace_to_token_this_line()

                found_id = self.read_num()
                if found_id is None:
                    self.report_error_with_line("expected a rule id")
                rule_id = int(found_id)

                self.skip_n(len(found_id))
                self.skip_whitespace_lns_required()
            elif key == "type":
                self.skip_n(len(key))
                self.skip_whitespace_to_token_this_line()

                rule_type = self.read_word()
                if rule_type is None:
                    self.report_error_with_line('expected "replacated" rule type')
                elif rule_type != "replicated":
                    self.report_error_with_line(
                        'not "replacated" rules are not supported'
                    )

                self.skip_n(len(rule_type))
                self.skip_whitespace_lns_required()
            elif key == "min_size":
                self.skip_n(len(key))
                self.skip_whitespace_to_token_this_line()

                found_min_size = self.read_num()
                if found_min_size is None:
                    self.report_error_with_line("expected min_size")
                rule_min_size = int(found_min_size)
                self.skip_n(len(found_min_size))
                self.skip_whitespace_lns_required()
            elif key == "max_size":
                self.skip_n(len(key))
                self.skip_whitespace_to_token_this_line()

                found_max_size = self.read_num()
                if found_max_size is None:
                    self.report_error_with_line("expected max_size")
                rule_max_size = int(found_max_size)
                self.skip_n(len(found_max_size))
                self.skip_whitespace_lns_required()
            elif key == "step":
                rules = self.parse_rule_steps(seen_buckets)
                if not self.match_substr("}"):
                    self.report_error_with_line("expected an end of rule declaration")
                self.skip_n(1)

                if rule_id is None:
                    self.report_error_with_line("no rule's id was declared")
                return Rule(
                    name=name,
                    id=rule_id,
                    min_size=rule_min_size,
                    max_size=rule_max_size,
                    rules=rules,
                )
            else:
                self.report_error_with_line("unexpected rule field")

    def parse_rule_steps(self, seen_buckets: set[str]) -> list[StepT]:
        rules: list[StepT] = []
        while True:
            if not self.match_substr("step"):
                if self.match_substr("}"):
                    break
                self.report_error_with_line("expected rule `take` step")
            self.skip_n(len("step"))
            self.skip_whitespace_to_token_this_line()

            choice = self.read_word()
            if choice is None:
                self.report_error_with_line("expected step type")
            self.skip_n(len(choice))
            self.skip_whitespace_to_token_this_line()
            match choice:
                case "take":
                    rules.append(self.parse_step_take(seen_buckets))
                case "choose" | "chooseleaf":
                    rules.append(self.parse_step_choose(choice != "choose"))
                case "emit":
                    rules.append(StepEmit())
                case _:
                    self.report_error_with_line("unexpected step type")

            self.skip_whitespace_lns_required()

        if len(rules) == 0:
            self.report_error_with_line("rule with no steps")
        elif not isinstance(rules[-1], StepEmit):
            self.report_error_with_line("last step of rule has to be emit")
        return rules

    def parse_step_take(self, seen_buckets: set[str]) -> StepTake:
        bucket = self.read_word()
        if bucket is None:
            self.report_error_with_line("expected bucket name")
        if bucket not in seen_buckets:
            self.report_error_with_line("unknown bucket name")

        self.skip_n(len(bucket))
        self.skip_whitespace_to_token_this_line()

        class_opt = self.read_word()
        if class_opt is None:
            return StepTake(bucket)

        if class_opt != "class":
            self.report_error_with_line(
                "expected to see class option on the same line with `take` step"
            )
        self.skip_n(len(class_opt))
        self.skip_whitespace_to_token_this_line()

        cls = self.read_word()
        if cls is None:
            self.report_error_with_line("expected a device class")
        self.skip_n(len(cls))

        return StepTake(bucket, cls)

    def parse_step_choose(self, is_chooseleaf: bool) -> StepChoose:
        choice_opt = self.read_word()
        if choice_opt is None:
            self.report_error_with_line("expected `firstn` option")
        if choice_opt != "firstn":
            self.report_error_with_line("only `firstn` option is supported")
        self.skip_n(len(choice_opt))
        self.skip_whitespace_to_token_this_line()

        N = self.read_num()
        if N is None:
            self.report_error_with_line("expected a number")
        self.skip_n(len(N))
        self.skip_whitespace_to_token_this_line()

        if not self.match_substr("type"):
            self.report_error_with_line("expected a `type` keyword")
        self.skip_n(len("type"))
        self.skip_whitespace_to_token_this_line()

        bucket_type: (BucketT | None) | Literal["osd"] = self.read_bucket_type()
        if bucket_type is None:
            if not self.match_substr("osd"):
                self.report_error_with_line("expected a bucket type")
            bucket_type = "osd"

        self.skip_n(len(bucket_type))

        return StepChoose(
            is_chooseleaf=is_chooseleaf, n=int(N), bucket_type=bucket_type
        )
