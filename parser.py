"""
devices ::= (device\n)+
device ::= "device" "int" "osd.{INT}" [class STR] 

buckets ::= (bucket)+
bucket ::= [bucket-type](STR) [bucket-name](STR) "{"
    "id" [a unique negative numeric ID] "-" INT
    "weight" [the relative capacity/capability of the item(s)] INT
    "alg" [the bucket type: uniform | list | tree | straw2 ]  
    "hash" [the hash type: 0 by default] 
    "item" [item-name] STR weight [weight] INT
"}"

rules ::= (rules)*
rule ::= "rule" <rulename> {
    id <unique number>
    type [replicated | erasure]
    min_size <min-size>
    max_size <max-size>
    step take <bucket-type> [class <class-name>]
    step [choose|chooseleaf] [firstn|indep] <N> <bucket-type>
    step emit
}
"""

import platform
import sys


assert platform.system() == "Linux", "Systems other than GNU/Linux are NOT supported"

from dataclasses import dataclass
from enum import Enum, StrEnum, auto
from typing import Generator, NoReturn, Optional, Self


@dataclass()
class Device:
    seq_num: int
    name: str  # osd.{NUM}
    device_class: str | None = None


class AlgType(Enum):
    uniform = auto()
    list = auto()
    tree = auto()
    straw2 = auto()


@dataclass
class Bucket:
    name: str
    btype: str
    # weight: float  # don't know how to handle weight for a bucket
    id: int
    children: dict[str, tuple[Self | Device, float]]
    alg: AlgType
    # hash: int = 0 # will NOT have hash field


@dataclass
class TakeStep:
    bucket_type: str
    device_class: str | None = None


@dataclass
class ChoiceStep:
    is_chooseleaf: bool
    bucket_type: str


@dataclass
class Rule:
    name: str
    id: int

    min_size: int
    max_size: int

    take: TakeStep
    choose: ChoiceStep


class BucketT(StrEnum):
    osd = auto()
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

    def __lt__(self, value: str) -> bool:
        h = {
            "osd": 0,
            "host": 1,
            "chassis": 2,
            "rack": 3,
            "row": 4,
            "pdu": 5,
            "pod": 6,
            "room": 7,
            "datacenter": 8,
            "region": 9,
            "root": 10,
        }
        return h[self] < h[value]


class Parser:
    def __init__(self, text: str):
        self.text = text
        self.cursor = 0

        self.last_newline_pos = -1
        self.row = 1
        self.col = 1

    def parse(self) -> tuple[list[Bucket], list[Rule]]:
        self.skip_whitespace_lns()

        seen_devices = {d.name: d for d in self.parse_devices()}

        buckets: list[Bucket] = []
        seen_buckets: set[str] = set()

        root_node: Optional[Bucket] = None
        for b in self.parse_buckets(seen_devices):
            buckets.append(b)
            seen_buckets.add(b.name)

            if b.btype == "root":
                if root_node is not None:
                    self.report_error_with_line(
                        f"root node already registered: {root_node.name}"
                    )
                root_node = b

        if root_node is None:
            self.report_error_with_line("no root node found")

        seen_buckets_c = {b for b in seen_buckets}
        seen_buckets_c.remove(root_node.name)

        def traverse(cur: Bucket):
            for name, child in cur.children.items():
                if isinstance(child[0], Bucket):
                    seen_buckets_c.remove(name)
                    traverse(child[0])

        traverse(root_node)
        if len(seen_buckets_c) > 0:
            self.report_error("found disconected nodes: " + ",".join(seen_buckets_c))

        rules = list(self.parse_rules(seen_buckets))
        return buckets, rules

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

    def bucket_type(self) -> BucketT | None:
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
                maybe_type = self.bucket_type()
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
                yield Device(int(device_num), "osd." + osd_id)
                continue

            self.skip_n(len("class"))
            self.skip_whitespace_to_token_this_line()
            class_name = self.read_word()
            if class_name is None:
                self.report_error_with_line("expected a device class")
            self.skip_n(len(class_name))
            yield Device(int(device_num), "osd." + osd_id, class_name)
            self.skip_whitespace_lns_required()

    def parse_buckets(
        self, seen_devices: dict[str, Device]
    ) -> Generator[Bucket, None, None]:
        seen_ids: set[str] = set()
        seen_buckets: dict[str, Bucket] = {}
        child2parent: dict[str, str] = {}

        while True:
            bucket_type = self.bucket_type()
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

            b = self.parse_bucket_block(
                bucket_name,
                bucket_type,
                seen_devices,
                seen_buckets,
                child2parent,
                seen_ids,
            )
            seen_buckets[b.name] = b

            yield b
            self.skip_whitespace_lns_required()

    def parse_bucket_block(
        self,
        bname: str,
        btype: str,
        seen_devices: dict[str, Device],
        seen_buckets: dict[str, Bucket],
        child2parent: dict[str, str],
        seen_ids: set[str],
    ) -> Bucket:
        if not self.match_substr("{"):
            self.report_error_with_line("expected a bucket block start")
        self.skip_n(1)
        self.skip_whitespace_lns_required()

        b_id: int | None = None
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

                b_id = -int(bucket_id)
            elif field == "alg":
                if b_alg is not None:
                    self.report_error_with_line("found double declaration of a field")

                self.skip_n(len(field))
                self.skip_whitespace_to_token_this_line()

                alg = self.read_word()
                if alg is None:
                    self.report_error_with_line(
                        "expected a bucket algorith (one of [uniform | list | tree | straw2])"
                    )
                self.skip_n(len(alg))

                if alg == "uniform":
                    b_alg = AlgType.uniform
                elif alg == "list":
                    b_alg = AlgType.list
                elif alg == "tree":
                    b_alg = AlgType.tree
                elif alg == "straw2":
                    b_alg = AlgType.straw2
                else:
                    self.report_error_with_line(
                        "unknown alg type: only uniform, list, tree, straw2 are allowed"
                    )
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

                cdict = self.parse_bucket_items(
                    bname, seen_devices, seen_buckets, child2parent
                )

                if not self.match_substr("}"):
                    self.report_error_with_line("expected a bucket block end")
                self.skip_n(1)

                return Bucket(bname, btype, b_id, cdict, b_alg)
            else:
                self.report_error_with_line("unknown field")
            self.skip_whitespace_lns_required()

    def parse_bucket_items(
        self,
        parent: str,
        seen_devices: dict[str, Device],
        seen_buckets: dict[str, Bucket],
        child2parent: dict[str, str],
    ) -> dict[str, tuple[Bucket | Device, float]]:
        res: dict[str, tuple[Bucket | Device, float]] = {}
        while True:
            item = self.parse_bucket_item(
                parent, seen_buckets, seen_devices, child2parent
            )
            if item is None:
                break
            res[item[0].name] = item
            self.skip_whitespace_lns_required()
        return res

    def parse_bucket_item(
        self,
        parent: str,
        seen_buckets: dict[str, Bucket],
        seen_devices: dict[str, Device],
        child2parent: dict[str, str],
    ) -> tuple[Bucket | Device, float] | None:
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

        if item_name not in seen_buckets and item_name not in seen_devices:
            self.report_error_with_line("unknown item")
        if (p := child2parent.get(item_name)) is not None:
            self.report_error_with_line(f"item already registered at {p}")
        child2parent[item_name] = parent

        self.skip_n(len(item_name))
        self.skip_whitespace_to_token_this_line()

        weight: float | None = None
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
                weight = float(w)

                self.skip_n(len(w))
                self.skip_whitespace_to_token_this_line()
            else:
                self.report_error_with_line("unexpected attribute")

        if weight is None:
            self.report_error_with_line("no weight was declared")

        if (b := seen_buckets.get(item_name)) is not None:
            return (b, weight)
        else:
            return (seen_devices[item_name], weight)

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
                (take, choose) = self.parse_rule_steps(seen_buckets)
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
                    take=take,
                    choose=choose,
                )
            else:
                self.report_error_with_line("unexpected rule field")

    def parse_rule_steps(self, seen_buckets: set[str]) -> tuple[TakeStep, ChoiceStep]:
        t = self.parse_step_take(seen_buckets)
        self.skip_whitespace_lns_required()
        c = self.parse_step_choose()
        self.skip_whitespace_lns_required()
        self.parse_step_emit()
        self.skip_whitespace_lns_required()
        return (t, c)

    def parse_step_take(self, seen_buckets: set[str]) -> TakeStep:
        if not self.match_substr("step"):
            self.report_error_with_line("expected rule `take` step")
        self.skip_n(len("step"))
        self.skip_whitespace_to_token_this_line()

        if not self.match_substr("take"):
            self.report_error_with_line("expected rule `take` step")
        self.skip_n(len("take"))
        self.skip_whitespace_to_token_this_line()

        bucket = self.read_word()
        if bucket is None:
            self.report_error_with_line("expected bucket name")
        if bucket not in seen_buckets:
            self.report_error_with_line("unknown bucket name")

        self.skip_n(len(bucket))
        self.skip_whitespace_to_token_this_line()

        class_opt = self.read_word()
        if class_opt is None:
            self.skip_whitespace_lns_required()
            return TakeStep(bucket)

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

        return TakeStep(bucket, cls)

    def parse_step_choose(self) -> ChoiceStep:
        if not self.match_substr("step"):
            self.report_error_with_line("expected rule `choose` step")
        self.skip_n(len("step"))
        self.skip_whitespace_to_token_this_line()

        choice = self.read_word()
        if choice not in ("choose", "chooseleaf"):
            self.report_error_with_line("expected `choose` or `chooseleaf`")
        self.skip_n(len(choice))
        self.skip_whitespace_to_token_this_line()

        choice_opt = self.read_word()
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

        bucket_type = self.bucket_type()
        if bucket_type is None:
            self.report_error_with_line("expected a bucket type")
        self.skip_n(len(bucket_type))

        return ChoiceStep(is_chooseleaf=choice != "choose", bucket_type=bucket_type)

    def parse_step_emit(self) -> None:
        if not self.match_substr("step"):
            self.report_error_with_line("expected rule `take` step")
        self.skip_n(len("step"))
        self.skip_whitespace_to_token_this_line()

        if not self.match_substr("emit"):
            self.report_error_with_line("expected `emit`")
        self.skip_n(len("emit"))
