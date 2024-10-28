from pprint import pprint
from crush import Tunables, apply
from map import text
from parser import Parser


def main():
    p = Parser(text)
    (root, rules) = p.parse()

    res = apply(0, root, rules[0], 2, 3, Tunables(5))

    pprint(res)


if __name__ == "__main__":
    main()
