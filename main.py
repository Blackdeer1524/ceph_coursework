from pprint import pprint
from map import text
from parser import Parser


def main():
    t = Parser(text)
    (root, rules) = t.parse()

    # seen_devices = {d.name for d in devices}

    pprint(root)
    pprint(rules)


if __name__ == "__main__":
    main()
