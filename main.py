from pprint import pprint
from map import text
from parser import Parser


def main():
    t = Parser(text)
    (buckets, rules) = t.parse()

    # seen_devices = {d.name for d in devices}

    m = {}
    for b in buckets:
        m[b.name] = b

    pprint(buckets)
    pprint(rules)


if __name__ == "__main__":
    main()
