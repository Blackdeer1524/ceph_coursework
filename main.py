
def read_from_stdin_til_eof() -> Generator[str, None, None]:
    while True:
        s = sys.stdin.readline()
        if s == "":
            return
        yield s

def main():
    q = open("./maps/default_map").readlines()
    m = "".join(q)
    # m = "".join(read_from_stdin_til_eof())
    # print(m)

    cfg = PoolParams(size=3, min_size=2, pg_count=100)
    tunables = Tunables(5)

    p = Parser(m)
    r = p.parse()

    osd2pg = map_pg(r.root, r.rules[0], tunables, cfg)
    # pprint(osd2pg)
    pprint({key: len(value) for key, value in osd2pg.items()})


if __name__ == "__main__":
    main()
