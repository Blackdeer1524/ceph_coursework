from pprint import pprint
from map import text
from parser import Parser


def main():
    p = Parser(text)
    (root, rules) = p.parse()
    
    pprint(root)
    pprint(rules)


if __name__ == "__main__":
    main()
