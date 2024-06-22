import json
from parser_1 import Parser

def main():
    with open('config.json', 'r') as file:
        config = json.load(file)

    parser = Parser(config)
    parser.run()

if __name__ == "__main__":
    main()