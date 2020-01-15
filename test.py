from tests import *


def main():
    for func in TESTS:
        print('Test: %s' % func.__name__)
        func()


if __name__ == '__main__':
    main()
