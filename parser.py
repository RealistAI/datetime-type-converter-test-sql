import argparse

def arg_parser():
    parser = argparse.ArgumentParser(description='Translating Sql')
    parser.add_argument('--timestamp_to_datetime', dest='timestamp_to_datetime', action='store', type=bool, help='')
    parser.add_argument('--lowercase_to_uppercase', dest='lowercase_to_uppercase', action='store', type=bool, help='')

    args = parser.parse_args()
    args_dict = dict(vars(args))
    return args_dict
