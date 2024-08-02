import argparse
import random
import datetime
import rstr

parser = argparse.ArgumentParser("Generate a JSON file based on the example json")

parser.add_argument(
    "--example", type=str, help="Path to the example json file", required=True
)
parser.add_argument("--output", type=str, help="Path to the output file", required=True)
parser.add_argument(
    "--count", type=int, help="Number of json records to generate", required=True
)

args = parser.parse_args()

# Read the config file
with open(args.example, "r") as f:
    example = f.read().split()

# Value types
value_types = ["int", "float", "str", "bool", "timestamp"]


def choice(value_list, k):
    if k == len(value_list):
        return value_list
    # Ensure that all values are chosen at least once
    if k < len(value_list):
        raise ValueError("k must be greater than or equal to the length of value_list.")

    # Shuffle the list and extend it
    extended_list = value_list * (k // len(value_list)) + random.sample(value_list, k % len(value_list))

    # Shuffle the extended list to ensure randomness
    random.shuffle(extended_list)

    return extended_list

# Generate the json records
def gen_int(num_range):
    return str(random.randint(num_range[0], num_range[1]))


def gen_float(num_range):
    return str(random.uniform(num_range[0], num_range[1]))


def gen_bool():
    return str(random.choice([True, False])).lower()


def gen_str(regex_str):
    return '"' + rstr.xeger(regex_str[1:-1]) + '"'


def gen_timestamp():
    return (
        '"'
        + datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        + '"'
    )


def gen_based_on_ndv(value_type, ndv, kargs):
    unique_values = []
    num = int(ndv * args.count) + 1 if ndv != 1 else args.count
    for _ in range(num):
        if value_type == "int":
            unique_values.append(gen_int(kargs))
        elif value_type == "float":
            unique_values.append(gen_float(kargs))
        elif value_type == "bool":
            unique_values.append(gen_bool())
        elif value_type == "str":
            unique_values.append(gen_str(kargs))
    random.shuffle(unique_values)
    return unique_values


def gen_value_list():
    value_lists = []
    for i, word in enumerate(example):
        if word == "int":
            ndv = float(example[i + 1])
            num_range = [0, 1000]
            value_lists.append(gen_based_on_ndv("int", ndv, num_range))
        elif word == "float":
            ndv = float(example[i + 1])
            num_range = [0, 1000]
            value_lists.append(gen_based_on_ndv("float", ndv, num_range))
        elif word == "bool":
            ndv = float(example[i + 1])
            value_lists.append(gen_based_on_ndv("bool", ndv, None))
        elif word == "str":
            ndv = float(example[i + 1])
            regex_str = example[i + 2]
            value_lists.append(gen_based_on_ndv("str", ndv, regex_str))
    choice_lists = []
    for value_list in value_lists:
        choice_lists.append(choice(value_list, args.count))
    return choice_lists


def gen_json(value_lists, cnt):
    skip_next = False
    skip_twice = False
    i = 0
    json = ""
    for word in example:
        if skip_next:
            skip_next = False
            continue
        if skip_twice:
            skip_next = True
            skip_twice = False
            continue
        skip_next = True
        if word == "int" or word == "float" or word == "bool":
            json += value_lists[i][cnt]
            i += 1
        elif word == "str":
            json += value_lists[i][cnt]
            i += 1
            skip_twice = True
            skip_next = False
        elif word == "timestamp":
            json += str(gen_timestamp())
            skip_next = False
        else:
            json += word
            skip_next = False
    return json


if __name__ == "__main__":
    value_lists = gen_value_list()
    for i in range(args.count,):
        json = gen_json(value_lists, i)
        json.replace("\n", "")
        print(json)
