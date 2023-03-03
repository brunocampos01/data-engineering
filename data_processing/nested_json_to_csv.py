import json
import os

import pandas as pd

here = os.path.abspath(os.path.dirname(__file__))
path_origin = ''.join(here + '/purchase_orders.json')
path_dest = here + '/purchase_orders.csv'


def load_json(path_json: str) -> dict:
    data = open(path_json, 'r').read()
    return json.loads(data)


def flatten_json(data):
    """
    A recursive function to flatten a nested JSON.
    """
    # Initialize an empty dictionary to store flattened data
    flattened = {}

    # Loop through each key-value pair in the JSON
    for key, value in data.items():
        # If the value is a dictionary, recursively flatten it
        if isinstance(value, dict):
            # Append the flattened dictionary to the current key with '_' separator
            for k, v in flatten_json(value).items():
                flattened[f"{key}_{k}"] = v
        # If the value is a list, loop through each item and recursively flatten it
        elif isinstance(value, list):
            # Loop through each item in the list
            for i, item in enumerate(value):
                # If the item is a dictionary, recursively flatten it
                if isinstance(item, dict):
                    # Append the flattened dictionary to the current key with '_' separator and index as suffix
                    for k, v in flatten_json(item).items():
                        flattened[f"{key}_{i}_{k}"] = v
                else:
                    # If the item is not a dictionary, store it in the flattened dictionary with index as suffix
                    flattened[f"{key}_{i}"] = item
        else:
            # If the value is not a dictionary or list, store it in the flattened dictionary
            flattened[key] = value

    return flattened


def flatten_json_to_csv(json_flatted: dict, path_d: str) -> None:
    # Convert the flattened data into a Pandas DataFrame
    df = pd.DataFrame([flattened])
    df.to_csv(path_d, sep=',')


if __name__ == '__main__':
    json_data = load_json(path_origin)
    flattened = flatten_json(json_data)
    flatten_json_to_csv(json_flatted=flattened, path_d=path_dest)
