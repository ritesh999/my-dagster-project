from get_data import get_data_json
from load_to_mongodb import load_to_mongodb


if __name__ == "__main__":
    get_data_json()
    load_to_mongodb()

