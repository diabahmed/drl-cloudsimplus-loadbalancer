import os
import json
import pycurl
import random
import shutil
import numpy as np
from io import BytesIO
import torch
import sys

import importlib
from utils.parse_config import dict_from_config

CONFIG_FILE = "config.yml"


def _find_replica_id(hostname):
    response_buffer = BytesIO()
    # Define the socket path and container URL
    unix_socket_path = "/run/docker.sock"
    container_url = f"http://docker/containers/{hostname}/json"

    # Initialize a cURL object
    curl = pycurl.Curl()

    # Set cURL options
    curl.setopt(pycurl.UNIX_SOCKET_PATH, unix_socket_path)
    curl.setopt(pycurl.URL, container_url)
    curl.setopt(pycurl.WRITEFUNCTION, response_buffer.write)
    curl.perform()
    curl.close()

    response_data = response_buffer.getvalue().decode("utf-8")
    replica_id = json.loads(response_data)["Name"].split("-")[-1]
    return replica_id


def set_seed_for_all(seed):
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    torch.cuda.manual_seed(seed)
    torch.cuda.manual_seed_all(seed)
    torch.backends.cudnn.deterministic = True
    torch.backends.cudnn.benchmark = False
    torch.use_deterministic_algorithms(True, warn_only=True)
    # torch.cuda.synchronize()
    # torch.backends.cudnn.enabled = False
    os.environ["OMP_NUM_THREADS"] = "1"
    os.environ["MKL_NUM_THREADS"] = "1"
    os.environ["PYTHONHASHSEED"] = str(seed)


def write_seed_to_file(seed, log_dir, filename="seed.txt"):
    """
    Creates a text file and writes the given seed number to it.

    Parameters:
        seed (int): The seed number to write.
        filename (str): The name of the file to create or overwrite. Default is 'seed.txt'.
    """
    filepath = os.path.join(log_dir, filename)
    try:
        with open(filepath, "w") as file:
            file.write(str(seed))
    except Exception as e:
        print(f"An error occurred while writing to the file: {e}")


def main():
    if os.getenv("RUN_MODE") == "batch":
        hostname = os.getenv("HOSTNAME")
        replica_id = _find_replica_id(hostname)
        params = dict_from_config(replica_id, CONFIG_FILE)
        params.update(run_mode="batch")
    elif os.getenv("RUN_MODE") == "serial":
        experiment_id = os.getenv("EXPERIMENT_ID")
        num_experiments = int(os.getenv("NUM_EXPERIMENTS"))
        params = dict_from_config(experiment_id, CONFIG_FILE)
        params.update(run_mode="serial")
        params.update(num_experiments=num_experiments)
    if params["seed"] == "random":
        params["seed"] = np.random.randint(0, sys.maxsize)
    else:
        set_seed_for_all(params["seed"])

    params["log_dir"] = None
    if params["save_experiment"]:
        params["log_dir"] = os.path.join(
            params["base_log_dir"],
            params["experiment_type_dir"],
            params["experiment_name"],
        )
        # Create folder if needed
        os.makedirs(params["log_dir"], exist_ok=True)
        # Make a copy of the config file in the log directory
        shutil.copy(CONFIG_FILE, params["log_dir"])
        write_seed_to_file(params["seed"], params["log_dir"])

    try:
        module = importlib.import_module(params["mode"])
    except ModuleNotFoundError:
        print(
            f"Mode {params['mode']} was not found. Available modes are: 'train', 'transfer', 'test'."
        )
    func = getattr(module, params["mode"])
    func(params)


if __name__ == "__main__":
    main()
