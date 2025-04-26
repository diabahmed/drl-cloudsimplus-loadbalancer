import yaml


def dict_from_config(replica_id, config):
    with open(config, "r") as file:
        config = yaml.safe_load(file)

    # first we read the common parameters and we overwrite them with the specific experiment parameters
    params = {**config["common"], **config[f"experiment_{replica_id}"]}
    return params
