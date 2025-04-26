import os
import json
import numpy as np
import gymnasium as gym
import gym_cloudsimplus  # noqa: F401
import torch

import stable_baselines3 as sb3
import sb3_contrib
from stable_baselines3.common.noise import NormalActionNoise
from stable_baselines3.common.monitor import Monitor
from stable_baselines3.common.logger import configure
from stable_baselines3.common.vec_env import DummyVecEnv, SubprocVecEnv
from callbacks.save_on_best_training_reward_callback import (
    SaveOnBestTrainingRewardCallback,
)

from utils.trace_utils import csv_to_cloudlet_descriptor


def train(params):
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    # Select the appropriate algorithm
    if (
        params["vm_allocation_policy"] == "fromfile"
        or params["vm_allocation_policy"] == "heuristic"
    ):
        # If the vm_allocation_policy is fromfile or heuristic, pick a default algorithm
        # so the code triggers the simulation environment creation
        # NOTE: the algorithm decision through learning is not used at all in this case
        algorithm = getattr(sb3, "PPO")
    elif params["vm_allocation_policy"] == "rl":
        if hasattr(sb3, params["algorithm"]):
            algorithm = getattr(sb3, params["algorithm"])
        elif hasattr(sb3_contrib, params["algorithm"]):
            algorithm = getattr(sb3_contrib, params["algorithm"])
        else:
            raise AttributeError(f"Algorithm {params['algorithm']} not found.")
    else:
        raise AttributeError(
            f"vm_allocation_policy {params['vm_allocation_policy']} not found."
        )

    policy = "MultiInputPolicy"  # when state is Spaces.Dict()
    # policy = "MlpPolicy" # when state is not Spaces.Dict()

    # if hasattr(algorithm, "ent_coef"):
    # algorithm.ent_coef = 0.01
    # algorithm.learning_rate=0.1,
    # algorithm.clip_range=0.7,
    # algorithm.n_steps=1024

    # Read jobs
    job_trace_path = os.path.join("mnt", "traces", f"{params['job_trace_filename']}")
    jobs = csv_to_cloudlet_descriptor(job_trace_path)

    # Create and wrap the environment
    env = gym.make("SingleDC-v0", params=params, jobs_as_json=json.dumps(jobs))

    log_destination = ["stdout"]
    callback = None
    if params["save_experiment"]:
        log_destination.extend(["tensorboard"])
        # the callback writes all the other .csv files and saves the model (with replay buffer) when the reward is the best
        callback = SaveOnBestTrainingRewardCallback(log_dir=params["log_dir"])

    # Monitor needs the environment to have a render_mode set
    # If render_mode is None, it will give a warning.
    #   add info_keywords if needed
    # If log_dir is None, it will not log anything
    env = Monitor(env, params["log_dir"])

    # see https://stable-baselines3.readthedocs.io/en/master/modules/a2c.html note
    if params["algorithm"] == "A2C":
        device = "cpu"
        env = SubprocVecEnv([lambda: env], start_method="fork")
    else:
        env = DummyVecEnv([lambda: env])

    # Instantiate the agent
    model = algorithm(policy=policy, device=device, env=env, seed=params["seed"])

    max_hosts = params["max_hosts"]
    host_count = params["host_count"]
    host_pes = params["host_pes"]
    small_vm_pes = params["small_vm_pes"]
    min_job_pes = 1
    cur_max_vms = host_count * host_pes // small_vm_pes
    cur_max_jobs = host_count * host_pes // min_job_pes
    max_vms = max_hosts * host_pes // small_vm_pes
    max_jobs = max_hosts * host_pes // min_job_pes
    infr_obs_length = 1 + max_hosts + max_vms + max_jobs
    max_pes_per_node = max_hosts * host_pes

    # Calculate the start and end indices for the last 700 elements of Part 1
    start_idx = (1 + host_count + cur_max_vms + cur_max_jobs) * (max_pes_per_node + 1)
    end_idx = infr_obs_length * (max_pes_per_node + 1)  # Exclusive

    # Access the weights of the input layer
    weights = model.policy.mlp_extractor.policy_net[0].weight

    # zero out and freeze the weights
    with torch.no_grad():
        weights[:, start_idx:end_idx] = 0
        weights[:, start_idx:end_idx].requires_grad = False

    # the logger can write to stdout, progress.csv and tensorboard
    logger = configure(params["log_dir"], log_destination)
    model.set_logger(logger)

    # Add some action noise for exploration if applicable
    if hasattr(model, "action_noise"):
        n_actions = env.action_space.shape[-1]
        action_noise = NormalActionNoise(
            mean=np.zeros(n_actions), sigma=0.1 * np.ones(n_actions)
        )
        model.action_noise = action_noise

    if hasattr(model, "ent_coef") and params.get("ent_coef"):
        model.ent_coef = params["ent_coef"]

    # Train the agent
    model.learn(total_timesteps=params["timesteps"], log_interval=1, callback=callback)

    # Close the environment and free the resources
    env.close()

    # Delete the model from memory
    del model
