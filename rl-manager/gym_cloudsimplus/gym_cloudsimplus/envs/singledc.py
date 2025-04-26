import gymnasium as gym
import json
import os
import csv
import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
from gymnasium import spaces
from py4j.java_gateway import JavaGateway, GatewayParameters
# from sklearn.preprocessing import StandardScaler

# TODO: the two environments should support both continuous and
# discrete action spaces

class VectorQuantizer(nn.Module):
    def __init__(self, num_embeddings, embedding_dim, commitment_cost=0.25):
        super(VectorQuantizer, self).__init__()
        self.num_embeddings = num_embeddings
        self.embedding_dim = embedding_dim
        self.commitment_cost = commitment_cost

        # Initialize the codebook (embedding vectors)
        self.embedding = nn.Embedding(num_embeddings, embedding_dim)
        self.embedding.weight.data.uniform_(-1 / num_embeddings, 1 / num_embeddings)

    def forward(self, x):
        # Flatten input to (batch_size * num_features, embedding_dim)
        flat_x = x.view(-1, self.embedding_dim)

        # Compute distances between input and embedding vectors
        distances = torch.cdist(flat_x, self.embedding.weight, p=2)  # L2 distance
        indices = torch.argmin(distances, dim=1)  # Closest embedding index

        # Get quantized vectors
        quantized = self.embedding(indices).view(x.shape)

        # Compute commitment loss
        e_latent_loss = F.mse_loss(quantized.detach(), x)
        q_latent_loss = F.mse_loss(quantized, x.detach())
        loss = q_latent_loss + self.commitment_cost * e_latent_loss

        # Add quantization noise during backward pass
        quantized = x + (quantized - x).detach()
        return quantized, indices, loss


class VQVAE(nn.Module):
    def __init__(
        self,
        input_dim,
        latent_dim,
        num_embeddings,
        dropout=0.0,
        use_batch_norm=False,
        commitment_cost=0.25,
    ):
        super(VQVAE, self).__init__()
        self.encoder = nn.Sequential(
            nn.Linear(input_dim, 128),
            nn.ReLU(),
            nn.BatchNorm1d(128) if use_batch_norm else nn.Identity(),
            nn.Dropout(dropout),
            nn.Linear(128, latent_dim),
            nn.ReLU(),
        )
        self.quantizer = VectorQuantizer(num_embeddings, latent_dim, commitment_cost)
        self.decoder = nn.Sequential(
            nn.Linear(latent_dim, 128),
            nn.ReLU(),
            nn.BatchNorm1d(128) if use_batch_norm else nn.Identity(),
            nn.Dropout(dropout),
            nn.Linear(128, input_dim),
            nn.Sigmoid(),  # Assuming input values are normalized to [0, 1]
        )

    def forward(self, x):
        # Encode
        latent = self.encoder(x)
        # Quantize
        quantized, indices, quantization_loss = self.quantizer(latent)
        # Decode
        reconstructed = self.decoder(quantized)
        return latent, quantized, reconstructed, quantization_loss


class SimpleAutoencoder(nn.Module):
    def __init__(self, input_dim, latent_dim):
        super(SimpleAutoencoder, self).__init__()
        # Encoder
        self.encoder = nn.Sequential(
            nn.Linear(input_dim, 128), nn.ReLU(), nn.Linear(128, latent_dim), nn.ReLU()
        )
        # Decoder
        self.decoder = nn.Sequential(
            nn.Linear(latent_dim, 128),
            nn.ReLU(),
            nn.Linear(128, input_dim),
            nn.Sigmoid(),  # Assuming input values are normalized to [0, 1]
        )

    def forward(self, x):
        latent = self.encoder(x)
        reconstructed = self.decoder(latent)
        return latent, reconstructed


# Define Autoencoder Model
class Autoencoder(nn.Module):
    def __init__(self, input_dim, latent_dim, dropout=0.0, use_batch_norm=False):
        super(Autoencoder, self).__init__()
        # Encoder
        self.encoder = nn.Sequential(
            nn.Linear(input_dim, 128),
            nn.ReLU(),
            nn.BatchNorm1d(128) if use_batch_norm else nn.Identity(),
            nn.Dropout(dropout),
            nn.Linear(128, latent_dim),
            nn.ReLU(),
            nn.BatchNorm1d(latent_dim) if use_batch_norm else nn.Identity(),
            nn.Sigmoid(),
        )
        # Decoder
        self.decoder = nn.Sequential(
            nn.Linear(latent_dim, 128),
            nn.ReLU(),
            nn.BatchNorm1d(128) if use_batch_norm else nn.Identity(),
            nn.Dropout(dropout),
            nn.Linear(128, input_dim),
            nn.Sigmoid(),  # Assuming input values are normalized to [0, 1]
        )

    def forward(self, x):
        latent = self.encoder(x)
        reconstructed = self.decoder(latent)
        return latent, reconstructed


# Based on https://gymnasium.farama.org/api/env/
class SingleDC(gym.Env):
    """
    Action space - Continuous

    A vector of 2 continuous numbers

    The first number is in range [-1,1]
    Positive value - create a VM
    The range shows the id of host that the VM will be placed

    Negative value - destroy a VM
    The range shows the id of the VM that will be destroyed

    The second number is in range [0,1]
    It shows the size of the VM that will be created/destroyed

    Action space - Discrete

    A vector of 3 discrete numbers ...

    Observation space
    """

    metadata = {"render_modes": ["human", "ansi"]}
    # default port = 25333
    gateway_parameters = GatewayParameters(address="gateway", auto_convert=True)
    params = {}
    VM_CORES = [2, 4, 8]

    def __init__(self, params, jobs_as_json="[]", render_mode="ansi"):
        super(SingleDC, self).__init__()
        self.params = params

        self.gateway = JavaGateway(gateway_parameters=self.gateway_parameters)
        self.simulation_environment = self.gateway.entry_point
        self.vm_allocation_policy = params["vm_allocation_policy"]

        if self.vm_allocation_policy == "fromfile":
            self.vm_allocation_policy = "fromfile"
            with open(
                os.path.join("mnt", "policies", params["algorithm"]), mode="r"
            ) as file:
                csv_reader = csv.reader(file)
                _ = next(csv_reader)  # skip the header
                self.action_file_data = list(csv_reader)

        self.host_count = params["host_count"]
        self.host_pes = params["host_pes"]
        self.small_vm_pes = params["small_vm_pes"]
        self.large_vm_multiplier = params["large_vm_multiplier"]

        # if you want to support 1-10 hosts then when calculating max_vms_count and
        # observation rows, put self.max_hosts instead of host_count
        # and in action_space put self.max_hosts instead of host_count

        self.max_hosts = params["max_hosts"]
        self.action_types_count = 3  # do nothing, create vm, destroy vm
        self.vm_types_count = 3  # small, medium, large

        # it is reasonable to assume that the minimum amount of cores per job will be 1
        self.min_job_pes = 1
        self.large_vm_pes = self.small_vm_pes * self.large_vm_multiplier
        self.max_vms = self.max_hosts * int(self.host_pes) // int(self.small_vm_pes)
        # self.max_jobs = self.max_vms * int(self.small_vm_pes) // self.min_job_pes
        self.max_jobs = self.max_hosts * int(self.host_pes) // self.min_job_pes

        # Old for continuous action space
        # self.action_space = spaces.Box(
        #     low=np.array([-1.0, 0.0]),
        #     high=np.array([1.0, 1.0]),
        #     shape=(2,),
        #     dtype=np.float32
        # )

        # New for discrete action space
        # [action, id, type^]
        # action = {0: do nothing, 1: create vm, 2: destroy vm}
        # type = {0: small, 1: medium, 2: large}
        # ^ needed only when action = 1
        self.action_space = spaces.MultiDiscrete(
            np.array(
                [
                    self.action_types_count,
                    self.max_hosts,
                    self.max_vms,
                    self.vm_types_count,
                ]
            )
        )

        # TODO: this has to bemultiplied by 2 because we need 2 values for each tree node
        self.infr_obs_length = 1 + self.max_hosts + self.max_vms + self.max_jobs

        if params["enable_autoencoder_observation"]:
            self.input_dim = 1 + self.max_hosts + self.max_vms + self.max_jobs
            # print(self.input_dim)
            self.autoencoder_latent_dim = 64
            self.infr_obs_space = spaces.Box(
                low=0.0,
                high=1.0,
                shape=(self.autoencoder_latent_dim,),
                dtype=np.float32,
            )

            self.autoencoder = Autoencoder(
                self.input_dim, self.autoencoder_latent_dim, use_batch_norm=True
            )
            self.autoencoder.load_state_dict(
                torch.load(
                    "mnt/autoencoders/AE_10hosts_64_BN.pth",
                    weights_only=True,
                )
            )
            self.autoencoder.eval()
        # # self.autoencoder = VQVAE(
        # #     self.input_dim, self.autoencoder_latent_dim, 64, 0, True
        # # )
        else:
            # if tree data are scaled to [0, 1]
            # self.infr_obs_space = spaces.Box(
            #     low=0,
            #     high=1,
            #     shape=(self.infr_obs_length,),
            #     dtype=np.float32,
            # )
            # if tree data are not scaled
            max_pes_per_node = self.max_hosts * self.host_pes
            self.infr_obs_space = spaces.MultiDiscrete(
                (max_pes_per_node + 1) * np.ones(self.infr_obs_length),
                dtype=np.int32,
            )

            # print(self.infr_obs_space)

        # we set the maximum number of cores waiting in total to be the number of cores in the largest VM
        # because even if there are more cores waiting, we cannot do anything more than creating a large VM
        # again +1 because it starts from 0 and we need to include the last element (which is large_vm_pes)
        self.job_cores_waiting_obs_space = spaces.Discrete(self.large_vm_pes + 1)

        # 2d array
        # else:
        #     self.observation_rows = 1 + self.max_hosts + self.max_vms + self.max_jobs
        #     self.observation_cols = 4
        #     self.observation_space = spaces.Box(
        #         low=0,
        #         high=1,
        #         shape=(self.observation_rows, self.observation_cols),
        #         dtype=np.float32,
        #     )

        self.observation_space = spaces.Dict(
            {
                "infr_state": self.infr_obs_space,
                "job_cores_waiting_state": self.job_cores_waiting_obs_space,
            }
        )

        if render_mode is not None and render_mode not in self.metadata["render_modes"]:
            gym.logger.warn(
                "Invalid render mode" 'Render modes allowed: ["human" | "ansi"]'
            )

        self.render_mode = render_mode

        self.simulation_id = self.simulation_environment.createSimulation(
            params, jobs_as_json
        )

    def action_masks(self) -> list[bool]:
        """
        Return a list of masks for the current environment.
        The list should have the same length as the action space.
        Each element should be a list of booleans, where True means that the
        action is valid and False means that the action is invalid.
        """
        host_cores_utilized_sum = np.sum(self.host_cores_utilized)

        # the max vms the current datacenter can support (based on the number of hosts)
        current_max_vms = self.host_count * int(self.host_pes) // int(self.small_vm_pes)
        if host_cores_utilized_sum == 0:  # no VMs running
            action_type_mask = [True, True, False]
            host_mask = [True] * self.host_count + [False] * (
                self.max_hosts - self.host_count
            )
            vm_mask = (
                [True] + [False] * (self.max_vms - 1)
            )  # just allow one value to be True for the algorithm to work. It does not matter which one
            vm_type_mask = [True, True, False]
        elif self.vms_running == current_max_vms:  # all VMs are running
            action_type_mask = [True, False, True]
            host_mask = [True] + [False] * (self.max_hosts - 1)
            vm_mask = [True] * current_max_vms + False * (
                self.max_vms - current_max_vms
            )
            vm_type_mask = [True, False, False]
        elif all(
            self.host_pes - num < self.VM_CORES[0] for num in self.host_cores_utilized
        ):  # all hosts are full
            action_type_mask = [True, False, True]
            host_mask = [True] + [False] * (self.max_hosts - 1)
            vm_mask = [True] * self.vms_running + [False] * (
                self.max_vms - self.vms_running
            )
            vm_type_mask = [True, False, False]
        elif all(
            self.host_pes - num < self.VM_CORES[1] for num in self.host_cores_utilized
        ):  # can't create M, L VMs
            action_type_mask = [True, True, True]
            host_mask = [True] * self.host_count + [False] * (
                self.max_hosts - self.host_count
            )
            vm_mask = [True] * self.vms_running + [False] * (
                self.max_vms - self.vms_running
            )
            vm_type_mask = [True, False, False]
        elif all(
            self.host_pes - num < self.VM_CORES[2] for num in self.host_cores_utilized
        ):  # can't create L VMs
            action_type_mask = [True, True, True]
            host_mask = [True] * self.host_count + [False] * (
                self.max_hosts - self.host_count
            )
            vm_mask = [True] * self.vms_running + [False] * (
                self.max_vms - self.vms_running
            )
            vm_type_mask = [True, True, False]
        else:  # the most common case
            full_host_indices = [
                i
                for i, num in enumerate(self.host_cores_utilized)
                if self.host_pes - num < self.VM_CORES[0]
            ]
            action_type_mask = [True, True, True]
            host_mask = [
                False if i in full_host_indices else True
                for i in range(self.host_count)
            ] + [False] * (self.max_hosts - self.host_count)
            vm_mask = [True] * self.vms_running + [False] * (
                self.max_vms - self.vms_running
            )
            vm_type_mask = [True, True, True]

        masks = [action_type_mask, host_mask, vm_mask, vm_type_mask]
        return [item for sublist in masks for item in sublist]

    def _pad_observation(self, obs, target_dim):
        obs_len = len(obs)
        if obs_len >= target_dim:
            return obs[:target_dim]  # Truncate if necessary
        # Create a zero array of target_dim and copy obs into it
        padded_obs = np.zeros(target_dim, dtype=obs.dtype)
        padded_obs[:obs_len] = obs
        return padded_obs

    def _min_max_normalize(self, array, min_val=0, max_val=100):
        """
        Normalize array to the range [0, 1] based on given min_val and max_val.
        """
        array = np.array(array, dtype=np.float32)
        return (array - min_val) / (max_val - min_val)

    def _get_observation(self, result):
        raw_obs = result.getObservation()
        infr_obs = self._to_nparray(
            raw_obs.getInfrastructureObservation(), dtype=np.int32
        )
        infr_obs = self._pad_observation(infr_obs, self.infr_obs_length)

        if self.params["enable_autoencoder_observation"]:
            infr_obs = self._pad_observation(infr_obs, self.input_dim)
            infr_obs = self._min_max_normalize(infr_obs)
            infr_obs = torch.tensor(infr_obs, dtype=torch.float32).unsqueeze(0)
            autoencoder_out = self.autoencoder(infr_obs)
            infr_obs = autoencoder_out[0].squeeze().detach().numpy()  # latent space

        job_cores_waiting_obs = raw_obs.getJobCoresWaitingObservation()

        return {
            "infr_state": infr_obs,
            "job_cores_waiting_state": job_cores_waiting_obs,
        }

    def reset(self, seed=None, options=None):
        super(SingleDC, self).reset()
        self.current_step = 0
        self.host_cores_utilized = np.zeros(self.max_hosts, dtype=np.int32)
        self.vms_running = 0

        if seed is None:
            seed = 0

        result = self.simulation_environment.reset(self.simulation_id, seed)

        obs = self._get_observation(result)
        raw_info = result.getInfo()
        info = self._raw_info_to_dict(raw_info)

        return obs, info

    def step(self, action):
        # Py4J cannot translate np.array(dtype=np.float32) to java List<double>
        # Fix1: make it dtype=np.float64 and for some reason it works :)
        # Fix2: before sending it to java, convert it to python list first
        # Here, we adopt Fix2
        self.current_step += 1

        if self.vm_allocation_policy == "fromfile":
            if self.current_step - 1 >= len(self.action_file_data):
                raise ValueError(
                    "The number of steps in the simulation exceeds the number of actions in the file"
                )
            action = self.action_file_data[self.current_step - 1]
            action = [int(x) for x in action]
        else:
            action = action.tolist()

        result = self.simulation_environment.step(self.simulation_id, action)

        reward = result.getReward()
        raw_info = result.getInfo()
        terminated = result.isTerminated()
        truncated = result.isTruncated()

        obs = self._get_observation(result)

        ############################################################
        # ENABLE FOR ZERO OBSERVATION TESTING
        # obs = {
        #     "infr_state": np.zeros(self.infr_obs_length),
        #     "job_cores_waiting_state": 0,
        # }
        ############################################################

        info = self._raw_info_to_dict(raw_info)

        if self.render_mode == "human":
            self.render()

        # Update the number of VMs per host
        if info["isValid"]:
            # print(f"action: {action} was valid")
            if action[0] == 1:
                host_id = action[1]
                vm_type = action[3]
                self.host_cores_utilized[host_id] += self.VM_CORES[vm_type]
                self.vms_running += 1
            elif action[0] == 2:
                host_id = info["host_affected"]
                self.host_cores_utilized[host_id] -= info["cores_changed"]
                self.vms_running -= 1
        # else:
        #     print(f"action: {action} was invalid")

        return (obs, reward, terminated, truncated, info)

    def render(self):
        if self.render_mode is None:
            gym.logger.warn(
                "You are calling render method "
                "without specifying any render mode. "
                "You can specify the render_mode at initialization, "
                f'e.g. gym("{self.spec.id}", render_mode="human")'
            )
            return
        # result is a string with arrays encoded as json
        result = self.simulation_environment.render(self.simulation_id)
        obs_data = json.loads(result)
        obs_data_dict = dict(obs_data)
        if self.render_mode == "human":
            for key in obs_data:
                print(f"{key} -> {obs_data_dict[key]}")
            return
        elif self.render_mode == "ansi":
            return str(obs_data)
        else:
            return super(SingleDC, self).render()

    def close(self):
        # close the resources
        self.simulation_environment.close(self.simulation_id)
        # close the python client side
        self.gateway.close()

    def _raw_info_to_dict(self, raw_info):
        info = {
            "job_wait_reward": raw_info.getJobWaitReward(),
            "running_vm_cores_reward": raw_info.getRunningVmCoresReward(),
            "unutilized_vm_cores_reward": raw_info.getUnutilizedVmCoresReward(),
            "invalid_reward": raw_info.getInvalidReward(),
            "isValid": raw_info.isValid(),
            # "host_metrics": json.loads(raw_info.getHostMetricsAsJson()),
            # "vm_metrics": json.loads(raw_info.getVmMetricsAsJson()),
            # "job_metrics": json.loads(raw_info.getJobMetricsAsJson()),
            "job_wait_time": json.loads(raw_info.getJobWaitTimeAsJson()),
            "unutilized_vm_core_ratio": raw_info.getUnutilizedVmCoreRatio(),
            "observation_tree_array": json.loads(
                raw_info.getObservationTreeArrayAsJson()
            ),
            "host_affected": raw_info.getHostAffected(),
            "cores_changed": raw_info.getCoresChanged(),
            # "dot_string": raw_info.getDotString(),
        }
        return info

    def _to_nparray(self, raw_obs, dtype=np.float32):
        obs = list(raw_obs)
        return np.array(obs, dtype=dtype)
