import os
import numpy as np
import pandas as pd
from typing import Any, Dict, Union
import torch

# from stable_baselines3.common import results_plotter
# from stable_baselines3.common.results_plotter import plot_results
from stable_baselines3.common.results_plotter import load_results, ts2xy
from stable_baselines3.common.callbacks import BaseCallback


class SaveOnBestTrainingRewardCallback(BaseCallback):
    """
    Callback for saving a model based on the training reward.

    :param log_dir: Path to the folder where the model will be saved.
            It must contain the file created by the ``Monitor`` wrapper.
    :param verbose: Verbosity level: 0 for no output, 1 for info messages, 2 for debug messages
    """

    def __init__(
        self,
        log_dir: str,
        save_replay_buffer: bool = True,
        verbose: int = 0,
    ) -> None:
        super(SaveOnBestTrainingRewardCallback, self).__init__(verbose)
        self.log_dir = log_dir
        self.save_replay_buffer = save_replay_buffer
        self.model_save_path = os.path.join(log_dir, "best_model")
        self.best_reward = -np.inf
        self.previous_best_episode_num = None  # to delete the previous best
        self.isValid = None
        self.current_episode_num = 0
        self.best_episode_filename_prefix = "best_episode"

        self._clear_episode_details()

    def get(self, attr) -> Any:
        return self.training_env.env_method("get_wrapper_attr", attr)[0]

    # The episode details will be written to the file best_episode_{episode_num}.csv
    def _create_episode_details_dict(self) -> Dict:
        timesteps = np.arange(
            self.num_timesteps - self.current_episode_length + 1, self.num_timesteps + 1
        )
        # will be written to best_episode_{episode_num}.csv
        episode_details = {
            "timestep": timesteps,
            # "obs": self.observations,
            # "action": self.actions,
            "job_wait_reward": self.job_wait_rewards,
            "running_vm_cores_reward": self.running_vm_cores_rewards,
            "unutilized_vm_cores_reward": self.unutilized_vm_cores_rewards,
            "invalid_reward": self.invalid_rewards,
            "reward": self.rewards,
            "isValid": self.isValid,
            # "next_obs": self.new_observations,
        }
        return episode_details

    def _clear_episode_details(self) -> None:
        self.observations = []
        self.actions = []
        self.rewards = []
        self.new_observations = []
        # self.host_metrics = []
        # self.vm_metrics = []
        # self.job_metrics = []
        self.job_wait_time = []
        self.job_wait_rewards = []
        self.running_vm_cores_rewards = []
        self.unutilized_vm_cores_rewards = []
        self.invalid_rewards = []
        self.unutilized_vm_core_ratio = []
        self.isValid = []
        self.current_episode_length = 0
        self.observation_tree_arrays = []
        # self.episode_dot_strings = []

    def _delete_previous_best(self) -> None:
        if self.previous_best_episode_num:
            log_file = os.path.join(
                self.log_dir,
                f"{self.best_episode_filename_prefix}_{self.previous_best_episode_num}.csv",
            )
            os.remove(log_file)

    def _extract_observation_from_locals(self, obs) -> Union[list, dict]:
        if isinstance(self.locals[obs], dict):
            obs_dict = self.locals[obs]
            processed_obs = {}
            for key, value in obs_dict.items():
                value = value[0].cpu() if isinstance(value, torch.Tensor) else value[0]
                processed_obs[key] = value
        elif isinstance(self.locals[obs], torch.Tensor):
            processed_obs = self.locals[obs][0].cpu().numpy()
        elif isinstance(self.locals[obs], np.ndarray):
            processed_obs = self.locals[obs][0]
        else:
            raise TypeError(f"Unknown observation type: {type(self.locals[obs])}")
        return processed_obs

    def _save_timestep_details(self) -> None:
        # print(self.locals)
        self.observations.append(self._extract_observation_from_locals("obs_tensor"))
        self.actions.append(self.locals["actions"][0])
        self.rewards.append(self.locals["rewards"][0])
        self.new_observations.append(self._extract_observation_from_locals("new_obs"))
        # self.host_metrics.append(self.locals["infos"][0]["host_metrics"])
        # self.vm_metrics.append(self.locals["infos"][0]["vm_metrics"])
        # self.job_metrics.append(self.locals["infos"][0]["job_metrics"])
        self.job_wait_time.append(self.locals["infos"][0]["job_wait_time"])
        self.job_wait_rewards.append(self.locals["infos"][0]["job_wait_reward"])
        self.running_vm_cores_rewards.append(
            self.locals["infos"][0]["running_vm_cores_reward"]
        )
        self.unutilized_vm_cores_rewards.append(
            self.locals["infos"][0]["unutilized_vm_cores_reward"]
        )
        self.invalid_rewards.append(self.locals["infos"][0]["invalid_reward"])
        self.isValid.append(self.locals["infos"][0]["isValid"])
        self.unutilized_vm_core_ratio.append(
            self.locals["infos"][0]["unutilized_vm_core_ratio"]
        )
        self.observation_tree_arrays.append(
            self.locals["infos"][0]["observation_tree_array"]
        )
        # self.episode_dot_strings.append(self.locals["infos"][0]["dot_string"])

    def _maybe_save_replay_buffer(self) -> None:
        if (
            hasattr(self.model, "replay_buffer")
            and self.model.replay_buffer is not None
            and self.save_replay_buffer
        ):
            replay_buffer_path = os.path.join(self.log_dir, "best_model_replay_buffer")
            if self.verbose >= 1:
                print((f"Saving replay buffer to" f"{replay_buffer_path}"))
            self.model.save_replay_buffer(replay_buffer_path)

    def _create_csv_paths(self) -> Dict:
        # host_metrics_path = os.path.join(self.log_dir, "host_metrics.csv")
        # vm_metrics_path = os.path.join(self.log_dir, "vm_metrics.csv")
        # job_metrics_path = os.path.join(self.log_dir, "job_metrics.csv")
        job_wait_time_path = os.path.join(self.log_dir, "job_wait_time.csv")
        episode_actions_path = os.path.join(self.log_dir, "actions.csv")
        unutilized_vm_core_ratio_path = os.path.join(
            self.log_dir, "unutilized_vm_core_ratio.csv"
        )
        episode_details_path = os.path.join(
            self.log_dir,
            f"{self.best_episode_filename_prefix}_{self.current_episode_num}.csv",
        )
        paths = {
            # "host_metrics": host_metrics_path,
            # "vm_metrics": vm_metrics_path,
            # "job_metrics": job_metrics_path,
            "job_wait_time": job_wait_time_path,
            "actions": episode_actions_path,
            "unutilized_vm_core_ratio": unutilized_vm_core_ratio_path,
            "episode_details": episode_details_path,
        }
        return paths

    def _create_dataframes(self) -> Dict:
        df_dict = {
            # "host_metrics": pd.DataFrame(self.host_metrics),
            # "vm_metrics": pd.DataFrame(self.vm_metrics),
            # "job_metrics": pd.DataFrame(self.job_metrics),
            "job_wait_time": pd.DataFrame(self.job_wait_time),
            "unutilized_vm_core_ratio": pd.DataFrame(self.unutilized_vm_core_ratio),
            "episode_actions": pd.DataFrame(self.actions),
            "episode_details": pd.DataFrame(self._create_episode_details_dict()),
        }
        return df_dict

    def _write_dataframes_to_csvs(self, df_dict, path_dict) -> None:
        # df_dict["host_metrics"].to_csv(path_dict["host_metrics"], header=False)
        # df_dict["vm_metrics"].to_csv(path_dict["vm_metrics"], header=False)
        # df_dict["job_metrics"].to_csv(path_dict["job_metrics"], header=False)
        df_dict["job_wait_time"].to_csv(path_dict["job_wait_time"], header=False)
        df_dict["unutilized_vm_core_ratio"].to_csv(
            path_dict["unutilized_vm_core_ratio"], header=False
        )
        df_dict["episode_actions"].to_csv(
            path_dict["actions"],
            index=False,
            header=["action", "hostId", "vmId", "vmType"],
        )
        df_dict["episode_details"].to_csv(path_dict["episode_details"], index=False)

    def _maybe_save_best_episode_details(self) -> None:
        self._delete_previous_best()
        self.previous_best_episode_num = self.current_episode_num

        path_dict = self._create_csv_paths()

        if self.verbose >= 1:
            print((f"Saving simulation details to" f"{path_dict}"))

        df_dict = self._create_dataframes()
        self._write_dataframes_to_csvs(df_dict, path_dict)

    def _maybe_print_current_episode_info(self, current_reward) -> None:
        if self.verbose >= 1:
            # -1 because it is called when reset is already done,
            # so episode number is already incremented
            # you can access it also with self.locals['self'].num_timesteps
            print(f"Current timesteps: {self.num_timesteps - 1}")
            print(f"Episode number: {self.current_episode_num}")
            print(f"Episode length: {len(self.observations)}")
            print(f"Best reward: {self.best_reward:.2f}")
            print(f"Current reward: {current_reward:.2f}")

    def _save_new_best(self) -> None:
        if self.verbose >= 1:
            print(f"Saving new best model to {self.model_save_path}")
        self.model.save(self.model_save_path)
        self._maybe_save_replay_buffer()
        self._maybe_save_best_episode_details()

    # Write episode info in a row in the log progress.csv
    def _write_progress_log_row(self) -> None:
        ep_first_timestep = self.num_timesteps - self.current_episode_length + 1
        ep_last_timestep = self.num_timesteps
        self.logger.record("train/episode_num", self.current_episode_num)
        self.logger.record("train/episode_length", self.current_episode_length)
        self.logger.record("train/ep_first_timestep", ep_first_timestep)
        self.logger.record("train/ep_last_timestep", ep_last_timestep)
        self.logger.record("train/ep_total_rew", np.sum(self.rewards))
        self.logger.record("train/ep_valid_count", np.sum(self.isValid))
        self.logger.record("train/ep_job_wait_rew", np.sum(self.job_wait_rewards))
        self.logger.record(
            "train/ep_running_vm_cores_rew", np.sum(self.running_vm_cores_rewards)
        )
        self.logger.record(
            "train/ep_unutil_vm_cores_rew", np.sum(self.unutilized_vm_cores_rewards)
        )
        self.logger.record("train/ep_inv_rew", np.sum(self.invalid_rewards))
        self.logger.dump()

    def _write_observation_tree_arrays_to_file(self, filename, mode="a") -> None:
        filepath = os.path.join(self.log_dir, filename)
        with open(filepath, mode) as file:
            for array in self.observation_tree_arrays:
                file.write(f"{array}\n")
            file.write("\n")

    # def _write_dot_strings_to_file(self) -> None:
    #     dot_path = os.path.join(self.log_dir, "dot_graphs.txt")
    #     with open(dot_path, "w") as file:
    #         for s in self.episode_dot_strings:
    #             file.write(f"{s}\n")

    def _on_step(self) -> bool:
        # because the environment is a VecEnv environment, the variables are dones
        # and infos instead of done and info. Also, the variables are tuples that
        # allow to return the done and info information for all environments.
        # We know that we have a VecEnv but only 1 environment, so we just take
        # the first element of the tuple.
        self.current_episode_length += 1
        self._save_timestep_details()

        if self.locals["dones"][0]:
            self.current_episode_num += 1
            if self.verbose >= 1:
                print("Episode terminated")
            self._write_progress_log_row()
            self._write_observation_tree_arrays_to_file(
                "observation_tree_arrays.csv", "a"
            )
            # self._write_dot_strings_to_file()
            # Retrieve training reward
            x, y = ts2xy(load_results(self.log_dir), "timesteps")
            if len(x) == 0:
                return True
            # Training reward for this episode
            current_reward = y[-1]
            self._maybe_print_current_episode_info(current_reward)

            # New best model, save the agent and the episode details
            if current_reward > self.best_reward:
                self.best_reward = current_reward
                self._save_new_best()
                self._write_observation_tree_arrays_to_file(
                    "best_obs_tree_arrays.csv", "w"
                )

            self._clear_episode_details()
        return True
