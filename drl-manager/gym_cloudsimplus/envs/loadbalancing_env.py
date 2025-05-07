import os
import time
import traceback
import logging
import json
import numpy as np
import gymnasium as gym
from pprint import PrettyPrinter
from gymnasium import spaces
from py4j.java_gateway import JavaGateway, GatewayParameters, Py4JNetworkError

pp = PrettyPrinter(width=200)

logger = logging.getLogger(__name__.split('.')[-1])

# Based on https://gymnasium.farama.org/api/env/
class LoadBalancingEnv(gym.Env):
    """
    Gymnasium Environment for interacting with the CloudSim Plus LoadBalancerGateway.

    Action Space (MultiDiscrete):
    - Dim 0: Action Type (0: NoOp, 1: AssignCloudlet, 2: CreateVm, 3: DestroyVm)
    - Dim 1: Target VM ID (Used for Assign/Destroy, range 0 to max_potential_vms-1)
    - Dim 2: Target Host ID (Used for Create, range 0 to num_hosts-1)
    - Dim 3: VM Type Index (Used for Create, 0:S, 1:M, 2:L)

    Observation Space (Dict):
    - host_loads: Box(num_hosts) - CPU Load [0,1]
    - host_ram_usage_ratio: Box(num_hosts) - RAM Usage [0,1]
    - vm_loads: Box(max_potential_vms) - Padded CPU Load [0,1] (-1 padding)
    - vm_types: Box(max_potential_vms) - Padded VM Type (-1:Off, 1:S, 2:M, 3:L)
    - vm_host_map: Box(max_potential_vms) - Padded Host ID (-1:Off)
    - infrastructure_observation: Box(TREE_ARRAY_MAX_LEN) - Padded Tree Array
    - waiting_cloudlets: Box(1) - Normalized count
    - next_cloudlet_pes: Box(1) - Normalized PEs needed
    """
    metadata = {"render_modes": ["human", "ansi"]}

    def __init__(self, config_params: dict, render_mode="ansi"):
        super(LoadBalancingEnv, self).__init__()

        logger.info("Initializing LoadBalancingEnv...")
        self.config = config_params

        if render_mode is not None and render_mode not in self.metadata["render_modes"]:
            gym.logger.warn(
                "Invalid render mode" 'Render modes allowed: ["human" | "ansi"]'
            )
        self.render_mode = render_mode

        # --- Connect to Py4J Gateway ---
        gateway_host = self.config.get("gateway_host", "0.0.0.0")
        gateway_port = self.config.get("gateway_port", 25333)
        logger.info(f"Attempting to connect to Gateway on {gateway_host}:{gateway_port}...")
        retries = 5
        while retries > 0:
            try:
                # Set auto_convert=True for easier type handling
                self.gateway = JavaGateway(
                    gateway_parameters=GatewayParameters(auto_convert=True)
                )
                # Test connection
                self.gateway.jvm.System.out.println("Python Env connected!")
                self.loadbalancer_gateway = self.gateway.entry_point
                logger.info("Successfully connected to Java Gateway.")
                break # Exit loop on successful connection
            except (ConnectionRefusedError, Py4JNetworkError) as e:
                retries -= 1
                logger.warning(f"Gateway connection failed: {e}. Retrying in 5 seconds... ({retries} retries left)")
                if retries == 0:
                    logger.error("Max retries reached. Could not connect to Gateway.")
                    raise
                time.sleep(5)

        # --- Configure Simulation (Call Java side) ---
        try:
            logger.info("\nConfiguring simulation...")
            # Convert Python dict to Java HashMap for Py4J
            params = self.gateway.jvm.java.util.HashMap()
            for key, value in self.config.items():
                params.put(key, value)
            self.loadbalancer_gateway.configureSimulation(params)
            logger.info("Java simulation configured via gateway.")
        except Exception as e:
             logger.error(f"Failed to configure simulation via gateway: {e}")
             self.close() # Close gateway connection if config fails
             raise

        # --- Define Spaces (based on config) ---
        self.num_hosts = int(self.config["hosts_count"])
        self.host_pes = int(self.config["host_pes"])
        self.datacenter_cores = self.num_hosts * self.host_pes
        self.small_vm_pes = int(self.config["small_vm_pes"])
        self.large_vm_multiplier = int(self.config["large_vm_multiplier"])
        self.large_vm_pes = self.small_vm_pes * self.large_vm_multiplier
        self.min_job_pes = 1 # Smallest possible job PE count

        # Calculate max_potential_vms (same logic as in Java)
        if self.num_hosts <= 0 or self.host_pes <= 0 or self.small_vm_pes <= 0:
                initial_vm_total = self.config.get("initial_s_vm_count",0) + self.config.get("initial_m_vm_count",0) + self.config.get("initial_l_vm_count",0)
                self.max_potential_vms = max(10, initial_vm_total)
        else:
                self.max_potential_vms = int(np.ceil((self.datacenter_cores / self.small_vm_pes) * 1.1))

        # Calculate max_potential_jobs (based on max infrastructure capacity)
        # This assumes jobs could fill all datacenter cores if needed
        max_potential_jobs = self.datacenter_cores // self.min_job_pes

        # Calculate max tree array length (2 entries per node: Resource + ChildCount)
        # Formula: 2 (DC Node) + 2 * num_hosts + 2 * max_potential_vms + 2 * max_potential_jobs
        # We use num_hosts here because hosts are fixed. Vms and Jobs use potential max.
        tree_array_max_len = 2 + (2 * self.num_hosts) + (2 * self.max_potential_vms) + (2 * max_potential_jobs)
        logger.info(f"Calculated TREE_ARRAY_MAX_LEN = {tree_array_max_len} (based on {self.num_hosts} hosts, {self.max_potential_vms} potential VMs, {max_potential_jobs} potential jobs)")

        # Action Space
        self.action_space = spaces.MultiDiscrete(
            np.array([
                4,                      # 4 action types (NoOp, Assign, Create, Destroy)
                self.max_potential_vms, # Target VM ID (0 to max_potential_vms-1)
                self.num_hosts,         # Target Host ID (0 to num_hosts-1)
                3                       # 3 VM types (S, M, L -> indices 0, 1, 2)
            ], dtype=np.int32)
        )
        logger.info(f"Action Space defined: {self.action_space}")

        # Observation Space (Using calculated lengths)
        self.observation_space = spaces.Dict({
                # Host Info (Fixed size based on config)
                "host_loads": spaces.Box(low=0.0, high=1.0, shape=(self.num_hosts,), dtype=np.float32),
                "host_ram_usage_ratio": spaces.Box(low=0.0, high=1.0, shape=(self.num_hosts,), dtype=np.float32),

                # VM Info (Padded, Indexed by VM ID)
                "vm_loads": spaces.Box(low=0.0, high=1.0, shape=(self.max_potential_vms,), dtype=np.float32),
                "vm_types": spaces.Box(low=0, high=3, shape=(self.max_potential_vms,), dtype=np.int32),
                "vm_host_map": spaces.Box(low=-1, high=self.num_hosts - 1, shape=(self.max_potential_vms,), dtype=np.int32),

                # Infrastructure Tree Array (Padded)
                "infrastructure_observation": spaces.Box(low=0, high=np.iinfo(np.int32).max, shape=(tree_array_max_len,), dtype=np.int32),

                # Queue Info (Normalized Scalars)
                "waiting_cloudlets": spaces.Box(low=0.0, high=1.0, shape=(1,), dtype=np.float32),
                "next_cloudlet_pes": spaces.Box(low=0.0, high=1.0, shape=(1,), dtype=np.float32)
        })
        logger.info(f"Observation Space defined: {self.observation_space.spaces.keys()}")

        # Normalization factors
        self.max_queue_norm = float(self.config.get("max_queue_norm", 100.0))
        self.max_pes_norm = float(max(self.config.get("max_job_pes", 8), self.large_vm_pes))
        if self.max_queue_norm <= 0: self.max_queue_norm = 1.0
        if self.max_pes_norm <= 0: self.max_pes_norm = 1.0
        logger.info(f"Normalization factors: max_queue={self.max_queue_norm}, max_pes={self.max_pes_norm}")

    def reset(self, seed=None, options=None):
        super(LoadBalancingEnv, self).reset()
        self.current_step = 0

        # Use stored seed or generate one if None
        current_seed = seed if seed is not None else self.config.get("seed", 42)
        logger.info(f"Resetting environment with seed: {current_seed}")

        try:
            reset_result_java = self.loadbalancer_gateway.reset(current_seed)
            observation = self._get_obs(reset_result_java.getObservation())
            info = self._process_info(reset_result_java.getInfo())
            info["observation_tree_array"] = json.loads(reset_result_java.getInfo().getObservationTreeArrayAsJson())
            info["completed_cloudlets_wait_time"] = json.loads(reset_result_java.getInfo().getCompletedCloudletWaitTimesAsJson())
            info["actual_vm_count"] = reset_result_java.getObservation().getActualVmCount()
            info["actual_host_count"] = reset_result_java.getObservation().getActualHostCount()

            logger.debug("Reset successful.")
            logger.debug(f"Initial Observation: {observation}") # Can be very verbose
            logger.debug(f"Initial Info: {info}")

            # Store initial state info needed for action masking
            self._update_internal_state(observation)

            return observation, info
        except Exception as e:
             logger.error(f"Error during env reset: {e}")
             traceback.print_exc()
             # Return dummy observation/info if reset fails critically
             dummy_obs = {key: np.zeros(space.shape, dtype=space.dtype) for key, space in self.observation_space.spaces.items()}
             return dummy_obs, {"error": "Reset failed"}

    def _get_obs(self, java_obs_state):
        """Converts the Java ObservationState object to a NumPy dictionary."""
        if java_obs_state is None:
            logger.error("Received null Java ObservationState!")
            # Return a zeroed-out observation matching the space structure
            return {key: np.zeros(space.shape, dtype=space.dtype) for key, space in self.observation_space.spaces.items()}

        # Extract arrays (Py4J automatically converts Java arrays to Python tuples/lists)
        host_loads = self._to_nparray(java_obs_state.getHostLoads(), dtype=np.float32)
        host_ram_ratios = self._to_nparray(java_obs_state.getHostRamUsageRatio(), dtype=np.float32)
        vm_loads = self._to_nparray(java_obs_state.getVmLoads(), dtype=np.float32)
        vm_types = self._to_nparray(java_obs_state.getVmTypes(), dtype=np.int32)
        vm_host_map = self._to_nparray(java_obs_state.getVmHostMap(), dtype=np.int32)
        infra_obs_java = self._to_nparray(java_obs_state.getInfrastructureObservation(), dtype=np.int32)

        # Ensure fixed size by padding/truncating Tree Array
        target_len = self.observation_space["infrastructure_observation"].shape[0]
        infra_obs_padded = np.zeros(target_len, dtype=np.int32)
        len_to_copy = min(len(infra_obs_java), target_len)
        infra_obs_padded[:len_to_copy] = infra_obs_java[:len_to_copy]
        if len(infra_obs_java) >= target_len:
            logger.warning(f"Infrastructure observation truncated from {len(infra_obs_java)} to {target_len}")

        # Normalize scalar values
        waiting_cloudlets_norm = self._to_nparray([min(java_obs_state.getWaitingCloudlets() / self.max_queue_norm, 1.0)], dtype=np.float32)
        next_cloudlet_pes_norm = self._to_nparray([min(java_obs_state.getNextCloudletPes() / self.max_pes_norm, 1.0)], dtype=np.float32)

        # Ensure array sizes match observation space definition (should be handled by Java padding now)
        # Add defensive checks or resizing if necessary
        assert host_loads.shape == self.observation_space["host_loads"].shape, f"Host loads shape mismatch"
        assert host_ram_ratios.shape == self.observation_space["host_ram_usage_ratio"].shape, f"Host RAM shape mismatch"
        assert vm_loads.shape == self.observation_space["vm_loads"].shape, f"VM loads shape mismatch"
        assert vm_types.shape == self.observation_space["vm_types"].shape, f"VM types shape mismatch"
        assert vm_host_map.shape == self.observation_space["vm_host_map"].shape, f"VM Host Map shape mismatch"

        obs_dict = {
            "host_loads": host_loads,
            "host_ram_usage_ratio": host_ram_ratios,
            "vm_loads": vm_loads,
            "vm_types": vm_types,
            "vm_host_map": vm_host_map,
            "infrastructure_observation": infra_obs_padded,
            "waiting_cloudlets": waiting_cloudlets_norm,
            "next_cloudlet_pes": next_cloudlet_pes_norm,
        }
        # logger.debug(f"Processed Observation: {obs_dict}")
        return obs_dict

    def _to_nparray(self, raw_obs, dtype=np.float32):
        obs = list(raw_obs)
        return np.array(obs, dtype=dtype)

    def _process_info(self, java_info_obj):
        """Converts the Java StepInfo object map to a Python dict."""
        if java_info_obj is None:
            return {}
        try:
             # Py4J should convert the map returned by toMap() automatically
            info_map = java_info_obj.toMap()
            return dict(info_map)
        except Exception as e:
            logger.error(f"Error processing info object: {e}")
            return {"error": str(e)} # Return error info

    def step(self, action: np.ndarray):
        self.current_step += 1

        # Ensure action is in the correct format (list of integers)
        if not isinstance(action, np.ndarray):
            action = np.array(action) # Convert if not already numpy array
        action_list = action.astype(int).tolist()

        try:
            step_result_java = self.loadbalancer_gateway.step(action_list)

            observation = self._get_obs(step_result_java.getObservation())
            reward = float(step_result_java.getReward()) # Ensure float
            terminated = bool(step_result_java.isTerminated())
            truncated = bool(step_result_java.isTruncated())
            info = self._process_info(step_result_java.getInfo())
            info["observation_tree_array"] = json.loads(step_result_java.getInfo().getObservationTreeArrayAsJson())
            info["completed_cloudlets_wait_time"] = json.loads(step_result_java.getInfo().getCompletedCloudletWaitTimesAsJson())
            info["actual_vm_count"] = step_result_java.getObservation().getActualVmCount()
            info["actual_host_count"] = step_result_java.getObservation().getActualHostCount()

            # Update internal state for action masking
            self._update_internal_state(observation)

            if self.render_mode == "human":
                self.render()

            logger.debug(f"Step Result: Obs keys={list(observation.keys())}, Rew={reward:.2f}, Term={terminated}, Trunc={truncated}, Info keys={list(info.keys())}")

            return (observation, reward, terminated, truncated, info)

        except Exception as e:
             logger.error(f"Error during env step: {e}")
             traceback.print_exc()
             # Return dummy observation/info and set done=True if step fails critically
             dummy_obs = {key: np.zeros(space.shape, dtype=space.dtype) for key, space in self.observation_space.spaces.items()}
             return dummy_obs, 0.0, True, False, {"error": "Step failed"}

    def _update_internal_state(self, observation: dict):
        """Stores the latest observation dictionary for use in action masking."""
        if observation is None:
             logger.warning("Attempted to update internal state with None observation.")
             return
        self.last_observation = observation

    def render(self):
        """Renders the environment.
        - human: Parses JSON from Java and pretty-prints the dictionary.
        - ansi: Returns the raw JSON string from Java.
        - dict: Parses JSON from Java and returns the Python dictionary.
        """
        if self.render_mode is None:
            gym.logger.warn(
                "You are calling render method "
                "without specifying any render mode. "
                "You can specify the render_mode at initialization, "
                f'e.g. gym("{self.spec.id}", render_mode="human")'
            )
            return

        try:
            raw_render_info = self.loadbalancer_gateway.getRenderInfo()
            json_string = self.java_gateway_entry_point.getRenderInfoAsJson()

            if self.render_mode == "human":
                try:
                    data = json.loads(json_string)
                    print("--- Render Info ---")
                    pp.pformat(data)
                    print("-------------------")
                except json.JSONDecodeError:
                    print("--- Render Info (Raw) ---")
                    print(raw_render_info)
                    print("-----------------------")
                return

            elif self.render_mode == "ansi":
                return str(json.loads(json_string))
            else:
                return super(LoadBalancingEnv, self).render()

        except Exception as e:
            error_msg = f"Error getting/processing render info: {e}"
            if self.render_mode == "human":
                print(error_msg)
            else:
                return error_msg if self.render_mode == 'ansi' else {"error": error_msg}

    def close(self):
        logger.info("Closing environment and gateway connection...")
        try:
            if hasattr(self, 'loadbalancer_gateway') and self.loadbalancer_gateway:
                # Request Java side close (which might trigger JVM shutdown)
                self.loadbalancer_gateway.close()
        except Exception as e:
             logger.error(f"Error during gateway close request: {e}")
        finally:
             if hasattr(self, 'gateway') and self.gateway:
                 # Close the Python client-side connection
                 self.gateway.shutdown()
                 logger.info("Py4J Gateway client shut down.")


    def action_masks(self) -> list[bool]:
        """
        Generates a boolean mask for the current action space.
        True indicates a valid action component, False indicates invalid.
        """
        if self.last_observation is None:
            # If no observation yet (e.g., before first reset), allow only NoOp
            logger.warning("Action mask called before first observation. Allowing only NoOp.")
            total_action_dims = self.action_space.nvec.sum()
            mask = [False] * total_action_dims
            mask[0] = True # Allow NoOp (action type 0)
             # Allow dummy values for other components when NoOp is selected (SB3 might need this)
            mask[4] = True # Allow dummy vm_id 0
            mask[4 + self.max_potential_vms] = True # Allow dummy host_id 0
            mask[4 + self.max_potential_vms + self.num_hosts] = True # Allow dummy vm_type 0
            return mask

        # --- Extract relevant state from the last observation ---
        vm_types = self.last_observation['vm_types'] # Array indicating type (0=Off) or index by ID
        host_loads = self.last_observation['host_loads']
        host_ram_ratios = self.last_observation['host_ram_usage_ratio']
        # Ensure waiting_cloudlets is treated as a scalar after extraction
        waiting_cloudlets = self.last_observation['waiting_cloudlets'][0] if isinstance(self.last_observation['waiting_cloudlets'], np.ndarray) else self.last_observation['waiting_cloudlets']
        actual_vm_count = self.last_observation.get('actual_vm_count', np.count_nonzero(vm_types))

        # --- Initialize Masks ---
        action_type_mask = [True, False, False, False]
        vm_id_mask = [False] * self.max_potential_vms
        host_id_mask = [False] * self.num_hosts
        vm_type_mask = [False] * 3

        # --- Determine Valid Actions Based on State ---

        # 1. Assign Cloudlet (Action Type 1)
        can_assign = waiting_cloudlets > 1e-6 # Use a small epsilon for float comparison
        if can_assign:
            action_type_mask[1] = True
            active_vm_found = False
            for vm_id in range(self.max_potential_vms):
                if vm_types[vm_id] > 0: # Check if VM exists (type > 0)
                    vm_id_mask[vm_id] = True
                    active_vm_found = True
            if not active_vm_found: # If queue has items but no VMs exist
                 action_type_mask[1] = False # Cannot assign
            # Allow dummy host/type if assign is possible
            if action_type_mask[1]:
                 host_id_mask[0] = True
                 vm_type_mask[0] = True

        # 2. Create VM (Action Type 2)
        can_potentially_create = actual_vm_count < self.max_potential_vms # Basic check
        if can_potentially_create:
            suitable_host_found = False
            for host_id in range(self.num_hosts):
                 # Simple check: Host not overloaded? (Refine if needed)
                 if host_loads[host_id] < 0.99 and host_ram_ratios[host_id] < 0.99:
                     host_id_mask[host_id] = True
                     suitable_host_found = True
            if suitable_host_found:
                 action_type_mask[2] = True
                 vm_type_mask = [True, True, True] # Allow creating S, M, L
                 vm_id_mask[0] = True # Allow dummy VM ID
            else: # No suitable host found
                 action_type_mask[2] = False
                 # Ensure dummy values are still allowed if NoOp/Other actions viable
                 host_id_mask[0] = True # Keep default host enabled for other actions
                 vm_type_mask[0] = True # Keep default type enabled for other actions

        # 3. Destroy VM (Action Type 3)
        can_destroy = actual_vm_count > 0 # Check if any VMs exist
        if can_destroy:
            action_type_mask[3] = True
            active_vm_found_for_destroy = False
            for vm_id in range(self.max_potential_vms):
                 if vm_types[vm_id] > 0:
                     vm_id_mask[vm_id] = True
                     active_vm_found_for_destroy = True
            if not active_vm_found_for_destroy: # Should not happen if actual_vm_count > 0, but safety check
                 action_type_mask[3] = False
            # Allow dummy host/type if destroy is possible
            if action_type_mask[3]:
                 host_id_mask[0] = True
                 vm_type_mask[0] = True

        # Ensure at least one dummy value is allowed for irrelevant components if ANY action type > 0 is allowed
        # This helps SB3's sampling when irrelevant parts of the MultiDiscrete are used.
        if any(action_type_mask[1:]): # If any action other than NoOp is possible
             if not any(vm_id_mask): vm_id_mask[0] = True
             if not any(host_id_mask): host_id_mask[0] = True
             if not any(vm_type_mask): vm_type_mask[0] = True
        else: # Only NoOp is possible
             vm_id_mask[0] = True
             host_id_mask[0] = True
             vm_type_mask[0] = True


        # --- Combine masks into a single flat list ---
        final_mask = action_type_mask + vm_id_mask + host_id_mask + vm_type_mask

        if len(final_mask) != self.action_space.nvec.sum():
             logger.error(f"Mask length mismatch!")
             return [True] * self.action_space.nvec.sum()

        return final_mask
