from gymnasium.envs.registration import register

register(
    id="LoadBalancingScaling-v0",
    entry_point="gym_cloudsimplus.envs:LoadBalancingEnv",
    # Optional: Add max_episode_steps if you want Gym to handle truncation
    # max_episode_steps=1000,
)
