from gymnasium.envs.registration import register

register(
    id="SingleDC-v0",
    entry_point="gym_cloudsimplus.envs:SingleDC",
)
