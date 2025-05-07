from setuptools import setup, find_packages

setup(
    name="gym_cloudsimplus_lb",
    version="0.1.0", # Initial version
    install_requires=["gymnasium", "py4j", "numpy"],
    packages=find_packages(),
    # Optional: Specify python version requirement
    python_requires='>=3.12',
)
