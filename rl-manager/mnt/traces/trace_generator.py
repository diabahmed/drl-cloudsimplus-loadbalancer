import argparse
import pandas as pd
import numpy as np


def generate_trace(
    output_file,
    num_jobs,
    distribution,
    mean=10,
    std=5,
    max_time=30,
    max_entropy=30,
    max_entropy_type="cores",
    random_seed=42,
):
    """
    Generate a synthetic data trace with random job arrivals based on the specified distribution.

    Args:
        output_file (str): Path to save the generated CSV file.
        num_jobs (int): Total number of jobs to generate.
        distribution (str): The distribution for arrival times ('random', 'uniform', 'gaussian').
        mean (int): Mean for Gaussian distribution (default: 10).
        std (int): Standard deviation for Gaussian distribution (default: 5).
        max_time (int): Maximum possible arrival time (used in uniform and random distributions).
        max_entropy (int): Maximum number of cores that can arrive at the same time.
        max_entropy_type (str): Type of limit ('jobs' or 'cores'). Controls whether max_entropy limits jobs or total cores per slot.
        random_seed (int): Seed for reproducibility.
    """
    np.random.seed(random_seed)

    # Generate arrival times based on the chosen distribution, starting from 1
    if distribution == "random":
        arrival_times = np.random.randint(1, max_time + 1, size=num_jobs)
    elif distribution == "uniform":
        arrival_times = np.random.choice(
            range(1, max_time + 1), size=num_jobs, replace=True
        )
    elif distribution == "gaussian":
        # Create a Gaussian distribution, but clip values to ensure they stay within max_time
        arrival_times = np.abs(np.random.normal(mean, std, num_jobs).astype(int)) + 1
        arrival_times = np.clip(
            arrival_times, 1, max_time
        )  # Ensure arrival times are within bounds
    else:
        raise ValueError(
            "Invalid distribution. Choose from 'random', 'uniform', or 'gaussian'."
        )

    # Ensure we don't exceed max_time with the generated arrival times
    arrival_times = np.clip(arrival_times, 1, max_time)

    # Generate job data (mi and allocated_cores)
    mi_values = (
        np.random.randint(1, 6, size=num_jobs) * 50000
    )  # Random values from the set {10, 20, 30, 40, 50}

    if max_entropy_type == "jobs":
        allocated_cores = np.random.choice(
            [1, 2, 4, 8], size=num_jobs
        )  # Random values from the set {1, 2, 4, 8}
    elif max_entropy_type == "cores":
        # For cores, we scale the core assignment using a Gaussian distribution
        allocated_cores = np.random.choice([1, 2, 4, 8], size=num_jobs)
        allocated_cores = allocated_cores.astype(int)
        allocated_cores[allocated_cores == 0] = 1  # Ensure there are no zero cores
    else:
        raise ValueError("Invalid max_entropy_type. Choose from 'jobs' or 'cores'.")

    # Create the initial DataFrame
    df = pd.DataFrame(
        {
            "job_id": range(num_jobs),
            "arrival_time": arrival_times,
            "mi": mi_values,
            "allocated_cores": allocated_cores,
        }
    )

    # Now handle the max entropy logic
    adjusted_rows = []
    for time in sorted(df["arrival_time"].unique()):
        # Filter the jobs for this arrival time
        time_jobs = df[df["arrival_time"] == time]

        if max_entropy_type == "jobs":
            # Limit the number of jobs in the slot
            num_jobs_at_time = np.random.randint(0, max_entropy + 1)
            if len(time_jobs) > num_jobs_at_time:
                time_jobs = time_jobs.sample(n=num_jobs_at_time, replace=False)
        elif max_entropy_type == "cores":
            # Limit the total number of cores in the slot
            cumulative_cores = 0
            selected_jobs = []
            for _, job in time_jobs.iterrows():
                if cumulative_cores + job["allocated_cores"] > max_entropy:
                    break  # Stop adding jobs if total cores exceed max_entropy
                cumulative_cores += job["allocated_cores"]
                selected_jobs.append(job)
            time_jobs = pd.DataFrame(selected_jobs)
        else:
            raise ValueError("Invalid max_entropy_type. Choose from 'jobs' or 'cores'.")

        # Add the filtered jobs to the list of adjusted rows
        adjusted_rows.extend(time_jobs.to_dict(orient="records"))

    # Create the adjusted DataFrame
    adjusted_df = pd.DataFrame(adjusted_rows)

    # Sort by arrival time
    adjusted_df = adjusted_df.sort_values(by="arrival_time").reset_index(drop=True)
    adjusted_df["job_id"] = adjusted_df.index  # Reassign job IDs after sorting

    # Check if we reached the required number of jobs and adjust
    while len(adjusted_df) < num_jobs:
        # Add more jobs randomly to meet num_jobs
        remaining_jobs = num_jobs - len(adjusted_df)
        extra_jobs = df.sample(n=remaining_jobs, replace=True)
        adjusted_df = pd.concat([adjusted_df, extra_jobs])

    # Sort again by arrival time before saving to ensure order
    adjusted_df = adjusted_df.sort_values(by="arrival_time").reset_index(drop=True)
    adjusted_df["job_id"] = adjusted_df.index  # Reassign job IDs after sorting

    # Save to CSV
    adjusted_df.to_csv(output_file, index=False)
    print(f"Data trace generated and saved to {output_file}")


# Main function to handle command-line arguments
def main():
    parser = argparse.ArgumentParser(description="Generate synthetic data traces.")
    parser.add_argument(
        "--output_file",
        type=str,
        required=True,
        help="Path to save the generated CSV file.",
    )
    parser.add_argument(
        "--num_jobs", type=int, default=2000, help="Number of jobs to generate."
    )
    parser.add_argument(
        "--distribution",
        type=str,
        choices=["random", "uniform", "gaussian"],
        default="gaussian",
        help="Distribution to use for job arrival times.",
    )
    parser.add_argument(
        "--mean",
        type=int,
        default=10,
        help="Mean for Gaussian distribution (ignored for other distributions).",
    )
    parser.add_argument(
        "--std",
        type=int,
        default=5,
        help="Standard deviation for Gaussian distribution (ignored for other distributions).",
    )
    parser.add_argument(
        "--max_time",
        type=int,
        default=30,
        help="Maximum arrival time for random/uniform distributions.",
    )
    parser.add_argument(
        "--max_entropy",
        type=int,
        default=500,
        help="Maximum number of cores that can arrive at the same time.",
    )
    parser.add_argument(
        "--max_entropy_type",
        type=str,
        choices=["jobs", "cores"],
        default="cores",
        help="Controls whether max_entropy limits jobs or total cores per time slot.",
    )
    args = parser.parse_args()

    generate_trace(
        args.output_file,
        args.num_jobs,
        args.distribution,
        args.mean,
        args.std,
        args.max_time,
        args.max_entropy,
        args.max_entropy_type,
    )


if __name__ == "__main__":
    main()
