import pandas as pd
import numpy as np
import argparse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--trace", type=str, required=True, help="The trace data to add noise to"
    )
    parser.add_argument(
        "--output",
        type=str,
        required=True,
        help="The output file to save the noisy trace",
    )
    parser.add_argument(
        "--noise-level",
        type=float,
        default=0.1,
        help="The noise level to add to the trace",
    )
    parser.add_argument(
        "--seed", type=int, default=42, help="The random seed for reproducibility"
    )
    args = parser.parse_args()
    # Set random seed for reproducibility
    np.random.seed(args.seed)

    # Load the trace file
    df = pd.read_csv(args.trace)

    # Add new jobs during dead periods
    arrival_times = df["arrival_time"].unique()
    min_time, max_time = min(arrival_times), max(arrival_times)
    dead_periods = set(range(min_time, max_time + 1)) - set(arrival_times)
    for _ in range(
        int(len(df) * args.noise_level)
    ):  # Add jobs proportional to the noise level
        new_job = {
            "job_id": len(df),
            "arrival_time": np.random.choice(list(dead_periods) + list(arrival_times)),
            "mi": 10,
            "allocated_cores": np.random.choice(
                [1, 2, 4, 8]
            ),  # Choose cores from the set {1, 2, 4, 8}
        }
        df = pd.concat([df, pd.DataFrame([new_job])], ignore_index=True)

    # Modify existing jobs
    for idx in np.random.choice(
        df.index, size=int(len(df) * args.noise_level), replace=False
    ):
        df.at[idx, "arrival_time"] += np.random.choice(
            [-1, 0, 1]
        )  # Slightly shift arrival time
        df.at[idx, "allocated_cores"] = max(
            1, np.random.choice([1, 2, 4, 8])
        )  # Choose cores from the set {1, 2, 4, 8}

    # Remove some jobs
    drop_indices = np.random.choice(
        df.index, size=int(len(df) * args.noise_level), replace=False
    )
    df = df.drop(drop_indices)

    # Reassign job IDs to ensure continuity
    df = df.reset_index(drop=True)
    df["job_id"] = df.index

    # Sort jobs by arrival time
    df = df.sort_values(by="arrival_time").reset_index(drop=True)

    # Save the noisy trace
    df.to_csv(args.output, index=False)
    print(f"Noisy trace saved to {args.output}")


if __name__ == "__main__":
    main()
