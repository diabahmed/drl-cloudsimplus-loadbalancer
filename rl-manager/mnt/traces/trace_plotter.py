import argparse
import pandas as pd
import matplotlib.pyplot as plt


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--trace", type=str, help="The trace data to print")
    parser.add_argument(
        "--count-cores",
        type=bool,
        default=True,
        help="Count the number of cores instead of jobs",
    )
    args = parser.parse_args()

    # Read the trace data
    df = pd.read_csv(args.trace)

    # Group by arrival time and count the number of jobs
    if not args.count_cores:
        grouped = df.groupby("arrival_time").size()
    else:
        grouped = df.groupby("arrival_time")["allocated_cores"].sum()

    # Plot the bar chart
    plt.figure(figsize=(10, 6))
    # Add grid
    plt.grid(axis="y", linestyle="--", alpha=0.7)
    plt.bar(grouped.index, grouped.values, color="skyblue", edgecolor="black")

    # Add labels and title with increased font size
    plt.xlabel("Arrival Time (s)", fontsize=19)
    plt.ylabel(
        "# Jobs" if not args.count_cores else "# Requested Cores",
        fontsize=19,
    )
    # plt.title("Job Arrival Distribution", fontsize=16)
    plt.xticks(fontsize=17)
    plt.yticks(fontsize=17)

    # Save the plot
    output_filename = args.trace.split(".")[0] + ".pdf"
    plt.savefig(output_filename, bbox_inches="tight", pad_inches=0)
    print(f"Plot saved as {output_filename}")


if __name__ == "__main__":
    main()
