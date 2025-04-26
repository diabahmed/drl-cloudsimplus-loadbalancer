import argparse
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--trace", type=str, help="The trace data to print", required=True
    )
    parser.add_argument(
        "--count-cores",
        type=bool,
        default=True,
        help="Count the number of cores instead of jobs",
    )
    args = parser.parse_args()

    # Read the trace data
    df = pd.read_csv(args.trace)

    # Create a pivot table for the stacked bar plot
    if not args.count_cores:
        # Count jobs with different core requests at each arrival time
        pivot_df = (
            df.groupby(["arrival_time", "allocated_cores"]).size().unstack(fill_value=0)
        )
    else:
        # Sum cores by arrival time for each core value
        pivot_df = (
            df.groupby(["arrival_time", "allocated_cores"])["allocated_cores"]
            .sum()
            .unstack(fill_value=0)
        )

    pivot_df.columns = [f"{col}-core" for col in pivot_df.columns]

    # Plot the stacked bar chart
    plt.figure(figsize=(8, 5.5))
    ax = plt.gca()

    # Plot bars manually to control the x positions (i.e., arrival_time)
    bottom_vals = np.zeros(len(pivot_df))  # To stack the bars on top of each other

    # Plot each "core type" as a separate layer in the stacked bars
    for i, col in enumerate(pivot_df.columns):
        ax.bar(
            pivot_df.index,
            pivot_df[col],
            bottom=bottom_vals,
            label=col,
            color=plt.cm.tab20.colors[i],
            edgecolor="black",
        )
        bottom_vals += pivot_df[col]  # Update bottom values for stacking

    # Add grid
    plt.grid(axis="y", linestyle="--", alpha=0.7)

    # Add labels and title with increased font size
    plt.xlabel("Arrival Time (s)", fontsize=22)
    plt.ylabel(
        "# Jobs" if not args.count_cores else "Requested Cores",
        fontsize=22,
    )

    # Set x-axis ticks based on arrival_time
    plt.xticks(fontsize=20, rotation=0)
    plt.yticks(fontsize=20)

    # Adjust legend for better readability
    plt.legend(
        title="Job Type",
        fontsize=20,
        title_fontsize=22,
        loc="upper left",
        bbox_to_anchor=(0.57, 1),
    )

    # Save the plot
    output_filename = args.trace.split(".")[0] + "_stacked.pdf"
    plt.tight_layout()
    plt.savefig(output_filename, bbox_inches="tight", pad_inches=0)
    print(f"Stacked plot saved as {output_filename}")


if __name__ == "__main__":
    main()
