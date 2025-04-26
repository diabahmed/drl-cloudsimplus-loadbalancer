import argparse
import pandas as pd
import matplotlib.pyplot as plt


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--trace", type=str, help="The trace data to print")
    args = parser.parse_args()

    # Read the trace data
    df = pd.read_csv(args.trace)

    # Group the data for the stacked bar chart
    pivot_df = (
        df.groupby(["arrival_time", "allocated_cores"]).size().unstack(fill_value=0)
    )

    # Plot the stacked bar chart
    plt.figure(figsize=(10, 6))
    bar_width = 0.8  # Set bar width
    ax = plt.gca()  # Get current axes
    colors = plt.cm.tab20.colors  # Use a colormap for distinct colors

    # Trackers for drawing stacked bars and lines
    bar_positions = range(len(pivot_df.index))
    cumulative_heights = {idx: 0 for idx in bar_positions}

    for col_idx, core_count in enumerate(pivot_df.columns):
        bar_heights = pivot_df[core_count].values
        bars = ax.bar(
            bar_positions,
            bar_heights,
            bottom=[cumulative_heights[idx] for idx in bar_positions],
            width=bar_width,
            color=colors[col_idx % len(colors)],
            edgecolor="black",
            label=f"{core_count} cores",
        )

        # Update cumulative heights and add horizontal lines
        for idx, height in enumerate(bar_heights):
            current_bottom = cumulative_heights[idx]
            for _ in range(height):
                current_bottom += core_count
                plt.hlines(
                    y=current_bottom,
                    xmin=idx - bar_width / 2,
                    xmax=idx + bar_width / 2,
                    colors="black",
                    linestyles="solid",
                    linewidth=0.5,
                )
            cumulative_heights[idx] += core_count * height

    # Add grid
    plt.grid(axis="y", linestyle="--", alpha=0.7)

    # Add labels and title with increased font size
    plt.xlabel("Arrival Time (s)", fontsize=19)
    plt.ylabel("# Requested Cores", fontsize=19)
    plt.xticks(bar_positions, pivot_df.index, fontsize=17, rotation=45)
    plt.yticks(fontsize=17)

    # Adjust legend for better readability
    plt.legend(
        title="Allocated Cores",
        fontsize=13,
        title_fontsize=14,
        loc="upper left",
        bbox_to_anchor=(1, 1),
    )

    # Save the plot
    output_filename = args.trace.split(".")[0] + "_stacked_with_job_lines.png"
    plt.tight_layout()
    plt.savefig(output_filename, bbox_inches="tight", pad_inches=0)
    print(f"Stacked plot with individual lines saved as {output_filename}")


if __name__ == "__main__":
    main()
