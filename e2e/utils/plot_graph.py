import re
import numpy as np
import matplotlib.pyplot as plt
import argparse
import os

def plot_memory_growth(file_path, total_time_hours, parameter_name):
    """
    Plot memory usage over time from a log file and save the plot as PNG.
    """
    # Read the file
    with open(file_path, 'r') as f:
        content = f.readlines()

    # Extract memory usage (MiB) using regex
    mem_usages_mib = []
    for line in content:
        match = re.search(r'(\d+\.?\d*)MiB', line)
        if match:
            mem_usages_mib.append(float(match.group(1)))

    if not mem_usages_mib:
        print("No memory usage data found in the file.")
        return

    # Calculate elapsed time points
    num_points = len(mem_usages_mib)
    time_interval_seconds = (total_time_hours * 3600) / num_points
    time_points_hours = np.arange(num_points) * (time_interval_seconds / 3600)

    # Plotting
    plt.figure(figsize=(12, 6))
    plt.plot(time_points_hours, mem_usages_mib, marker='o', markersize=2, linestyle='-', linewidth=1)
    plt.title(f'{parameter_name} Memory Consumption Over {total_time_hours} Hours')
    plt.xlabel('Elapsed Time (hours)')
    plt.ylabel('Memory Usage (MiB)')
    plt.grid(True)

    # Linear regression trendline
    coeffs = np.polyfit(time_points_hours, mem_usages_mib, 1)
    trendline = np.poly1d(coeffs)
    plt.plot(time_points_hours, trendline(time_points_hours), color='red', linestyle='--',
             label=f'Linear Fit: {coeffs[0]:.2f} MiB/hour')
    plt.legend()

    plt.tight_layout()

    # Save the figure
    save_filename = f"{parameter_name.replace(' ', '_').lower()}_memory_plot.png"
    save_path = os.path.join(os.getcwd(), save_filename)
    plt.savefig(save_path)
    print(f"Plot saved to {save_path}")

    # Also show the plot
    plt.show()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Plot memory usage over time from log file and save graph")
    parser.add_argument("--file", required=True, help="Path to the input file")
    parser.add_argument("--hours", type=float, required=True, help="Total measurement time in hours")
    parser.add_argument("--param", type=str, required=True, help="Parameter name for labeling")

    args = parser.parse_args()

    plot_memory_growth(file_path=args.file, total_time_hours=args.hours, parameter_name=args.param)
