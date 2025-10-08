import argparse
import csv
from pathlib import Path
import matplotlib.pyplot as plt


def parse_log_file(file_path):
    results = []
    with open(file_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            results.append({
                "lvol_count": int(row['lvol_count']),
                "shutdown_time_sec": float(row['shutdown_time_sec']),
                "restart_time_sec": float(row['restart_time_sec'])
            })
    return results


def generate_graph(results, input_path):
    output_path = Path("logs") / "lvol_shutdown_restart_graph.png"
    x = [r['lvol_count'] for r in results]
    shutdown = [r['shutdown_time_sec'] for r in results]
    restart = [r['restart_time_sec'] for r in results]

    plt.plot(x, shutdown, marker='o', label='Shutdown Time (s)')
    plt.plot(x, restart, marker='x', label='Restart Time (s)')

    for i in range(len(x)):
        plt.annotate(f"{shutdown[i]:.1f}s", (x[i], shutdown[i]), textcoords="offset points", xytext=(0, 5), ha='center')
        plt.annotate(f"{restart[i]:.1f}s", (x[i], restart[i]), textcoords="offset points", xytext=(0, -10), ha='center')

    plt.title("Node Outage Time vs. Number of lvols")
    plt.xlabel("Number of lvols")
    plt.ylabel("Time (seconds)")
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    output_path.parent.mkdir(exist_ok=True)
    plt.savefig(output_path)
    plt.show()
    print(f"Graph saved to {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate graph from lvol outage log CSV")
    parser.add_argument('--input', type=str, required=True, help="Path to the CSV log file")

    args = parser.parse_args()
    input_file = Path(args.input)

    if not input_file.exists():
        raise FileNotFoundError(f"Log file not found: {input_file}")

    results = parse_log_file(input_file)
    generate_graph(results, input_file)
