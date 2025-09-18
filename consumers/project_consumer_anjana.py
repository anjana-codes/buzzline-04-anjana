"""
project_consumer_anjana.py

Read JSON messages from buzz_live.json and visualize:
1. Top 3 Authors Leaderboard (bar chart with different colors).
2. Message Length Categories Over Time (line chart with different colors).
"""

#####################################
# Import Modules
#####################################

import json
import time
import pathlib
from collections import Counter, defaultdict
import matplotlib.pyplot as plt
from utils.utils_logger import logger

#####################################
# Set up Paths
#####################################

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
DATA_FOLDER = PROJECT_ROOT / "data"
DATA_FILE = DATA_FOLDER / "buzz_live.json"

#####################################
# Helpers
#####################################

def categorize_length(msg: str) -> str:
    """Categorize message by length into Short / Medium / Long."""
    length = len(msg)
    if length < 50:
        return "Short"
    elif length < 150:
        return "Medium"
    else:
        return "Long"

#####################################
# Main Consumer
#####################################

def consume_json_live(file_path):
    author_counts = Counter()
    category_counts_time = {"Short": [], "Medium": [], "Long": []}
    category_cumulative = {"Short": 0, "Medium": 0, "Long": 0}
    time_steps = []

    plt.ion()
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    t = 0
    with open(file_path, "r") as f:
        while True:
            line = f.readline()
            if not line:
                time.sleep(0.5)
                continue

            try:
                msg = json.loads(line.strip())
            except json.JSONDecodeError:
                logger.warning("Invalid JSON line skipped")
                continue

            # ---- Update Author Counts ----
            author = msg.get("author", "Unknown")
            author_counts[author] += 1

            # ---- Update Category Counts ----
            category = categorize_length(msg.get("message", ""))
            category_cumulative[category] += 1

            # ---- Track Time ----
            t += 1
            time_steps.append(t)
            for c in ["Short", "Medium", "Long"]:
                category_counts_time[c].append(category_cumulative[c])

            # ---- Top 3 Authors Leaderboard ----
            ax1.clear()
            top3 = author_counts.most_common(3)
            authors_colors = ["lightcoral", "lightblue", "lightgreen"]
            if top3:
                authors, counts = zip(*top3)
                ax1.barh(authors, counts, color=authors_colors[:len(authors)])
            ax1.set_title("Top 3 Authors Leaderboard")
            ax1.set_xlabel("Message Count")

            # ---- Line Chart: Message Length Categories ----
            ax2.clear()
            category_colors = {"Short": "blue", "Medium": "orange", "Long": "green"}
            for cat in ["Short", "Medium", "Long"]:
                ax2.plot(time_steps, category_counts_time[cat], label=cat, color=category_colors[cat])
            ax2.set_title("Message Length Categories Over Time")
            ax2.set_xlabel("Time")
            ax2.set_ylabel("Cumulative Messages")
            ax2.legend()

            plt.tight_layout()
            plt.pause(0.5)

#####################################
# Run
#####################################

if __name__ == "__main__":
    logger.info(f"Starting live consumer on {DATA_FILE}")
    consume_json_live(DATA_FILE)
