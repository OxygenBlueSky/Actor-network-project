# Shared utilities for the actor-network pipeline.
# Handles API setup, file I/O, and console formatting.

import json
import os

import pyalex

import config

#===== API SETUP ============================================================

def setup_pyalex():
    """Configure pyalex with API key, email (polite pool), and retry settings."""
    pyalex.config.api_key = config.OPENALEX_API_KEY
    pyalex.config.email = config.CONTACT_EMAIL
    pyalex.config.max_retries = 3

#===== FILE I/O =============================================================

def ensure_data_dir():
    """Create the data/ directory if it doesn't exist."""
    os.makedirs(config.DATA_DIR, exist_ok=True)

def save_json(data, filename):
    """Write data to DATA_DIR/filename as pretty-printed JSON."""
    ensure_data_dir()
    filepath = os.path.join(config.DATA_DIR, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    # Report what was saved so the user can verify
    if isinstance(data, dict) and "works" in data:
        print(f"  Saved {len(data['works'])} works to {filepath}")
    elif isinstance(data, list):
        print(f"  Saved {len(data)} records to {filepath}")
    else:
        print(f"  Saved to {filepath}")

def load_json(filename):
    """Read JSON from DATA_DIR/filename. Raises a helpful error if missing."""
    filepath = os.path.join(config.DATA_DIR, filename)
    if not os.path.exists(filepath):
        raise FileNotFoundError(
            f"File not found: {filepath}\n"
            f"Have you run the prerequisite phase that produces this file?"
        )
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)

#===== CONSOLE FORMATTING ===================================================

def print_section(title):
    """Print a formatted section header for terminal readability."""
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")

def print_stat(label, value):
    """Print a right-aligned label: value pair."""
    print(f"  {label:<40} {value}")
