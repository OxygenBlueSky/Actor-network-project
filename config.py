# Configuration for the homeopathy actor-network pilot study.
# Edit API credentials and tunable parameters here — pipeline scripts read from this file.

import os

#===== API CREDENTIALS ======================================================

OPENALEX_API_KEY = "tTIkox05gJA9kl3oohJFvC"
CONTACT_EMAIL = "anezka.sokol@proton.me"

#===== PATHS ================================================================

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

#===== PHASE 1: SEED HARVEST ===============================================

YEAR_RANGE = (2015, 2025)
WORK_TYPES = "article|review"
# Each query has its own max_results cap. Primary gets 500 (core field);
# secondary queries get 250 to reduce noise from generic water/crystallography papers.
SEARCH_QUERIES = [
    {
        "label": "primary",
        "query": "homeopathy OR homeopathic OR homoeopathy OR homoeopathic",
        "max_results": 500,
    },
    {
        "label": "high_dilution",
        "query": '"high dilution" AND (potentized OR succussion OR dynamized)',
        "max_results": 250,
    },
    {
        "label": "ultramolecular",
        "query": '"ultra high dilution" OR ultramolecular',
        "max_results": 250,
    },
    {
        "label": "water_memory",
        "query": '"water memory" OR "water structure" AND dilution',
        "max_results": 250,
    },
    {
        "label": "biocrystallization",
        "query": 'biocrystallization OR (crystallization AND "copper chloride")',
        "max_results": 250,
    },
]

#===== PHASE 3: CITATION EXPANSION =========================================

TOP_CITED_REFS_N = 100   # fetch metadata for the N most-cited references
BATCH_SIZE = 50           # IDs per batch API call (stay under URL length limits)

#===== PHASE 4: AUTHOR EXPANSION ===========================================

TOP_AUTHORS_N = 30        # expand publication lists for the N most prolific authors
MAX_AUTHOR_WORKS = 200    # max works to fetch per author
PROLIFIC_THRESHOLD = 500  # flag authors above this as potential disambiguation errors

#===== PHASE 6: TRIMMED NETWORK ============================================

# Minimum seed publications for an author to appear in the trimmed network.
# Start with 2 (removes one-paper visitors); raise to 3 if still too large.
MIN_AUTHOR_PAPERS = 2

# Maximum number of topics to keep in trimmed network (by frequency)
MAX_TOPICS_TRIMMED = 50
