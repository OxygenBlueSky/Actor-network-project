"""
Phase 4: Author Profile Expansion
==================================
For the most prolific authors in the seed set, fetches their full publication
lists to map their broader research profiles. This reveals what else these
researchers work on beyond homeopathy — their "other life" in science.

Does NOT harvest references from these additional works (that would explode
the graph).

Usage:
    python phase4_author_expansion.py

Requires: data/seed_works.json from Phase 1.
"""

import time
from collections import Counter
from datetime import datetime

import pyalex
from pyalex import Works, Authors

import config
from utils import (
    setup_pyalex, load_json, save_json, print_section, print_stat
)

#===== AUTHOR COUNTING ======================================================

def count_author_publications(works):
    """
    Count how many seed works each author appears in. Returns a list of
    (author_id, author_name, seed_count) sorted by count descending.
    """
    author_counts = Counter()
    author_names = {}
    for w in works:
        for a in w.get("authorships", []):
            aid = a.get("author_id")
            if aid:
                author_counts[aid] += 1
                author_names[aid] = a.get("author_name", "Unknown")

    ranked = [
        (aid, author_names[aid], count)
        for aid, count in author_counts.most_common()
    ]
    return ranked

#===== AUTHOR METADATA ======================================================

def fetch_author_metadata(author_id):
    """
    Fetch the OpenAlex Author entity to get works_count, cited_by_count,
    last known institution, and ORCID. Used to check for disambiguation
    errors (suspiciously high works_count).
    """
    try:
        bare_id = author_id.replace("https://openalex.org/", "")
        author = Authors()[bare_id]
        return {
            "author_id": author.get("id"),
            "display_name": author.get("display_name"),
            "orcid": author.get("orcid"),
            "works_count": author.get("works_count", 0),
            "cited_by_count": author.get("cited_by_count", 0),
            "last_known_institutions": [
                {
                    "institution_id": inst.get("id"),
                    "institution_name": inst.get("display_name"),
                    "country_code": inst.get("country_code"),
                }
                for inst in (author.get("last_known_institutions") or [])
            ],
        }
    except Exception as e:
        print(f"    WARNING: Could not fetch metadata for {author_id}: {e}")
        return None

#===== AUTHOR WORKS =========================================================

def fetch_author_works(author_id, max_works):
    """
    Fetch an author's full publication list (up to max_works). Returns a list
    of simplified work dicts with just enough info for topic profiling.
    """
    bare_id = author_id.replace("https://openalex.org/", "")
    works_list = []
    try:
        query = Works().filter(authorships={"author": {"id": bare_id}})
        for page in query.paginate(per_page=200, n_max=max_works):
            for work in page:
                works_list.append(work)
            time.sleep(0.3)
    except Exception as e:
        print(f"    WARNING: Error fetching works for {bare_id}: {e}")
        print(f"    Collected {len(works_list)} works before failure.")

    return works_list

#===== RESEARCH PROFILE =====================================================

def build_research_profile(author_id, author_name, seed_count,
                           author_meta, author_works):
    """
    Build a research profile for an author by analyzing the topics across
    all their publications. This reveals their full research scope — not
    just what they publish in the homeopathy literature.
    """
    # Count topics across all of this author's works
    topic_counts = Counter()
    field_counts = Counter()
    domain_counts = Counter()
    year_list = []

    for w in author_works:
        year = w.get("publication_year")
        if year:
            year_list.append(year)
        for t in w.get("topics", []):
            topic_name = t.get("display_name")
            if topic_name:
                topic_counts[topic_name] += 1
            subfield = t.get("subfield") or {}
            field = t.get("field") or {}
            domain = t.get("domain") or {}
            if field.get("display_name"):
                field_counts[field["display_name"]] += 1
            if domain.get("display_name"):
                domain_counts[domain["display_name"]] += 1

    # Flag potential disambiguation errors
    total_works = author_meta.get("works_count", 0) if author_meta else 0
    disambiguation_flag = total_works > config.PROLIFIC_THRESHOLD

    return {
        "author_id": author_id,
        "author_name": author_name,
        "seed_works_count": seed_count,
        "total_works_in_openalex": total_works,
        "cited_by_count": (author_meta.get("cited_by_count", 0)
                           if author_meta else 0),
        "orcid": author_meta.get("orcid") if author_meta else None,
        "last_known_institutions": (
            author_meta.get("last_known_institutions", [])
            if author_meta else []
        ),
        "disambiguation_flag": disambiguation_flag,
        "year_range": (
            [min(year_list), max(year_list)] if year_list else None
        ),
        "fetched_works_count": len(author_works),
        # Top 20 topics by frequency — the author's research fingerprint
        "top_topics": [
            {"topic_name": name, "count": count}
            for name, count in topic_counts.most_common(20)
        ],
        "top_fields": [
            {"field_name": name, "count": count}
            for name, count in field_counts.most_common(10)
        ],
        "top_domains": [
            {"domain_name": name, "count": count}
            for name, count in domain_counts.most_common(5)
        ],
    }

#===== MAIN =================================================================

def main():
    setup_pyalex()

    # Load seed works from Phase 1
    print_section("PHASE 4: AUTHOR EXPANSION")
    seed_data = load_json("seed_works.json")
    works = seed_data["works"]
    print(f"  Loaded {len(works)} seed works")

    # Rank authors by number of seed publications
    ranked_authors = count_author_publications(works)
    print(f"  Total unique authors: {len(ranked_authors)}")

    top_n = config.TOP_AUTHORS_N
    top_authors = ranked_authors[:top_n]

    print_section(f"TOP {top_n} AUTHORS BY SEED PUBLICATIONS")
    for aid, name, count in top_authors:
        print(f"  [{count:3d} papers] {name}")

    # For each top author: fetch metadata + full publication list + build profile
    profiles = []
    for i, (aid, name, seed_count) in enumerate(top_authors):
        print(f"\n  [{i+1}/{top_n}] Expanding: {name} ({seed_count} seed papers)")

        # Fetch author-level metadata (works_count, institutions, etc.)
        author_meta = fetch_author_metadata(aid)

        # Check disambiguation flag before investing in full fetch
        if author_meta and author_meta.get("works_count", 0) > config.PROLIFIC_THRESHOLD:
            print(f"    ⚠ DISAMBIGUATION WARNING: {author_meta['works_count']} "
                  f"total works (threshold: {config.PROLIFIC_THRESHOLD})")

        # Fetch their full publication list
        author_works = fetch_author_works(aid, config.MAX_AUTHOR_WORKS)
        print(f"    Fetched {len(author_works)} works")

        # Build research profile from topic analysis
        profile = build_research_profile(
            aid, name, seed_count, author_meta, author_works
        )
        profiles.append(profile)

        time.sleep(0.3)  # courtesy delay between authors

    # Print summary of author profiles
    print_section("AUTHOR PROFILE SUMMARY")

    # How many were flagged for disambiguation issues?
    flagged = [p for p in profiles if p["disambiguation_flag"]]
    print_stat("Authors expanded", len(profiles))
    print_stat("Disambiguation warnings", len(flagged))
    if flagged:
        for p in flagged:
            print(f"    ⚠ {p['author_name']}: "
                  f"{p['total_works_in_openalex']} total works")

    # Aggregate field distribution across all top authors' publications
    all_fields = Counter()
    for p in profiles:
        for f in p["top_fields"]:
            all_fields[f["field_name"]] += f["count"]

    print_section("RESEARCH FIELDS ACROSS TOP AUTHORS")
    for field, count in all_fields.most_common(15):
        print_stat(f"  {field}", count)

    # Save to disk
    output = {
        "metadata": {
            "expansion_timestamp": datetime.now().isoformat(),
            "top_n_authors": top_n,
            "profiles_built": len(profiles),
            "disambiguation_warnings": len(flagged),
        },
        "author_profiles": profiles,
    }
    save_json(output, "author_profiles.json")
    print("\n  Phase 4 complete.")


if __name__ == "__main__":
    main()
