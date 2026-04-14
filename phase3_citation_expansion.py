"""
Phase 3: One-Hop Citation Expansion
====================================
Finds the most-cited references across all seed papers and fetches their
metadata. These are the "intellectual backbone" — the works that homeopathy
researchers collectively build on. They reveal connections to neighboring
fields (water science, nanoscience, placebo research, etc.).

Does NOT follow references further (no second hop).

Usage:
    python phase3_citation_expansion.py

Requires: data/seed_works.json from Phase 1.
"""

import time
from collections import Counter
from datetime import datetime

from pyalex import Works

import config
from utils import (
    setup_pyalex, load_json, save_json, print_section, print_stat
)

#===== REFERENCE COUNTING ===================================================

def count_reference_frequency(works):
    """
    Count how often each referenced work ID appears across all seed papers.
    A high count means many homeopathy papers cite this work — it's a
    shared intellectual foundation.

    Returns a Counter of {work_id: citation_count}.
    """
    ref_counter = Counter()
    for w in works:
        for ref_id in w.get("referenced_works", []):
            ref_counter[ref_id] += 1
    return ref_counter

#===== BATCH FETCHING =======================================================

def fetch_works_by_ids(work_ids):
    """
    Fetch full metadata for a list of OpenAlex work IDs, batched to stay
    under URL length limits. Uses the pipe | operator to request multiple
    IDs per API call (up to BATCH_SIZE at a time).

    Returns a list of raw work dicts.
    """
    results = []
    batch_size = config.BATCH_SIZE

    for i in range(0, len(work_ids), batch_size):
        batch = work_ids[i:i + batch_size]
        # Strip the full URL prefix — pyalex filter needs bare IDs like W1234567890
        bare_ids = [wid.replace("https://openalex.org/", "") for wid in batch]
        id_filter = "|".join(bare_ids)

        print(f"    Fetching batch {i // batch_size + 1} "
              f"({len(batch)} IDs)...")

        try:
            # Filter by multiple IDs in one call using pipe-separated values
            batch_results = Works().filter(openalex=id_filter).get(per_page=200)
            results.extend(batch_results)
        except Exception as e:
            print(f"    ERROR fetching batch: {e}")
            print(f"    Skipping {len(batch)} works in this batch.")

        time.sleep(0.5)  # courtesy delay between batch calls

    return results

#===== ENTITY EXTRACTION (reused from phase1) ===============================

def extract_reference_entities(work):
    """
    Extract a simplified entity dict for a referenced work. Same structure
    as phase1's extract_entities but marked as a non-seed (expanded) work.
    """
    # Authorships
    authorships = []
    for a in work.get("authorships", []):
        author_info = a.get("author", {})
        institutions = []
        for inst in a.get("institutions", []):
            institutions.append({
                "institution_id": inst.get("id"),
                "institution_name": inst.get("display_name"),
                "country_code": inst.get("country_code"),
                "type": inst.get("type"),
                "ror": inst.get("ror"),
            })
        authorships.append({
            "author_id": author_info.get("id"),
            "author_name": author_info.get("display_name"),
            "orcid": author_info.get("orcid"),
            "position": a.get("author_position"),
            "is_corresponding": a.get("is_corresponding"),
            "institutions": institutions,
        })

    # Topics
    topics = []
    for t in work.get("topics", []):
        subfield = t.get("subfield") or {}
        field = t.get("field") or {}
        domain = t.get("domain") or {}
        topics.append({
            "topic_id": t.get("id"),
            "topic_name": t.get("display_name"),
            "score": t.get("score"),
            "subfield_name": subfield.get("display_name"),
            "field_name": field.get("display_name"),
            "domain_name": domain.get("display_name"),
        })

    # Grants
    grants_raw = work.get("grants") or work.get("awards") or []
    grants = []
    for g in grants_raw:
        funder = g.get("funder") or {}
        grants.append({
            "funder_id": funder.get("id") or g.get("funder_id"),
            "funder_name": funder.get("display_name") or g.get("funder_name"),
            "award_id": g.get("award_id"),
        })

    return {
        "work_id": work.get("id"),
        "doi": work.get("doi"),
        "title": work.get("title") or work.get("display_name"),
        "publication_year": work.get("publication_year"),
        "type": work.get("type"),
        "cited_by_count": work.get("cited_by_count", 0),
        "is_seed": False,  # marks this as an expanded reference, not a seed work
        "authorships": authorships,
        "topics": topics,
        "grants": grants,
    }

#===== MAIN =================================================================

def main():
    setup_pyalex()

    # Load seed works from Phase 1
    print_section("PHASE 3: CITATION EXPANSION")
    seed_data = load_json("seed_works.json")
    works = seed_data["works"]
    print(f"  Loaded {len(works)} seed works")

    # Count how often each reference appears across seed papers
    ref_freq = count_reference_frequency(works)
    print(f"  Total unique references: {len(ref_freq)}")

    # Take the top N most-cited references — these are the intellectual backbone
    top_n = config.TOP_CITED_REFS_N
    top_refs = ref_freq.most_common(top_n)

    print_section(f"TOP {top_n} MOST-CITED REFERENCES")
    print(f"  Citation count range: {top_refs[-1][1]} – {top_refs[0][1]}")
    print(f"  (i.e., the #{top_n}th reference is cited by "
          f"{top_refs[-1][1]} seed papers)")

    # Exclude any references that are already in the seed set (no need to re-fetch)
    seed_ids = {w["work_id"] for w in works}
    refs_to_fetch = [ref_id for ref_id, _ in top_refs if ref_id not in seed_ids]
    refs_already_in_seed = top_n - len(refs_to_fetch)
    print(f"  Already in seed set: {refs_already_in_seed}")
    print(f"  Need to fetch: {len(refs_to_fetch)}")

    # Batch-fetch metadata for the top references
    print(f"\n  Fetching metadata for {len(refs_to_fetch)} references...")
    raw_refs = fetch_works_by_ids(refs_to_fetch)
    print(f"  Successfully fetched: {len(raw_refs)}")

    # Extract structured entities
    expanded_works = [extract_reference_entities(r) for r in raw_refs]

    # Print a preview of what we found — these reveal the neighboring fields
    print_section("INTELLECTUAL BACKBONE — TOP 20 MOST-CITED")
    for ref_id, count in top_refs[:20]:
        # Find the title in our fetched data or seed data
        title = "???"
        for ew in expanded_works:
            if ew["work_id"] == ref_id:
                title = ew.get("title", "???")
                break
        else:
            # Check if it's in seed set
            for sw in works:
                if sw["work_id"] == ref_id:
                    title = sw.get("title", "???")
                    break
        # Truncate long titles for readable output
        if len(title) > 70:
            title = title[:67] + "..."
        print(f"  [{count:3d} citations] {title}")

    # Show topic distribution of the expanded references
    field_counts = Counter()
    for ew in expanded_works:
        for t in ew.get("topics", []):
            field = t.get("field_name")
            if field:
                field_counts[field] += 1

    print_section("REFERENCE TOPICS (fields)")
    for field, count in field_counts.most_common(15):
        print_stat(f"  {field}", count)

    # Save everything to disk
    # Include the full frequency table so later phases can use it
    ref_freq_serializable = {ref_id: count for ref_id, count in ref_freq.items()}

    output = {
        "metadata": {
            "expansion_timestamp": datetime.now().isoformat(),
            "total_unique_references": len(ref_freq),
            "top_n_requested": top_n,
            "fetched_count": len(expanded_works),
            "already_in_seed": refs_already_in_seed,
            "min_citation_count": top_refs[-1][1] if top_refs else 0,
        },
        "reference_frequency": ref_freq_serializable,
        "top_reference_ids": [ref_id for ref_id, _ in top_refs],
        "expanded_works": expanded_works,
    }
    save_json(output, "citation_expansion.json")
    print("\n  Phase 3 complete.")


if __name__ == "__main__":
    main()
