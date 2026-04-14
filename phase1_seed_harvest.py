"""
Phase 1+2: Seed Harvest and Entity Extraction
==============================================
Searches OpenAlex for homeopathy-related works (2015-2025) using multiple
queries, deduplicates results, extracts structured entities, and saves
to data/seed_works.json.

Usage:
    python phase1_seed_harvest.py
"""

import time
from collections import Counter
from datetime import datetime

from pyalex import Works

import config
from utils import setup_pyalex, save_json, print_section, print_stat

#===== HARVEST ==============================================================

def harvest_query(label, query_text, max_results):
    """
    Search OpenAlex for works matching query_text, with year and type filters.
    Uses cursor pagination (cheaper than offset). Tags each result with
    the query label so we can track which query found each work.

    Returns a list of raw work dicts, each with '_source_query' added.
    """
    print(f"\n  Querying: [{label}] (max {max_results})")
    print(f"    Search: {query_text}")

    year_filter = f"{config.YEAR_RANGE[0]}-{config.YEAR_RANGE[1]}"

    works_list = []
    try:
        # title_and_abstract.search searches both title and abstract fields
        query = (
            Works()
            .search(query_text)
            .filter(publication_year=year_filter, type=config.WORK_TYPES)
        )

        # paginate() uses cursor pagination internally — cheaper than offset
        for page in query.paginate(per_page=200, n_max=max_results):
            for work in page:
                work["_source_query"] = label
                works_list.append(work)
            # Polite delay between pages to avoid hitting rate limits
            time.sleep(0.2)

    except Exception as e:
        print(f"    ERROR during harvest: {e}")
        print(f"    Collected {len(works_list)} works before failure.")

    print(f"    Found: {len(works_list)} works")
    return works_list

#===== DEDUPLICATION ========================================================

def deduplicate_works(all_works):
    """
    Deduplicate works by OpenAlex ID. When a work appears in multiple queries,
    merge the source_query labels into a list (useful for analyzing query overlap).

    Returns a dict of {work_id: work_dict} with '_source_queries' (list).
    """
    deduped = {}
    for work in all_works:
        work_id = work.get("id")
        if not work_id:
            continue
        if work_id in deduped:
            # Work already seen — just add the new query label
            existing_queries = deduped[work_id]["_source_queries"]
            new_label = work["_source_query"]
            if new_label not in existing_queries:
                existing_queries.append(new_label)
        else:
            # First time seeing this work — convert single label to list
            work["_source_queries"] = [work["_source_query"]]
            del work["_source_query"]
            deduped[work_id] = work

    return deduped

#===== ENTITY EXTRACTION ====================================================

def extract_entities(work):
    """
    Flatten a raw OpenAlex work dict into a clean, consistent structure.
    Uses .get() throughout so missing fields never cause crashes — they
    just produce None or empty lists.
    """
    # Extract authorships with nested institutions
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

    # Extract topics with their full hierarchy (topic → subfield → field → domain)
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

    # Primary topic (the single most relevant topic assigned by OpenAlex)
    pt = work.get("primary_topic") or {}
    pt_subfield = pt.get("subfield") or {}
    pt_field = pt.get("field") or {}
    pt_domain = pt.get("domain") or {}
    primary_topic = {
        "topic_id": pt.get("id"),
        "topic_name": pt.get("display_name"),
        "subfield_name": pt_subfield.get("display_name"),
        "field_name": pt_field.get("display_name"),
        "domain_name": pt_domain.get("display_name"),
    } if pt.get("id") else None

    # Grants/awards — OpenAlex has used both field names at different times
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
        "source_queries": work.get("_source_queries", []),
        "authorships": authorships,
        "referenced_works": work.get("referenced_works", []),
        "topics": topics,
        "primary_topic": primary_topic,
        "grants": grants,
    }

#===== SUMMARY STATISTICS ===================================================

def print_summary(works):
    """Print harvest summary stats so the user can sanity-check before proceeding."""

    print_section("SEED HARVEST SUMMARY")
    print_stat("Total works (deduplicated)", len(works))

    # Works per source query (a work can appear under multiple queries)
    query_counts = Counter()
    for w in works:
        for q in w["source_queries"]:
            query_counts[q] += 1

    print_section("Works per Query")
    for label, count in query_counts.most_common():
        print_stat(f"  {label}", count)

    # Query overlap — how many works matched more than one query
    multi_query = [w for w in works if len(w["source_queries"]) > 1]
    print_stat("Works matching multiple queries", len(multi_query))

    # Year distribution
    year_counts = Counter(w["publication_year"] for w in works)
    print_section("Year Distribution")
    for year in sorted(year_counts):
        print_stat(f"  {year}", year_counts[year])

    # Type distribution
    type_counts = Counter(w["type"] for w in works)
    print_section("Work Types")
    for wtype, count in type_counts.most_common():
        print_stat(f"  {wtype}", count)

    # Unique entities
    all_authors = set()
    all_institutions = set()
    all_countries = set()
    all_topics = set()
    all_funders = set()
    authors_with_orcid = set()
    works_with_grants = 0

    for w in works:
        for a in w["authorships"]:
            if a["author_id"]:
                all_authors.add(a["author_id"])
                if a["orcid"]:
                    authors_with_orcid.add(a["author_id"])
            for inst in a["institutions"]:
                if inst["institution_id"]:
                    all_institutions.add(inst["institution_id"])
                if inst["country_code"]:
                    all_countries.add(inst["country_code"])
        for t in w["topics"]:
            if t["topic_id"]:
                all_topics.add(t["topic_id"])
        if w["grants"]:
            works_with_grants += 1
            for g in w["grants"]:
                if g["funder_id"]:
                    all_funders.add(g["funder_id"])

    print_section("Unique Entities")
    print_stat("Authors", len(all_authors))
    print_stat("Institutions", len(all_institutions))
    print_stat("Countries", len(all_countries))
    print_stat("Topics", len(all_topics))
    print_stat("Funders", len(all_funders))

    # Coverage rates — important to know what data is available
    print_section("Data Coverage")
    orcid_pct = (len(authors_with_orcid) / len(all_authors) * 100
                 if all_authors else 0)
    grant_pct = (works_with_grants / len(works) * 100
                 if works else 0)
    print_stat("Authors with ORCID", f"{len(authors_with_orcid)} ({orcid_pct:.1f}%)")
    print_stat("Works with grant/funder info", f"{works_with_grants} ({grant_pct:.1f}%)")

    # Citation stats
    citations = [w["cited_by_count"] for w in works]
    citations.sort(reverse=True)
    median_idx = len(citations) // 2
    print_section("Citation Statistics")
    print_stat("Max cited_by_count", citations[0] if citations else 0)
    print_stat("Median cited_by_count", citations[median_idx] if citations else 0)
    print_stat("Total references collected",
               sum(len(w["referenced_works"]) for w in works))

#===== MAIN =================================================================

def main():
    setup_pyalex()

    print_section("PHASE 1: SEED HARVEST")
    print(f"  Year range: {config.YEAR_RANGE[0]}-{config.YEAR_RANGE[1]}")
    print(f"  Work types: {config.WORK_TYPES}")
    print(f"  Number of queries: {len(config.SEARCH_QUERIES)}")

    # Harvest all queries, collecting raw results
    all_raw_works = []
    for sq in config.SEARCH_QUERIES:
        results = harvest_query(sq["label"], sq["query"], sq["max_results"])
        all_raw_works.extend(results)

    print(f"\n  Total raw results (before dedup): {len(all_raw_works)}")

    # Deduplicate by OpenAlex work ID
    deduped = deduplicate_works(all_raw_works)
    print(f"  After deduplication: {len(deduped)} unique works")
    print(f"  Duplicates removed: {len(all_raw_works) - len(deduped)}")

    # Extract structured entities from each work
    print("\n  Extracting entities...")
    works = [extract_entities(w) for w in deduped.values()]

    # Print summary statistics for the user to review
    print_summary(works)

    # Save to disk — this is our checkpoint for all subsequent phases
    output = {
        "metadata": {
            "harvest_timestamp": datetime.now().isoformat(),
            "year_range": list(config.YEAR_RANGE),
            "work_types": config.WORK_TYPES,
            "queries": config.SEARCH_QUERIES,
            "total_raw_results": len(all_raw_works),
            "total_deduplicated": len(works),
        },
        "works": works,
    }
    save_json(output, "seed_works.json")
    print("\n  Phase 1 complete. Review the summary above, then run Phase 3 or 4.")


if __name__ == "__main__":
    main()
