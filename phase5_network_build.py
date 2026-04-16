"""
Phase 5: Network Construction
==============================
Builds a heterogeneous directed graph from the seed works, expanded
references, and author profiles. Node types: work, author, institution,
funder, topic, country. Edge types: authored, cited, affiliated, funded,
has_topic, located_in.

Usage:
    python phase5_network_build.py

Requires:
    data/seed_works.json         (Phase 1)
    data/citation_expansion.json (Phase 3)
    data/author_profiles.json    (Phase 4)
"""

import json
import pickle
import os
from collections import Counter, defaultdict
from datetime import datetime

import networkx as nx

import config
from utils import load_json, save_json, print_section, print_stat

#===== NODE HELPERS =========================================================

def add_work_node(G, work, is_seed=True):
    """
    Add a work node with all its metadata attributes. These attributes
    become columns in Gephi's data table for coloring/filtering.
    """
    work_id = work["work_id"]
    if not work_id:
        return

    # Determine the "primary source" — the first query that found this work,
    # used for simple single-color partitioning in Gephi
    source_queries = work.get("source_queries", [])
    primary_source = source_queries[0] if source_queries else "expanded_ref"

    # Primary topic field for thematic coloring
    pt = work.get("primary_topic") or {}

    G.add_node(work_id,
               node_type="work",
               label=_truncate(work.get("title", ""), 60),
               title=work.get("title", ""),
               doi=work.get("doi", ""),
               publication_year=work.get("publication_year"),
               work_type=work.get("type", ""),
               cited_by_count=work.get("cited_by_count", 0),
               is_seed=is_seed,
               source_queries="|".join(source_queries),
               primary_source=primary_source,
               # Topic-based attributes for thematic coloring
               primary_topic=pt.get("topic_name", ""),
               primary_subfield=pt.get("subfield_name", ""),
               primary_field=pt.get("field_name", ""),
               primary_domain=pt.get("domain_name", ""))


def add_author_node(G, author_id, author_name, orcid=None):
    """Add an author node, updating name/orcid if the node already exists."""
    if not author_id:
        return
    if author_id in G.nodes:
        # Update with better data if available (some entries lack names)
        if author_name and not G.nodes[author_id].get("label"):
            G.nodes[author_id]["label"] = author_name
        if orcid and not G.nodes[author_id].get("orcid"):
            G.nodes[author_id]["orcid"] = orcid
    else:
        G.add_node(author_id,
                   node_type="author",
                   label=author_name or "",
                   orcid=orcid or "")


def add_institution_node(G, inst):
    """Add an institution node from an institution dict."""
    inst_id = inst.get("institution_id")
    if not inst_id:
        return
    if inst_id not in G.nodes:
        G.add_node(inst_id,
                   node_type="institution",
                   label=inst.get("institution_name", ""),
                   country_code=inst.get("country_code", ""),
                   institution_type=inst.get("type", ""),
                   ror=inst.get("ror", ""))


def add_topic_node(G, topic):
    """Add a topic node from a topic dict."""
    topic_id = topic.get("topic_id")
    if not topic_id:
        return
    if topic_id not in G.nodes:
        G.add_node(topic_id,
                   node_type="topic",
                   label=topic.get("topic_name", ""),
                   subfield=topic.get("subfield_name", ""),
                   field=topic.get("field_name", ""),
                   domain=topic.get("domain_name", ""))


def add_funder_node(G, funder_id, funder_name):
    """Add a funder node."""
    if not funder_id:
        return
    if funder_id not in G.nodes:
        G.add_node(funder_id,
                   node_type="funder",
                   label=funder_name or "")


def add_country_node(G, country_code):
    """Add a country node from an ISO 3166-1 alpha-2 code."""
    if not country_code:
        return
    node_id = f"country:{country_code}"
    if node_id not in G.nodes:
        G.add_node(node_id,
                   node_type="country",
                   label=country_code)
    return node_id

#===== EDGE HELPERS =========================================================

def add_work_edges(G, work):
    """
    Add all edges radiating from a single work node:
    authored, cited, has_topic, funded, plus institution/country edges.
    """
    work_id = work["work_id"]
    if not work_id:
        return

    # Author → Work edges (with position attribute for first/last distinction)
    for a in work.get("authorships", []):
        author_id = a.get("author_id")
        if not author_id:
            continue

        add_author_node(G, author_id, a.get("author_name"), a.get("orcid"))
        G.add_edge(author_id, work_id,
                   edge_type="authored",
                   position=a.get("position", ""))

        # Author → Institution edges (affiliation on this specific paper)
        for inst in a.get("institutions", []):
            inst_id = inst.get("institution_id")
            if not inst_id:
                continue
            add_institution_node(G, inst)

            # Use a combined key to allow weighted affiliation edges
            if G.has_edge(author_id, inst_id):
                G[author_id][inst_id]["weight"] += 1
            else:
                G.add_edge(author_id, inst_id,
                           edge_type="affiliated",
                           weight=1)

            # Institution → Country
            cc = inst.get("country_code")
            if cc:
                country_node = add_country_node(G, cc)
                if not G.has_edge(inst_id, country_node):
                    G.add_edge(inst_id, country_node,
                               edge_type="located_in")

    # Work → Work edges (citations, directed)
    for ref_id in work.get("referenced_works", []):
        if not ref_id:
            continue
        # Add a stub node if the referenced work isn't in the graph yet
        if ref_id not in G.nodes:
            G.add_node(ref_id, node_type="work", label="", is_seed=False,
                       stub=True)
        G.add_edge(work_id, ref_id, edge_type="cited")

    # Work → Topic edges
    for t in work.get("topics", []):
        topic_id = t.get("topic_id")
        if not topic_id:
            continue
        add_topic_node(G, t)
        G.add_edge(work_id, topic_id,
                   edge_type="has_topic",
                   score=t.get("score", 0))

    # Funder → Work edges
    for g in work.get("grants", []):
        funder_id = g.get("funder_id")
        if not funder_id:
            continue
        add_funder_node(G, funder_id, g.get("funder_name"))
        G.add_edge(funder_id, work_id, edge_type="funded")

#===== AUTHOR DEDUPLICATION ==================================================

def find_duplicate_authors(G):
    """
    Find authors that share the same display name (case-insensitive,
    whitespace-normalized). These are likely the same person split into
    multiple OpenAlex profiles.

    Returns a dict of {normalized_name: [list of author node IDs]},
    only for names that appear more than once.
    """
    name_to_ids = defaultdict(list)
    for node, data in G.nodes(data=True):
        if data.get("node_type") != "author":
            continue
        name = data.get("label", "").strip()
        if not name:
            continue
        # Normalize: lowercase, collapse whitespace, strip punctuation
        normalized = " ".join(name.lower().split())
        name_to_ids[normalized].append(node)

    # Keep only names with 2+ IDs (the actual duplicates)
    duplicates = {name: ids for name, ids in name_to_ids.items()
                  if len(ids) > 1}
    return duplicates


def merge_duplicate_authors(G, duplicates):
    """
    Merge duplicate author nodes. For each set of duplicates:
    1. Pick the canonical node (most edges = most data)
    2. Redirect all edges from duplicates to the canonical node
    3. Merge attributes (combine ORCIDs, sum counts)
    4. Mark the label with * to indicate a merge happened
    5. Remove the duplicate nodes

    Returns a list of merge records for the report.
    """
    merge_log = []

    for normalized_name, author_ids in duplicates.items():
        # Pick canonical: the one with the most edges (best-connected profile)
        author_ids_sorted = sorted(
            author_ids,
            key=lambda aid: G.degree(aid),
            reverse=True
        )
        canonical_id = author_ids_sorted[0]
        duplicate_ids = author_ids_sorted[1:]

        canonical_data = G.nodes[canonical_id]
        canonical_name = canonical_data.get("label", "")

        # Collect info for the merge log
        merge_record = {
            "canonical_id": canonical_id,
            "canonical_name": canonical_name,
            "merged_ids": [],
        }

        for dup_id in duplicate_ids:
            dup_data = dict(G.nodes[dup_id])
            merge_record["merged_ids"].append({
                "id": dup_id,
                "name": dup_data.get("label", ""),
                "orcid": dup_data.get("orcid", ""),
                "edges": G.degree(dup_id),
            })

            # Merge ORCID if the canonical lacks one
            if dup_data.get("orcid") and not canonical_data.get("orcid"):
                canonical_data["orcid"] = dup_data["orcid"]

            # Redirect all edges from the duplicate to the canonical node
            # Incoming edges (something → duplicate)
            for source, _, edata in list(G.in_edges(dup_id, data=True)):
                if source == canonical_id:
                    continue  # skip self-loops
                if not G.has_edge(source, canonical_id):
                    G.add_edge(source, canonical_id, **edata)
                else:
                    # Edge already exists — increment weight if applicable
                    existing = G[source][canonical_id]
                    if "weight" in existing and "weight" in edata:
                        existing["weight"] += edata["weight"]

            # Outgoing edges (duplicate → something)
            for _, target, edata in list(G.out_edges(dup_id, data=True)):
                if target == canonical_id:
                    continue
                if not G.has_edge(canonical_id, target):
                    G.add_edge(canonical_id, target, **edata)
                else:
                    existing = G[canonical_id][target]
                    if "weight" in existing and "weight" in edata:
                        existing["weight"] += edata["weight"]

            # Remove the duplicate node (and all its now-redirected edges)
            G.remove_node(dup_id)

        # Mark the canonical label with * to indicate a merge
        canonical_data["label"] = canonical_name + " *"
        canonical_data["merged_from"] = "|".join(
            d["id"] for d in merge_record["merged_ids"]
        )

        merge_log.append(merge_record)

    return merge_log


def print_and_save_merge_report(merge_log):
    """Print the duplicate author report and save to data/author_merges.json."""
    print_section(f"AUTHOR DEDUPLICATION: {len(merge_log)} MERGES")

    for rec in merge_log:
        canonical = rec["canonical_name"]
        merged = rec["merged_ids"]
        ids_str = ", ".join(
            f"{m['name']} ({m['edges']} edges, orcid={m['orcid'] or 'none'})"
            for m in merged
        )
        print(f"  {canonical} * ← merged: {ids_str}")

    # Save full merge log for reference
    save_json(merge_log, "author_merges.json")


#===== AUTHOR ROLE CLASSIFICATION ===========================================

def compute_author_roles(G):
    """
    For each author, determine their primary role (first / middle / last)
    based on which position they most frequently occupy across all papers.

    'first' typically = lead researcher / PhD student
    'last'  typically = senior author / PI
    'middle' = collaborator

    Stored as node attributes:
      primary_role:  the most common position (e.g. "last")
      role_detail:   counts string (e.g. "first:5|middle:2|last:20")
    """
    author_positions = {}  # {author_id: Counter of positions}

    for source, target, edata in G.edges(data=True):
        if edata.get("edge_type") != "authored":
            continue
        # authored edges go author → work
        author_id = source
        position = edata.get("position", "")
        if not position:
            continue
        if author_id not in author_positions:
            author_positions[author_id] = Counter()
        author_positions[author_id][position] += 1

    for aid, pos_counts in author_positions.items():
        if aid not in G.nodes:
            continue
        # Most common position = primary role
        primary_role = pos_counts.most_common(1)[0][0]
        G.nodes[aid]["primary_role"] = primary_role
        # Detailed breakdown for tooltips
        detail = "|".join(f"{pos}:{count}"
                          for pos, count in pos_counts.most_common())
        G.nodes[aid]["role_detail"] = detail

    return author_positions

#===== AUTHOR SEED METRICS ==================================================

def compute_author_seed_metrics(G):
    """
    For ALL authors in the graph, compute metrics from their seed papers:
      seed_works_count:  number of papers in the network
      seed_citations:    sum of cited_by_count across their papers (proxy
                         for impact within this field, available for everyone)

    This runs before enrich_author_profiles so the Phase 4 enrichment
    can overwrite seed_works_count with the more precise value if available.
    """
    for node, data in G.nodes(data=True):
        if data.get("node_type") != "author":
            continue

        paper_count = 0
        citation_sum = 0
        for _, work_id, edata in G.out_edges(node, data=True):
            if edata.get("edge_type") != "authored":
                continue
            paper_count += 1
            work_data = G.nodes.get(work_id, {})
            cites = work_data.get("cited_by_count", 0)
            if isinstance(cites, (int, float)):
                citation_sum += cites

        data["seed_works_count"] = paper_count
        data["seed_citations"] = citation_sum

#===== AUTHOR PROFILE ENRICHMENT ===========================================

def enrich_author_profiles(G, profiles):
    """
    Add metadata from Phase 4 author profiles to existing author nodes.
    This adds total_works, cited_by_count, disambiguation_flag, and
    top research fields as node attributes.
    """
    for p in profiles:
        aid = p["author_id"]
        if aid not in G.nodes:
            continue
        G.nodes[aid]["seed_works_count"] = p["seed_works_count"]
        G.nodes[aid]["total_works_openalex"] = p["total_works_in_openalex"]
        G.nodes[aid]["author_cited_by"] = p["cited_by_count"]
        G.nodes[aid]["disambiguation_flag"] = p["disambiguation_flag"]
        # Top 3 research fields as a readable string for Gephi tooltips
        top_fields = [f["field_name"] for f in p.get("top_fields", [])[:3]]
        G.nodes[aid]["research_fields"] = "|".join(top_fields)

#===== UTILITY ==============================================================

def _truncate(text, max_len):
    """Truncate text for readable node labels in Gephi."""
    if not text:
        return ""
    return text[:max_len - 3] + "..." if len(text) > max_len else text

#===== MAIN =================================================================

def main():
    print_section("PHASE 5: NETWORK CONSTRUCTION")

    # Load all three data files
    seed_data = load_json("seed_works.json")
    seed_works = seed_data["works"]
    print(f"  Loaded {len(seed_works)} seed works")

    citation_data = load_json("citation_expansion.json")
    expanded_refs = citation_data["expanded_works"]
    print(f"  Loaded {len(expanded_refs)} expanded references")

    author_data = load_json("author_profiles.json")
    author_profiles = author_data["author_profiles"]
    print(f"  Loaded {len(author_profiles)} author profiles")

    # Build the graph
    G = nx.DiGraph()

    # Add seed works and all their edges
    print("\n  Adding seed works...")
    for w in seed_works:
        add_work_node(G, w, is_seed=True)
        add_work_edges(G, w)

    # Add expanded reference works (from Phase 3)
    # These already have authorships, topics, grants from the API fetch
    print("  Adding expanded references...")
    for w in expanded_refs:
        add_work_node(G, w, is_seed=False)
        add_work_edges(G, w)

    # Detect and merge duplicate authors (same name, different OpenAlex IDs)
    print("  Detecting duplicate authors...")
    duplicates = find_duplicate_authors(G)
    print(f"    Found {len(duplicates)} duplicate name groups")
    if duplicates:
        merge_log = merge_duplicate_authors(G, duplicates)
        print_and_save_merge_report(merge_log)

    # Classify each author's primary role (first/middle/last) based on
    # their most frequent position across all papers in the network
    print("  Computing author roles...")
    author_positions = compute_author_roles(G)
    role_counts = Counter(G.nodes[aid].get("primary_role", "?")
                          for aid in author_positions)
    for role, count in role_counts.most_common():
        print(f"    {role}: {count} authors")

    # Compute seed_works_count and seed_citations for ALL authors
    # (available for everyone, not just the Phase 4 top 30)
    print("  Computing author seed metrics...")
    compute_author_seed_metrics(G)

    # Enrich author nodes with profile data from Phase 4
    # (overwrites seed_works_count for top 30 with more precise values)
    print("  Enriching author profiles...")
    enrich_author_profiles(G, author_profiles)

    # Remove stub nodes that have no edges beyond a single citation
    # (these are references that weren't in our top-100 expansion — keeping
    # them would bloat the graph with thousands of unlabeled nodes)
    print("  Pruning stub reference nodes...")
    stubs_before = sum(1 for _, d in G.nodes(data=True)
                       if d.get("stub", False))
    stubs_to_remove = [
        n for n, d in G.nodes(data=True)
        if d.get("stub", False) and G.degree(n) <= 1
    ]
    G.remove_nodes_from(stubs_to_remove)
    stubs_after = sum(1 for _, d in G.nodes(data=True)
                      if d.get("stub", False))
    print(f"    Stubs before: {stubs_before}, removed: {len(stubs_to_remove)}, "
          f"kept (multi-cited): {stubs_after}")

    # Print graph statistics
    print_section("GRAPH STATISTICS")
    print_stat("Total nodes", G.number_of_nodes())
    print_stat("Total edges", G.number_of_edges())

    # Node counts by type
    type_counts = Counter(d.get("node_type", "unknown")
                          for _, d in G.nodes(data=True))
    print_section("Nodes by Type")
    for ntype, count in type_counts.most_common():
        print_stat(f"  {ntype}", count)

    # Edge counts by type
    edge_type_counts = Counter(d.get("edge_type", "unknown")
                               for _, _, d in G.edges(data=True))
    print_section("Edges by Type")
    for etype, count in edge_type_counts.most_common():
        print_stat(f"  {etype}", count)

    # Connected components (on undirected view)
    undirected = G.to_undirected()
    components = list(nx.connected_components(undirected))
    print_section("Connectivity")
    print_stat("Connected components", len(components))
    print_stat("Largest component size", len(max(components, key=len)))
    print_stat("Isolated nodes", sum(1 for c in components if len(c) == 1))

    # Save as pickle for Phase 6
    pickle_path = os.path.join(config.DATA_DIR, "network.gpickle")
    with open(pickle_path, "wb") as f:
        pickle.dump(G, f)
    print(f"\n  Saved graph to {pickle_path}")
    print("  Phase 5 complete.")


if __name__ == "__main__":
    main()
