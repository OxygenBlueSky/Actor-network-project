"""
Phase 6: Export
===============
Exports the network in three formats:
  1. GEXF for Gephi (full heterogeneous network with all attributes)
  2. CSV pair for VOSviewer (co-authorship projection only)
  3. Summary statistics as Markdown

Usage:
    python phase6_export.py

Requires: data/network.gpickle from Phase 5.
"""

import csv
import os
import pickle
from collections import Counter
from itertools import combinations

import networkx as nx

import config
from utils import load_json, print_section, print_stat

#===== GEXF EXPORT ==========================================================

def export_gexf(G, filepath, cap_citations=None):
    """
    Export the full network as GEXF (Graph Exchange XML Format).
    GEXF preserves all node/edge attributes and is natively supported
    by Gephi. Boolean values must be converted to strings for GEXF.

    cap_citations: if set, clamp cited_by_count to this value so the
    color gradient in Gephi isn't stretched by extreme outliers.
    A capped version is stored as 'citations_capped' alongside the raw value.
    """
    G_copy = G.copy()
    for node, data in G_copy.nodes(data=True):
        # Add a capped citation count for usable color gradients
        if cap_citations and "cited_by_count" in data:
            raw = data["cited_by_count"]
            if isinstance(raw, (int, float)):
                data["citations_capped"] = min(raw, cap_citations)

        for key, val in data.items():
            if isinstance(val, bool):
                data[key] = str(val)
            elif val is None:
                data[key] = ""

    for u, v, data in G_copy.edges(data=True):
        for key, val in data.items():
            if isinstance(val, bool):
                data[key] = str(val)
            elif val is None:
                data[key] = ""

    nx.write_gexf(G_copy, filepath)
    print(f"  GEXF saved to {filepath}")

#===== VOSVIEWER CO-AUTHORSHIP EXPORT =======================================

def build_coauthorship_network(G):
    """
    Project the bipartite author–work graph onto authors only.
    Two authors are connected if they co-authored at least one work.
    Edge weight = number of co-authored works.

    Returns a new undirected Graph with only author nodes.
    """
    coauthor_G = nx.Graph()

    # Find all work nodes
    work_nodes = [n for n, d in G.nodes(data=True)
                  if d.get("node_type") == "work"]

    for work_id in work_nodes:
        # Find all authors connected to this work via "authored" edges
        authors_on_paper = []
        for pred in G.predecessors(work_id):
            edge_data = G[pred][work_id]
            if edge_data.get("edge_type") == "authored":
                authors_on_paper.append(pred)

        # Add author nodes to the co-authorship graph
        for aid in authors_on_paper:
            if aid not in coauthor_G.nodes:
                node_data = G.nodes[aid]
                coauthor_G.add_node(
                    aid,
                    label=node_data.get("label", ""),
                    orcid=node_data.get("orcid", ""),
                    seed_works_count=node_data.get("seed_works_count", 0),
                    research_fields=node_data.get("research_fields", ""),
                )

        # Connect every pair of co-authors on this paper
        for a1, a2 in combinations(authors_on_paper, 2):
            if coauthor_G.has_edge(a1, a2):
                coauthor_G[a1][a2]["weight"] += 1
            else:
                coauthor_G.add_edge(a1, a2, weight=1)

    return coauthor_G


def export_vosviewer_csvs(coauthor_G, map_path, net_path):
    """
    Export co-authorship network in VOSviewer's tab-separated format.

    Map file: id, label, weight (total co-authorship links as node weight)
    Network file: id1, id2, weight (number of co-authored papers)

    VOSviewer will compute its own layout; we don't need x/y coordinates.
    """
    # Assign integer IDs (VOSviewer prefers sequential integers)
    node_list = sorted(coauthor_G.nodes())
    id_map = {node: i + 1 for i, node in enumerate(node_list)}

    # Map file — one row per author
    with open(map_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter="\t")
        writer.writerow(["id", "label", "weight"])
        for node in node_list:
            data = coauthor_G.nodes[node]
            label = data.get("label", node)
            # Node weight = number of co-authorship connections
            weight = coauthor_G.degree(node)
            writer.writerow([id_map[node], label, weight])

    # Network file — one row per co-authorship edge
    with open(net_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter="\t")
        writer.writerow(["id1", "id2", "weight"])
        for u, v, data in coauthor_G.edges(data=True):
            writer.writerow([id_map[u], id_map[v], data.get("weight", 1)])

    print(f"  VOSviewer map saved to {map_path}")
    print(f"  VOSviewer network saved to {net_path}")
    print(f"    Authors: {coauthor_G.number_of_nodes()}, "
          f"Co-authorship links: {coauthor_G.number_of_edges()}")

#===== SUMMARY STATISTICS ===================================================

def generate_summary_stats(G, seed_works, filepath):
    """
    Compute and write comprehensive summary statistics as a Markdown file.
    Also prints to stdout so the user can review immediately.
    """
    lines = []

    def add(text=""):
        lines.append(text)
        print(text)

    add("# Homeopathy Actor-Network Pilot — Summary Statistics\n")

    # Overall graph metrics
    add("## Graph Overview\n")
    add(f"- **Total nodes:** {G.number_of_nodes()}")
    add(f"- **Total edges:** {G.number_of_edges()}")

    type_counts = Counter(d.get("node_type", "?")
                          for _, d in G.nodes(data=True))
    add("\n### Nodes by type\n")
    add("| Node type | Count |")
    add("|-----------|-------|")
    for ntype, count in type_counts.most_common():
        add(f"| {ntype} | {count} |")

    edge_type_counts = Counter(d.get("edge_type", "?")
                               for _, _, d in G.edges(data=True))
    add("\n### Edges by type\n")
    add("| Edge type | Count |")
    add("|-----------|-------|")
    for etype, count in edge_type_counts.most_common():
        add(f"| {etype} | {count} |")

    # Top 20 authors by publication count in seed set
    author_pub_counts = Counter()
    author_names = {}
    for w in seed_works:
        for a in w.get("authorships", []):
            aid = a.get("author_id")
            if aid:
                author_pub_counts[aid] += 1
                author_names[aid] = a.get("author_name", "Unknown")

    add("\n## Top 20 Authors by Seed Publications\n")
    add("| Rank | Author | Seed papers |")
    add("|------|--------|-------------|")
    for rank, (aid, count) in enumerate(author_pub_counts.most_common(20), 1):
        add(f"| {rank} | {author_names.get(aid, '?')} | {count} |")

    # Top 20 institutions by publication count
    inst_pub_counts = Counter()
    inst_names = {}
    for w in seed_works:
        for a in w.get("authorships", []):
            for inst in a.get("institutions", []):
                iid = inst.get("institution_id")
                if iid:
                    inst_pub_counts[iid] += 1
                    inst_names[iid] = inst.get("institution_name", "Unknown")

    add("\n## Top 20 Institutions by Publication Count\n")
    add("| Rank | Institution | Papers |")
    add("|------|-------------|--------|")
    for rank, (iid, count) in enumerate(inst_pub_counts.most_common(20), 1):
        add(f"| {rank} | {inst_names.get(iid, '?')} | {count} |")

    # Top 10 countries
    country_counts = Counter()
    for w in seed_works:
        for a in w.get("authorships", []):
            for inst in a.get("institutions", []):
                cc = inst.get("country_code")
                if cc:
                    country_counts[cc] += 1

    add("\n## Top 10 Countries\n")
    add("| Rank | Country | Papers |")
    add("|------|---------|--------|")
    for rank, (cc, count) in enumerate(country_counts.most_common(10), 1):
        add(f"| {rank} | {cc} | {count} |")

    # Top 10 funders
    funder_counts = Counter()
    funder_names = {}
    for w in seed_works:
        for g in w.get("grants", []):
            fid = g.get("funder_id")
            if fid:
                funder_counts[fid] += 1
                funder_names[fid] = g.get("funder_name", "Unknown")

    add("\n## Top 10 Funders\n")
    add("| Rank | Funder | Papers funded |")
    add("|------|--------|---------------|")
    for rank, (fid, count) in enumerate(funder_counts.most_common(10), 1):
        add(f"| {rank} | {funder_names.get(fid, '?')} | {count} |")

    # Top 20 most-cited reference works (the intellectual backbone)
    # Use citation_expansion data for this
    try:
        citation_data = load_json("citation_expansion.json")
        ref_freq = citation_data.get("reference_frequency", {})
        expanded_works = {w["work_id"]: w
                          for w in citation_data.get("expanded_works", [])}
        seed_by_id = {w["work_id"]: w for w in seed_works}

        # Sort by frequency
        top_refs = sorted(ref_freq.items(), key=lambda x: x[1], reverse=True)

        add("\n## Top 20 Most-Cited References (Intellectual Backbone)\n")
        add("| Rank | Citations | Title | Year |")
        add("|------|-----------|-------|------|")
        for rank, (ref_id, count) in enumerate(top_refs[:20], 1):
            # Look up title in expanded works or seed set
            w = expanded_works.get(ref_id) or seed_by_id.get(ref_id) or {}
            title = w.get("title", "???")
            year = w.get("publication_year", "?")
            if title and len(title) > 60:
                title = title[:57] + "..."
            add(f"| {rank} | {count} | {title} | {year} |")
    except FileNotFoundError:
        add("\n*(Citation expansion data not found — skipping backbone table)*\n")

    # Topic / field distribution
    field_counts = Counter()
    domain_counts = Counter()
    for w in seed_works:
        pt = w.get("primary_topic") or {}
        field = pt.get("field_name")
        domain = pt.get("domain_name")
        if field:
            field_counts[field] += 1
        if domain:
            domain_counts[domain] += 1

    add("\n## Topic Distribution (Primary Field)\n")
    add("| Field | Works | % |")
    add("|-------|-------|---|")
    total_with_field = sum(field_counts.values())
    for field, count in field_counts.most_common(15):
        pct = count / total_with_field * 100 if total_with_field else 0
        add(f"| {field} | {count} | {pct:.1f}% |")

    add("\n## Domain Distribution\n")
    add("| Domain | Works | % |")
    add("|--------|-------|---|")
    total_with_domain = sum(domain_counts.values())
    for domain, count in domain_counts.most_common():
        pct = count / total_with_domain * 100 if total_with_domain else 0
        add(f"| {domain} | {count} | {pct:.1f}% |")

    # Query overlap
    query_overlap = Counter()
    for w in seed_works:
        sq = w.get("source_queries", [])
        query_overlap[len(sq)] += 1

    add("\n## Query Overlap\n")
    add("| Queries matched | Works |")
    add("|-----------------|-------|")
    for n_queries in sorted(query_overlap):
        add(f"| {n_queries} | {query_overlap[n_queries]} |")

    # Write to file
    with open(filepath, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"\n  Summary saved to {filepath}")

#===== TRIMMED NETWORK ======================================================

def build_trimmed_network(G, seed_works):
    """
    Build a smaller, more navigable subgraph by keeping only core contributors.

    Strategy:
    1. Keep authors with MIN_AUTHOR_PAPERS+ seed publications
    2. Keep seed works that have at least one retained author
    3. Keep expanded reference works (Phase 3 backbone)
    4. Keep institutions/countries connected to retained authors
    5. Keep only the top MAX_TOPICS_TRIMMED topics by frequency
    6. Keep funders connected to retained works
    7. Re-add only edges between retained nodes

    Returns a new DiGraph and prints size comparison.
    """
    min_papers = config.MIN_AUTHOR_PAPERS
    max_topics = config.MAX_TOPICS_TRIMMED

    # Step 1: Identify core authors (those with enough seed publications)
    author_pub_counts = Counter()
    for w in seed_works:
        for a in w.get("authorships", []):
            aid = a.get("author_id")
            if aid:
                author_pub_counts[aid] += 1

    core_authors = {aid for aid, count in author_pub_counts.items()
                    if count >= min_papers}

    # Step 2: Identify works to keep — seed works with a core author,
    # plus expanded references (the intellectual backbone from Phase 3)
    seed_work_ids = {w["work_id"] for w in seed_works}
    core_work_ids = set()
    for w in seed_works:
        author_ids = {a.get("author_id") for a in w.get("authorships", [])}
        if author_ids & core_authors:
            core_work_ids.add(w["work_id"])

    # Keep expanded reference works (Phase 3 backbone) but NOT stubs.
    # Stubs are reference placeholders with no metadata — they leak through
    # because the stub flag can be lost in pickle/GEXF round-trips.
    # Robust check: real expanded works have a title; stubs don't.
    for node, data in G.nodes(data=True):
        if data.get("node_type") != "work":
            continue
        is_seed = data.get("is_seed")
        # Skip seed works (handled above) and stubs (no title = never fetched)
        if is_seed is True or is_seed == "True":
            continue
        title = data.get("title", "")
        if title and title.strip():
            core_work_ids.add(node)

    # Step 3: Identify top topics by frequency across retained works
    topic_freq = Counter()
    for node in core_work_ids:
        if node not in G.nodes:
            continue
        for _, target, edata in G.out_edges(node, data=True):
            if edata.get("edge_type") == "has_topic":
                topic_freq[target] += 1
    top_topics = {tid for tid, _ in topic_freq.most_common(max_topics)}

    # Step 4: Identify institutions and countries connected to core authors
    core_institutions = set()
    core_countries = set()
    for aid in core_authors:
        if aid not in G.nodes:
            continue
        for _, target, edata in G.out_edges(aid, data=True):
            if edata.get("edge_type") == "affiliated":
                core_institutions.add(target)
        # Also check as undirected (some edges point the other way)
        for source, _, edata in G.in_edges(aid, data=True):
            if edata.get("edge_type") == "affiliated":
                core_institutions.add(source)

    for iid in core_institutions:
        if iid not in G.nodes:
            continue
        for _, target, edata in G.out_edges(iid, data=True):
            if edata.get("edge_type") == "located_in":
                core_countries.add(target)

    # Step 5: Identify funders connected to retained works
    core_funders = set()
    for wid in core_work_ids:
        if wid not in G.nodes:
            continue
        for source, _, edata in G.in_edges(wid, data=True):
            if edata.get("edge_type") == "funded":
                core_funders.add(source)

    # Combine all nodes to keep
    keep_nodes = (core_work_ids | core_authors | core_institutions
                  | top_topics | core_countries | core_funders)

    # Build the trimmed subgraph
    T = G.subgraph(keep_nodes).copy()

    # Print what we kept
    print(f"\n  Trimming criteria:")
    print(f"    Authors with {min_papers}+ seed papers: {len(core_authors)}")
    print(f"    Works with a core author + backbone refs: {len(core_work_ids)}")
    print(f"    Top topics kept: {len(top_topics)}")
    print(f"    Institutions: {len(core_institutions)}")
    print(f"    Countries: {len(core_countries)}")
    print(f"    Funders: {len(core_funders)}")
    print(f"\n  Full network:    {G.number_of_nodes():,} nodes, "
          f"{G.number_of_edges():,} edges")
    print(f"  Trimmed network: {T.number_of_nodes():,} nodes, "
          f"{T.number_of_edges():,} edges")

    # Node type breakdown
    type_counts = Counter(d.get("node_type", "?")
                          for _, d in T.nodes(data=True))
    for ntype, count in type_counts.most_common():
        print(f"    {ntype}: {count}")

    return T

#===== MAIN =================================================================

def main():
    print_section("PHASE 6: EXPORT")

    # Load the network
    pickle_path = os.path.join(config.DATA_DIR, "network.gpickle")
    with open(pickle_path, "rb") as f:
        G = pickle.load(f)
    print(f"  Loaded graph: {G.number_of_nodes()} nodes, "
          f"{G.number_of_edges()} edges")

    # Load seed works for summary stats
    seed_data = load_json("seed_works.json")
    seed_works = seed_data["works"]

    # 1. Export full GEXF for Gephi (archive copy with everything)
    print_section("EXPORTING FULL GEXF")
    gexf_path = os.path.join(config.DATA_DIR, "homeopathy_network_full.gexf")
    export_gexf(G, gexf_path)

    # 2. Build and export trimmed network (the one you'll actually explore)
    # Cap citations at 100 so the color gradient isn't dominated by outliers.
    # Raw cited_by_count is preserved; use 'citations_capped' for coloring.
    print_section("BUILDING TRIMMED NETWORK")
    T = build_trimmed_network(G, seed_works)
    trimmed_gexf = os.path.join(config.DATA_DIR, "homeopathy_network_trimmed.gexf")
    export_gexf(T, trimmed_gexf, cap_citations=100)

    # 3. Build co-authorship projection from trimmed network for VOSviewer
    print_section("EXPORTING VOSVIEWER CO-AUTHORSHIP (trimmed)")
    coauthor_G = build_coauthorship_network(T)
    map_path = os.path.join(config.DATA_DIR, "coauthor_map.csv")
    net_path = os.path.join(config.DATA_DIR, "coauthor_net.csv")
    export_vosviewer_csvs(coauthor_G, map_path, net_path)

    # 4. Generate summary statistics
    print_section("SUMMARY STATISTICS")
    stats_path = os.path.join(config.DATA_DIR, "summary_stats.md")
    generate_summary_stats(G, seed_works, stats_path)

    print(f"\n  Phase 6 complete. Output files in {config.DATA_DIR}/")
    print("  - homeopathy_network_full.gexf     → Archive (22K nodes)")
    print("  - homeopathy_network_trimmed.gexf  → Explore in Gephi")
    print("  - coauthor_map.csv                 → VOSviewer map file")
    print("  - coauthor_net.csv                 → VOSviewer network file")
    print("  - summary_stats.md                 → Summary statistics")
    print(f"\n  If trimmed is still too large, raise MIN_AUTHOR_PAPERS "
          f"in config.py (currently {config.MIN_AUTHOR_PAPERS})")


if __name__ == "__main__":
    main()
