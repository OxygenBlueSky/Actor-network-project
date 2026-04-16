"""
Build Web Graph
===============
Reads the co-authorship GEXF, computes a force-directed layout, maps
node colors and sizes to match the Gephi Lite settings, and exports
a JSON file for the Sigma.js web viewer.

Usage:
    python build_web_graph.py

Outputs:
    docs/graph_data.json   — pre-laid-out graph for the web viewer
"""

import json
import os
import math
import random
from collections import defaultdict

import networkx as nx

import config

#===== COLOR SCALE ==========================================================

def citation_to_color(value, min_val, max_val):
    """
    Map seed_citations_capped to an orange gradient matching Gephi settings.
    Low citations = light orange/yellow, high citations = deep orange/red.
    """
    if max_val == min_val:
        t = 0.5
    else:
        t = (value - min_val) / (max_val - min_val)
    # Gradient: light yellow (255,235,180) → orange (240,140,30) → deep red-orange (200,50,20)
    if t < 0.5:
        # Light yellow to orange
        s = t * 2
        r = int(255 - 15 * s)
        g = int(235 - 95 * s)
        b = int(180 - 150 * s)
    else:
        # Orange to deep red-orange
        s = (t - 0.5) * 2
        r = int(240 - 40 * s)
        g = int(140 - 90 * s)
        b = int(30 - 10 * s)
    return f"rgb({r},{g},{b})"


def size_interpolate(value, min_val, max_val, size_min=3, size_max=25):
    """
    Map seed_works_count to node size, interpolated between size_min and size_max.
    Uses square root scaling so high-count nodes don't dominate too much.
    """
    if max_val == min_val:
        return (size_min + size_max) / 2
    t = (value - min_val) / (max_val - min_val)
    t_sqrt = math.sqrt(t)  # square root scaling for visual balance
    return size_min + t_sqrt * (size_max - size_min)

#===== FORCEATLAS2 LAYOUT (pure Python) =====================================

def forceatlas2_layout(G, iterations=300, gravity=0.05, scaling=1.0,
                       strong_gravity=True, linlog=True):
    """
    Pure-Python ForceAtlas2 layout. Applies three forces per iteration:
      1. Repulsion between all node pairs (coulomb-like, scaled by node mass)
      2. Attraction along edges (linLog uses log distance for tighter clusters)
      3. Gravity toward center (prevents disconnected components from drifting)

    Returns a dict of {node_id: (x, y)}.
    """
    nodes = list(G.nodes())
    n = len(nodes)
    node_idx = {node: i for i, node in enumerate(nodes)}

    # Node mass = degree + 1 (more connections = harder to move)
    degrees = [G.degree(node) + 1 for node in nodes]

    # Initialize with small random positions (tight start helps convergence)
    random.seed(42)
    x = [random.uniform(-10, 10) for _ in range(n)]
    y = [random.uniform(-10, 10) for _ in range(n)]

    # Edge list as index pairs
    edges = [(node_idx[u], node_idx[v]) for u, v in G.edges()]

    # Step size decays over iterations for convergence
    speed = 1.0

    for iteration in range(iterations):
        dx = [0.0] * n
        dy = [0.0] * n

        # 1. Repulsion: all pairs push apart
        # Force = scaling * mass_i * mass_j / distance^2
        for i in range(n):
            for j in range(i + 1, n):
                vx = x[i] - x[j]
                vy = y[i] - y[j]
                dist_sq = vx * vx + vy * vy + 0.001
                dist = math.sqrt(dist_sq)

                # Repulsion inversely proportional to distance
                force = scaling * degrees[i] * degrees[j] / dist_sq
                fx = force * vx / dist
                fy = force * vy / dist
                dx[i] += fx
                dy[i] += fy
                dx[j] -= fx
                dy[j] -= fy

        # 2. Attraction: edges pull nodes together
        for i, j in edges:
            vx = x[j] - x[i]
            vy = y[j] - y[i]
            dist = math.sqrt(vx * vx + vy * vy) + 0.001

            if linlog:
                # Log attraction creates tighter, more separated clusters
                force = math.log(1 + dist)
            else:
                force = dist

            fx = force * vx / dist
            fy = force * vy / dist
            dx[i] += fx
            dy[i] += fy
            dx[j] -= fx
            dy[j] -= fy

        # 3. Gravity: pull toward origin
        for i in range(n):
            dist = math.sqrt(x[i] * x[i] + y[i] * y[i]) + 0.001
            if strong_gravity:
                g_force = gravity * degrees[i]
            else:
                g_force = gravity * degrees[i] / dist
            dx[i] -= g_force * x[i] / dist
            dy[i] -= g_force * y[i] / dist

        # Apply with speed control — cap max movement per node per step
        total_disp = 0
        max_disp = max(5.0, 50.0 / (1 + iteration * 0.1))  # shrinks over time
        for i in range(n):
            disp = math.sqrt(dx[i] * dx[i] + dy[i] * dy[i]) + 0.001
            capped = min(disp, max_disp)
            x[i] += dx[i] / disp * capped * speed
            y[i] += dy[i] / disp * capped * speed
            total_disp += capped

        # Slow down gradually
        speed = max(0.1, 1.0 / (1 + iteration * 0.01))

        if (iteration + 1) % 50 == 0:
            avg_disp = total_disp / n
            print(f"    Iteration {iteration + 1}/{iterations}, "
                  f"avg displacement: {avg_disp:.2f}, speed: {speed:.3f}")

    return {nodes[i]: (x[i], y[i]) for i in range(n)}

#===== MAIN =================================================================

def main():
    # Load the co-authorship network
    gexf_path = os.path.join(config.DATA_DIR, "coauthorship_network.gexf")
    print(f"  Loading {gexf_path}...")
    G = nx.read_gexf(gexf_path)
    print(f"  Loaded: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")

    # Compute ForceAtlas2-like layout in pure Python (no scipy dependency).
    # Uses attraction along edges + repulsion between all nodes + gravity.
    print("  Computing layout (this may take 30-60 seconds)...")
    pos = forceatlas2_layout(G, iterations=400, gravity=0.05, scaling=1.0,
                             strong_gravity=True, linlog=True)

    # Gather value ranges for color/size mapping
    citations = []
    work_counts = []
    for node, data in G.nodes(data=True):
        sc = data.get("seed_citations_capped", data.get("seed_citations", 0))
        try:
            sc = float(sc)
        except (TypeError, ValueError):
            sc = 0
        citations.append(sc)

        wc = data.get("seed_works_count", 0)
        try:
            wc = float(wc)
        except (TypeError, ValueError):
            wc = 0
        work_counts.append(wc)

    cite_min, cite_max = min(citations), max(citations)
    wc_min, wc_max = min(work_counts), max(work_counts)
    print(f"  seed_citations_capped range: {cite_min} – {cite_max}")
    print(f"  seed_works_count range: {wc_min} – {wc_max}")

    # Build the JSON structure for Sigma.js
    nodes = []
    for node, data in G.nodes(data=True):
        x, y = pos[node]

        # Color by seed_citations_capped
        sc = data.get("seed_citations_capped", data.get("seed_citations", 0))
        try:
            sc = float(sc)
        except (TypeError, ValueError):
            sc = 0
        color = citation_to_color(sc, cite_min, cite_max)

        # Size by seed_works_count
        wc = data.get("seed_works_count", 0)
        try:
            wc = float(wc)
        except (TypeError, ValueError):
            wc = 0
        size = size_interpolate(wc, wc_min, wc_max)

        # Build tooltip content from available attributes
        label = data.get("label", node)
        role = data.get("primary_role", "")
        role_detail = data.get("role_detail", "")
        fields = data.get("research_fields", "").replace("|", ", ")
        orcid = data.get("orcid", "")
        seed_wc = data.get("seed_works_count", "?")
        seed_cit = data.get("seed_citations", "?")

        nodes.append({
            "key": node,
            "attributes": {
                "label": label,
                "x": round(x, 2),
                "y": round(y, 2),
                "size": round(size, 2),
                "color": color,
                # Tooltip fields
                "primary_role": role,
                "role_detail": role_detail,
                "research_fields": fields,
                "orcid": orcid,
                "seed_works_count": seed_wc,
                "seed_citations": seed_cit,
            }
        })

    edges = []
    for i, (source, target, data) in enumerate(G.edges(data=True)):
        weight = data.get("weight", 1)
        try:
            weight = float(weight)
        except (TypeError, ValueError):
            weight = 1
        edges.append({
            "key": f"e{i}",
            "source": source,
            "target": target,
            "attributes": {
                "weight": weight,
            }
        })

    graph_data = {
        "nodes": nodes,
        "edges": edges,
    }

    # Write to docs/
    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "docs", "graph_data.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(graph_data, f)
    print(f"  Saved {len(nodes)} nodes and {len(edges)} edges to {out_path}")
    file_size_mb = os.path.getsize(out_path) / (1024 * 1024)
    print(f"  File size: {file_size_mb:.1f} MB")


if __name__ == "__main__":
    main()
