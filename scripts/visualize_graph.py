"""
Generate a Graphviz visualization of the rule dependency graph.
Produces:
  - rule_graph.dot   (DOT source — readable/editable)
  - rule_graph.png   (rendered PNG, opened automatically)

Run:
    python visualize_graph.py

Requires: pip install graphviz  AND  brew install graphviz
"""
import os
from pathlib import Path

# Locate the 'dot' binary — check common macOS/Linux paths
DOT_PATHS = [
    "/opt/homebrew/bin/dot",
    "/opt/homebrew/var/homebrew/tmp/.cellar/graphviz/14.1.4/bin/dot",
    "/usr/local/bin/dot",
    "/usr/bin/dot",
]
dot_dir = None
for dp in DOT_PATHS:
    if os.path.exists(dp):
        dot_dir = os.path.dirname(dp)
        break

if dot_dir:
    os.environ["PATH"] = dot_dir + ":" + os.environ.get("PATH", "")
    print(f"Using dot binary from: {dot_dir}")
else:
    print("WARNING: 'dot' binary not found in standard paths. Install with: brew install graphviz")

import graphviz
import networkx as nx

from autograph_rule_engine.core.parser import RuleParser
from autograph_rule_engine.core.graph_builder import GraphBuilder
from autograph_rule_engine.backends.local_backend import LocalGraphBackend

# ------------------------------------------------------------------ #
# 1. Load rules & build graph
# ------------------------------------------------------------------ #
RULES_DIR = Path(__file__).parent.parent / "rules" / "loan_pricing"
parser = RuleParser()
rules = parser.load_directory(RULES_DIR)
builder = GraphBuilder()
graph = builder.build(rules)

backend = LocalGraphBackend()
backend.load_from_rule_graph(graph)
levels = list(nx.topological_generations(backend.graph))

root_rules = set(graph.get_root_rules())
leaf_rules = set(graph.get_leaf_rules())

# ------------------------------------------------------------------ #
# 2. Build Graphviz DOT diagram
# ------------------------------------------------------------------ #
dot = graphviz.Digraph(
    name="RuleEngine",
    comment="Loan Pricing Rule Dependency Graph",
    format="png",
    engine="dot",
)

# Global graph attributes — large canvas, big fonts, curved arrows
dot.attr(
    rankdir="LR",
    bgcolor="white",
    fontname="Helvetica",
    splines="curved",
    nodesep="1.2",
    ranksep="3.0",
    pad="1.0",
    dpi="150",
    size="30,20!",   # force 30x20 inch canvas
    concentrate="false",
)
dot.attr("node",
    fontname="Helvetica",
    fontsize="22",
    style="filled,rounded",
    shape="box",
    margin="0.5,0.3",
    penwidth="3",
    width="4",
    height="1.6",
)
dot.attr("edge",
    fontname="Helvetica",
    fontsize="18",
    color="#555555",
    fontcolor="#222222",
    arrowsize="1.4",
    penwidth="2.5",
    arrowhead="vee",
)

# Action type icons (unicode)
ACTION_ICON = {
    "compute":   "COMPUTE",
    "lookup":    "LOOKUP",
    "aggregate": "AGGREGATE",
    "decision":  "DECISION",
}

# Color palettes: (fillcolor, color/border, fontcolor)
COLORS = {
    "root":   ("#27ae60", "#1e8449", "#ffffff"),   # green
    "leaf":   ("#e74c3c", "#922b21", "#ffffff"),   # red
    "middle": ("#2980b9", "#1a5276", "#ffffff"),   # blue
}

# ------------------------------------------------------------------ #
# 3a. Add PAYLOAD INPUT nodes (leftmost column)
# ------------------------------------------------------------------ #
with dot.subgraph() as sg:
    sg.attr(rank="same")
    for field in sorted(graph.payload_fields):
        sg.node(
            f"PAYLOAD_{field}",
            label=f"📥 {field}",
            fillcolor="#f39c12",
            color="#c0392b",
            fontcolor="#ffffff",
            fontsize="20",
            style="filled,rounded",
            shape="parallelogram",
            margin="0.3,0.2",
            width="3.5",
            height="0.9",
        )

# Draw edges from payload fields → root rules that consume them
for rule_id in root_rules:
    rule = graph.rules[rule_id]
    for inp in rule.inputs:
        if inp in graph.payload_fields:
            dot.edge(
                f"PAYLOAD_{inp}",
                rule_id,
                xlabel=inp,
                color="#e67e22",
                fontcolor="#e67e22",
                penwidth="2.0",
                arrowsize="1.2",
                style="dashed",
            )

# ------------------------------------------------------------------ #
# 3b. Add rule nodes grouped by execution level (same rank = same column)
# ------------------------------------------------------------------ #
for level_idx, level in enumerate(levels):
    with dot.subgraph() as sg:
        sg.attr(rank="same")
        for rule_id in sorted(level):
            rule = graph.rules[rule_id]
            atype = str(rule.action.type)
            icon = ACTION_ICON.get(atype, atype.upper())

            if rule_id in root_rules:
                fillcolor, bordercolor, fontcolor = COLORS["root"]
                role = "ROOT"
            elif rule_id in leaf_rules:
                fillcolor, bordercolor, fontcolor = COLORS["leaf"]
                role = "OUTPUT"
            else:
                fillcolor, bordercolor, fontcolor = COLORS["middle"]
                role = ""

            # Build node label — simplified for readability
            outputs_str = ", ".join(rule.outputs)
            role_badge  = f"[{role}] " if role else ""
            label = (
                f"{role_badge}{rule_id}\n"
                f"{rule.name}\n"
                f"[{icon}]\n"
                f"→ {outputs_str}"
            )

            sg.node(
                rule_id,
                label=label,
                fillcolor=fillcolor,
                color=bordercolor,
                fontcolor=fontcolor,
            )

# ------------------------------------------------------------------ #
# 4. Add edges between rules with field name labels
#    xlabel works with curved splines
# ------------------------------------------------------------------ #
for from_id, to_ids in graph.edges.items():
    for to_id in to_ids:
        shared = set(graph.rules[from_id].outputs) & set(graph.rules[to_id].inputs)
        field_label = "\n".join(sorted(shared))
        dot.edge(from_id, to_id, xlabel=field_label)

# ------------------------------------------------------------------ #
# 5. Add a legend subgraph
# ------------------------------------------------------------------ #
with dot.subgraph(name="cluster_legend") as legend:
    legend.attr(
        label="Legend",
        style="filled,rounded",
        fillcolor="#f5f5f5",
        color="#aaaaaa",
        fontcolor="#333333",
        fontsize="20",
        fontname="Helvetica",
    )
    legend.node("L_root",   "ROOT\n(payload-fed)",   fillcolor="#27ae60", color="#1e8449", fontcolor="#ffffff", style="filled,rounded", shape="box", fontsize="18", width="2.5")
    legend.node("L_middle", "INTERMEDIATE\nrule",    fillcolor="#2980b9", color="#1a5276", fontcolor="#ffffff", style="filled,rounded", shape="box", fontsize="18", width="2.5")
    legend.node("L_leaf",   "OUTPUT\n(terminal)",    fillcolor="#e74c3c", color="#922b21", fontcolor="#ffffff", style="filled,rounded", shape="box", fontsize="18", width="2.5")
    legend.edge("L_root",   "L_middle", style="invis")
    legend.edge("L_middle", "L_leaf",   style="invis")

# ------------------------------------------------------------------ #
# 6. Render
# ------------------------------------------------------------------ #
OUTPUT_DIR  = Path(__file__).parent.parent
OUTPUT_STEM = str(OUTPUT_DIR / "rule_graph")

# Save DOT source
dot_source = dot.source
dot_path = OUTPUT_STEM + ".dot"
with open(dot_path, "w") as f:
    f.write(dot_source)
print(f"\nDOT source saved: {dot_path}")

# Render PNG
try:
    rendered = dot.render(OUTPUT_STEM, cleanup=False, view=False)
    print(f"PNG rendered  : {rendered}")

    # Open the image
    import subprocess
    subprocess.run(["open", rendered], check=False)
    print("Graph opened in Preview.")

except graphviz.backend.execute.ExecutableNotFound:
    print("\nGraphviz 'dot' binary not found.")
    print("The DOT source has been saved to rule_graph.dot")
    print("To render manually after installing graphviz:")
    print("  brew install graphviz")
    print(f"  dot -Tpng {dot_path} -o {OUTPUT_STEM}.png")
    print(f"  open {OUTPUT_STEM}.png")

except Exception as e:
    print(f"\nRender error: {e}")
    print(f"DOT source saved at: {dot_path}")
    print("You can render it at https://dreampuf.github.io/GraphvizOnline by pasting the .dot file contents.")