from flask import Flask, jsonify, request
from flask_cors import CORS
from kafka import KafkaConsumer
import json
import threading
import networkx as nx

app = Flask(__name__)
CORS(app)

# ── In-memory traffic store ──────────────────────────────
latest_traffic = {}

# ── Road network graph (Hyderabad area connections) ─────
# Each edge = road between two locations
# weight = base distance in km
G = nx.Graph()

ROAD_NETWORK = [
    ("Madhapur",     "Kondapur",      3),
    ("Madhapur",     "Hitech City",   2),
    ("Madhapur",     "Gachibowli",    4),
    ("Kondapur",     "Miyapur",       4),
    ("Kondapur",     "Hitech City",   2),
    ("Hitech City",  "Gachibowli",    3),
    ("Gachibowli",   "Banjara Hills", 6),
    ("Banjara Hills","Jubilee Hills", 3),
    ("Banjara Hills","Begumpet",      5),
    ("Jubilee Hills","Begumpet",      4),
    ("Jubilee Hills","Ameerpet",      5),
    ("Begumpet",     "Ameerpet",      2),
    ("Begumpet",     "Secunderabad",  4),
    ("Ameerpet",     "Kukatpally",    6),
    ("Ameerpet",     "Hitech City",   7),
    ("Kukatpally",   "Miyapur",       3),
    ("Secunderabad", "Uppal",         8),
    ("Secunderabad", "LB Nagar",     10),
    ("Uppal",        "LB Nagar",      6),
    ("Uppal",        "Dilsukhnagar",  4),
    ("LB Nagar",     "Dilsukhnagar",  3),
    ("Dilsukhnagar", "Charminar",     5),
    ("Charminar",    "Banjara Hills", 7),
    ("Charminar",    "LB Nagar",      6),
    ("Miyapur",      "Madhapur",      6),
]

# Build base graph
for src, dst, dist in ROAD_NETWORK:
    G.add_edge(src, dst, base_km=dist, weight=dist)


# ── Traffic weight multiplier ────────────────────────────
def get_traffic_multiplier(location):
    """Heavy traffic = higher cost = avoided in pathfinding."""
    info = latest_traffic.get(location, {})
    status = info.get("status", "SMOOTH")
    if status == "HEAVY":
        return 4.0   # heavily penalized
    elif status == "MODERATE":
        return 2.0
    return 1.0       # SMOOTH = normal cost


def get_travel_time(km, location):
    """Estimate travel time in minutes based on traffic."""
    info = latest_traffic.get(location, {})
    status = info.get("status", "SMOOTH")
    speeds = {"HEAVY": 10, "MODERATE": 30, "SMOOTH": 60}  # km/h
    speed = speeds.get(status, 60)
    return round((km / speed) * 60, 1)


def update_graph_weights():
    """Recalculate edge weights based on current traffic data."""
    for src, dst, data in G.edges(data=True):
        base = data["base_km"]
        # Use average multiplier of both endpoints
        mult = (get_traffic_multiplier(src) + get_traffic_multiplier(dst)) / 2
        G[src][dst]["weight"] = base * mult


# ── Kafka consumer thread ────────────────────────────────
def kafka_consumer_thread():
    consumer = KafkaConsumer(
        "traffic",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        consumer_timeout_ms=-1
    )
    print("✅ Kafka consumer started")
    for message in consumer:
        record = message.value
        loc = record.get("location")
        if loc:
            latest_traffic[loc] = {
                "location": loc,
                "status":   record.get("traffic_status"),
                "count":    record.get("count"),
                "speed":    record.get("speed")
            }


# ── API Endpoints ────────────────────────────────────────

@app.route("/traffic", methods=["GET"])
def get_traffic():
    return jsonify(list(latest_traffic.values()))


@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "locations": len(latest_traffic),
        "data": list(latest_traffic.keys())
    })


@app.route("/route", methods=["GET"])
def get_route():
    """
    Find best route from source to destination.
    Usage: /route?from=Madhapur&to=Charminar
    """
    src = request.args.get("from")
    dst = request.args.get("to")

    if not src or not dst:
        return jsonify({"error": "Provide ?from=X&to=Y"}), 400

    if src not in G.nodes or dst not in G.nodes:
        return jsonify({"error": f"Unknown location: {src} or {dst}"}), 404

    # Update weights from current live traffic
    update_graph_weights()

    try:
        # Dijkstra's shortest path (traffic-weighted)
        path = nx.dijkstra_path(G, src, dst, weight="weight")

        # Build step-by-step breakdown
        steps = []
        total_time = 0
        total_km = 0

        for i in range(len(path) - 1):
            a, b = path[i], path[i + 1]
            km = G[a][b]["base_km"]
            t  = get_travel_time(km, b)
            status = latest_traffic.get(b, {}).get("status", "UNKNOWN")

            total_km   += km
            total_time += t
            steps.append({
                "from":   a,
                "to":     b,
                "km":     km,
                "time_min": t,
                "status": status
            })

        return jsonify({
            "source":          src,
            "destination":     dst,
            "path":            path,
            "total_km":        round(total_km, 1),
            "total_time_min":  round(total_time, 1),
            "steps":           steps
        })

    except nx.NetworkXNoPath:
        return jsonify({"error": "No path found between locations"}), 404


@app.route("/graph", methods=["GET"])
def get_graph():
    """
    Returns full graph data for D3.js visualization.
    Nodes = locations, edges = roads with traffic weight.
    """
    update_graph_weights()

    nodes = []
    for node in G.nodes:
        info = latest_traffic.get(node, {})
        nodes.append({
            "id":     node,
            "status": info.get("status", "UNKNOWN"),
            "count":  info.get("count", 0),
            "speed":  info.get("speed", 0)
        })

    edges = []
    for src, dst, data in G.edges(data=True):
        edges.append({
            "source":   src,
            "target":   dst,
            "km":       data["base_km"],
            "weight":   round(data["weight"], 2)
        })

    # Congestion score using betweenness centrality
    centrality = nx.betweenness_centrality(G, weight="weight")

    return jsonify({
        "nodes":       nodes,
        "edges":       edges,
        "centrality":  {k: round(v, 4) for k, v in centrality.items()}
    })


if __name__ == "__main__":
    t = threading.Thread(target=kafka_consumer_thread, daemon=True)
    t.start()
    app.run(debug=False, port=5000)