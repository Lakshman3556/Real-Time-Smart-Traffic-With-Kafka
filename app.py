"""
app.py — Smart Traffic Dashboard Backend
Endpoints:
  GET  /traffic          Live traffic per location
  GET  /health           Health check
  GET  /graph            Road graph + centrality
  GET  /route            Dijkstra route (single best)
  GET  /multiroute       Top-3 alternative routes
  GET  /history          Rolling 20-min history per location
  GET  /predict          Time-of-day traffic prediction
  GET  /stats            Aggregate city statistics
  GET  /export           Download traffic as CSV
  GET  /incidents        List active incidents
  POST /incidents        Report new incident
  DELETE /incidents/<id> Resolve incident
  GET  /spark            Spark window data
"""

from flask import Flask, jsonify, request, Response
from flask_cors import CORS
from kafka import KafkaConsumer
import json, threading, os, glob, time, uuid, csv, io
from datetime import datetime, timedelta
from collections import deque
import networkx as nx

app  = Flask(__name__)
CORS(app)

# ─────────────────────────────────────────────────────────
# SHARED STATE
# ─────────────────────────────────────────────────────────
latest_traffic = {}                        # loc → latest record
history        = {loc: deque(maxlen=120)   # loc → last 120 records (~4 min at 2s)
                  for loc in [
    "Madhapur","Kondapur","Hitech City","Gachibowli","Banjara Hills",
    "Jubilee Hills","Begumpet","Ameerpet","Secunderabad","Kukatpally",
    "Miyapur","LB Nagar","Dilsukhnagar","Uppal","Charminar"]}
incidents      = {}                        # id → incident dict
spark_windows  = []                        # recent Spark records

# ─────────────────────────────────────────────────────────
# ROAD NETWORK GRAPH
# ─────────────────────────────────────────────────────────
G = nx.Graph()
ROAD_NETWORK = [
    ("Madhapur","Kondapur",3),    ("Madhapur","Hitech City",2),
    ("Madhapur","Gachibowli",4),  ("Madhapur","Miyapur",6),
    ("Kondapur","Miyapur",4),     ("Kondapur","Hitech City",2),
    ("Hitech City","Gachibowli",3),("Hitech City","Ameerpet",7),
    ("Gachibowli","Banjara Hills",6),
    ("Miyapur","Kukatpally",3),
    ("Banjara Hills","Jubilee Hills",3),("Banjara Hills","Begumpet",5),
    ("Banjara Hills","Charminar",7),
    ("Jubilee Hills","Begumpet",4),("Jubilee Hills","Ameerpet",5),
    ("Begumpet","Ameerpet",2),    ("Begumpet","Secunderabad",4),
    ("Ameerpet","Kukatpally",6),
    ("Secunderabad","Uppal",8),   ("Secunderabad","LB Nagar",10),
    ("Uppal","LB Nagar",6),       ("Uppal","Dilsukhnagar",4),
    ("LB Nagar","Dilsukhnagar",3),("LB Nagar","Charminar",6),
    ("Dilsukhnagar","Charminar",5),
]
for s, d, w in ROAD_NETWORK:
    G.add_edge(s, d, base_km=w, weight=w)

def traffic_mult(loc):
    s = latest_traffic.get(loc, {}).get("status", "SMOOTH")
    return {"HEAVY": 4.0, "MODERATE": 2.0}.get(s, 1.0)

def travel_time_min(km, loc):
    s     = latest_traffic.get(loc, {}).get("status", "SMOOTH")
    speed = {"HEAVY": 10, "MODERATE": 30, "SMOOTH": 60}.get(s, 60)
    return round((km / speed) * 60, 1)

def update_weights():
    for s, d, data in G.edges(data=True):
        m = (traffic_mult(s) + traffic_mult(d)) / 2
        G[s][d]["weight"] = data["base_km"] * m

# ─────────────────────────────────────────────────────────
# BACKGROUND THREADS
# ─────────────────────────────────────────────────────────
def kafka_thread():
    consumer = KafkaConsumer(
        "traffic",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        consumer_timeout_ms=-1
    )
    print("✅ Kafka consumer started")
    for msg in consumer:
        r   = msg.value
        loc = r.get("location")
        if not loc:
            continue
        rec = {
            "location":  loc,
            "status":    r.get("traffic_status"),
            "count":     r.get("count"),
            "speed":     r.get("speed"),
            "ts":        r.get("timestamp"),
            "vehicle_id":r.get("vehicle_id")
        }
        latest_traffic[loc] = rec
        if loc in history:
            history[loc].append({**rec, "recorded_at": datetime.now().isoformat()})

def spark_reader_thread():
    seen = set()
    while True:
        try:
            for fpath in sorted(glob.glob("output/*.json"))[-20:]:
                if fpath in seen:
                    continue
                seen.add(fpath)
                with open(fpath) as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            try:
                                spark_windows.append(json.loads(line))
                            except:
                                pass
            if len(spark_windows) > 200:
                del spark_windows[:len(spark_windows)-200]
        except:
            pass
        time.sleep(5)

# ─────────────────────────────────────────────────────────
# ENDPOINTS
# ─────────────────────────────────────────────────────────
@app.route("/traffic")
def get_traffic():
    return jsonify(list(latest_traffic.values()))

@app.route("/health")
def health():
    return jsonify({"locations": len(latest_traffic), "data": list(latest_traffic.keys())})

# ── GRAPH ─────────────────────────────────────────────────
@app.route("/graph")
def get_graph():
    update_weights()
    nodes = []
    for n in G.nodes:
        info = latest_traffic.get(n, {})
        nodes.append({"id":n,"status":info.get("status","UNKNOWN"),
                       "count":info.get("count",0),"speed":info.get("speed",0)})
    edges = [{"source":s,"target":d,"km":data["base_km"],"weight":round(data["weight"],2)}
             for s,d,data in G.edges(data=True)]
    centrality = nx.betweenness_centrality(G, weight="weight")
    return jsonify({"nodes":nodes,"edges":edges,
                    "centrality":{k:round(v,4) for k,v in centrality.items()}})

# ── SINGLE ROUTE ──────────────────────────────────────────
@app.route("/route")
def get_route():
    src = request.args.get("from")
    dst = request.args.get("to")
    if not src or not dst:
        return jsonify({"error":"Provide ?from=X&to=Y"}), 400
    if src not in G or dst not in G:
        return jsonify({"error":"Unknown location"}), 404
    update_weights()
    try:
        path  = nx.dijkstra_path(G, src, dst, weight="weight")
        steps, total_km, total_t = [], 0, 0
        for i in range(len(path)-1):
            a, b = path[i], path[i+1]
            km   = G[a][b]["base_km"]
            t    = travel_time_min(km, b)
            total_km += km; total_t += t
            steps.append({"from":a,"to":b,"km":km,"time_min":t,
                          "status":latest_traffic.get(b,{}).get("status","UNKNOWN")})
        return jsonify({"source":src,"destination":dst,"path":path,
                        "total_km":round(total_km,1),"total_time_min":round(total_t,1),"steps":steps})
    except nx.NetworkXNoPath:
        return jsonify({"error":"No path found"}), 404

# ── MULTI-ROUTE (top 3 alternative paths) ─────────────────
@app.route("/multiroute")
def get_multiroute():
    src = request.args.get("from")
    dst = request.args.get("to")
    if not src or not dst:
        return jsonify({"error":"Provide ?from=X&to=Y"}), 400
    update_weights()
    routes = []
    try:
        # Generate top-3 simple paths, score each
        all_paths = list(nx.shortest_simple_paths(G, src, dst, weight="weight"))[:6]
        for path in all_paths:
            steps, total_km, total_t, heavy_count = [], 0, 0, 0
            for i in range(len(path)-1):
                a, b = path[i], path[i+1]
                km   = G[a][b]["base_km"]
                t    = travel_time_min(km, b)
                st   = latest_traffic.get(b,{}).get("status","UNKNOWN")
                total_km += km; total_t += t
                if st == "HEAVY": heavy_count += 1
                steps.append({"from":a,"to":b,"km":km,"time_min":t,"status":st})

            # Score: lower is better; fewer heavy segments = preferred
            score = total_t + heavy_count * 15
            routes.append({
                "path": path, "steps": steps,
                "total_km": round(total_km,1),
                "total_time_min": round(total_t,1),
                "heavy_zones": heavy_count,
                "score": round(score,1),
                "label": ""
            })

        # Sort by score and keep top 3
        routes.sort(key=lambda r: r["score"])
        routes = routes[:3]
        labels = ["🟢 Recommended","🟡 Alternative","🔴 Slowest"]
        for i, r in enumerate(routes):
            r["label"] = labels[i]

        return jsonify({"source":src,"destination":dst,"routes":routes})
    except Exception as e:
        return jsonify({"error":str(e)}), 404

# ── HISTORY ───────────────────────────────────────────────
@app.route("/history")
def get_history():
    loc = request.args.get("location")
    if loc:
        return jsonify(list(history.get(loc, [])))
    # Return last 10 records per location
    result = {}
    for l, dq in history.items():
        result[l] = list(dq)[-10:]
    return jsonify(result)

# ── PREDICT ───────────────────────────────────────────────
@app.route("/predict")
def predict():
    """
    Simple time-of-day model + recent trend.
    Returns predicted status for next 30 min for each location.
    """
    hour = datetime.now().hour
    future_hour = (hour + 1) % 24

    def predict_status(loc):
        # Rush hour model
        if 8 <= future_hour <= 11 or 17 <= future_hour <= 21:
            base = "HEAVY"
        elif 6 <= future_hour <= 7 or 12 <= future_hour <= 13:
            base = "MODERATE"
        elif 0 <= future_hour <= 5 or 22 <= future_hour <= 23:
            base = "SMOOTH"
        else:
            base = "MODERATE"

        # Weight recent trend from history
        recent = list(history.get(loc, []))[-10:]
        if recent:
            statuses   = [r["status"] for r in recent]
            heavy_pct  = statuses.count("HEAVY") / len(statuses)
            mod_pct    = statuses.count("MODERATE") / len(statuses)
            smooth_pct = statuses.count("SMOOTH") / len(statuses)
            if heavy_pct > 0.5:   trend = "HEAVY"
            elif smooth_pct > 0.5: trend = "SMOOTH"
            else:                  trend = "MODERATE"
        else:
            trend = base

        # Blend: 60% time model, 40% recent trend
        order  = ["SMOOTH","MODERATE","HEAVY"]
        blended_idx = round(order.index(base)*0.6 + order.index(trend)*0.4)
        return order[min(blended_idx, 2)]

    predictions = []
    for loc in G.nodes:
        current = latest_traffic.get(loc, {})
        pred    = predict_status(loc)
        predictions.append({
            "location":    loc,
            "current":     current.get("status","UNKNOWN"),
            "predicted":   pred,
            "predicted_at":f"+30 min ({future_hour:02d}:xx)",
            "improving":   (pred=="SMOOTH" and current.get("status")!="SMOOTH"),
            "worsening":   (pred=="HEAVY"  and current.get("status")!="HEAVY"),
        })
    return jsonify({"predictions":predictions,"model_hour":future_hour})

# ── STATS ─────────────────────────────────────────────────
@app.route("/stats")
def get_stats():
    data   = list(latest_traffic.values())
    total  = len(data)
    heavy  = sum(1 for d in data if d.get("status")=="HEAVY")
    mod    = sum(1 for d in data if d.get("status")=="MODERATE")
    smooth = sum(1 for d in data if d.get("status")=="SMOOTH")
    speeds = [d.get("speed",0) for d in data if d.get("speed")]
    counts = [d.get("count",0) for d in data if d.get("count")]

    # Congestion index 0-100
    idx = round(((heavy*100 + mod*50 + smooth*10) / max(total*100,1))*100)

    # Hourly breakdown from history
    hourly = {}
    for loc, dq in history.items():
        for r in dq:
            ts = r.get("recorded_at","")
            try:
                h = datetime.fromisoformat(ts).hour
                if h not in hourly: hourly[h]={"HEAVY":0,"MODERATE":0,"SMOOTH":0}
                hourly[h][r.get("status","SMOOTH")] = hourly[h].get(r.get("status","SMOOTH"),0)+1
            except:
                pass

    return jsonify({
        "total_zones":   total,
        "heavy":         heavy,
        "moderate":      mod,
        "smooth":        smooth,
        "avg_speed":     round(sum(speeds)/len(speeds),1) if speeds else 0,
        "avg_vehicles":  round(sum(counts)/len(counts),1) if counts else 0,
        "total_vehicles":sum(counts),
        "congestion_index": idx,
        "hourly_breakdown": {str(k):v for k,v in sorted(hourly.items())}
    })

# ── EXPORT CSV ────────────────────────────────────────────
@app.route("/export")
def export_csv():
    data = list(latest_traffic.values())
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=["location","status","count","speed","ts"])
    writer.writeheader()
    writer.writerows(data)
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition":"attachment;filename=traffic_export.csv"}
    )

# ── INCIDENTS ─────────────────────────────────────────────
@app.route("/incidents", methods=["GET","POST"])
def manage_incidents():
    if request.method == "GET":
        return jsonify(list(incidents.values()))

    body = request.get_json()
    inc_id = str(uuid.uuid4())[:8]
    incident = {
        "id":          inc_id,
        "type":        body.get("type","ACCIDENT"),   # ACCIDENT|ROADBLOCK|EVENT|FLOOD
        "location":    body.get("location"),
        "lat":         body.get("lat"),
        "lng":         body.get("lng"),
        "description": body.get("description",""),
        "severity":    body.get("severity","MEDIUM"),  # LOW|MEDIUM|HIGH
        "reported_at": datetime.now().strftime("%H:%M:%S"),
        "active":      True
    }
    incidents[inc_id] = incident
    print(f"🚨 Incident reported: {incident}")
    return jsonify({"success":True,"id":inc_id,"incident":incident})

@app.route("/incidents/<inc_id>", methods=["DELETE"])
def resolve_incident(inc_id):
    if inc_id in incidents:
        incidents[inc_id]["active"] = False
        del incidents[inc_id]
        return jsonify({"success":True})
    return jsonify({"error":"Not found"}), 404

# ── SPARK ─────────────────────────────────────────────────
@app.route("/spark")
def get_spark():
    return jsonify({
        "windows":      spark_windows[-100:],
        "total_count":  len(spark_windows),
        "output_files": len(glob.glob("output/*.json")) if os.path.exists("output") else 0,
        "spark_active": os.path.exists("checkpoint")
    })

# ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    threading.Thread(target=kafka_thread,        daemon=True).start()
    threading.Thread(target=spark_reader_thread, daemon=True).start()
    print("🚀 Flask started on http://127.0.0.1:5000")
    app.run(debug=False, port=5000)