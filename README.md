# Real-Time Smart Traffic Dashboard with Kafka & Spark

This project is a real-time smart traffic monitoring system. It simulates live city traffic, streams the data using Apache Kafka, processes and aggregates the data using Apache Spark Structured Streaming, and serves a dynamic dashboard via a Python Flask backend.

## 🏗️ Architecture

The system consists of four main components:
1. **Traffic Producer (`traffic_producer.py`)**: Generates simulated, realistic traffic data for various city locations based on the time of day, and publishes it to a Kafka topic.
2. **Spark Streaming Processor (`spark_stream.py`)**: Consumes the raw traffic data from Kafka in real-time, categorizes traffic status, aggregates data into time windows, and writes the output to JSON files in the `output/` directory.
3. **Flask Backend (`app.py`)**: A REST API server that runs background threads to read directly from Kafka (for live map updates) and the Spark output files. It provides endpoints for traffic status, route calculation (Dijkstra), traffic history, predictions, and incident management.
4. **Frontend Dashboard (`index.html`)**: A responsive UI that visualizes the road network, real-time traffic statuses, and statistics.

---

## ⚙️ Prerequisites

Before running this project, ensure you have the following installed on your system:
- **Java 8 or 11** (Required for Apache Spark and Kafka)
- **Python 3.8+**
- **Apache Kafka** (which includes Zookeeper)

---

## 🚀 How to Run the Project

Follow these steps exactly in the order presented. It is recommended to run each step in a separate terminal window.

### Step 1: Start Zookeeper & Kafka Server
First, start your local Kafka environment. Navigate to your Kafka installation directory.

**Start Zookeeper:**
*On Windows:*
```bash
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
```
*On Linux/Mac:*
```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
```

**Start Kafka Server:**
*On Windows:*
```bash
.\bin\windows\kafka-server-start.bat .\config\server.properties
```
*On Linux/Mac:*
```bash
bin/kafka-server-start.sh config/server.properties
```

*(Optional)* Create the Kafka topic named `traffic`:
```bash
bin/kafka-topics.sh --create --topic traffic --bootstrap-server localhost:9092
```

### Step 2: Setup Python Environment
Navigate to the project directory and install the required Python packages:

```bash
pip install flask flask-cors kafka-python pyspark networkx
```

### Step 3: Start the Traffic Producer
In a new terminal, run the script that generates traffic data. Keep this running.
```bash
python traffic_producer.py
```
*You should see output indicating that data is being sent to Kafka.*

### Step 4: Start the Spark Streaming Job
In another terminal, start the PySpark streaming script. This reads from Kafka and performs window aggregations.
```bash
python spark_stream.py
```
*This process will run continuously and generate aggregated JSON files inside an `output/` folder.*

### Step 5: Start the Flask Backend Server
Open a new terminal and start the main web backend API.
```bash
python app.py
```
*The Flask server will start on `http://127.0.0.1:5000`.*

### Step 6: Open the Dashboard
Finally, to view the user interface:
Simply open the **`index.html`** file in your preferred web browser (e.g., Chrome, Edge, Safari). No web server is strictly required for the frontend, as it fetches data directly from the local Flask API.

---

## 🛑 Stopping the Project
To cleanly stop the project:
1. Press `Ctrl + C` in the terminals running `app.py`, `spark_stream.py`, and `traffic_producer.py`.
2. Stop the Kafka Server using `Ctrl + C` in its terminal.
3. Stop the Zookeeper Server using `Ctrl + C`.

## 📁 Project Structure
- `app.py`: Flask application and backend APIs.
- `traffic_producer.py`: Kafka producer generating simulated traffic.
- `spark_stream.py`: PySpark streaming job.
- `index.html`: Web-based dashboard UI.
- `output/`: Auto-generated folder containing Spark window aggregations.
- `checkpoint/`: Auto-generated Spark checkpoint directory for state tracking.