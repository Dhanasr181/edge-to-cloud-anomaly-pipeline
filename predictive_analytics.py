import os
import time
import pandas as pd
from dotenv import load_dotenv
from azure.cosmos import CosmosClient, exceptions
from sklearn.ensemble import IsolationForest
import numpy as np

load_dotenv()

# Cosmos DB Configuration
COSMOS_URI = os.getenv("COSMOS_URI")
COSMOS_KEY = os.getenv("COSMOS_KEY")
DATABASE_NAME = "IoTDataDB"
CONTAINER_NAME = "Telemetry"

def get_cosmos_data(container, hours_back=1):
    """Fetch recent telemetry data from Cosmos DB."""
    # Note: Cosmos DB SQL API uses a custom dialect.
    # We retrieve the top X items, or query by timestamp if indexed properly.
    # For simplicity, we get the latest 500 records.
    query = "SELECT * FROM c ORDER BY c._ts DESC OFFSET 0 LIMIT 500"
    
    items = list(container.query_items(
        query=query,
        enable_cross_partition_query=True
    ))
    
    if not items:
        return pd.DataFrame()
        
    parsed_data = []
    for item in items:
        try:
            # Parse OPC UA JSON format structure
            if "Messages" in item and len(item["Messages"]) > 0:
                for msg in item["Messages"]:
                    if "Payload" in msg:
                        payload = msg["Payload"]
                        temp = payload.get("Temperature", {}).get("Value")
                        press = payload.get("Pressure", {}).get("Value")
                        vib = payload.get("Vibration", {}).get("Value")
                        timestamp = msg.get("Timestamp")
                        
                        if temp is not None and press is not None and vib is not None:
                            parsed_data.append({
                                'timestamp': timestamp,
                                'temperature': temp,
                                'pressure': press,
                                'vibration': vib
                            })
        except Exception as e:
            print(f"Error parsing item: {e}")
            continue

    if not parsed_data:
        return pd.DataFrame()

    df = pd.DataFrame(parsed_data)
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp').reset_index(drop=True)
    return df

def analyze_anomalies(df):
    """Use Isolation Forest to detect anomalies in telemetry."""
    print("\n--- Anomaly Detection ---")
    features = ['temperature', 'pressure', 'vibration']
    
    # Check if all features exist
    if not all(feature in df.columns for feature in features):
        print("Missing required features for anomaly detection.")
        return
        
    X = df[features].dropna()
    if len(X) < 10:
        print("Not enough data points for robust anomaly detection.")
        return

    # Train Isolation Forest
    model = IsolationForest(contamination=0.1, random_state=42)
    model.fit(X)
    
    # Predict (1 = normal, -1 = anomaly)
    predictions = model.predict(X)
    df['is_anomaly'] = predictions == -1
    
    anomalies = df[df['is_anomaly']]
    
    print(f"Total records analyzed: {len(df)}")
    print(f"Anomalies detected: {len(anomalies)}")
    if len(anomalies) > 0:
        print("Recent Anomalies:")
        # Show the most recent 5 anomalies
        print(anomalies[['timestamp', 'temperature', 'pressure', 'vibration']].tail(5))
    else:
        print("No anomalies detected.")

def predictive_maintenance(df):
    """Calculate Remaining Useful Life (RUL) heuristically based on vibration."""
    print("\n--- Predictive Maintenance (RUL) ---")
    if 'vibration' not in df.columns:
        print("Missing vibration data for RUL calculation.")
        return

    # Simple heuristic: If moving average of vibration exceeds a threshold, failure is imminent.
    # Let's say baseline vibration is 1.0. Critical threshold is 3.5.
    CRITICAL_VIBRATION = 3.5
    
    # Calculate 5-period moving average
    df['vib_ma'] = df['vibration'].rolling(window=5, min_periods=1).mean()
    
    latest_vib_ma = df['vib_ma'].iloc[-1]
    print(f"Current Vibration (Moving Average): {latest_vib_ma:.2f} g")
    
    if latest_vib_ma >= CRITICAL_VIBRATION:
        print("ALERT: Equipment vibration is critical. Failure expected soon! Maintenance required IMMEDIATELY.")
    elif latest_vib_ma > 2.0:
        # Estimate RUL based on rate of degradation
        print("WARNING: Elevated vibration detected. Degradation accelerating.")
        rul_hours = max(0, (CRITICAL_VIBRATION - latest_vib_ma) * 10) # arbitrary multiplier for demo
        print(f"Estimated Remaining Useful Life (RUL): ~{rul_hours:.1f} hours.")
    else:
        print("Equipment is operating normally. No immediate maintenance required.")

def main():
    if not COSMOS_URI or not COSMOS_KEY:
        print("WARNING: COSMOS_URI or COSMOS_KEY not found in .env.")
        exit(1)

    client = CosmosClient(COSMOS_URI, credential=COSMOS_KEY)
    try:
        database = client.get_database_client(DATABASE_NAME)
        container = database.get_container_client(CONTAINER_NAME)
    except exceptions.CosmosResourceNotFoundError:
        print("Database or container not found. Make sure hub_to_cosmos.py has run successfully.")
        exit(1)

    print("Fetching data from Cosmos DB...")
    df = get_cosmos_data(container)
    
    if df.empty:
        print("No data found in Cosmos DB.")
        return

    analyze_anomalies(df)
    predictive_maintenance(df)

if __name__ == "__main__":
    main()
