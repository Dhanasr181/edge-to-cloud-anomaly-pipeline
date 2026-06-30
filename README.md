# OPC UA to Azure IoT Hub Pipeline

This project implements an Industrial Internet of Things (IIoT) pipeline that simulates generating OPC UA telemetry data and publishing it securely to Azure IoT Hub over MQTT with mutual TLS (mTLS) authentication.

## Architecture Overview

The pipeline follows this industrial flow:
**PLC (Simulated)** ➔ **OPC UA Publisher** ➔ **MQTT + TLS (`stunnel`)** ➔ **Azure IoT Hub**

1. **OPC UA Publisher**: Built using the open-source `open62541` C library. It packages data into standardized OPC UA JSON payloads.
2. **TLS Proxy (`stunnel`)**: Azure IoT Hub requires strict TLS encryption. Because the underlying MQTT C-client used by `open62541` does not natively support TLS, we use `stunnel` as a secure local proxy. The publisher sends unencrypted MQTT data locally to `stunnel`, which wraps it in military-grade TLS encryption using X.509 certificates and forwards it to Azure IoT Hub.

## What Codes Were Developed & Why?

* **`Dockerfile`**: 
  * **Why:** To create a reproducible, isolated Linux environment.
  * **What it does:** Installs all necessary dependencies (Cmake, OpenSSL, stunnel, etc.), builds the `open62541` library from source with PubSub/MQTT enabled, compiles the publisher/subscriber C applications, and injects our custom certificate generation and thumbprint extraction scripts.
* **`docker-compose.yml`**:
  * **Why:** To easily manage and orchestrate the multiple services (Publisher, Subscriber, and Thumbprint Extractors) without typing long Docker commands.
  * **What it does:** Defines the containers, mounts the `./certs` volumes so certificates are saved to your local machine, and passes the environment variables (Device ID and Hub Name) into the containers.
* **`generate_certs.sh` (Inside Dockerfile)**:
  * **Why:** Azure requires devices to authenticate. We chose X.509 self-signed certificates.
  * **What it does:** Automatically generates a 2048-bit RSA key and an X.509 certificate tailored specifically to your Device ID. It also automatically configures `stunnel` to route traffic to your specific Azure IoT Hub.
* **`extract_thumbprint.sh` (Inside Dockerfile)**:
  * **Why:** Azure IoT Hub requires you to input the SHA-256 "Thumbprint" of your certificate when registering the device in the portal.
  * **What it does:** Extracts the exact thumbprint hash from the generated certificate and prints it to the logs so you can easily copy and paste it into Azure.

---

## How to Change IoT Hub or Device Details in the Future

If you create a new IoT Hub, or want to deploy a new device (e.g., `device3`), you must follow these exact steps to ensure the security certificates are generated correctly:

### Step 1: Update the configuration
1. Open the `.env` file (or `docker-compose.yml` if you hardcoded them there).
2. Change `IOT_HUB_NAME` to your new Azure IoT Hub URL (e.g., `new-hub.azure-devices.net`).
3. Change `IOT_DEVICE_ID` to your new device name (e.g., `device3`).

### Step 2: Delete old certificates (CRITICAL)
**Yes, you MUST create new certificates!** X.509 certificates are hardcoded with the specific Device ID. If you change the Device ID, the old certificate is invalid.
To force the system to generate new certificates:
1. Delete all files inside the `./certs` folder on your host machine.
2. Delete all files inside the `./certs-sub` folder (if using the subscriber).

### Step 3: Restart Docker to generate new certificates
Run the following commands in your terminal:
```bash
docker-compose down
docker-compose up -d
```
When the containers start, the `generate_certs.sh` script will notice the `./certs` folder is empty and will automatically generate brand-new certificates using your new Device ID.

### Step 4: Get the new Thumbprints
Run the following command to get the new SHA-256 thumbprint:
```bash
docker logs cert-thumbprint-extractor
```

### Step 5: Register the device in Azure IoT Hub
Go to the Azure Portal ➔ Your IoT Hub ➔ Devices ➔ **+ Add Device**.
* **Device ID:** (Must exactly match what you put in the `.env` file)
* **Authentication type:** Self-signed X509 Certificate
* **Thumbprints:** Paste the SHA-256 thumbprint you copied in Step 4 into both the Primary and Secondary boxes.

Once saved, your containers will immediately connect and begin transmitting telemetry to the new hub!

---

### 💾 Cosmos DB Integration & Data Engineering Pipeline

Because Azure IoT Hub automatically encodes telemetry strings into binary formatting during direct message routing, the data lands in Cosmos DB with its core payload hidden inside a Base64 string. 

The analytical layer in Databricks handles this by bypassing third-party UI connectors and executing a programmatic extraction pipeline.

#### 1. Raw Document Ingestion Format
Data is read natively from the Unity Catalog Volume as a JSON array. A typical wrapper document lands in the `Telemetry` container with the following structural layout:

* **System Metadata:** Tracks internal parameters (`id`, `_ts`, `SystemProperties`) generated during the IoT Hub ingestion path.
* **Encrypted Payload Storage:** The raw open62541 variables are contained inside the **`Body`** column as an encrypted Base64 string block: `"eyJNZXNzYWdlSWQiOm51bGwsIk1lc3Nh..."`

#### 2. Programmatic Processing Steps
To convert this raw document stream into a valid machine learning matrix, the Databricks pipeline processes the telemetry through four distinct stages:

1. **Multiline Parsing:** Instructs the Spark engine to ingest the target file as a unified array array structure rather than individual standalone lines (`.option("multiline", "true")`).
2. **Base64 Decryption:** Executes the native Spark `unbase64(col("Body"))` function directly inside the JVM, casting the binary array output back into clean, human-readable JSON string text.
3. **Strict Schema Mapping:** Applies an explicit, custom Apache Spark `StructType` schema representing the official open62541 message payload pattern. This prevents runtime processing exceptions if malformed edge packets enter the pipeline.
4. **Data Flattening Matrix:** Extracts specific array indices out of the nested JSON structure (`col("ParsedData.Messages")[0]["Payload"]...`) and surfaces them into flat, queryable columns (`temperature`, `pressure`, `vibration`) optimized for feature scaling algorithms.
