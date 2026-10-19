# 🚀 Sales Data Analyzer & BI Analytics Engine (Distributed Architecture)

An enterprise-grade, distributed sales analytics platform engineered to process high-volume datasets (100,000+ to multi-million rows) using **Apache Hadoop MapReduce** with automated fallback to **Apache PySpark**, rendering real-time executive BI dashboards.

---

## 🌟 Key Features

- **🐘 Genuine Apache Hadoop MapReduce**: Priority 1 distributed execution engine using streaming mappers and reducers.
- **⚡ Automatic PySpark Fallback**: Priority 2 automated failover using PySpark DataFrames & Spark SQL when Hadoop is unavailable.
- **🛡️ Strict Engine Selection Policy**: Priority 1 (Hadoop) ➔ Priority 2 (PySpark) ➔ Priority 3 (Explicit Error). Strictly eliminates silent in-memory Pandas/NumPy calculation for core analytics.
- **🏷️ Real-Time Engine Indication**: Clear, prominent badges and status markers showing which engine processed the dataset across the UI dashboard, progress tracker, API responses, and server logs.
- **🔍 Automated Schema & Type Inference**: Dynamically detects column headers, data roles, and data types (`Date`, `Currency`, `Number`, `Category`, `Text`).
- **📈 Executive Analytics Dashboard**: Real-time KPI summaries (Total Revenue, Orders, Average Order Value), chronological trends, top products, geographic distributions, channel breakdowns, and customer insights.
- **🧹 Data Quality & Auto-Flush**: Detects nulls, invalid dates, and duplicates with a 10-minute automated storage cleanup timer.
- **🔒 Zero Frontend Breaking Changes**: 100% backward compatible with existing frontend API contracts.

---

## 📋 System Prerequisites

Ensure the following tools are installed on your host machine:

| Component | Minimum Version | Verification Command |
| :--- | :--- | :--- |
| **Python** | 3.9+ (Python 3.10 – 3.14 supported) | `python3 --version` |
| **Java** | OpenJDK 11 or higher | `java -version` |
| **Node.js** | v18.0.0 or higher | `node -v` |
| **npm** | v9.0.0 or higher | `npm -v` |
| **Apache Hadoop** *(Optional)* | Hadoop 3.x with `hadoop-streaming.jar` | `hadoop version` |
| **Apache PySpark** | PySpark 3.5.x | Installed via `backend/requirements.txt` |

> [!NOTE]
> If Apache Hadoop is not installed or the cluster is offline, the backend **automatically and seamlessly falls back to Apache PySpark** without requiring any configuration changes.

---

## 🛠️ Step-by-Step Guide: Running the Project

### 1️⃣ Clone or Open the Repository

```bash
cd sales-analyzer-main
```

---

### 2️⃣ Backend Setup (FastAPI Distributed Engine)

Open a terminal in the project directory:

```bash
# 1. Navigate to backend directory
cd backend

# 2. Create Python virtual environment (if not already created)
python3 -m venv venv

# 3. Activate the virtual environment
source venv/bin/activate
# On Windows PowerShell: .\venv\Scripts\Activate.ps1

# 4. Install backend dependencies
pip install --upgrade pip
pip install -r requirements.txt

# 5. (Optional) Check which engine is active before starting
python3 -m processing.engine_manager

# 6. Start the FastAPI backend server
python3 main.py
```

The backend server starts and listens on **`http://localhost:8000`**.

---

### 3️⃣ Frontend Setup (React + Vite + Tailwind CSS)

Open a **second terminal window** in the project directory:

```bash
# 1. Navigate to frontend directory
cd frontend

# 2. Install Node.js dependencies
npm install

# 3. Start the Vite development server
npm run dev
```

The frontend application starts and opens on **`http://localhost:3000`** (or the port shown in terminal).

---

### 4️⃣ Testing with Sample Datasets

A production-ready 100,000-row synthetic sales dataset (`sales_data_100k.csv`, ~12.1 MB) is pre-packaged in:
```
backend/sample_datasets/sales_data_100k.csv
```

To generate a fresh synthetic dataset with custom size:
```bash
cd backend
source venv/bin/activate
python3 generate_sample_data.py
```

1. Open `http://localhost:3000` in your web browser.
2. Drag and drop `backend/sample_datasets/sales_data_100k.csv` (or any sales CSV) into the upload area.
3. Review the automatically detected column mapping and click **"Proceed to Analytics"**.
4. Watch the distributed processing step indicator in real time.
5. Explore the interactive Executive BI Dashboard.

---

## 🔍 How to Verify Which Engine Processed a Dataset

You can verify the active engine through multiple built-in indicators:

### 1. Dashboard UI (Header)
Directly under the dashboard heading on the web interface, the engine badge is displayed next to the filename:
```
Dataset: sales_data_100k.csv (Apache Hadoop MapReduce)
# or
Dataset: sales_data_100k.csv (Apache PySpark (DataFrame & SQL Engine))
```

### 2. Processing Screen (Progress Step)
During dataset processing, the real-time progress tracker explicitly shows the active engine:
```
[Engine: Apache Hadoop MapReduce] Preparing distributed MapReduce streaming job...
# or
[Engine: Apache PySpark] Executing distributed DataFrame transformations & SQL aggregations...
```

### 3. Engine Health & Capability Endpoint (`GET /api/engines`)
Query the diagnostic endpoint to inspect the runtime state of all registered engines:
```bash
curl http://localhost:8000/api/engines
```
**Example Response:**
```json
{
  "active_engine": "hadoop",
  "active_engine_name": "Apache Hadoop MapReduce",
  "engine_indication": "[ACTIVE ENGINE: Apache Hadoop MapReduce]",
  "hadoop": {
    "available": true,
    "status_message": "Hadoop MapReduce operational (Hadoop LocalJobRunner / Cluster streaming mode)."
  },
  "pyspark": {
    "available": true,
    "status_message": "PySpark 3.5.9 operational (master=local[*])."
  }
}
```

### 4. API Service Status (`GET /`)
```bash
curl http://localhost:8000/
```
```json
{
  "message": "Sales Analytics Engine Active",
  "status": "Ready",
  "copyright": "© 2026 ARK",
  "active_engine": "hadoop",
  "active_engine_name": "Apache Hadoop MapReduce",
  "engine_indication": "[ACTIVE ENGINE: Apache Hadoop MapReduce]"
}
```

### 5. CLI Quick Probe
Run directly inside `backend/`:
```bash
python3 -m processing.engine_manager
```

---

## 📂 Project Structure

```
sales-analyzer-main/
├── README.md                          # Comprehensive project documentation
├── LICENSE                            # License file
├── backend/                           # FastAPI distributed backend
│   ├── main.py                        # FastAPI entrypoint and REST routes
│   ├── models.py                      # Pydantic schemas and response contracts
│   ├── schema_detection.py            # Streamed CSV schema and type inference
│   ├── generate_sample_data.py        # Synthetic dataset generator
│   ├── requirements.txt               # Python package dependencies
│   ├── hadoop_jobs/                   # Hadoop MapReduce streaming components
│   │   ├── mapper.py                  # Stream-based standard input mapper
│   │   ├── reducer.py                 # Stream-based key aggregation reducer
│   │   └── job_runner.py              # Asynchronous job dispatcher and state store
│   ├── processing/                    # Distributed engine adapters
│   │   ├── base_engine.py             # Engine abstract base class and custom exceptions
│   │   ├── hadoop_engine.py           # Real Hadoop streaming MapReduce adapter
│   │   ├── pyspark_engine.py          # Real Apache PySpark DataFrame / SQL adapter
│   │   ├── engine_manager.py          # Central engine orchestrator & fallback logic
│   │   └── result_normalizer.py       # Engine output normalizer for dashboard contract
│   ├── sample_datasets/               # Ready-to-use sample datasets
│   │   └── sales_data_100k.csv        # 100,000-row sample dataset
│   └── storage/                       # Isolated job workspace & upload buffer
└── frontend/                          # React + TypeScript + Vite frontend
    ├── package.json                   # Node.js dependencies and scripts
    ├── vite.config.ts                 # Vite bundler and proxy configuration
    └── src/                           # Frontend UI components and pages
        ├── App.tsx                    # Main application shell and routing
        ├── api.ts                     # Axios backend API client
        ├── types.ts                   # TypeScript interfaces
        └── pages/                     # Upload, Review, Processing, and Dashboard screens
```

---

## 🔒 Copyright & License

© 2026 ARK. All rights reserved.
