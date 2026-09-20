# 🚀 Sales Data Analyzer & BI Analytics Engine

An interactive, high-performance web application engineered to analyze large sales datasets (100,000+ to 1,000,000+ rows) in sub-seconds using an in-memory vectorized analytics engine and render modern executive BI dashboards.

---

## 🌟 Key Features

- **⚡ Sub-Second Analytics**: Process 100,000+ row datasets in **< 1 second** using C-accelerated Pandas/NumPy vectorization.
- **🔍 Auto Schema & Type Inference**: Automatically infers column headers and data types (`Date`, `Currency`, `Number`, `Category`, `Text`) upon file upload.
- **📈 Executive Dashboard**: Total Revenue, Total Orders, Average Order Value (AOV), Monthly Sales Trends, Top 10 Products, Regional Performance, Payment & Channel Breakdowns, and Customer Analytics.
- **🛡️ Data Quality & Auto-Flush**: Detects missing values, invalid dates, and duplicates with an automated 10-minute cleanup countdown timer.

---

## 📋 Prerequisites

Before running the application on another system, make sure the following are installed:

- **Python**: 3.9 or higher (`python3 --version` / `python --version`)
- **Node.js**: v18.0.0 or higher (`node -v`)
- **npm**: v9.0.0 or higher (`npm -v`)
- **Git** (optional, to clone repository)

---

## 🛠️ Step-by-Step Guide: Running on Another System

### 1️⃣ Clone or Download the Project

```bash
git clone <your-repository-url>
cd sales-analyzer
```
*(Or extract the project ZIP archive and open a terminal inside the project root directory).*

---

### 2️⃣ Backend Setup (FastAPI Python Server)

Open a terminal in the project directory and run:

#### **Linux / macOS:**
```bash
# Navigate to backend directory
cd backend

# Create a virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate

# Install backend dependencies
pip install --upgrade pip
pip install -r requirements.txt

# Start the backend server (runs on http://localhost:8000)
python3 main.py
```

#### **Windows (Command Prompt / PowerShell):**
```cmd
:: Navigate to backend directory
cd backend

:: Create virtual environment
python -m venv venv

:: Activate virtual environment (Command Prompt)
venv\Scripts\activate
:: OR in PowerShell:
:: .\venv\Scripts\Activate.ps1

:: Install backend dependencies
pip install --upgrade pip
pip install -r requirements.txt

:: Start backend server
python main.py
```

> **Note:** Backend will start on `http://localhost:8000`. Keep this terminal window running.

---

### 3️⃣ (Optional) Generate Synthetic Test Datasets

To generate a test dataset of 100,000 sales records (~12 MB):

```bash
# Inside backend directory with venv activated:
python generate_sample_data.py
```

This creates a sample file at `backend/sample_datasets/sales_data_100k.csv`.

---

### 4️⃣ Frontend Setup (React + Vite + Tailwind)

Open a **new separate terminal window** in the project root directory:

```bash
# Navigate to frontend directory
cd frontend

# Install Node dependencies
npm install

# Start Vite development server
npm run dev
```

The terminal will display the local URL (typically `http://localhost:3000`).

---

### 5️⃣ Access the Application

Open your browser and navigate to:
```
http://localhost:3000
```

1. Upload any sales dataset (`.csv` or `.xlsx`).
2. Review detected columns.
3. Click **Run Analytics** to view the live executive dashboard!

---

## 🛠️ Project Architecture & Performance Notes

- **Backend Framework**: FastAPI + Pandas / NumPy Vectorized Engine
- **Frontend Framework**: React 18 + Vite + Tailwind CSS + Lucide Icons + Recharts
- **API Proxy**: Frontend automatically proxies `/api` calls to `http://localhost:8000`.

---

## 🔒 Copyright & License

© 2026 ARK. All rights reserved.
