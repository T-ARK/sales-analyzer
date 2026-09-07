# ARK Sales Data Analyzer

An interactive, high-performance web application built to analyze large sales datasets (100,000+ to 1,000,000+ rows) using MapReduce algorithms and render modern BI analytics dashboards in seconds.

---

## 🌟 Key Features

1. **Effortless Upload & Schema Auto-Detection**
   - Drag & drop CSV or Excel datasets.
   - Instantly inspects sample headers and infers data types (`Date`, `Currency`, `Number`, `Category`, `Text`) with zero manual mapping required.

2. **MapReduce Processing Pipeline**
   - High-performance MapReduce streaming engine across data splits.
   - Computes Total Revenue, Total Orders, Average Order Value (AOV), Monthly Sales Trends, Top 10 Products, and Category Breakdowns.

3. **Executive BI Analytics Dashboard & Auto-Flush**
   - Clean, modern light theme (`whitebg`).
   - Automatic 10-minute countdown flush timer: Deletes dataset files and flushes output state automatically after 10 minutes.
   - Interactive Recharts components, sortable table, and CSV export.

---

## 🔒 Copyright & Ownership

© 2026 ARK. All rights reserved.

---

## 🚀 Quick Start Guide

### 1. Backend Setup (FastAPI + Python)
```bash
cd backend
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

### 2. Frontend Setup (React + Vite + Tailwind)
```bash
cd frontend
npm install
npm run dev
```

Open `http://localhost:3000` in your browser.

---

## 📊 MapReduce Aggregation Flow

```
[Uploaded Dataset] 
       │
       ▼
[HDFS Data Splits]
       │
       ├──► Mapper 1 ──┐
       ├──► Mapper 2 ──┼──► [Shuffle & Sort] ──► Reducer ──► [Dashboard JSON]
       └──► Mapper N ──┘
```

1. **Mapper (`mapper.py`)**: Tokenizes CSV lines, extracts `date`, `metric`, `region`, `category`, `product`, and `quantity`, emitting tab-separated key-value pairs.
2. **Shuffle & Sort**: Groups emitted pairs by key prefix (`KPI`, `TREND`, `PRODUCT`, `REGION`, `CATEGORY`).
3. **Reducer (`reducer.py`)**: Computes sums, averages, counts, growth deltas, and outputs structured JSON.
