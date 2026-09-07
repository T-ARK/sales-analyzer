import os
import uuid
import threading
from typing import List, Optional
from fastapi import FastAPI, File, UploadFile, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from models import (
    SchemaDetectionResult, ColumnOverrideRequest, AnalysisJobStatus, 
    DashboardAnalytics, ColumnInfo
)
from schema_detection import detect_schema
from hadoop_jobs.job_runner import (
    run_mapreduce_job_async, JOB_STATUS_STORE, RESULT_STORE
)
from generate_sample_data import generate_100k_dataset

app = FastAPI(
    title="Sales Data Processing API",
    version="1.0.0",
    description="Sales Analytics Engine Backend"
)

# Enable CORS for React Vite frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

STORAGE_DIR = os.path.join(os.path.dirname(__file__), 'storage')
os.makedirs(STORAGE_DIR, exist_ok=True)

DATASET_SCHEMAS = {}

def schedule_auto_flush(dataset_id: str, file_path: str, delay_seconds: int = 600):
    """Automatically delete stored dataset file and results after 10-minute countdown."""
    def cleanup():
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
            if dataset_id in DATASET_SCHEMAS:
                del DATASET_SCHEMAS[dataset_id]
            if dataset_id in RESULT_STORE:
                del RESULT_STORE[dataset_id]
            print(f"[AUTO-FLUSH] Flushed dataset {dataset_id} after 10 min countdown.")
        except Exception as e:
            print(f"[AUTO-FLUSH ERROR] {e}")

    timer = threading.Timer(delay_seconds, cleanup)
    timer.daemon = True
    timer.start()

@app.get("/")
def read_root():
    return {"message": "Sales Analytics Engine Active", "status": "Ready", "copyright": "© 2026 ARK"}

@app.post("/api/upload", response_model=SchemaDetectionResult)
async def upload_dataset(file: UploadFile = File(...)):
    """Receives CSV or Excel dataset file, performs instant sample schema & column detection."""
    filename = file.filename or "uploaded_sales_data.csv"
    if not (filename.endswith('.csv') or filename.endswith('.xlsx') or filename.endswith('.xls')):
        raise HTTPException(status_code=400, detail="Invalid file format. Only .csv and .xlsx files are supported.")

    dataset_id = str(uuid.uuid4())[:8]
    file_path = os.path.join(STORAGE_DIR, f"{dataset_id}_{filename}")

    with open(file_path, "wb") as buffer:
        content = await file.read()
        buffer.write(content)

    detection_result = detect_schema(file_path, dataset_id, filename)
    DATASET_SCHEMAS[dataset_id] = {
        'file_path': file_path,
        'filename': filename,
        'columns': detection_result.columns
    }

    # Schedule automatic flush after 10 minutes (600 seconds)
    schedule_auto_flush(dataset_id, file_path, 600)

    return detection_result

@app.post("/api/analyze")
def trigger_analysis(payload: ColumnOverrideRequest, background_tasks: BackgroundTasks):
    """Triggers background MapReduce execution pipeline using confirmed column metadata."""
    dataset_id = payload.dataset_id
    if dataset_id not in DATASET_SCHEMAS:
        raise HTTPException(status_code=404, detail="Dataset ID not found. Please upload a file first.")

    dataset_info = DATASET_SCHEMAS[dataset_id]
    file_path = dataset_info['file_path']
    confirmed_columns = payload.columns

    job_id = f"job_{dataset_id}_{str(uuid.uuid4())[:6]}"

    # Dispatch background MapReduce execution thread
    thread = threading.Thread(
        target=run_mapreduce_job_async,
        args=(job_id, dataset_id, file_path, confirmed_columns)
    )
    thread.daemon = True
    thread.start()

    return {"job_id": job_id, "dataset_id": dataset_id, "status": "queued"}

@app.get("/api/jobs/{job_id}/status", response_model=AnalysisJobStatus)
def get_job_status(job_id: str):
    """Endpoint for polling MapReduce progress state."""
    if job_id not in JOB_STATUS_STORE:
        raise HTTPException(status_code=404, detail="Job ID not found.")
    return JOB_STATUS_STORE[job_id]

@app.get("/api/results/{dataset_id}", response_model=DashboardAnalytics)
def get_dashboard_results(dataset_id: str):
    """Retrieves computed analytics results for a completed dataset."""
    if dataset_id not in RESULT_STORE:
        raise HTTPException(status_code=404, detail="Results not ready or dataset ID not found.")
    return RESULT_STORE[dataset_id]

@app.post("/api/flush/{dataset_id}")
def flush_dataset(dataset_id: str):
    """Explicitly flushes and deletes dataset files and cached results."""
    if dataset_id in DATASET_SCHEMAS:
        file_path = DATASET_SCHEMAS[dataset_id].get('file_path')
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
            except Exception:
                pass
        del DATASET_SCHEMAS[dataset_id]
    if dataset_id in RESULT_STORE:
        del RESULT_STORE[dataset_id]
    return {"status": "flushed", "dataset_id": dataset_id}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
