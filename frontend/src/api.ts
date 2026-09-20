import axios from 'axios';
import { SchemaDetectionResult, JobStatus, DashboardAnalytics, ColumnInfo } from './types';

const API_BASE = (import.meta.env.VITE_API_URL || '').replace(/\/$/, '') + '/api';

export const uploadDataset = async (file: File): Promise<SchemaDetectionResult> => {
  const formData = new FormData();
  formData.append('file', file);
  const response = await axios.post<SchemaDetectionResult>(`${API_BASE}/upload`, formData, {
    headers: { 'Content-Type': 'multipart/form-data' },
  });
  return response.data;
};

export const flushDataset = async (datasetId: string): Promise<void> => {
  try {
    await axios.post(`${API_BASE}/flush/${datasetId}`);
  } catch (err) {
    console.error('Failed to flush dataset:', err);
  }
};

export const triggerAnalysis = async (
  datasetId: string,
  columns: ColumnInfo[]
): Promise<{ job_id: string; dataset_id: string }> => {
  const response = await axios.post<{ job_id: string; dataset_id: string }>(`${API_BASE}/analyze`, {
    dataset_id: datasetId,
    columns,
  });
  return response.data;
};

export const pollJobStatus = async (jobId: string): Promise<JobStatus> => {
  const response = await axios.get<JobStatus>(`${API_BASE}/jobs/${jobId}/status`);
  return response.data;
};

export const getDashboardResults = async (datasetId: string): Promise<DashboardAnalytics> => {
  const response = await axios.get<DashboardAnalytics>(`${API_BASE}/results/${datasetId}`);
  return response.data;
};
