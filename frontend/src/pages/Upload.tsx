import React, { useState, useRef } from 'react';
import { UploadCloud, Cpu, AlertCircle } from 'lucide-react';
import { uploadDataset } from '../api';
import { SchemaDetectionResult } from '../types';

interface UploadProps {
  onSuccess: (result: SchemaDetectionResult) => void;
}

export const Upload: React.FC<UploadProps> = ({ onSuccess }) => {
  const [isDragging, setIsDragging] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);

  const handleFile = async (file: File) => {
    if (!file.name.endsWith('.csv') && !file.name.endsWith('.xlsx') && !file.name.endsWith('.xls')) {
      setError('Unsupported file format. Please upload a .csv or .xlsx file.');
      return;
    }
    setError(null);
    setLoading(true);
    try {
      const result = await uploadDataset(file);
      onSuccess(result);
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to parse file schema.');
      setLoading(false);
    }
  };

  return (
    <div className="max-w-4xl mx-auto px-4 py-12 flex flex-col items-center justify-center min-h-[80vh]">
      {/* Header */}
      <div className="text-center mb-8">
        <div className="inline-flex items-center space-x-2 px-3.5 py-1 rounded-full bg-indigo-50 border border-indigo-200 text-indigo-700 text-xs font-mono mb-4 font-semibold">
          <Cpu size={14} />
          <span>High-Performance Analytics Engine</span>
        </div>
        <h1 className="text-4xl sm:text-5xl font-bold tracking-tight text-slate-900 mb-3">
          Sales Data Analyzer
        </h1>
        <p className="text-slate-600 max-w-lg mx-auto text-sm leading-relaxed">
          Upload large transaction sales datasets. Automatically detects schemas, executes MapReduce aggregations, and flushes output safely after 10 minutes.
        </p>
      </div>

      {/* Main Drag and Drop Box */}
      <div
        onDragOver={(e) => {
          e.preventDefault();
          setIsDragging(true);
        }}
        onDragLeave={() => setIsDragging(false)}
        onDrop={(e) => {
          e.preventDefault();
          setIsDragging(false);
          if (e.dataTransfer.files && e.dataTransfer.files[0]) {
            handleFile(e.dataTransfer.files[0]);
          }
        }}
        onClick={() => fileInputRef.current?.click()}
        className={`w-full bg-white border-2 border-dashed rounded-2xl p-10 sm:p-14 text-center cursor-pointer transition-all duration-200 relative overflow-hidden group shadow-sm ${
          isDragging
            ? 'border-indigo-600 bg-indigo-50/50 scale-[1.01]'
            : 'border-slate-300 hover:border-indigo-400 hover:bg-slate-50/80'
        }`}
      >
        <input
          type="file"
          ref={fileInputRef}
          onChange={(e) => {
            if (e.target.files && e.target.files[0]) {
              handleFile(e.target.files[0]);
            }
          }}
          accept=".csv,.xlsx,.xls"
          className="hidden"
        />

        <div className="flex flex-col items-center justify-center relative z-10">
          <div className="w-16 h-16 rounded-2xl bg-indigo-50 border border-indigo-100 flex items-center justify-center mb-5 text-indigo-600 group-hover:scale-110 transition-transform">
            {loading ? (
              <div className="w-8 h-8 border-2 border-indigo-600 border-t-transparent rounded-full animate-spin" />
            ) : (
              <UploadCloud size={32} />
            )}
          </div>

          <h3 className="text-lg font-semibold text-slate-900 mb-1">
            {loading ? 'Reading Dataset Header & Schema...' : 'Drop your sales data here or click to browse'}
          </h3>
          <p className="text-xs text-slate-500 mb-4 max-w-sm">
            Supports CSV or Excel spreadsheets containing order transactions, revenue, regions, and dates.
          </p>

          <div className="flex items-center space-x-3 text-[11px] font-mono text-slate-500 bg-slate-100 px-3 py-1.5 rounded-lg border border-slate-200">
            <span>Formats: .CSV, .XLSX</span>
            <span>•</span>
            <span>Supports 100k+ to 1M+ rows</span>
          </div>
        </div>
      </div>

      {error && (
        <div className="mt-4 w-full p-4 rounded-xl bg-red-50 border border-red-200 text-red-700 text-xs flex items-center space-x-2">
          <AlertCircle size={16} />
          <span>{error}</span>
        </div>
      )}
    </div>
  );
};
