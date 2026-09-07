import React, { useState } from 'react';
import { SchemaDetectionResult, ColumnInfo, ColumnType } from '../types';
import { Sparkles, ArrowRight, Table, CheckCircle2, RotateCcw } from 'lucide-react';
import { triggerAnalysis } from '../api';

interface ColumnReviewProps {
  schemaResult: SchemaDetectionResult;
  onAnalyze: (jobId: string) => void;
  onBack: () => void;
}

const TYPE_COLORS: Record<ColumnType, string> = {
  date: 'bg-indigo-50 text-indigo-700 border-indigo-200',
  currency: 'bg-emerald-50 text-emerald-700 border-emerald-200',
  number: 'bg-sky-50 text-sky-700 border-sky-200',
  category: 'bg-amber-50 text-amber-700 border-amber-200',
  text: 'bg-slate-100 text-slate-700 border-slate-200',
};

export const ColumnReview: React.FC<ColumnReviewProps> = ({ schemaResult, onAnalyze, onBack }) => {
  const [columns, setColumns] = useState<ColumnInfo[]>(schemaResult.columns);
  const [loading, setLoading] = useState(false);

  const handleTypeChange = (index: number, newType: ColumnType) => {
    const updated = [...columns];
    updated[index] = {
      ...updated[index],
      inferred_type: newType,
      role: newType === 'date' ? 'time_axis' : newType === 'currency' ? 'metric' : 'dimension'
    };
    setColumns(updated);
  };

  const handleStartAnalysis = async () => {
    setLoading(true);
    try {
      const res = await triggerAnalysis(schemaResult.dataset_id, columns);
      onAnalyze(res.job_id);
    } catch (err) {
      alert('Failed to trigger MapReduce analysis job.');
      setLoading(false);
    }
  };

  return (
    <div className="max-w-5xl mx-auto px-4 py-10">
      {/* Top Action Header */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4 mb-8">
        <div>
          <div className="flex items-center space-x-2 text-xs text-emerald-700 mb-1 font-mono font-medium">
            <CheckCircle2 size={14} />
            <span>Schema Auto-Detected</span>
          </div>
          <h2 className="text-2xl sm:text-3xl font-bold text-slate-900 tracking-tight">
            Review Attribute Schema
          </h2>
          <p className="text-xs text-slate-600 mt-1">
            File: <span className="font-mono text-slate-900 font-semibold">{schemaResult.filename}</span> • ~{schemaResult.total_rows_estimated.toLocaleString()} rows estimated
          </p>
        </div>

        <div className="flex items-center space-x-3">
          <button
            onClick={onBack}
            className="px-4 py-2.5 rounded-xl border border-slate-300 bg-white hover:bg-slate-50 text-slate-700 text-xs font-semibold flex items-center space-x-2 transition-all shadow-sm"
          >
            <RotateCcw size={14} />
            <span>Change Dataset</span>
          </button>

          <button
            onClick={handleStartAnalysis}
            disabled={loading}
            className="px-6 py-2.5 rounded-xl bg-indigo-600 hover:bg-indigo-700 text-white text-xs font-bold flex items-center space-x-2 transition-all shadow-md active:scale-95 disabled:opacity-50"
          >
            <Sparkles size={16} />
            <span>{loading ? 'Submitting Job...' : 'Analyze Data'}</span>
            <ArrowRight size={16} />
          </button>
        </div>
      </div>

      {/* Grid of Columns */}
      <div className="bg-white border border-slate-200 rounded-2xl p-6 shadow-sm">
        <div className="flex items-center justify-between mb-4 pb-4 border-b border-slate-200">
          <div className="flex items-center space-x-2 text-slate-800 text-xs font-semibold">
            <Table size={16} className="text-indigo-600" />
            <span>Detected Columns & Inferred Types ({columns.length})</span>
          </div>
          <span className="text-[11px] text-slate-500 font-mono">No manual mapping required</span>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {columns.map((col, idx) => (
            <div
              key={idx}
              className="bg-slate-50/70 border border-slate-200 hover:border-slate-300 p-4 rounded-xl flex items-center justify-between transition-all"
            >
              <div className="flex-1 min-w-0 mr-4">
                <div className="flex items-center space-x-2 mb-1">
                  <span className="font-semibold text-sm text-slate-900 truncate font-mono">
                    {col.name}
                  </span>
                  <span className="text-[10px] font-mono px-2 py-0.5 rounded bg-slate-200 text-slate-700 font-medium">
                    {col.role}
                  </span>
                </div>

                <div className="text-[11px] text-slate-500 font-mono truncate">
                  Samples: {col.sample_values.join(', ') || 'N/A'}
                </div>
              </div>

              <div className="relative">
                <select
                  value={col.inferred_type}
                  onChange={(e) => handleTypeChange(idx, e.target.value as ColumnType)}
                  className={`text-xs font-mono font-semibold px-3 py-1.5 rounded-lg border appearance-none cursor-pointer focus:outline-none focus:ring-2 focus:ring-indigo-500 ${TYPE_COLORS[col.inferred_type]}`}
                >
                  <option value="date" className="bg-white text-slate-900">📅 Date</option>
                  <option value="currency" className="bg-white text-slate-900">💰 Currency</option>
                  <option value="number" className="bg-white text-slate-900">🔢 Number</option>
                  <option value="category" className="bg-white text-slate-900">🏷️ Category</option>
                  <option value="text" className="bg-white text-slate-900">📝 Text</option>
                </select>
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
};
