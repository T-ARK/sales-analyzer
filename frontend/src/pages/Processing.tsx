import React, { useEffect, useState } from 'react';
import { pollJobStatus } from '../api';
import { JobStatus } from '../types';
import { HardDrive, Cpu, Layers, CheckCircle2, AlertTriangle } from 'lucide-react';

interface ProcessingProps {
  jobId: string;
  datasetId: string;
  onComplete: () => void;
}

export const Processing: React.FC<ProcessingProps> = ({ jobId, datasetId, onComplete }) => {
  const [status, setStatus] = useState<JobStatus | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let interval: any = null;

    const checkStatus = async () => {
      try {
        const res = await pollJobStatus(jobId);
        setStatus(res);
        if (res.status === 'completed') {
          clearInterval(interval);
          setTimeout(() => {
            onComplete();
          }, 800);
        } else if (res.status === 'failed') {
          clearInterval(interval);
          setError(res.error_message || 'MapReduce processing failed.');
        }
      } catch (err: any) {
        setError('Failed to reach server to fetch job status.');
      }
    };

    checkStatus();
    interval = setInterval(checkStatus, 1000);

    return () => {
      if (interval) clearInterval(interval);
    };
  }, [jobId, onComplete]);

  const progress = status?.progress_pct || 15;
  const currentStepMsg = status?.current_step || 'Initializing processing engine...';

  const steps = [
    { label: 'Data File Ingestion & Parsing', icon: HardDrive, targetPct: 25 },
    { label: 'MapReduce Mapper Execution', icon: Cpu, targetPct: 60 },
    { label: 'Shuffle, Sort & Reducer Aggregation', icon: Layers, targetPct: 85 },
    { label: 'Dashboard Report Generation', icon: CheckCircle2, targetPct: 100 },
  ];

  return (
    <div className="max-w-2xl mx-auto px-4 py-16 flex flex-col items-center justify-center min-h-[75vh]">
      <div className="w-full bg-white border border-slate-200 rounded-2xl p-8 sm:p-10 shadow-lg text-center">
        {/* Animated Processing Spinner */}
        <div className="relative w-20 h-20 mx-auto mb-6 flex items-center justify-center">
          <div className="absolute inset-0 rounded-full border-4 border-indigo-100 animate-pulse" />
          <div className="absolute inset-0 rounded-full border-4 border-indigo-600 border-t-transparent animate-spin" />
          <Cpu size={32} className="text-indigo-600 animate-bounce" />
        </div>

        <h2 className="text-2xl font-bold text-slate-900 mb-2">
          Processing Sales Aggregations
        </h2>
        <p className="text-xs font-mono text-indigo-600 font-semibold mb-8">
          {currentStepMsg}
        </p>

        {/* Progress Bar */}
        <div className="w-full bg-slate-100 rounded-full h-3.5 mb-8 p-0.5 border border-slate-200 relative overflow-hidden">
          <div
            className="bg-gradient-to-r from-indigo-600 via-sky-500 to-emerald-500 h-full rounded-full transition-all duration-500 ease-out"
            style={{ width: `${progress}%` }}
          />
        </div>

        {/* Pipeline Step Indicators */}
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-3 text-left">
          {steps.map((step, idx) => {
            const isFinished = progress >= step.targetPct;
            const isCurrent = progress < step.targetPct && (idx === 0 || progress >= steps[idx - 1].targetPct);
            const Icon = step.icon;

            return (
              <div
                key={idx}
                className={`p-3.5 rounded-xl border flex items-center space-x-3 transition-all ${
                  isFinished
                    ? 'bg-emerald-50 border-emerald-200 text-emerald-700 font-medium'
                    : isCurrent
                    ? 'bg-indigo-50 border-indigo-300 text-indigo-700 font-semibold animate-pulse'
                    : 'bg-slate-50 border-slate-200 text-slate-400'
                }`}
              >
                <Icon size={18} />
                <span className="text-xs truncate">{step.label}</span>
              </div>
            );
          })}
        </div>

        {error && (
          <div className="mt-6 p-4 rounded-xl bg-red-50 border border-red-200 text-red-700 text-xs flex items-center space-x-2 text-left">
            <AlertTriangle size={16} />
            <span>{error}</span>
          </div>
        )}
      </div>
    </div>
  );
};
