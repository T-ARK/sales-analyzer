import React, { useState, useEffect } from 'react';
import { Upload } from './pages/Upload';
import { ColumnReview } from './pages/ColumnReview';
import { Processing } from './pages/Processing';
import { Dashboard } from './pages/Dashboard';
import { SchemaDetectionResult } from './types';
import { flushDataset } from './api';
import { Database, Clock, ShieldCheck } from 'lucide-react';

type ScreenState = 'UPLOAD' | 'REVIEW' | 'PROCESSING' | 'DASHBOARD';

export const App: React.FC = () => {
  const [screen, setScreen] = useState<ScreenState>('UPLOAD');
  const [schemaResult, setSchemaResult] = useState<SchemaDetectionResult | null>(null);
  const [activeJobId, setActiveJobId] = useState<string | null>(null);
  const [secondsRemaining, setSecondsRemaining] = useState<number>(600); // 10-minute countdown

  // 10-minute auto-flush countdown timer
  useEffect(() => {
    if (!schemaResult) {
      setSecondsRemaining(600);
      return;
    }

    const interval = setInterval(() => {
      setSecondsRemaining((prev) => {
        if (prev <= 1) {
          clearInterval(interval);
          handleFlushAndReset();
          return 0;
        }
        return prev - 1;
      });
    }, 1000);

    return () => clearInterval(interval);
  }, [schemaResult]);

  const handleUploadSuccess = (result: SchemaDetectionResult) => {
    setSchemaResult(result);
    setSecondsRemaining(600); // reset 10-minute timer for new dataset
    setScreen('REVIEW');
  };

  const handleAnalyzeTriggered = (jobId: string) => {
    setActiveJobId(jobId);
    setScreen('PROCESSING');
  };

  const handleProcessingComplete = () => {
    setScreen('DASHBOARD');
  };

  const handleFlushAndReset = async () => {
    if (schemaResult?.dataset_id) {
      await flushDataset(schemaResult.dataset_id);
    }
    setSchemaResult(null);
    setActiveJobId(null);
    setSecondsRemaining(600);
    setScreen('UPLOAD');
  };

  const formatTimer = (totalSeconds: number) => {
    const mins = Math.floor(totalSeconds / 60);
    const secs = totalSeconds % 60;
    return `${String(mins).padStart(2, '0')}:${String(secs).padStart(2, '0')}`;
  };

  return (
    <div className="min-h-screen flex flex-col justify-between bg-slate-50 text-slate-900 font-sans selection:bg-indigo-500 selection:text-white">
      {/* Global Top Navbar */}
      <header className="border-b border-slate-200 bg-white/90 backdrop-blur-md sticky top-0 z-50 shadow-sm">
        <div className="max-w-7xl mx-auto px-4 h-16 flex items-center justify-between">
          <div
            onClick={handleFlushAndReset}
            className="flex items-center space-x-3 cursor-pointer group"
          >
            <div className="w-9 h-9 rounded-xl bg-indigo-50 border border-indigo-200 flex items-center justify-center text-indigo-600 group-hover:scale-105 transition-transform">
              <Database size={20} />
            </div>
            <div>
              <span className="font-bold text-sm text-slate-900 tracking-tight block">
                Sales Data Analyzer
              </span>
              <span className="text-[10px] font-mono text-slate-500 block -mt-0.5">
                Hadoop MapReduce Engine
              </span>
            </div>
          </div>

          <div className="flex items-center space-x-4 text-xs">
            {schemaResult && (
              <div className="flex items-center space-x-2 text-amber-700 bg-amber-50 px-3 py-1.5 rounded-lg border border-amber-200 font-mono font-medium">
                <Clock size={14} className="text-amber-600 animate-pulse" />
                <span>Auto-flush in {formatTimer(secondsRemaining)}</span>
              </div>
            )}

            <div className="hidden sm:flex items-center space-x-2 text-slate-600 bg-slate-100 px-3 py-1.5 rounded-lg border border-slate-200 font-mono">
              <ShieldCheck size={14} className="text-emerald-600" />
              <span>Engine Status: Active</span>
            </div>
          </div>
        </div>
      </header>

      {/* Screen Routing */}
      <main className="flex-1">
        {screen === 'UPLOAD' && (
          <Upload onSuccess={handleUploadSuccess} />
        )}

        {screen === 'REVIEW' && schemaResult && (
          <ColumnReview
            schemaResult={schemaResult}
            onAnalyze={handleAnalyzeTriggered}
            onBack={handleFlushAndReset}
          />
        )}

        {screen === 'PROCESSING' && activeJobId && schemaResult && (
          <Processing
            jobId={activeJobId}
            datasetId={schemaResult.dataset_id}
            onComplete={handleProcessingComplete}
          />
        )}

        {screen === 'DASHBOARD' && schemaResult && (
          <Dashboard
            datasetId={schemaResult.dataset_id}
            onReset={handleFlushAndReset}
            secondsRemaining={secondsRemaining}
          />
        )}
      </main>

      {/* Global Footer */}
      <footer className="border-t border-slate-200 py-6 text-center text-xs text-slate-500 font-mono bg-white">
        <div className="max-w-7xl mx-auto px-4 flex flex-col sm:flex-row items-center justify-between gap-2">
          <span>Sales Data Analyzer</span>
          <span>© 2026 ARK. All rights reserved.</span>
        </div>
      </footer>
    </div>
  );
};
