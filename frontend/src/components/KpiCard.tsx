import React from 'react';
import { TrendingUp, TrendingDown, LucideIcon } from 'lucide-react';

interface KpiCardProps {
  label: string;
  value: string;
  subtext?: string;
  growthRate?: number;
  icon: LucideIcon;
  accentColor?: string;
}

export const KpiCard: React.FC<KpiCardProps> = ({
  label,
  value,
  subtext,
  growthRate,
  icon: Icon,
  accentColor = 'border-indigo-100'
}) => {
  const isPositive = growthRate !== undefined && growthRate >= 0;

  return (
    <div className={`bg-white border border-slate-200 rounded-xl p-5 relative overflow-hidden hover:border-slate-300 transition-all shadow-sm ${accentColor}`}>
      <div className="flex justify-between items-start mb-3">
        <span className="text-[11px] font-semibold uppercase tracking-wider text-slate-500 font-mono">
          {label}
        </span>
        <div className="p-2 rounded-lg bg-indigo-50 text-indigo-600 border border-indigo-100">
          <Icon size={18} />
        </div>
      </div>

      <div className="flex items-baseline space-x-3 mb-2">
        <span className="text-3xl font-bold font-mono tracking-tight text-slate-900">
          {value}
        </span>
      </div>

      <div className="flex items-center text-xs text-slate-500 font-medium">
        {growthRate !== undefined && (
          <span
            className={`inline-flex items-center px-2 py-0.5 rounded-full font-mono text-[11px] font-semibold mr-2 ${
              isPositive
                ? 'bg-emerald-50 text-emerald-700 border border-emerald-200'
                : 'bg-red-50 text-red-700 border border-red-200'
            }`}
          >
            {isPositive ? (
              <TrendingUp size={12} className="mr-1 inline" />
            ) : (
              <TrendingDown size={12} className="mr-1 inline" />
            )}
            {isPositive ? `+${growthRate}%` : `${growthRate}%`}
          </span>
        )}
        {subtext && <span>{subtext}</span>}
      </div>
    </div>
  );
};
