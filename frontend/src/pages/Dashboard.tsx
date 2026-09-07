import React, { useState, useEffect, useMemo } from 'react';
import { DashboardAnalytics } from '../types';
import { getDashboardResults } from '../api';
import { KpiCard } from '../components/KpiCard';
import {
  DollarSign, ShoppingBag, TrendingUp, Layers, Package,
  Download, RefreshCw, Search, Server, Clock, ShieldCheck,
  CreditCard, Globe, MapPin, UserCheck, Users, Percent, AlertCircle, CheckCircle2
} from 'lucide-react';
import {
  AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  BarChart, Bar, PieChart, Pie, Cell, Legend
} from 'recharts';

interface DashboardProps {
  datasetId: string;
  onReset: () => void;
  secondsRemaining?: number;
}

const CATEGORY_COLORS = ['#4F46E5', '#0284C7', '#059669', '#D97706', '#EC4899', '#8B5CF6'];
const STATUS_COLORS: Record<string, string> = {
  Completed: '#059669',
  Pending: '#D97706',
  Returned: '#0284C7',
  Cancelled: '#DC2626'
};

export const Dashboard: React.FC<DashboardProps> = ({ datasetId, onReset, secondsRemaining }) => {
  const [data, setData] = useState<DashboardAnalytics | null>(null);
  const [loading, setLoading] = useState(true);
  
  // Toggles
  const [trendMetric, setTrendMetric] = useState<'revenue' | 'orders' | 'units'>('revenue');
  const [topProductMetric, setTopProductMetric] = useState<'revenue' | 'units'>('revenue');
  const [cityMetric, setCityMetric] = useState<'revenue' | 'orders'>('revenue');
  const [salespersonMetric, setSalespersonMetric] = useState<'revenue' | 'orders'>('revenue');
  const [searchQuery, setSearchQuery] = useState('');

  useEffect(() => {
    const fetchResults = async () => {
      try {
        const res = await getDashboardResults(datasetId);
        setData(res);
      } catch (err) {
        alert('Failed to load dashboard analytics results.');
      } finally {
        setLoading(false);
      }
    };
    fetchResults();
  }, [datasetId]);

  const filteredRawSample = useMemo(() => {
    if (!data?.raw_sample) return [];
    return data.raw_sample.filter((row) => {
      return searchQuery === '' || Object.values(row).some(
        val => String(val).toLowerCase().includes(searchQuery.toLowerCase())
      );
    });
  }, [data?.raw_sample, searchQuery]);

  const handleExportCSV = () => {
    if (!data?.raw_sample || data.raw_sample.length === 0) return;
    const headers = Object.keys(data.raw_sample[0]);
    const csvRows = [headers.join(',')];
    data.raw_sample.forEach(row => {
      const values = headers.map(h => `"${String(row[h] || '').replace(/"/g, '""')}"`);
      csvRows.push(values.join(','));
    });
    const blob = new Blob([csvRows.join('\n')], { type: 'text/csv' });
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `analytics_report_${datasetId}.csv`;
    a.click();
  };

  const formatTimer = (totalSecs?: number) => {
    if (totalSecs === undefined) return '10:00';
    const m = Math.floor(totalSecs / 60);
    const s = totalSecs % 60;
    return `${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}`;
  };

  const formatCompactCurrency = (val: number) => {
    if (val >= 1e9) return `$${(val / 1e9).toFixed(2)}B`;
    if (val >= 1e6) return `$${(val / 1e6).toFixed(2)}M`;
    if (val >= 1e3) return `$${(val / 1e3).toFixed(1)}k`;
    return `$${val.toFixed(2)}`;
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-[70vh]">
        <div className="text-center space-y-3">
          <div className="w-10 h-10 border-4 border-indigo-600 border-t-transparent rounded-full animate-spin mx-auto" />
          <p className="text-xs font-mono text-slate-500">Loading MapReduce analytics dashboard...</p>
        </div>
      </div>
    );
  }

  if (!data) return null;

  return (
    <div className="max-w-7xl mx-auto px-4 py-8 space-y-8">
      {/* Header Bar */}
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-4 bg-white border border-slate-200 p-6 rounded-2xl shadow-sm">
        <div>
          <div className="flex items-center space-x-2 mb-1">
            <span className="text-[11px] font-mono px-2.5 py-0.5 rounded bg-emerald-50 text-emerald-700 border border-emerald-200 font-semibold flex items-center space-x-1">
              <CheckCircle2 size={12} />
              <span>MapReduce Processing Complete</span>
            </span>
            <span className="text-xs text-slate-500 font-mono">
              • {data.total_rows_processed.toLocaleString()} Rows Processed in {data.execution_stats.execution_time_sec}s
            </span>
          </div>
          <h1 className="text-2xl sm:text-3xl font-bold text-slate-900 tracking-tight">
            Sales Data Analytics Dashboard
          </h1>
          <p className="text-xs text-slate-500 mt-1">
            Dataset: <span className="font-mono text-slate-800 font-semibold">{data.filename}</span> ({data.execution_stats.engine})
          </p>
        </div>

        <div className="flex items-center space-x-3">
          <div className="hidden sm:flex items-center space-x-2 text-xs text-amber-800 bg-amber-50 px-3 py-2 rounded-xl border border-amber-200 font-mono font-medium">
            <Clock size={14} className="text-amber-600 animate-pulse" />
            <span>Auto-flush in {formatTimer(secondsRemaining)}</span>
          </div>

          <button
            onClick={handleExportCSV}
            className="px-4 py-2.5 rounded-xl bg-white hover:bg-slate-50 border border-slate-300 text-slate-700 text-xs font-semibold flex items-center space-x-2 transition-all shadow-sm"
          >
            <Download size={14} className="text-sky-600" />
            <span>Export CSV</span>
          </button>

          <button
            onClick={onReset}
            className="px-4 py-2.5 rounded-xl bg-indigo-600 hover:bg-indigo-700 text-white text-xs font-bold flex items-center space-x-2 transition-all shadow-md active:scale-95"
          >
            <RefreshCw size={14} />
            <span>Flush & New Upload</span>
          </button>
        </div>
      </div>

      {/* KPI Cards Row */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-4">
        <KpiCard
          label="Total Sales Revenue"
          value={formatCompactCurrency(data.kpis.total_revenue)}
          subtext={`Exact: $${data.kpis.total_revenue.toLocaleString('en-US', { minimumFractionDigits: 2 })}`}
          icon={DollarSign}
          accentColor="border-indigo-200"
        />

        <KpiCard
          label="Total Orders"
          value={data.kpis.total_orders.toLocaleString()}
          subtext="Unique transactions count"
          icon={ShoppingBag}
          accentColor="border-sky-200"
        />

        <KpiCard
          label="Total Units Sold"
          value={data.kpis.total_units_sold.toLocaleString()}
          subtext="SUM(Quantity) aggregated"
          icon={Package}
          accentColor="border-emerald-200"
        />

        <KpiCard
          label="Average Order Value"
          value={`$${data.kpis.avg_order_value.toLocaleString('en-US', { minimumFractionDigits: 2 })}`}
          subtext="Total Revenue / Total Orders"
          icon={TrendingUp}
          accentColor="border-amber-200"
        />

        <KpiCard
          label="Avg Discount"
          value={`${data.kpis.avg_discount_pct || 0}%`}
          subtext="Normalized discount %"
          icon={Percent}
          accentColor="border-purple-200"
        />
      </div>

      {/* CHARTS 1: Monthly Sales Trend */}
      <div className="bg-white border border-slate-200 rounded-2xl p-6 shadow-sm">
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4 mb-6 pb-4 border-b border-slate-200">
          <div>
            <h3 className="text-base font-bold text-slate-900 flex items-center space-x-2">
              <TrendingUp size={18} className="text-indigo-600" />
              <span>Monthly Sales Trend (YYYY-MM)</span>
            </h3>
            <p className="text-xs text-slate-500">Grouped dynamically by Year + Month from parsed transaction dates</p>
          </div>

          <div className="flex items-center bg-slate-100 p-1 rounded-xl border border-slate-200 text-xs font-mono">
            <button
              onClick={() => setTrendMetric('revenue')}
              className={`px-3 py-1 rounded-lg transition-all ${
                trendMetric === 'revenue' ? 'bg-indigo-600 text-white font-bold' : 'text-slate-600 hover:text-slate-900'
              }`}
            >
              Revenue
            </button>
            <button
              onClick={() => setTrendMetric('orders')}
              className={`px-3 py-1 rounded-lg transition-all ${
                trendMetric === 'orders' ? 'bg-indigo-600 text-white font-bold' : 'text-slate-600 hover:text-slate-900'
              }`}
            >
              Orders
            </button>
            <button
              onClick={() => setTrendMetric('units')}
              className={`px-3 py-1 rounded-lg transition-all ${
                trendMetric === 'units' ? 'bg-indigo-600 text-white font-bold' : 'text-slate-600 hover:text-slate-900'
              }`}
            >
              Units
            </button>
          </div>
        </div>

        <div className="h-72 w-full">
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={data.sales_trend} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
              <defs>
                <linearGradient id="colorTrend" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#4F46E5" stopOpacity={0.3} />
                  <stop offset="95%" stopColor="#4F46E5" stopOpacity={0.0} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" vertical={false} />
              <XAxis dataKey="period" stroke="#64748B" fontSize={11} tickLine={false} />
              <YAxis
                stroke="#64748B"
                fontSize={11}
                tickLine={false}
                tickFormatter={(val) => trendMetric === 'revenue' ? formatCompactCurrency(val) : val.toLocaleString()}
              />
              <Tooltip
                contentStyle={{ backgroundColor: '#FFFFFF', borderColor: '#CBD5E1', borderRadius: '12px', fontSize: '12px', color: '#0F172A', boxShadow: '0 4px 6px -1px rgba(0,0,0,0.1)' }}
                formatter={(val: any) => [trendMetric === 'revenue' ? `$${Number(val).toLocaleString()}` : val.toLocaleString(), trendMetric.toUpperCase()]}
              />
              <Area type="monotone" dataKey={trendMetric} stroke="#4F46E5" strokeWidth={3} fillOpacity={1} fill="url(#colorTrend)" />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* CHARTS 2: Top Products & Category Revenue Breakdown */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Top 10 Products */}
        <div className="bg-white border border-slate-200 rounded-2xl p-6 shadow-sm flex flex-col justify-between">
          <div>
            <div className="flex items-center justify-between mb-6 pb-4 border-b border-slate-200">
              <div>
                <h3 className="text-base font-bold text-slate-900 flex items-center space-x-2">
                  <Package size={18} className="text-sky-600" />
                  <span>Top 10 Products Performance</span>
                </h3>
                <p className="text-xs text-slate-500">Ranked by revenue or units sold from MapReduce</p>
              </div>

              <div className="flex items-center bg-slate-100 p-1 rounded-xl border border-slate-200 text-xs font-mono">
                <button
                  onClick={() => setTopProductMetric('revenue')}
                  className={`px-3 py-1 rounded-lg transition-all ${
                    topProductMetric === 'revenue' ? 'bg-indigo-600 text-white font-bold' : 'text-slate-600 hover:text-slate-900'
                  }`}
                >
                  Revenue
                </button>
                <button
                  onClick={() => setTopProductMetric('units')}
                  className={`px-3 py-1 rounded-lg transition-all ${
                    topProductMetric === 'units' ? 'bg-indigo-600 text-white font-bold' : 'text-slate-600 hover:text-slate-900'
                  }`}
                >
                  Units
                </button>
              </div>
            </div>

            <div className="h-72 w-full">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={data.top_products} layout="vertical" margin={{ top: 5, right: 20, left: 40, bottom: 5 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" horizontal={false} />
                  <XAxis type="number" stroke="#64748B" fontSize={11} tickFormatter={(val) => topProductMetric === 'revenue' ? formatCompactCurrency(val) : val.toLocaleString()} />
                  <YAxis dataKey="product_name" type="category" stroke="#475569" fontSize={10} width={120} tickLine={false} />
                  <Tooltip
                    contentStyle={{ backgroundColor: '#FFFFFF', borderColor: '#CBD5E1', borderRadius: '12px', fontSize: '12px', color: '#0F172A', boxShadow: '0 4px 6px -1px rgba(0,0,0,0.1)' }}
                    formatter={(val: any) => [topProductMetric === 'revenue' ? `$${Number(val).toLocaleString()}` : val.toLocaleString(), topProductMetric === 'revenue' ? 'Revenue' : 'Units Sold']}
                  />
                  <Bar
                    dataKey={topProductMetric === 'revenue' ? 'revenue' : 'units_sold'}
                    fill={topProductMetric === 'revenue' ? '#0284C7' : '#059669'}
                    radius={[0, 6, 6, 0]}
                  />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>
        </div>

        {/* Category Revenue Breakdown (ACTUAL Category Field!) */}
        <div className="bg-white border border-slate-200 rounded-2xl p-6 shadow-sm flex flex-col justify-between">
          <div>
            <div className="flex items-center justify-between mb-6 pb-4 border-b border-slate-200">
              <div>
                <h3 className="text-base font-bold text-slate-900 flex items-center space-x-2">
                  <Layers size={18} className="text-emerald-600" />
                  <span>Category Revenue Breakdown</span>
                </h3>
                <p className="text-xs text-slate-500">Actual Category field normalized across all dataset rows</p>
              </div>
            </div>

            <div className="h-72 w-full flex items-center justify-center">
              <ResponsiveContainer width="100%" height="100%">
                <PieChart>
                  <Pie
                    data={data.category_breakdown}
                    dataKey="revenue"
                    nameKey="category"
                    cx="50%"
                    cy="50%"
                    innerRadius={55}
                    outerRadius={90}
                    paddingAngle={4}
                  >
                    {data.category_breakdown.map((_, index) => (
                      <Cell key={`cell-${index}`} fill={CATEGORY_COLORS[index % CATEGORY_COLORS.length]} stroke="#FFFFFF" strokeWidth={2} />
                    ))}
                  </Pie>
                  <Tooltip
                    contentStyle={{ backgroundColor: '#FFFFFF', borderColor: '#CBD5E1', borderRadius: '12px', fontSize: '12px', color: '#0F172A', boxShadow: '0 4px 6px -1px rgba(0,0,0,0.1)' }}
                    formatter={(val: any) => [`$${Number(val).toLocaleString()}`, 'Revenue']}
                  />
                  <Legend
                    formatter={(value) => <span className="text-xs text-slate-700 font-mono">{value}</span>}
                    layout="horizontal"
                    verticalAlign="bottom"
                  />
                </PieChart>
              </ResponsiveContainer>
            </div>
          </div>
        </div>
      </div>

      {/* CHARTS 3: Order Status & Sales Channel */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Order Status Chart */}
        <div className="bg-white border border-slate-200 rounded-2xl p-6 shadow-sm flex flex-col justify-between">
          <div>
            <div className="flex items-center justify-between mb-6 pb-4 border-b border-slate-200">
              <div>
                <h3 className="text-base font-bold text-slate-900 flex items-center space-x-2">
                  <ShieldCheck size={18} className="text-purple-600" />
                  <span>Order Status Distribution</span>
                </h3>
                <p className="text-xs text-slate-500">Normalized order statuses (Completed, Pending, Returned, Cancelled)</p>
              </div>
            </div>

            <div className="h-72 w-full">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={data.order_status_breakdown} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" vertical={false} />
                  <XAxis dataKey="status" stroke="#64748B" fontSize={11} tickLine={false} />
                  <YAxis stroke="#64748B" fontSize={11} tickLine={false} tickFormatter={(val) => val.toLocaleString()} />
                  <Tooltip
                    contentStyle={{ backgroundColor: '#FFFFFF', borderColor: '#CBD5E1', borderRadius: '12px', fontSize: '12px', color: '#0F172A', boxShadow: '0 4px 6px -1px rgba(0,0,0,0.1)' }}
                    formatter={(val: any, name: any, props: any) => [val.toLocaleString(), `Orders (${props.payload.percentage}%)`]}
                  />
                  <Bar dataKey="orders" radius={[6, 6, 0, 0]}>
                    {data.order_status_breakdown.map((entry, idx) => (
                      <Cell key={`status-cell-${idx}`} fill={STATUS_COLORS[entry.status] || CATEGORY_COLORS[idx % CATEGORY_COLORS.length]} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>
        </div>

        {/* Sales Channel Analysis */}
        <div className="bg-white border border-slate-200 rounded-2xl p-6 shadow-sm flex flex-col justify-between">
          <div>
            <div className="flex items-center justify-between mb-6 pb-4 border-b border-slate-200">
              <div>
                <h3 className="text-base font-bold text-slate-900 flex items-center space-x-2">
                  <Globe size={18} className="text-indigo-600" />
                  <span>Sales Channel Analysis</span>
                </h3>
                <p className="text-xs text-slate-500">Revenue & transaction breakdown across Online, Retail, Marketplace, Store</p>
              </div>
            </div>

            <div className="h-72 w-full">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={data.sales_channel_breakdown} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" vertical={false} />
                  <XAxis dataKey="channel" stroke="#64748B" fontSize={11} tickLine={false} />
                  <YAxis stroke="#64748B" fontSize={11} tickLine={false} tickFormatter={(val) => formatCompactCurrency(val)} />
                  <Tooltip
                    contentStyle={{ backgroundColor: '#FFFFFF', borderColor: '#CBD5E1', borderRadius: '12px', fontSize: '12px', color: '#0F172A', boxShadow: '0 4px 6px -1px rgba(0,0,0,0.1)' }}
                    formatter={(val: any) => [`$${Number(val).toLocaleString()}`, 'Revenue']}
                  />
                  <Bar dataKey="revenue" fill="#4F46E5" radius={[6, 6, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>
        </div>
      </div>

      {/* CHARTS 4: Payment Method & City Performance */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Payment Method Breakdown */}
        <div className="bg-white border border-slate-200 rounded-2xl p-6 shadow-sm flex flex-col justify-between">
          <div>
            <div className="flex items-center justify-between mb-6 pb-4 border-b border-slate-200">
              <div>
                <h3 className="text-base font-bold text-slate-900 flex items-center space-x-2">
                  <CreditCard size={18} className="text-amber-600" />
                  <span>Payment Method Analysis</span>
                </h3>
                <p className="text-xs text-slate-500">Normalized payment methods (Net Banking, UPI, Credit Card, Cash, etc.)</p>
              </div>
            </div>

            <div className="h-72 w-full">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={data.payment_method_breakdown} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" vertical={false} />
                  <XAxis dataKey="payment_method" stroke="#64748B" fontSize={11} tickLine={false} />
                  <YAxis stroke="#64748B" fontSize={11} tickLine={false} tickFormatter={(val) => formatCompactCurrency(val)} />
                  <Tooltip
                    contentStyle={{ backgroundColor: '#FFFFFF', borderColor: '#CBD5E1', borderRadius: '12px', fontSize: '12px', color: '#0F172A', boxShadow: '0 4px 6px -1px rgba(0,0,0,0.1)' }}
                    formatter={(val: any) => [`$${Number(val).toLocaleString()}`, 'Revenue']}
                  />
                  <Bar dataKey="revenue" fill="#D97706" radius={[6, 6, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>
        </div>

        {/* Top 10 Cities (Geographic Analysis) */}
        {data.city_performance && data.city_performance.length > 0 && (
          <div className="bg-white border border-slate-200 rounded-2xl p-6 shadow-sm flex flex-col justify-between">
            <div>
              <div className="flex items-center justify-between mb-6 pb-4 border-b border-slate-200">
                <div>
                  <h3 className="text-base font-bold text-slate-900 flex items-center space-x-2">
                    <MapPin size={18} className="text-emerald-600" />
                    <span>Top Cities Performance</span>
                  </h3>
                  <p className="text-xs text-slate-500">Ranked by revenue or order count</p>
                </div>

                <div className="flex items-center bg-slate-100 p-1 rounded-xl border border-slate-200 text-xs font-mono">
                  <button
                    onClick={() => setCityMetric('revenue')}
                    className={`px-3 py-1 rounded-lg transition-all ${
                      cityMetric === 'revenue' ? 'bg-indigo-600 text-white font-bold' : 'text-slate-600 hover:text-slate-900'
                    }`}
                  >
                    Revenue
                  </button>
                  <button
                    onClick={() => setCityMetric('orders')}
                    className={`px-3 py-1 rounded-lg transition-all ${
                      cityMetric === 'orders' ? 'bg-indigo-600 text-white font-bold' : 'text-slate-600 hover:text-slate-900'
                    }`}
                  >
                    Orders
                  </button>
                </div>
              </div>

              <div className="h-72 w-full">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={data.city_performance} layout="vertical" margin={{ top: 5, right: 20, left: 40, bottom: 5 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" horizontal={false} />
                    <XAxis type="number" stroke="#64748B" fontSize={11} tickFormatter={(val) => cityMetric === 'revenue' ? formatCompactCurrency(val) : val.toLocaleString()} />
                    <YAxis dataKey="city" type="category" stroke="#475569" fontSize={10} width={100} tickLine={false} />
                    <Tooltip
                      contentStyle={{ backgroundColor: '#FFFFFF', borderColor: '#CBD5E1', borderRadius: '12px', fontSize: '12px', color: '#0F172A', boxShadow: '0 4px 6px -1px rgba(0,0,0,0.1)' }}
                      formatter={(val: any) => [cityMetric === 'revenue' ? `$${Number(val).toLocaleString()}` : val.toLocaleString(), cityMetric.toUpperCase()]}
                    />
                    <Bar
                      dataKey={cityMetric}
                      fill={cityMetric === 'revenue' ? '#059669' : '#4F46E5'}
                      radius={[0, 6, 6, 0]}
                    />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </div>
          </div>
        )}
      </div>

      {/* DATA QUALITY REPORT SECTION */}
      <div className="bg-white border border-slate-200 rounded-2xl p-6 shadow-sm">
        <div className="flex items-center justify-between mb-6 pb-4 border-b border-slate-200">
          <div>
            <h3 className="text-base font-bold text-slate-900 flex items-center space-x-2">
              <ShieldCheck size={18} className="text-indigo-600" />
              <span>Data Quality & Pipeline Cleaning Report</span>
            </h3>
            <p className="text-xs text-slate-500">Empirical validation statistics generated during MapReduce processing</p>
          </div>

          <span className="text-xs font-mono font-semibold px-3 py-1 rounded-lg bg-emerald-50 text-emerald-700 border border-emerald-200">
            {data.data_quality.valid_rows.toLocaleString()} Valid Records ({(data.data_quality.valid_rows / max1(data.data_quality.total_rows_uploaded) * 100).toFixed(1)}%)
          </span>
        </div>

        <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-5 gap-4 text-center font-mono">
          <div className="p-4 bg-slate-50 rounded-xl border border-slate-200">
            <span className="text-[11px] text-slate-500 block uppercase font-semibold">Uploaded Rows</span>
            <span className="text-lg font-bold text-slate-900">{data.data_quality.total_rows_uploaded.toLocaleString()}</span>
          </div>

          <div className="p-4 bg-emerald-50/60 rounded-xl border border-emerald-200 text-emerald-900">
            <span className="text-[11px] text-emerald-700 block uppercase font-semibold">Valid Rows</span>
            <span className="text-lg font-bold">{data.data_quality.valid_rows.toLocaleString()}</span>
          </div>

          <div className="p-4 bg-slate-50 rounded-xl border border-slate-200">
            <span className="text-[11px] text-slate-500 block uppercase font-semibold">Normalized Values</span>
            <span className="text-lg font-bold text-indigo-600">{data.data_quality.normalized_values.toLocaleString()}</span>
          </div>

          <div className="p-4 bg-slate-50 rounded-xl border border-slate-200">
            <span className="text-[11px] text-slate-500 block uppercase font-semibold">Missing Values Repaired</span>
            <span className="text-lg font-bold text-slate-700">{data.data_quality.missing_values.toLocaleString()}</span>
          </div>

          <div className="p-4 bg-slate-50 rounded-xl border border-slate-200">
            <span className="text-[11px] text-slate-500 block uppercase font-semibold">Rejected Records</span>
            <span className="text-lg font-bold text-slate-700">{data.data_quality.rejected_records.toLocaleString()}</span>
          </div>
        </div>
      </div>

      {/* SAMPLE DATA TABLE */}
      <div className="bg-white border border-slate-200 rounded-2xl p-6 shadow-sm">
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4 mb-6 pb-4 border-b border-slate-200">
          <div>
            <h3 className="text-base font-bold text-slate-900 flex items-center space-x-2">
              <Server size={18} className="text-amber-600" />
              <span>Dataset Record View</span>
            </h3>
            <p className="text-xs text-slate-500">Showing top records from processed dataset ({filteredRawSample.length} displayed)</p>
          </div>

          <div className="flex items-center space-x-3">
            <div className="relative">
              <Search size={14} className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" />
              <input
                type="text"
                placeholder="Search records..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="bg-slate-50 border border-slate-300 text-xs text-slate-800 pl-9 pr-3 py-2 rounded-xl focus:outline-none focus:border-indigo-600 font-mono"
              />
            </div>
          </div>
        </div>

        {/* Scrollable Table */}
        <div className="overflow-x-auto">
          {filteredRawSample.length > 0 ? (
            <table className="w-full text-left border-collapse text-xs font-mono">
              <thead>
                <tr className="border-b border-slate-200 text-slate-600 bg-slate-50">
                  {Object.keys(filteredRawSample[0]).map((col, idx) => (
                    <th key={idx} className="p-3 font-semibold uppercase tracking-wider text-[11px]">
                      {col}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-200">
                {filteredRawSample.slice(0, 25).map((row, rIdx) => (
                  <tr key={rIdx} className="hover:bg-slate-50 text-slate-700 transition-colors">
                    {Object.values(row).map((val: any, cIdx) => (
                      <td key={cIdx} className="p-3 truncate max-w-xs">
                        {String(val)}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <div className="text-center py-8 text-xs text-slate-500 font-mono">
              No matching records found.
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

function max1(val: number): number {
  return val > 0 ? val : 1;
}
