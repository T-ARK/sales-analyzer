export type ColumnType = 'date' | 'currency' | 'number' | 'category' | 'text';
export type ColumnRole = 'time_axis' | 'metric' | 'geo_dimension' | 'category_dimension' | 'dimension';

export interface ColumnInfo {
  name: string;
  inferred_type: ColumnType;
  role: ColumnRole;
  sample_values: (string | number)[];
  confidence: number;
}

export interface SchemaDetectionResult {
  dataset_id: string;
  filename: string;
  total_rows_estimated: number;
  file_size_bytes: number;
  columns: ColumnInfo[];
}

export interface JobStatus {
  job_id: string;
  dataset_id: string;
  status: 'queued' | 'uploading_hdfs' | 'running_mapreduce' | 'aggregating' | 'completed' | 'failed';
  progress_pct: number;
  current_step: string;
  error_message?: string;
  execution_time_seconds?: number;
}

export interface KPIData {
  total_revenue: number;
  total_orders: number;
  avg_order_value: number;
  total_units_sold: number;
  growth_rate: number;
  avg_discount_pct?: number;
}

export interface TimeSeriesPoint {
  period: string;
  revenue: number;
  orders: number;
  units: number;
}

export interface ProductPerformance {
  product_name: string;
  revenue: number;
  units_sold: number;
}

export interface RegionalSales {
  region: string;
  revenue: number;
  orders: number;
  percentage: number;
}

export interface CategoryBreakdown {
  category: string;
  revenue: number;
  orders: number;
  percentage: number;
}

export interface OrderStatusBreakdown {
  status: string;
  orders: number;
  revenue: number;
  percentage: number;
}

export interface SalesChannelBreakdown {
  channel: string;
  revenue: number;
  orders: number;
  percentage: number;
}

export interface PaymentMethodBreakdown {
  payment_method: string;
  revenue: number;
  orders: number;
  percentage: number;
}

export interface CityPerformance {
  city: string;
  revenue: number;
  orders: number;
  units_sold: number;
}

export interface SalespersonPerformance {
  salesperson: string;
  revenue: number;
  orders: number;
  units_sold: number;
}

export interface CustomerAnalytics {
  total_unique_customers: number;
  avg_customer_spend: number;
  top_customers: Record<string, any>[];
}

export interface DiscountAnalytics {
  avg_discount_pct: number;
  discount_ranges: Record<string, any>[];
}

export interface DataQualityReport {
  total_rows_uploaded: number;
  valid_rows: number;
  invalid_rows: number;
  duplicate_rows: number;
  duplicate_sale_ids: number;
  missing_values: number;
  invalid_dates: number;
  invalid_numeric_values: number;
  normalized_values: number;
  rejected_records: number;
}

export interface DashboardAnalytics {
  dataset_id: string;
  filename: string;
  total_rows_processed: number;
  execution_stats: {
    engine: string;
    execution_time_sec: number;
    data_splits: number;
    reducers_completed: number;
    hdfs_block_size: string;
  };
  kpis: KPIData;
  sales_trend: TimeSeriesPoint[];
  top_products: ProductPerformance[];
  regional_sales: RegionalSales[];
  category_breakdown: CategoryBreakdown[];
  order_status_breakdown: OrderStatusBreakdown[];
  sales_channel_breakdown: SalesChannelBreakdown[];
  payment_method_breakdown: PaymentMethodBreakdown[];
  city_performance: CityPerformance[];
  salesperson_performance: SalespersonPerformance[];
  customer_analytics?: CustomerAnalytics;
  discount_analytics?: DiscountAnalytics;
  data_quality: DataQualityReport;
  raw_sample: Record<string, any>[];
}
