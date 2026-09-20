from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional

class ColumnInfo(BaseModel):
    name: str
    inferred_type: str  # 'date', 'currency', 'number', 'category', 'text'
    role: str           # 'time_axis', 'metric', 'geo_dimension', 'category_dimension', 'dimension'
    sample_values: List[Any] = Field(default_factory=list)
    confidence: float = 1.0

class SchemaDetectionResult(BaseModel):
    dataset_id: str
    filename: str
    total_rows_estimated: int
    file_size_bytes: int
    columns: List[ColumnInfo]

class ColumnOverrideRequest(BaseModel):
    dataset_id: str
    columns: List[ColumnInfo]

class AnalysisJobStatus(BaseModel):
    job_id: str
    dataset_id: str
    status: str  # 'queued', 'uploading_hdfs', 'running_mapreduce', 'aggregating', 'completed', 'failed'
    progress_pct: int
    current_step: str
    error_message: Optional[str] = None
    execution_time_seconds: Optional[float] = None

class KPIData(BaseModel):
    total_revenue: float
    total_orders: int
    avg_order_value: float
    total_units_sold: int
    growth_rate: float
    avg_discount_pct: Optional[float] = 0.0

class TimeSeriesPoint(BaseModel):
    period: str
    revenue: float
    orders: int
    units: int

class ProductPerformance(BaseModel):
    product_name: str
    revenue: float
    units_sold: int

class RegionalSales(BaseModel):
    region: str
    revenue: float
    orders: int
    percentage: float

class CategoryBreakdown(BaseModel):
    category: str
    revenue: float
    orders: int
    percentage: float

class OrderStatusBreakdown(BaseModel):
    status: str
    orders: int
    revenue: float
    percentage: float

class SalesChannelBreakdown(BaseModel):
    channel: str
    revenue: float
    orders: int
    percentage: float

class PaymentMethodBreakdown(BaseModel):
    payment_method: str
    revenue: float
    orders: int
    percentage: float

class CityPerformance(BaseModel):
    city: str
    revenue: float
    orders: int
    units_sold: int

class SalespersonPerformance(BaseModel):
    salesperson: str
    revenue: float
    orders: int
    units_sold: int

class CustomerAnalytics(BaseModel):
    total_unique_customers: int
    avg_customer_spend: float
    top_customers: List[Dict[str, Any]] = Field(default_factory=list)

class DiscountAnalytics(BaseModel):
    avg_discount_pct: float
    discount_ranges: List[Dict[str, Any]] = Field(default_factory=list)

class DataQualityReport(BaseModel):
    total_rows_uploaded: int
    valid_rows: int
    invalid_rows: int
    duplicate_rows: int
    duplicate_sale_ids: int
    missing_values: int
    invalid_dates: int
    invalid_numeric_values: int
    normalized_values: int
    rejected_records: int

class DashboardAnalytics(BaseModel):
    dataset_id: str
    filename: str
    total_rows_processed: int
    execution_stats: Dict[str, Any]
    kpis: KPIData
    sales_trend: List[TimeSeriesPoint]
    top_products: List[ProductPerformance]
    regional_sales: List[RegionalSales]
    category_breakdown: List[CategoryBreakdown]
    order_status_breakdown: List[OrderStatusBreakdown] = Field(default_factory=list)
    sales_channel_breakdown: List[SalesChannelBreakdown] = Field(default_factory=list)
    payment_method_breakdown: List[PaymentMethodBreakdown] = Field(default_factory=list)
    city_performance: List[CityPerformance] = Field(default_factory=list)
    salesperson_performance: List[SalespersonPerformance] = Field(default_factory=list)
    customer_analytics: Optional[CustomerAnalytics] = None
    discount_analytics: Optional[DiscountAnalytics] = None
    data_quality: DataQualityReport
    raw_sample: List[Dict[str, Any]]
