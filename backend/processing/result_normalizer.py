"""
Result normalizer for converting raw engine outputs to standard DashboardAnalytics model.
Guarantees backward compatibility with existing frontend expectations.
"""
import os
import csv
from typing import Dict, Any, List, Optional
from models import (
    DashboardAnalytics, KPIData, TimeSeriesPoint, ProductPerformance,
    RegionalSales, CategoryBreakdown, OrderStatusBreakdown, SalesChannelBreakdown,
    PaymentMethodBreakdown, CityPerformance, SalespersonPerformance,
    CustomerAnalytics, DiscountAnalytics, DataQualityReport
)


def extract_raw_sample(file_path: str, max_rows: int = 50) -> List[Dict[str, Any]]:
    """
    Stream only the top sample rows (up to 50) using standard library csv.reader
    without loading the entire dataset into memory.
    """
    if not os.path.exists(file_path):
        return []
    
    sample: List[Dict[str, Any]] = []
    try:
        if file_path.endswith('.csv'):
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                reader = csv.DictReader(f)
                for i, row in enumerate(reader):
                    if i >= max_rows:
                        break
                    # Clean empty/None values to empty string for frontend display
                    clean_row = {str(k).strip(): (v if v is not None else '') for k, v in row.items()}
                    sample.append(clean_row)
        else:
            # Fallback for non-CSV files (e.g., xlsx)
            import openpyxl
            wb = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
            sheet = wb.active
            rows = sheet.iter_rows(values_only=True)
            header = next(rows, None)
            if header:
                header_strs = [str(h).strip() if h is not None else f"col_{idx}" for idx, h in enumerate(header)]
                for i, row in enumerate(rows):
                    if i >= max_rows:
                        break
                    row_dict = {header_strs[idx]: (str(val) if val is not None else '') for idx, val in enumerate(row) if idx < len(header_strs)}
                    sample.append(row_dict)
            wb.close()
    except Exception as e:
        print(f"[SAMPLE_EXTRACT_WARN] Could not extract raw sample: {e}")
    
    return sample


def normalize_to_dashboard_analytics(
    raw_result: Dict[str, Any],
    dataset_id: str,
    file_path: str,
    engine_name: str,
    execution_time_sec: float,
    additional_stats: Optional[Dict[str, Any]] = None
) -> DashboardAnalytics:
    """
    Transforms engine-specific analytics dictionaries into standard DashboardAnalytics.
    Validates and guarantees all expected fields exist and match frontend contracts.
    """
    filename = os.path.basename(file_path)
    additional_stats = additional_stats or {}

    # Extract KPI values with safe defaults
    raw_kpis = raw_result.get('kpis', {})
    total_rev = round(float(raw_kpis.get('total_revenue', 0.0)), 2)
    total_orders = int(raw_kpis.get('total_orders', 0))
    avg_order_val = round(float(raw_kpis.get('avg_order_value', 0.0)), 2)
    total_units = int(raw_kpis.get('total_units_sold', 0))
    growth_rate = round(float(raw_kpis.get('growth_rate', 0.0)), 2)
    avg_discount = round(float(raw_kpis.get('avg_discount_pct', 0.0)), 2)

    kpis = KPIData(
        total_revenue=total_rev,
        total_orders=total_orders,
        avg_order_value=avg_order_val,
        total_units_sold=total_units,
        growth_rate=growth_rate,
        avg_discount_pct=avg_discount
    )

    # 1. Sales Trend
    sales_trend: List[TimeSeriesPoint] = []
    for item in raw_result.get('sales_trend', []):
        sales_trend.append(TimeSeriesPoint(
            period=str(item.get('period', '')),
            revenue=round(float(item.get('revenue', 0.0)), 2),
            orders=int(item.get('orders', 0)),
            units=int(item.get('units', 0))
        ))
    sales_trend.sort(key=lambda x: x.period)

    # 2. Top Products
    top_products: List[ProductPerformance] = []
    for item in raw_result.get('top_products', []):
        top_products.append(ProductPerformance(
            product_name=str(item.get('product_name', '')),
            revenue=round(float(item.get('revenue', 0.0)), 2),
            units_sold=int(item.get('units_sold', 0))
        ))
    top_products.sort(key=lambda x: x.revenue, reverse=True)
    top_products = top_products[:10]

    # 3. Regional Sales
    regional_sales: List[RegionalSales] = []
    for item in raw_result.get('regional_sales', []):
        regional_sales.append(RegionalSales(
            region=str(item.get('region', '')),
            revenue=round(float(item.get('revenue', 0.0)), 2),
            orders=int(item.get('orders', 0)),
            percentage=round(float(item.get('percentage', 0.0)), 1)
        ))

    # 4. Category Breakdown
    category_breakdown: List[CategoryBreakdown] = []
    for item in raw_result.get('category_breakdown', []):
        category_breakdown.append(CategoryBreakdown(
            category=str(item.get('category', '')),
            revenue=round(float(item.get('revenue', 0.0)), 2),
            orders=int(item.get('orders', 0)),
            percentage=round(float(item.get('percentage', 0.0)), 1)
        ))
    category_breakdown.sort(key=lambda x: x.revenue, reverse=True)

    # 5. Order Status Breakdown
    order_status_breakdown: List[OrderStatusBreakdown] = []
    for item in raw_result.get('order_status_breakdown', []):
        order_status_breakdown.append(OrderStatusBreakdown(
            status=str(item.get('status', '')),
            orders=int(item.get('orders', 0)),
            revenue=round(float(item.get('revenue', 0.0)), 2),
            percentage=round(float(item.get('percentage', 0.0)), 1)
        ))
    order_status_breakdown.sort(key=lambda x: x.orders, reverse=True)

    # 6. Sales Channel Breakdown
    sales_channel_breakdown: List[SalesChannelBreakdown] = []
    for item in raw_result.get('sales_channel_breakdown', []):
        sales_channel_breakdown.append(SalesChannelBreakdown(
            channel=str(item.get('channel', '')),
            revenue=round(float(item.get('revenue', 0.0)), 2),
            orders=int(item.get('orders', 0)),
            percentage=round(float(item.get('percentage', 0.0)), 1)
        ))
    sales_channel_breakdown.sort(key=lambda x: x.revenue, reverse=True)

    # 7. Payment Method Breakdown
    payment_method_breakdown: List[PaymentMethodBreakdown] = []
    for item in raw_result.get('payment_method_breakdown', []):
        payment_method_breakdown.append(PaymentMethodBreakdown(
            payment_method=str(item.get('payment_method', '')),
            revenue=round(float(item.get('revenue', 0.0)), 2),
            orders=int(item.get('orders', 0)),
            percentage=round(float(item.get('percentage', 0.0)), 1)
        ))
    payment_method_breakdown.sort(key=lambda x: x.revenue, reverse=True)

    # 8. City Performance
    city_performance: List[CityPerformance] = []
    for item in raw_result.get('city_performance', []):
        city_performance.append(CityPerformance(
            city=str(item.get('city', '')),
            revenue=round(float(item.get('revenue', 0.0)), 2),
            orders=int(item.get('orders', 0)),
            units_sold=int(item.get('units_sold', 0))
        ))
    city_performance.sort(key=lambda x: x.revenue, reverse=True)
    city_performance = city_performance[:10]

    # If regional_sales is empty but city_performance exists, derive regional sales from top cities
    if not regional_sales and city_performance:
        regional_sales = [
            RegionalSales(
                region=c.city,
                revenue=c.revenue,
                orders=c.orders,
                percentage=round((c.revenue / total_rev * 100), 1) if total_rev > 0 else 0.0
            ) for c in city_performance[:5]
        ]

    # 9. Salesperson Performance
    salesperson_performance: List[SalespersonPerformance] = []
    for item in raw_result.get('salesperson_performance', []):
        salesperson_performance.append(SalespersonPerformance(
            salesperson=str(item.get('salesperson', '')),
            revenue=round(float(item.get('revenue', 0.0)), 2),
            orders=int(item.get('orders', 0)),
            units_sold=int(item.get('units_sold', 0))
        ))
    salesperson_performance.sort(key=lambda x: x.revenue, reverse=True)
    salesperson_performance = salesperson_performance[:10]

    # 10. Customer Analytics
    raw_customer = raw_result.get('customer_analytics', {})
    customer_analytics = None
    if raw_customer:
        customer_analytics = CustomerAnalytics(
            total_unique_customers=int(raw_customer.get('total_unique_customers', 0)),
            avg_customer_spend=round(float(raw_customer.get('avg_customer_spend', 0.0)), 2),
            top_customers=raw_customer.get('top_customers', [])[:10]
        )

    # 11. Discount Analytics
    raw_discount = raw_result.get('discount_analytics', {})
    discount_analytics = None
    if raw_discount:
        discount_analytics = DiscountAnalytics(
            avg_discount_pct=round(float(raw_discount.get('avg_discount_pct', avg_discount)), 2),
            discount_ranges=raw_discount.get('discount_ranges', [])
        )

    # 12. Data Quality
    raw_dq = raw_result.get('data_quality', {})
    total_uploaded = int(raw_dq.get('total_rows_uploaded', 0))
    valid_rows = int(raw_dq.get('valid_rows', 0))
    duplicate_rows = int(raw_dq.get('duplicate_rows', max(0, valid_rows - total_orders)))
    duplicate_ids = int(raw_dq.get('duplicate_sale_ids', duplicate_rows))

    data_quality = DataQualityReport(
        total_rows_uploaded=total_uploaded,
        valid_rows=valid_rows,
        invalid_rows=int(raw_dq.get('invalid_rows', max(0, total_uploaded - valid_rows))),
        duplicate_rows=duplicate_rows,
        duplicate_sale_ids=duplicate_ids,
        missing_values=int(raw_dq.get('missing_values', 0)),
        invalid_dates=int(raw_dq.get('invalid_dates', 0)),
        invalid_numeric_values=int(raw_dq.get('invalid_numeric_values', 0)),
        normalized_values=int(raw_dq.get('normalized_values', 0)),
        rejected_records=int(raw_dq.get('rejected_records', max(0, total_uploaded - valid_rows)))
    )

    # Raw sample
    raw_sample = raw_result.get('raw_sample')
    if not raw_sample:
        raw_sample = extract_raw_sample(file_path, 50)

    # Determine execution statistics ensuring backwards compatibility
    splits = additional_stats.get('data_splits', max(1, int(valid_rows / 25000) if valid_rows > 0 else 1))
    reducers = additional_stats.get('reducers_completed', 1)
    block_size = additional_stats.get('hdfs_block_size', '128MB' if 'Hadoop' in engine_name else 'N/A')

    execution_stats = {
        'engine': engine_name,
        'execution_time_sec': round(execution_time_sec, 3),
        'data_splits': splits,
        'reducers_completed': reducers,
        'hdfs_block_size': block_size,
        'processing_engine': 'hadoop' if 'Hadoop' in engine_name else 'pyspark',
        'processing_status': 'success',
        **additional_stats
    }

    return DashboardAnalytics(
        dataset_id=dataset_id,
        filename=filename,
        total_rows_processed=valid_rows if valid_rows > 0 else total_uploaded,
        execution_stats=execution_stats,
        kpis=kpis,
        sales_trend=sales_trend,
        top_products=top_products,
        regional_sales=regional_sales,
        category_breakdown=category_breakdown,
        order_status_breakdown=order_status_breakdown,
        sales_channel_breakdown=sales_channel_breakdown,
        payment_method_breakdown=payment_method_breakdown,
        city_performance=city_performance,
        salesperson_performance=salesperson_performance,
        customer_analytics=customer_analytics,
        discount_analytics=discount_analytics,
        data_quality=data_quality,
        raw_sample=raw_sample
    )
