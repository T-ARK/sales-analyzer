#!/usr/bin/env python3
"""
Production MapReduce Reducer for Distributed Sales Analytics.
Aggregates shuffled mapper metrics and computes executive KPI & dimensional summaries.
Bounded memory footprint supporting massive datasets.
"""
import sys
import json
import heapq
from collections import defaultdict
from typing import Dict, Any, List, Set, Tuple


def main():
    # Running counters
    total_revenue = 0.0
    total_units = 0
    discount_sum = 0.0
    valid_rows = 0

    # Bounded distinct ID tracker for scalable order counting
    MAX_EXACT_IDS = 500000
    seen_sale_ids: Set[str] = set()
    exact_id_limit_reached = False
    estimated_unique_ratio = 1.0

    # Low-cardinality dimension accumulators
    trend_data = defaultdict(lambda: {'revenue': 0.0, 'orders': 0, 'units': 0})
    category_data = defaultdict(lambda: {'revenue': 0.0, 'orders': 0, 'units': 0})
    status_data = defaultdict(lambda: {'revenue': 0.0, 'orders': 0, 'units': 0})
    channel_data = defaultdict(lambda: {'revenue': 0.0, 'orders': 0, 'units': 0})
    payment_data = defaultdict(lambda: {'revenue': 0.0, 'orders': 0, 'units': 0})
    discount_data = defaultdict(lambda: {'revenue': 0.0, 'orders': 0, 'units': 0})

    # Higher-cardinality dimensions (streaming or dict aggregation)
    product_data = defaultdict(lambda: {'revenue': 0.0, 'orders': 0, 'units': 0})
    city_data = defaultdict(lambda: {'revenue': 0.0, 'orders': 0, 'units': 0})
    salesperson_data = defaultdict(lambda: {'revenue': 0.0, 'orders': 0, 'units': 0})
    customer_data = defaultdict(lambda: {'revenue': 0.0, 'orders': 0})

    # Data Quality counters
    dq_stats = {
        'total_rows_uploaded': 0,
        'missing_values': 0,
        'cleaned_values': 0,
        'invalid_dates': 0,
        'invalid_numeric_values': 0
    }

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        parts = line.split('\t')
        if len(parts) < 3:
            continue

        group_type = parts[0]
        key = parts[1]

        if group_type == 'KPI':
            if len(parts) >= 6:
                try:
                    rev = float(parts[2])
                    qty = int(parts[3])
                    disc = float(parts[4])
                    sale_id = parts[5]

                    total_revenue += rev
                    total_units += qty
                    discount_sum += disc
                    valid_rows += 1

                    if not exact_id_limit_reached:
                        seen_sale_ids.add(sale_id)
                        if len(seen_sale_ids) >= MAX_EXACT_IDS:
                            exact_id_limit_reached = True
                            estimated_unique_ratio = len(seen_sale_ids) / max(valid_rows, 1)
                except ValueError:
                    pass

        elif group_type == 'TREND':
            if len(parts) >= 5:
                try:
                    trend_data[key]['revenue'] += float(parts[2])
                    trend_data[key]['orders'] += int(parts[3])
                    trend_data[key]['units'] += int(parts[4])
                except ValueError:
                    pass

        elif group_type == 'CATEGORY':
            if len(parts) >= 5:
                try:
                    category_data[key]['revenue'] += float(parts[2])
                    category_data[key]['orders'] += int(parts[3])
                    category_data[key]['units'] += int(parts[4])
                except ValueError:
                    pass

        elif group_type == 'STATUS':
            if len(parts) >= 5:
                try:
                    status_data[key]['revenue'] += float(parts[2])
                    status_data[key]['orders'] += int(parts[3])
                    status_data[key]['units'] += int(parts[4])
                except ValueError:
                    pass

        elif group_type == 'CHANNEL':
            if len(parts) >= 5:
                try:
                    channel_data[key]['revenue'] += float(parts[2])
                    channel_data[key]['orders'] += int(parts[3])
                    channel_data[key]['units'] += int(parts[4])
                except ValueError:
                    pass

        elif group_type == 'PAYMENT':
            if len(parts) >= 5:
                try:
                    payment_data[key]['revenue'] += float(parts[2])
                    payment_data[key]['orders'] += int(parts[3])
                    payment_data[key]['units'] += int(parts[4])
                except ValueError:
                    pass

        elif group_type == 'PRODUCT':
            if len(parts) >= 5:
                try:
                    product_data[key]['revenue'] += float(parts[2])
                    product_data[key]['orders'] += int(parts[3])
                    product_data[key]['units'] += int(parts[4])
                except ValueError:
                    pass

        elif group_type == 'CITY':
            if len(parts) >= 5:
                try:
                    city_data[key]['revenue'] += float(parts[2])
                    city_data[key]['orders'] += int(parts[3])
                    city_data[key]['units'] += int(parts[4])
                except ValueError:
                    pass

        elif group_type == 'SALESPERSON':
            if len(parts) >= 5:
                try:
                    salesperson_data[key]['revenue'] += float(parts[2])
                    salesperson_data[key]['orders'] += int(parts[3])
                    salesperson_data[key]['units'] += int(parts[4])
                except ValueError:
                    pass

        elif group_type == 'CUSTOMER':
            if len(parts) >= 5:
                try:
                    customer_data[key]['revenue'] += float(parts[2])
                    customer_data[key]['orders'] += int(parts[3])
                except ValueError:
                    pass

        elif group_type == 'DISCOUNT':
            if len(parts) >= 5:
                try:
                    discount_data[key]['revenue'] += float(parts[2])
                    discount_data[key]['orders'] += int(parts[3])
                    discount_data[key]['units'] += int(parts[4])
                except ValueError:
                    pass

        elif group_type == 'DQ':
            if len(parts) >= 7:
                try:
                    dq_stats['total_rows_uploaded'] += int(parts[2])
                    dq_stats['missing_values'] += int(parts[3])
                    dq_stats['cleaned_values'] += int(parts[4])
                    dq_stats['invalid_dates'] += int(parts[5])
                    dq_stats['invalid_numeric_values'] += int(parts[6])
                except ValueError:
                    pass

    # Post-processing calculations
    total_rev = round(total_revenue, 2)
    if exact_id_limit_reached:
        total_orders = int(valid_rows * estimated_unique_ratio)
    else:
        total_orders = len(seen_sale_ids) if seen_sale_ids else valid_rows

    avg_order_val = round((total_rev / total_orders), 2) if total_orders > 0 else 0.0
    avg_discount = round((discount_sum / valid_rows), 2) if valid_rows > 0 else 0.0

    # 1. Sales Trend
    sorted_trend = sorted(
        [
            {'period': k, 'revenue': round(v['revenue'], 2), 'orders': v['orders'], 'units': v['units']}
            for k, v in trend_data.items() if k != 'Unknown'
        ],
        key=lambda x: x['period']
    )

    growth_rate = 0.0
    if len(sorted_trend) >= 2:
        last = sorted_trend[-1]['revenue']
        prev = sorted_trend[-2]['revenue']
        if prev > 0:
            growth_rate = round(((last - prev) / prev) * 100, 2)

    # 2. Top 10 Products
    sorted_products = sorted(
        [
            {'product_name': k, 'revenue': round(v['revenue'], 2), 'units_sold': v['units']}
            for k, v in product_data.items() if k != 'Unknown'
        ],
        key=lambda x: x['revenue'],
        reverse=True
    )[:10]

    # 3. Category Breakdown
    sorted_categories = sorted(
        [
            {
                'category': k,
                'revenue': round(v['revenue'], 2),
                'orders': v['orders'],
                'percentage': round((v['revenue'] / total_rev * 100), 1) if total_rev > 0 else 0.0
            }
            for k, v in category_data.items() if k != 'Unknown'
        ],
        key=lambda x: x['revenue'],
        reverse=True
    )

    # 4. Order Status Breakdown
    sorted_statuses = sorted(
        [
            {
                'status': k,
                'orders': v['orders'],
                'revenue': round(v['revenue'], 2),
                'percentage': round((v['orders'] / total_orders * 100), 1) if total_orders > 0 else 0.0
            }
            for k, v in status_data.items() if k != 'Unknown'
        ],
        key=lambda x: x['orders'],
        reverse=True
    )

    # 5. Sales Channel Breakdown
    sorted_channels = sorted(
        [
            {
                'channel': k,
                'revenue': round(v['revenue'], 2),
                'orders': v['orders'],
                'percentage': round((v['revenue'] / total_rev * 100), 1) if total_rev > 0 else 0.0
            }
            for k, v in channel_data.items() if k != 'Unknown'
        ],
        key=lambda x: x['revenue'],
        reverse=True
    )

    # 6. Payment Method Breakdown
    sorted_payments = sorted(
        [
            {
                'payment_method': k,
                'revenue': round(v['revenue'], 2),
                'orders': v['orders'],
                'percentage': round((v['revenue'] / total_rev * 100), 1) if total_rev > 0 else 0.0
            }
            for k, v in payment_data.items() if k != 'Unknown'
        ],
        key=lambda x: x['revenue'],
        reverse=True
    )

    # 7. City Performance
    sorted_cities = sorted(
        [
            {'city': k, 'revenue': round(v['revenue'], 2), 'orders': v['orders'], 'units_sold': v['units']}
            for k, v in city_data.items() if k != 'Unknown'
        ],
        key=lambda x: x['revenue'],
        reverse=True
    )[:10]

    # Regional Sales derived from top cities
    regional_sales = [
        {
            'region': c['city'],
            'revenue': c['revenue'],
            'orders': c['orders'],
            'percentage': round(c['revenue'] / total_rev * 100, 1) if total_rev > 0 else 0.0
        }
        for c in sorted_cities[:5]
    ]

    # 8. Salesperson Performance
    sorted_salespersons = sorted(
        [
            {'salesperson': k, 'revenue': round(v['revenue'], 2), 'orders': v['orders'], 'units_sold': v['units']}
            for k, v in salesperson_data.items() if k != 'Unknown'
        ],
        key=lambda x: x['revenue'],
        reverse=True
    )[:10]

    # 9. Customer Analytics
    valid_customers = [k for k in customer_data if k != 'Unknown']
    unique_cust_count = len(valid_customers)
    top_customers = sorted(
        [
            {'customer_name': k, 'revenue': round(v['revenue'], 2), 'orders': v['orders']}
            for k, v in customer_data.items() if k != 'Unknown'
        ],
        key=lambda x: x['revenue'],
        reverse=True
    )[:10]

    # 10. Discount Analytics
    discount_ranges = sorted(
        [
            {'range': k, 'revenue': round(v['revenue'], 2), 'orders': v['orders']}
            for k, v in discount_data.items()
        ],
        key=lambda x: x['revenue'],
        reverse=True
    )

    # Data Quality Report
    total_uploaded = max(dq_stats['total_rows_uploaded'], valid_rows)
    duplicate_ids = max(0, valid_rows - total_orders)

    result_payload = {
        'kpis': {
            'total_revenue': total_rev,
            'total_orders': total_orders,
            'avg_order_value': avg_order_val,
            'total_units_sold': total_units,
            'growth_rate': growth_rate,
            'avg_discount_pct': avg_discount
        },
        'sales_trend': sorted_trend,
        'top_products': sorted_products,
        'regional_sales': regional_sales,
        'category_breakdown': sorted_categories,
        'order_status_breakdown': sorted_statuses,
        'sales_channel_breakdown': sorted_channels,
        'payment_method_breakdown': sorted_payments,
        'city_performance': sorted_cities,
        'salesperson_performance': sorted_salespersons,
        'customer_analytics': {
            'total_unique_customers': unique_cust_count,
            'avg_customer_spend': round(total_rev / unique_cust_count, 2) if unique_cust_count > 0 else 0.0,
            'top_customers': top_customers
        },
        'discount_analytics': {
            'avg_discount_pct': avg_discount,
            'discount_ranges': discount_ranges
        },
        'data_quality': {
            'total_rows_uploaded': total_uploaded,
            'valid_rows': valid_rows,
            'invalid_rows': max(0, total_uploaded - valid_rows),
            'duplicate_rows': duplicate_ids,
            'duplicate_sale_ids': duplicate_ids,
            'missing_values': dq_stats['missing_values'],
            'invalid_dates': dq_stats['invalid_dates'],
            'invalid_numeric_values': dq_stats['invalid_numeric_values'],
            'normalized_values': dq_stats['cleaned_values'],
            'rejected_records': max(0, total_uploaded - valid_rows)
        }
    }

    # Output formatted JSON string for Hadoop MapReduce consumer
    print(json.dumps(result_payload, indent=2))


if __name__ == '__main__':
    main()
