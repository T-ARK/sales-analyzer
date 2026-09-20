#!/usr/bin/env python3
import sys
import json
from collections import defaultdict

def main():
    totals = {
        'revenue': 0.0,
        'units': 0,
        'discount_sum': 0.0,
        'valid_rows': 0,
        'seen_sale_ids': set(),
        'trend': defaultdict(lambda: {'revenue': 0.0, 'orders': 0, 'units': 0}),
        'category': defaultdict(lambda: {'revenue': 0.0, 'orders': 0, 'units': 0}),
        'status': defaultdict(lambda: {'revenue': 0.0, 'orders': 0, 'units': 0}),
        'channel': defaultdict(lambda: {'revenue': 0.0, 'orders': 0, 'units': 0}),
        'payment': defaultdict(lambda: {'revenue': 0.0, 'orders': 0, 'units': 0}),
        'product': defaultdict(lambda: {'revenue': 0.0, 'orders': 0, 'units': 0}),
        'city': defaultdict(lambda: {'revenue': 0.0, 'orders': 0, 'units': 0}),
        'salesperson': defaultdict(lambda: {'revenue': 0.0, 'orders': 0, 'units': 0}),
        'customer': defaultdict(lambda: {'revenue': 0.0, 'orders': 0, 'units': 0}),
        'discount': defaultdict(lambda: {'revenue': 0.0, 'orders': 0, 'units': 0}),
        'dq': {
            'total_rows_uploaded': 0,
            'valid_rows': 0,
            'missing_values': 0,
            'cleaned_values': 0,
            'invalid_dates': 0,
            'invalid_numeric_values': 0
        }
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
                    u = int(parts[3])
                    disc = float(parts[4])
                    s_id = parts[5]

                    totals['revenue'] += rev
                    totals['units'] += u
                    totals['discount_sum'] += disc
                    totals['valid_rows'] += 1
                    totals['seen_sale_ids'].add(s_id)
                except ValueError:
                    pass

        elif group_type in ('TREND', 'CATEGORY', 'STATUS', 'CHANNEL', 'PAYMENT', 'PRODUCT', 'CITY', 'SALESPERSON', 'CUSTOMER', 'DISCOUNT'):
            if len(parts) >= 5:
                try:
                    rev = float(parts[2])
                    ords = int(parts[3])
                    u = int(parts[4])

                    target_dict = None
                    if group_type == 'TREND': target_dict = totals['trend']
                    elif group_type == 'CATEGORY': target_dict = totals['category']
                    elif group_type == 'STATUS': target_dict = totals['status']
                    elif group_type == 'CHANNEL': target_dict = totals['channel']
                    elif group_type == 'PAYMENT': target_dict = totals['payment']
                    elif group_type == 'PRODUCT': target_dict = totals['product']
                    elif group_type == 'CITY': target_dict = totals['city']
                    elif group_type == 'SALESPERSON': target_dict = totals['salesperson']
                    elif group_type == 'CUSTOMER': target_dict = totals['customer']
                    elif group_type == 'DISCOUNT': target_dict = totals['discount']

                    if target_dict is not None:
                        target_dict[key]['revenue'] += rev
                        target_dict[key]['orders'] += ords
                        target_dict[key]['units'] += u
                except ValueError:
                    pass

        elif group_type == 'DQ':
            if len(parts) >= 7:
                try:
                    totals['dq']['total_rows_uploaded'] += int(parts[2])
                    totals['dq']['missing_values'] += int(parts[3])
                    totals['dq']['cleaned_values'] += int(parts[4])
                    totals['dq']['invalid_dates'] += int(parts[5])
                    totals['dq']['invalid_numeric_values'] += int(parts[6])
                except ValueError:
                    pass

    total_rev = round(totals['revenue'], 2)
    total_units = totals['units']
    total_valid_rows = totals['valid_rows']
    total_orders = len(totals['seen_sale_ids']) if totals['seen_sale_ids'] else total_valid_rows

    avg_order_val = round((total_rev / total_orders), 2) if total_orders > 0 else 0.0
    avg_discount = round((totals['discount_sum'] / total_valid_rows), 2) if total_valid_rows > 0 else 0.0

    # 1. Sales Trend
    sorted_trend = sorted(
        [{'period': k, 'revenue': round(v['revenue'], 2), 'orders': v['orders'], 'units': v['units']} for k, v in totals['trend'].items() if k != 'Unknown'],
        key=lambda x: x['period']
    )

    growth_rate = 0.0
    if len(sorted_trend) >= 2:
        last = sorted_trend[-1]['revenue']
        prev = sorted_trend[-2]['revenue']
        if prev > 0:
            growth_rate = round(((last - prev) / prev) * 100, 2)

    # 2. Top Products
    sorted_products = sorted(
        [{'product_name': k, 'revenue': round(v['revenue'], 2), 'units_sold': v['units']} for k, v in totals['product'].items() if k != 'Unknown'],
        key=lambda x: x['revenue'],
        reverse=True
    )[:10]

    # 3. Categories
    sorted_categories = sorted(
        [{
            'category': k,
            'revenue': round(v['revenue'], 2),
            'orders': v['orders'],
            'percentage': round((v['revenue'] / total_rev * 100), 1) if total_rev > 0 else 0.0
        } for k, v in totals['category'].items() if k != 'Unknown'],
        key=lambda x: x['revenue'],
        reverse=True
    )

    # 4. Order Status
    sorted_statuses = sorted(
        [{
            'status': k,
            'orders': v['orders'],
            'revenue': round(v['revenue'], 2),
            'percentage': round((v['orders'] / total_orders * 100), 1) if total_orders > 0 else 0.0
        } for k, v in totals['status'].items() if k != 'Unknown'],
        key=lambda x: x['orders'],
        reverse=True
    )

    # 5. Sales Channel
    sorted_channels = sorted(
        [{
            'channel': k,
            'revenue': round(v['revenue'], 2),
            'orders': v['orders'],
            'percentage': round((v['revenue'] / total_rev * 100), 1) if total_rev > 0 else 0.0
        } for k, v in totals['channel'].items() if k != 'Unknown'],
        key=lambda x: x['revenue'],
        reverse=True
    )

    # 6. Payment Method
    sorted_payments = sorted(
        [{
            'payment_method': k,
            'revenue': round(v['revenue'], 2),
            'orders': v['orders'],
            'percentage': round((v['revenue'] / total_rev * 100), 1) if total_rev > 0 else 0.0
        } for k, v in totals['payment'].items() if k != 'Unknown'],
        key=lambda x: x['revenue'],
        reverse=True
    )

    # 7. City Performance
    sorted_cities = sorted(
        [{'city': k, 'revenue': round(v['revenue'], 2), 'orders': v['orders'], 'units_sold': v['units']} for k, v in totals['city'].items() if k != 'Unknown'],
        key=lambda x: x['revenue'],
        reverse=True
    )[:10]

    # 8. Salesperson Performance
    sorted_salespersons = sorted(
        [{'salesperson': k, 'revenue': round(v['revenue'], 2), 'orders': v['orders'], 'units_sold': v['units']} for k, v in totals['salesperson'].items() if k != 'Unknown'],
        key=lambda x: x['revenue'],
        reverse=True
    )[:10]

    # 9. Customer Analytics
    unique_cust_count = len([k for k in totals['customer'] if k != 'Unknown'])
    top_customers = sorted(
        [{'customer_name': k, 'revenue': round(v['revenue'], 2), 'orders': v['orders']} for k, v in totals['customer'].items() if k != 'Unknown'],
        key=lambda x: x['revenue'],
        reverse=True
    )[:10]

    # 10. Discount Analytics
    discount_ranges = sorted(
        [{'range': k, 'revenue': round(v['revenue'], 2), 'orders': v['orders']} for k, v in totals['discount'].items()],
        key=lambda x: x['revenue'],
        reverse=True
    )

    # Data Quality Report
    total_uploaded = max(totals['dq']['total_rows_uploaded'], total_valid_rows)
    duplicate_ids = max(0, total_valid_rows - total_orders)

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
        'regional_sales': [{'region': c['city'], 'revenue': c['revenue'], 'orders': c['orders'], 'percentage': round(c['revenue']/total_rev*100, 1) if total_rev > 0 else 0.0} for c in sorted_cities[:5]],
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
            'valid_rows': total_valid_rows,
            'invalid_rows': max(0, total_uploaded - total_valid_rows),
            'duplicate_rows': duplicate_ids,
            'duplicate_sale_ids': duplicate_ids,
            'missing_values': totals['dq']['missing_values'],
            'invalid_dates': totals['dq']['invalid_dates'],
            'invalid_numeric_values': totals['dq']['invalid_numeric_values'],
            'normalized_values': totals['dq']['cleaned_values'],
            'rejected_records': max(0, total_uploaded - total_valid_rows)
        }
    }

    print(json.dumps(result_payload, indent=2))

if __name__ == '__main__':
    main()
