#!/usr/bin/env python3
import sys
import os
import csv
import re
from datetime import datetime

def normalize_string(val: str) -> str:
    if not val:
        return 'Unknown'
    return ' '.join(val.strip().split())

def normalize_category(val: str) -> str:
    s = normalize_string(val)
    s_lower = s.lower()
    if 'electronic' in s_lower:
        return 'Electronics'
    elif 'fashion' in s_lower:
        return 'Fashion'
    elif 'appliance' in s_lower:
        return 'Appliances'
    elif 'furniture' in s_lower:
        return 'Furniture'
    elif 'accessori' in s_lower:
        return 'Accessories'
    elif 'bag' in s_lower:
        return 'Bags'
    return s.title()

def normalize_status(val: str) -> str:
    s_lower = val.strip().lower()
    if 'complete' in s_lower:
        return 'Completed'
    elif 'cancel' in s_lower:
        return 'Cancelled'
    elif 'pend' in s_lower:
        return 'Pending'
    elif 'return' in s_lower:
        return 'Returned'
    return val.strip().title() if val else 'Unknown'

def normalize_channel(val: str) -> str:
    s_lower = val.strip().lower()
    if 'online' in s_lower:
        return 'Online'
    elif 'retail' in s_lower:
        return 'Retail'
    elif 'market' in s_lower:
        return 'Marketplace'
    elif 'store' in s_lower:
        return 'Store'
    return val.strip().title() if val else 'Unknown'

def normalize_payment(val: str) -> str:
    s_lower = val.strip().lower()
    if 'net' in s_lower or 'banking' in s_lower:
        return 'Net Banking'
    elif 'upi' in s_lower:
        return 'UPI'
    elif 'credit' in s_lower:
        return 'Credit Card'
    elif 'debit' in s_lower:
        return 'Debit Card'
    elif 'cash' in s_lower:
        return 'Cash'
    return val.strip().title() if val else 'Unknown'

def parse_date_period(val: str) -> str:
    val = val.strip()
    if not val:
        return 'Unknown'
    
    # Try ISO YYYY-MM-DD
    if re.match(r'^\d{4}-\d{1,2}-\d{1,2}', val):
        parts = val.split('-')
        return f"{parts[0]}-{parts[1].zfill(2)}"
    
    # Try DD/MM/YYYY or MM/DD/YYYY
    if '/' in val:
        parts = val.split('/')
        if len(parts) == 3:
            if len(parts[2]) == 4:
                # DD/MM/YYYY or MM/DD/YYYY
                month = parts[1] if int(parts[1]) <= 12 else parts[0]
                return f"{parts[2]}-{month.zfill(2)}"
            elif len(parts[0]) == 4:
                return f"{parts[0]}-{parts[1].zfill(2)}"

    # Try text date like Nov 27, 2024
    for fmt in ('%b %d, %Y', '%B %d, %Y', '%d %b %Y', '%d %B %Y'):
        try:
            dt = datetime.strptime(val, fmt)
            return dt.strftime('%Y-%m')
        except ValueError:
            pass

    return 'Unknown'

def main():
    col_id_idx = os.environ.get('COL_ID_IDX')
    col_txn_idx = os.environ.get('COL_TXN_IDX')
    col_date_idx = os.environ.get('COL_DATE_IDX')
    col_customer_idx = os.environ.get('COL_CUSTOMER_IDX')
    col_city_idx = os.environ.get('COL_CITY_IDX')
    col_product_idx = os.environ.get('COL_PRODUCT_IDX')
    col_category_idx = os.environ.get('COL_CATEGORY_IDX')
    col_qty_idx = os.environ.get('COL_QTY_IDX')
    col_price_idx = os.environ.get('COL_PRICE_IDX')
    col_discount_idx = os.environ.get('COL_DISCOUNT_IDX')
    col_channel_idx = os.environ.get('COL_CHANNEL_IDX')
    col_payment_idx = os.environ.get('COL_PAYMENT_IDX')
    col_status_idx = os.environ.get('COL_STATUS_IDX')
    col_salesperson_idx = os.environ.get('COL_SALESPERSON_IDX')
    col_revenue_idx = os.environ.get('COL_REVENUE_IDX')

    reader = csv.reader(sys.stdin)
    
    id_i = int(col_id_idx) if col_id_idx is not None else None
    txn_i = int(col_txn_idx) if col_txn_idx is not None else None
    date_i = int(col_date_idx) if col_date_idx is not None else None
    customer_i = int(col_customer_idx) if col_customer_idx is not None else None
    city_i = int(col_city_idx) if col_city_idx is not None else None
    product_i = int(col_product_idx) if col_product_idx is not None else None
    category_i = int(col_category_idx) if col_category_idx is not None else None
    qty_i = int(col_qty_idx) if col_qty_idx is not None else None
    price_i = int(col_price_idx) if col_price_idx is not None else None
    discount_i = int(col_discount_idx) if col_discount_idx is not None else None
    channel_i = int(col_channel_idx) if col_channel_idx is not None else None
    payment_i = int(col_payment_idx) if col_payment_idx is not None else None
    status_i = int(col_status_idx) if col_status_idx is not None else None
    salesperson_i = int(col_salesperson_idx) if col_salesperson_idx is not None else None
    revenue_i = int(col_revenue_idx) if col_revenue_idx is not None else None

    for row_idx, row in enumerate(reader):
        if not row:
            continue

        # ALWAYS SKIP HEADER ROW (row_idx == 0 or matches header column name)
        if row_idx == 0:
            continue
        first_col = str(row[0]).strip().lower()
        if 'sale_id' in first_col or 'order_id' in first_col or 'transaction_code' in first_col:
            continue

        missing_count = 0
        cleaned_count = 0
        invalid_date_flag = 0
        invalid_num_flag = 0

        # Extract & Clean Sale_ID / Transaction Code
        sale_id = row[id_i].strip() if id_i is not None and id_i < len(row) and row[id_i].strip() else f"ROW_{row_idx}"
        if id_i is None or id_i >= len(row) or not row[id_i].strip():
            missing_count += 1

        # Extract & Clean Quantity
        qty = 0
        if qty_i is not None and qty_i < len(row) and row[qty_i].strip():
            raw_qty = row[qty_i].replace(',', '').strip()
            try:
                qty = int(float(raw_qty))
            except ValueError:
                invalid_num_flag += 1
                qty = 0
        else:
            missing_count += 1
            qty = 1

        # Extract & Clean Revenue / Sale Amount
        revenue = 0.0
        if revenue_i is not None and revenue_i < len(row) and row[revenue_i].strip():
            raw_rev = row[revenue_i].replace('$', '').replace(',', '').strip()
            try:
                revenue = float(raw_rev)
            except ValueError:
                invalid_num_flag += 1
                revenue = 0.0
        elif price_i is not None and price_i < len(row) and row[price_i].strip():
            # Fallback to Unit_Price * Quantity
            raw_price = row[price_i].replace('$', '').replace(',', '').strip()
            try:
                price = float(raw_price)
                revenue = price * qty
            except ValueError:
                invalid_num_flag += 1
                revenue = 0.0
        else:
            missing_count += 1

        # Extract & Clean Date
        date_period = 'Unknown'
        if date_i is not None and date_i < len(row) and row[date_i].strip():
            date_period = parse_date_period(row[date_i])
            if date_period == 'Unknown':
                invalid_date_flag += 1
        else:
            missing_count += 1
            invalid_date_flag += 1

        # Extract & Normalize Categories and Dimensions
        raw_cat = row[category_i] if category_i is not None and category_i < len(row) else 'General'
        category = normalize_category(raw_cat)
        if raw_cat != category:
            cleaned_count += 1

        raw_stat = row[status_i] if status_i is not None and status_i < len(row) else 'Unknown'
        order_status = normalize_status(raw_stat)
        if raw_stat != order_status:
            cleaned_count += 1

        raw_chan = row[channel_i] if channel_i is not None and channel_i < len(row) else 'General'
        sales_channel = normalize_channel(raw_chan)
        if raw_chan != sales_channel:
            cleaned_count += 1

        raw_pay = row[payment_i] if payment_i is not None and payment_i < len(row) else 'General'
        payment_method = normalize_payment(raw_pay)
        if raw_pay != payment_method:
            cleaned_count += 1

        product = normalize_string(row[product_i]) if product_i is not None and product_i < len(row) else 'Standard Item'
        city = normalize_string(row[city_i]) if city_i is not None and city_i < len(row) else 'General'
        salesperson = normalize_string(row[salesperson_i]) if salesperson_i is not None and salesperson_i < len(row) else 'General'
        customer = normalize_string(row[customer_i]) if customer_i is not None and customer_i < len(row) else 'General'

        # Discount
        discount_pct = 0.0
        if discount_i is not None and discount_i < len(row) and row[discount_i].strip():
            raw_disc = row[discount_i].replace('%', '').strip()
            try:
                discount_pct = float(raw_disc)
            except ValueError:
                discount_pct = 0.0

        discount_range = '0%'
        if discount_pct > 20:
            discount_range = '>20%'
        elif discount_pct > 10:
            discount_range = '11-20%'
        elif discount_pct > 0:
            discount_range = '1-10%'

        # Emit MapReduce Key-Value Records
        print(f"KPI\tALL\t{revenue}\t{qty}\t{discount_pct}\t{sale_id}")
        print(f"TREND\t{date_period}\t{revenue}\t1\t{qty}")
        print(f"CATEGORY\t{category}\t{revenue}\t1\t{qty}")
        print(f"STATUS\t{order_status}\t{revenue}\t1\t{qty}")
        print(f"CHANNEL\t{sales_channel}\t{revenue}\t1\t{qty}")
        print(f"PAYMENT\t{payment_method}\t{revenue}\t1\t{qty}")
        print(f"PRODUCT\t{product}\t{revenue}\t1\t{qty}")
        print(f"CITY\t{city}\t{revenue}\t1\t{qty}")
        print(f"SALESPERSON\t{salesperson}\t{revenue}\t1\t{qty}")
        print(f"CUSTOMER\t{customer}\t{revenue}\t1\t{qty}")
        print(f"DISCOUNT\t{discount_range}\t{revenue}\t1\t{qty}")
        print(f"DQ\tSTATS\t1\t{missing_count}\t{cleaned_count}\t{invalid_date_flag}\t{invalid_num_flag}")

if __name__ == '__main__':
    main()
