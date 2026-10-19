#!/usr/bin/env python3
"""
Production MapReduce Mapper for Distributed Sales Analytics.
Extracts dimensions and metrics from streaming CSV rows and emits key-value pairs.
Memory footprint: O(1) streaming per row.
"""
import sys
import os
import csv
import json
import argparse
from typing import Dict, Optional, List


def parse_date_period(val: str) -> str:
    """Fast parsing of dates to YYYY-MM period."""
    if not val:
        return 'Unknown'
    val = val.strip()
    if len(val) < 7:
        return 'Unknown'

    # Fast check for ISO YYYY-MM-DD
    if val[:4].isdigit() and val[4] == '-':
        parts = val.split('-')
        if len(parts) >= 2:
            return f"{parts[0]}-{parts[1].zfill(2)}"

    # Check for DD/MM/YYYY or MM/DD/YYYY or YYYY/MM/DD
    if '/' in val:
        parts = val.split('/')
        if len(parts) == 3:
            if len(parts[2]) == 4:
                # DD/MM/YYYY or MM/DD/YYYY
                p0 = int(parts[0]) if parts[0].isdigit() else 0
                p1 = int(parts[1]) if parts[1].isdigit() else 0
                month = str(p1 if p1 <= 12 and p0 > 12 else p0).zfill(2)
                return f"{parts[2]}-{month}"
            elif len(parts[0]) == 4:
                return f"{parts[0]}-{parts[1].zfill(2)}"

    # Fast search for English 3-letter month abbreviations
    months = {
        'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
        'may': '05', 'jun': '06', 'jul': '07', 'aug': '08',
        'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12'
    }
    val_lower = val.lower()
    for m_abbr, m_num in months.items():
        if m_abbr in val_lower:
            # find 4-digit year
            for token in val.replace(',', ' ').split():
                if len(token) == 4 and token.isdigit():
                    return f"{token}-{m_num}"

    return 'Unknown'


def normalize_category(val: str) -> str:
    if not val or not val.strip():
        return 'General'
    s = val.strip()
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
    if not val or not val.strip():
        return 'Unknown'
    s_lower = val.strip().lower()
    if 'complete' in s_lower:
        return 'Completed'
    elif 'cancel' in s_lower:
        return 'Cancelled'
    elif 'pend' in s_lower:
        return 'Pending'
    elif 'return' in s_lower:
        return 'Returned'
    return val.strip().title()


def normalize_channel(val: str) -> str:
    if not val or not val.strip():
        return 'General'
    s_lower = val.strip().lower()
    if 'online' in s_lower:
        return 'Online'
    elif 'retail' in s_lower:
        return 'Retail'
    elif 'market' in s_lower:
        return 'Marketplace'
    elif 'store' in s_lower:
        return 'Store'
    return val.strip().title()


def normalize_payment(val: str) -> str:
    if not val or not val.strip():
        return 'General'
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
    return val.strip().title()


def clean_string(val: Optional[str], default_val: str) -> str:
    if not val:
        return default_val
    v = val.strip()
    return v.title() if v else default_val


def parse_args():
    parser = argparse.ArgumentParser(description="Sales Analyzer MapReduce Mapper")
    parser.add_argument("--config", help="JSON config file containing column indices")
    parser.add_argument("--col-id-idx", type=int, help="Sale/Order ID column index")
    parser.add_argument("--col-date-idx", type=int, help="Date column index")
    parser.add_argument("--col-customer-idx", type=int, help="Customer column index")
    parser.add_argument("--col-city-idx", type=int, help="City column index")
    parser.add_argument("--col-product-idx", type=int, help="Product column index")
    parser.add_argument("--col-category-idx", type=int, help="Category column index")
    parser.add_argument("--col-qty-idx", type=int, help="Quantity column index")
    parser.add_argument("--col-price-idx", type=int, help="Price column index")
    parser.add_argument("--col-revenue-idx", type=int, help="Revenue column index")
    parser.add_argument("--col-discount-idx", type=int, help="Discount column index")
    parser.add_argument("--col-channel-idx", type=int, help="Channel column index")
    parser.add_argument("--col-payment-idx", type=int, help="Payment column index")
    parser.add_argument("--col-status-idx", type=int, help="Status column index")
    parser.add_argument("--col-salesperson-idx", type=int, help="Salesperson column index")
    return parser.parse_known_args()


def load_column_map(args) -> Dict[str, Optional[int]]:
    mapping: Dict[str, Optional[int]] = {
        'id': None, 'date': None, 'customer': None, 'city': None,
        'product': None, 'category': None, 'qty': None, 'price': None,
        'revenue': None, 'discount': None, 'channel': None,
        'payment': None, 'status': None, 'salesperson': None
    }

    # 1. From config file
    config_file = args.config or os.environ.get('SCHEMA_MAPPING_FILE') or 'column_map.json'
    if os.path.exists(config_file):
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                mapping.update(data)
        except Exception:
            pass

    # 2. From CLI args
    for key in mapping.keys():
        arg_val = getattr(args, f"col_{key}_idx", None)
        if arg_val is not None:
            mapping[key] = arg_val

    # 3. From Environment Variables
    env_keys = {
        'id': 'COL_ID_IDX', 'date': 'COL_DATE_IDX', 'customer': 'COL_CUSTOMER_IDX',
        'city': 'COL_CITY_IDX', 'product': 'COL_PRODUCT_IDX', 'category': 'COL_CATEGORY_IDX',
        'qty': 'COL_QTY_IDX', 'price': 'COL_PRICE_IDX', 'revenue': 'COL_REVENUE_IDX',
        'discount': 'COL_DISCOUNT_IDX', 'channel': 'COL_CHANNEL_IDX',
        'payment': 'COL_PAYMENT_IDX', 'status': 'COL_STATUS_IDX',
        'salesperson': 'COL_SALESPERSON_IDX'
    }
    for k, env_var in env_keys.items():
        if mapping[k] is None and env_var in os.environ:
            try:
                mapping[k] = int(os.environ[env_var])
            except ValueError:
                pass

    return mapping


def detect_indices_from_header(header_row: List[str]) -> Dict[str, Optional[int]]:
    """Heuristic matching from CSV header names."""
    mapping: Dict[str, Optional[int]] = {
        'id': None, 'date': None, 'customer': None, 'city': None,
        'product': None, 'category': None, 'qty': None, 'price': None,
        'revenue': None, 'discount': None, 'channel': None,
        'payment': None, 'status': None, 'salesperson': None
    }
    headers_lower = [str(c).strip().lower() for c in header_row]

    def find_idx(patterns: List[str]) -> Optional[int]:
        for p in patterns:
            for idx, h in enumerate(headers_lower):
                if h == p:
                    return idx
        for p in patterns:
            for idx, h in enumerate(headers_lower):
                if p in h:
                    return idx
        return None

    mapping['id'] = find_idx(['order_id', 'sale_id', 'transaction_code', 'txn_code', 'id'])
    mapping['date'] = find_idx(['order_date', 'sale_date', 'date', 'time', 'timestamp'])
    mapping['customer'] = find_idx(['customer_name', 'customer', 'buyer', 'client'])
    mapping['city'] = find_idx(['city', 'region', 'state', 'location'])
    mapping['product'] = find_idx(['product_name', 'product', 'item_name', 'item', 'sku'])
    mapping['category'] = find_idx(['category', 'department', 'sector'])
    mapping['qty'] = find_idx(['quantity', 'qty', 'units', 'count'])
    mapping['price'] = find_idx(['unit_price', 'price', 'cost'])
    mapping['revenue'] = find_idx(['total_revenue', 'sale_amount', 'revenue', 'amount', 'total'])
    mapping['discount'] = find_idx(['discount', 'rebate'])
    mapping['channel'] = find_idx(['sales_channel', 'channel', 'medium'])
    mapping['payment'] = find_idx(['payment_method', 'payment', 'pay_method'])
    mapping['status'] = find_idx(['order_status', 'status'])
    mapping['salesperson'] = find_idx(['salesperson', 'sales_rep', 'rep', 'agent'])

    return mapping


def is_header_row(row: List[str]) -> bool:
    """Detect if row is a CSV header."""
    if not row:
        return False
    row_lower = [str(x).strip().lower() for x in row]
    header_tokens = {'order_id', 'sale_id', 'id', 'transaction_code', 'order_date', 'product_name', 'category', 'total_revenue'}
    matches = sum(1 for val in row_lower if any(tok in val for tok in header_tokens))
    return matches >= 2 or (len(row_lower) > 0 and row_lower[0] in ('order_id', 'sale_id', 'transaction_code', 'id'))


def main():
    args, _ = parse_args()
    col_map = load_column_map(args)

    reader = csv.reader(sys.stdin)
    row_count = 0

    id_i = col_map.get('id')
    date_i = col_map.get('date')
    customer_i = col_map.get('customer')
    city_i = col_map.get('city')
    product_i = col_map.get('product')
    category_i = col_map.get('category')
    qty_i = col_map.get('qty')
    price_i = col_map.get('price')
    rev_i = col_map.get('revenue')
    disc_i = col_map.get('discount')
    chan_i = col_map.get('channel')
    pay_i = col_map.get('payment')
    stat_i = col_map.get('status')
    sp_i = col_map.get('salesperson')

    for row in reader:
        if not row:
            continue

        # Check if first row is header
        if row_count == 0 and is_header_row(row):
            # If mapping is not fully populated, detect from header
            if any(v is None for v in (id_i, date_i, rev_i, price_i, qty_i)):
                detected = detect_indices_from_header(row)
                if id_i is None: id_i = detected.get('id')
                if date_i is None: date_i = detected.get('date')
                if customer_i is None: customer_i = detected.get('customer')
                if city_i is None: city_i = detected.get('city')
                if product_i is None: product_i = detected.get('product')
                if category_i is None: category_i = detected.get('category')
                if qty_i is None: qty_i = detected.get('qty')
                if price_i is None: price_i = detected.get('price')
                if rev_i is None: rev_i = detected.get('revenue')
                if disc_i is None: disc_i = detected.get('discount')
                if chan_i is None: chan_i = detected.get('channel')
                if pay_i is None: pay_i = detected.get('payment')
                if stat_i is None: stat_i = detected.get('status')
                if sp_i is None: sp_i = detected.get('salesperson')
            row_count += 1
            continue
        elif is_header_row(row):
            # Header in middle of split, skip it
            continue

        row_count += 1
        missing_count = 0
        cleaned_count = 0
        invalid_date_flag = 0
        invalid_num_flag = 0

        # 1. ID
        if id_i is not None and id_i < len(row) and row[id_i].strip():
            sale_id = row[id_i].strip()
        else:
            sale_id = f"ROW_{row_count}"
            missing_count += 1

        # 2. Quantity
        qty = 1
        if qty_i is not None and qty_i < len(row) and row[qty_i].strip():
            raw_qty = row[qty_i].replace(',', '').strip()
            try:
                qty = int(float(raw_qty))
            except ValueError:
                invalid_num_flag += 1
                qty = 1
        else:
            missing_count += 1

        # 3. Revenue
        revenue = 0.0
        if rev_i is not None and rev_i < len(row) and row[rev_i].strip():
            raw_rev = row[rev_i].replace('$', '').replace(',', '').strip()
            try:
                revenue = float(raw_rev)
            except ValueError:
                invalid_num_flag += 1
                revenue = 0.0
        elif price_i is not None and price_i < len(row) and row[price_i].strip():
            raw_price = row[price_i].replace('$', '').replace(',', '').strip()
            try:
                price = float(raw_price)
                revenue = price * qty
            except ValueError:
                invalid_num_flag += 1
                revenue = 0.0
        else:
            missing_count += 1

        # 4. Date Period
        date_period = 'Unknown'
        if date_i is not None and date_i < len(row) and row[date_i].strip():
            date_period = parse_date_period(row[date_i])
            if date_period == 'Unknown':
                invalid_date_flag += 1
        else:
            missing_count += 1
            invalid_date_flag += 1

        # 5. Dimensions & Normalization
        raw_cat = row[category_i] if category_i is not None and category_i < len(row) else ''
        category = normalize_category(raw_cat)
        if raw_cat and raw_cat != category:
            cleaned_count += 1

        raw_stat = row[stat_i] if stat_i is not None and stat_i < len(row) else ''
        status = normalize_status(raw_stat)
        if raw_stat and raw_stat != status:
            cleaned_count += 1

        raw_chan = row[chan_i] if chan_i is not None and chan_i < len(row) else ''
        channel = normalize_channel(raw_chan)
        if raw_chan and raw_chan != channel:
            cleaned_count += 1

        raw_pay = row[pay_i] if pay_i is not None and pay_i < len(row) else ''
        payment = normalize_payment(raw_pay)
        if raw_pay and raw_pay != payment:
            cleaned_count += 1

        product = clean_string(row[product_i] if product_i is not None and product_i < len(row) else None, 'Standard Item')
        city = clean_string(row[city_i] if city_i is not None and city_i < len(row) else None, 'General')
        salesperson = clean_string(row[sp_i] if sp_i is not None and sp_i < len(row) else None, 'General')
        customer = clean_string(row[customer_i] if customer_i is not None and customer_i < len(row) else None, 'General')

        # 6. Discount
        discount_pct = 0.0
        if disc_i is not None and disc_i < len(row) and row[disc_i].strip():
            raw_disc = row[disc_i].replace('%', '').strip()
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

        # Emit Key-Value Records for Reducer Shuffle & Aggregation
        # Format: GROUP_TYPE \t KEY \t METRICS...
        print(f"KPI\tALL\t{revenue:.2f}\t{qty}\t{discount_pct:.2f}\t{sale_id}")
        print(f"TREND\t{date_period}\t{revenue:.2f}\t1\t{qty}")
        print(f"CATEGORY\t{category}\t{revenue:.2f}\t1\t{qty}")
        print(f"STATUS\t{status}\t{revenue:.2f}\t1\t{qty}")
        print(f"CHANNEL\t{channel}\t{revenue:.2f}\t1\t{qty}")
        print(f"PAYMENT\t{payment}\t{revenue:.2f}\t1\t{qty}")
        print(f"PRODUCT\t{product}\t{revenue:.2f}\t1\t{qty}")
        print(f"CITY\t{city}\t{revenue:.2f}\t1\t{qty}")
        print(f"SALESPERSON\t{salesperson}\t{revenue:.2f}\t1\t{qty}")
        print(f"CUSTOMER\t{customer}\t{revenue:.2f}\t1\t{qty}")
        print(f"DISCOUNT\t{discount_range}\t{revenue:.2f}\t1\t{qty}")
        print(f"DQ\tSTATS\t1\t{missing_count}\t{cleaned_count}\t{invalid_date_flag}\t{invalid_num_flag}")


if __name__ == '__main__':
    main()
