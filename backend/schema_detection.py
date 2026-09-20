import os
import re
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Tuple
from models import ColumnInfo, SchemaDetectionResult

def estimate_row_count(file_path: str) -> int:
    """Accurately count or estimate total rows without loading entire memory."""
    if not os.path.exists(file_path):
        return 0
    file_size = os.path.getsize(file_path)
    if file_size == 0:
        return 0
    
    # Quick exact row count for files under 100MB
    if file_size < 100 * 1024 * 1024:
        with open(file_path, 'rb') as f:
            count = sum(1 for _ in f)
            return max(0, count - 1)
    
    # Fast estimation buffer sampling for large files (>100MB)
    with open(file_path, 'rb') as f:
        sample_buffer = f.read(256 * 1024)
        lines = sample_buffer.split(b'\n')
        if len(lines) > 2:
            sample_lines = lines[1:-1]
            avg_line_size = sum(len(line) for line in sample_lines) / len(sample_lines)
            if avg_line_size > 0:
                estimated_rows = int(file_size / avg_line_size)
                return max(estimated_rows, len(lines) - 1)
    
    df = pd.read_csv(file_path, nrows=100)
    return len(df)

def infer_column_meta(col_name: str, series: pd.Series) -> ColumnInfo:
    col_lower = col_name.lower().strip()
    clean_series = series.dropna()
    sample_vals = clean_series.head(3).tolist()
    
    clean_samples = []
    for val in sample_vals:
        if isinstance(val, (np.integer, int)):
            clean_samples.append(int(val))
        elif isinstance(val, (np.floating, float)):
            clean_samples.append(float(val))
        else:
            clean_samples.append(str(val))

    # 1. ID / Transaction Code
    if any(kw in col_lower for kw in ['sale_id', 'transaction_code', 'order_id', 'txn_code', 'transaction_id']) or (col_lower == 'id'):
        return ColumnInfo(
            name=col_name,
            inferred_type='number' if pd.api.types.is_numeric_dtype(clean_series) else 'text',
            role='dimension',
            sample_values=clean_samples,
            confidence=0.98
        )

    # 2. Date
    if any(kw in col_lower for kw in ['date', 'time', 'dt', 'timestamp']):
        return ColumnInfo(
            name=col_name,
            inferred_type='date',
            role='time_axis',
            sample_values=clean_samples,
            confidence=0.95
        )

    # 3. Customer Name / Salesperson
    if 'customer' in col_lower or 'buyer' in col_lower:
        return ColumnInfo(
            name=col_name,
            inferred_type='text',
            role='dimension',
            sample_values=clean_samples,
            confidence=0.90
        )
    if 'salesperson' in col_lower or 'sales_rep' in col_lower or 'rep' in col_lower:
        return ColumnInfo(
            name=col_name,
            inferred_type='text',
            role='dimension',
            sample_values=clean_samples,
            confidence=0.90
        )

    # 4. Product Name
    if 'product' in col_lower or 'sku' in col_lower or 'item' in col_lower:
        return ColumnInfo(
            name=col_name,
            inferred_type='text',
            role='dimension',
            sample_values=clean_samples,
            confidence=0.95
        )

    # 5. Category (Strict - distinct from Order Status!)
    if 'category' in col_lower or 'department' in col_lower or 'sector' in col_lower:
        return ColumnInfo(
            name=col_name,
            inferred_type='category',
            role='category_dimension',
            sample_values=clean_samples,
            confidence=0.98
        )

    # 6. Order Status
    if 'status' in col_lower:
        return ColumnInfo(
            name=col_name,
            inferred_type='category',
            role='dimension',
            sample_values=clean_samples,
            confidence=0.95
        )

    # 7. Sales Channel
    if 'channel' in col_lower or 'medium' in col_lower:
        return ColumnInfo(
            name=col_name,
            inferred_type='category',
            role='dimension',
            sample_values=clean_samples,
            confidence=0.95
        )

    # 8. Payment Method
    if 'payment' in col_lower or 'pay_method' in col_lower or 'pay_type' in col_lower:
        return ColumnInfo(
            name=col_name,
            inferred_type='category',
            role='dimension',
            sample_values=clean_samples,
            confidence=0.95
        )

    # 9. Geo Dimension (City / State / Region)
    if any(kw in col_lower for kw in ['city', 'state', 'region', 'country', 'location', 'zone']):
        return ColumnInfo(
            name=col_name,
            inferred_type='category',
            role='geo_dimension',
            sample_values=clean_samples,
            confidence=0.90
        )

    # 10. Quantity / Units
    if any(kw in col_lower for kw in ['quantity', 'qty', 'units', 'count']):
        return ColumnInfo(
            name=col_name,
            inferred_type='number',
            role='metric',
            sample_values=clean_samples,
            confidence=0.95
        )

    # 11. Discount
    if 'discount' in col_lower or 'rebate' in col_lower:
        return ColumnInfo(
            name=col_name,
            inferred_type='number',
            role='metric',
            sample_values=clean_samples,
            confidence=0.90
        )

    # 12. Unit Price vs Revenue / Sale Amount
    if any(kw in col_lower for kw in ['sale_amount', 'total_revenue', 'revenue', 'amount', 'total_price', 'total']):
        return ColumnInfo(
            name=col_name,
            inferred_type='currency',
            role='metric',
            sample_values=clean_samples,
            confidence=0.98
        )
    if 'unit_price' in col_lower or 'price' in col_lower or 'cost' in col_lower:
        return ColumnInfo(
            name=col_name,
            inferred_type='currency',
            role='metric',
            sample_values=clean_samples,
            confidence=0.90
        )

    # Fallbacks
    if pd.api.types.is_numeric_dtype(clean_series):
        return ColumnInfo(
            name=col_name,
            inferred_type='number',
            role='metric',
            sample_values=clean_samples,
            confidence=0.70
        )

    unique_ratio = clean_series.nunique() / max(len(clean_series), 1)
    if unique_ratio < 0.3:
        return ColumnInfo(
            name=col_name,
            inferred_type='category',
            role='dimension',
            sample_values=clean_samples,
            confidence=0.70
        )

    return ColumnInfo(
        name=col_name,
        inferred_type='text',
        role='dimension',
        sample_values=clean_samples,
        confidence=0.60
    )

def detect_schema(file_path: str, dataset_id: str, filename: str) -> SchemaDetectionResult:
    """Reads header and top sample rows to perform instant column and type detection."""
    file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
    estimated_rows = estimate_row_count(file_path)
    
    if filename.endswith('.xlsx') or filename.endswith('.xls'):
        df_sample = pd.read_excel(file_path, nrows=50)
    else:
        df_sample = pd.read_csv(file_path, nrows=50)

    columns = []
    for col in df_sample.columns:
        col_meta = infer_column_meta(str(col), df_sample[col])
        columns.append(col_meta)

    return SchemaDetectionResult(
        dataset_id=dataset_id,
        filename=filename,
        total_rows_estimated=estimated_rows,
        file_size_bytes=file_size,
        columns=columns
    )
