"""
PySpark Distributed Processing Engine Adapter.
Performs distributed sales analytics using Spark SQL and DataFrame transformations.
Guarantees scalable distributed computation without collecting raw rows into driver memory.
"""
import os
import sys
import time
import logging
from typing import Dict, Any, List, Optional, Tuple, Callable

from processing.base_engine import BaseProcessingEngine, PySparkExecutionError

logger = logging.getLogger("sales_analyzer.pyspark")


class PySparkEngine(BaseProcessingEngine):
    """Distributed processing engine powered by Apache PySpark."""

    def __init__(
        self,
        master: Optional[str] = None,
        driver_memory: Optional[str] = None,
        shuffle_partitions: Optional[int] = None
    ):
        super().__init__(name="pyspark")
        self.master = master or os.environ.get("SPARK_MASTER", "local[*]")
        self.driver_memory = driver_memory or os.environ.get("SPARK_DRIVER_MEMORY", "2g")
        self.shuffle_partitions = shuffle_partitions or int(os.environ.get("SPARK_SHUFFLE_PARTITIONS", "4"))
        self._spark_session = None

    def _get_spark_session(self):
        """Lazily initialize and configure SparkSession."""
        if self._spark_session is not None:
            return self._spark_session

        try:
            from pyspark.sql import SparkSession
            builder = (
                SparkSession.builder
                .appName("SalesAnalyzer_DistributedEngine")
                .master(self.master)
                .config("spark.driver.memory", self.driver_memory)
                .config("spark.sql.shuffle.partitions", str(self.shuffle_partitions))
                .config("spark.ui.enabled", "false")
                .config("spark.sql.adaptive.enabled", "true")
            )
            self._spark_session = builder.getOrCreate()
            # Set log level to WARN to prevent console clutter
            self._spark_session.sparkContext.setLogLevel("WARN")
            return self._spark_session
        except Exception as e:
            logger.error(f"Failed to initialize SparkSession: {e}")
            raise PySparkExecutionError(f"Could not initialize PySpark session: {str(e)}")

    def is_available(self) -> Tuple[bool, str]:
        """
        Check if PySpark is installed and capable of executing workloads.
        Tests both package availability and JVM runtime compatibility.
        """
        try:
            import pyspark
            # Test actual session instantiation / execution capability
            session = self._get_spark_session()
            # Probe with a lightweight distributed evaluation
            test_df = session.range(1)
            cnt = test_df.count()
            if cnt == 1:
                return True, f"PySpark {pyspark.__version__} operational (master={self.master})."
            return False, "PySpark probe returned unexpected count."
        except Exception as e:
            err_msg = str(e)
            logger.warning(f"PySpark availability check failed: {err_msg}")
            return False, f"PySpark unavailable: {err_msg}"

    def _match_columns(self, header: List[str], confirmed_columns: List[Any]) -> Dict[str, Optional[str]]:
        """Identify which dataframe column name corresponds to each business metric."""
        col_map: Dict[str, Optional[str]] = {
            'id': None, 'date': None, 'customer': None, 'city': None,
            'product': None, 'category': None, 'qty': None, 'price': None,
            'revenue': None, 'discount': None, 'channel': None,
            'payment': None, 'status': None, 'salesperson': None
        }

        header_lower = {h.lower().strip(): h for h in header}

        def find_col(candidates: List[str]) -> Optional[str]:
            for cand in candidates:
                if cand in header_lower:
                    return header_lower[cand]
            for cand in candidates:
                for h_l, orig in header_lower.items():
                    if cand in h_l:
                        return orig
            return None

        # 1. From confirmed column roles
        for col in confirmed_columns:
            name = getattr(col, 'name', str(col))
            role = getattr(col, 'role', '')
            inferred_type = getattr(col, 'inferred_type', '')
            if name in header:
                if role == 'time_axis' or inferred_type == 'date':
                    col_map['date'] = name
                elif role == 'category_dimension':
                    col_map['category'] = name
                elif role == 'geo_dimension':
                    col_map['city'] = name

        # 2. Heuristic resolution for remaining columns
        if not col_map['id']:
            col_map['id'] = find_col(['sale_id', 'order_id', 'transaction_code', 'txn_code', 'id'])
        if not col_map['date']:
            col_map['date'] = find_col(['order_date', 'sale_date', 'date', 'time', 'timestamp', 'dt'])
        if not col_map['customer']:
            col_map['customer'] = find_col(['customer_name', 'customer', 'buyer', 'client'])
        if not col_map['city']:
            col_map['city'] = find_col(['city', 'region', 'state', 'location'])
        if not col_map['product']:
            col_map['product'] = find_col(['product_name', 'product', 'item_name', 'item', 'sku'])
        if not col_map['category']:
            col_map['category'] = find_col(['category', 'department', 'sector'])
        if not col_map['qty']:
            col_map['qty'] = find_col(['quantity', 'qty', 'units', 'count'])
        if not col_map['price']:
            col_map['price'] = find_col(['unit_price', 'price', 'cost'])
        if not col_map['revenue']:
            col_map['revenue'] = find_col(['total_revenue', 'sale_amount', 'revenue', 'amount', 'total'])
        if not col_map['discount']:
            col_map['discount'] = find_col(['discount', 'rebate'])
        if not col_map['channel']:
            col_map['channel'] = find_col(['sales_channel', 'channel', 'medium'])
        if not col_map['payment']:
            col_map['payment'] = find_col(['payment_method', 'payment', 'pay_method'])
        if not col_map['status']:
            col_map['status'] = find_col(['order_status', 'status'])
        if not col_map['salesperson']:
            col_map['salesperson'] = find_col(['salesperson', 'sales_rep', 'rep', 'agent'])

        return col_map

    def execute_analysis(
        self,
        job_id: str,
        dataset_id: str,
        file_path: str,
        columns: List[Any],
        progress_callback: Optional[Callable[[int, str], None]] = None
    ) -> Dict[str, Any]:
        """
        Execute big data analytics using PySpark DataFrames & Spark SQL.
        """
        start_time = time.time()
        spark = self._get_spark_session()
        from pyspark.sql import functions as F
        from pyspark.sql import types as T

        if progress_callback:
            progress_callback(15, "Reading distributed CSV dataset with PySpark engine...")

        try:
            # 1. Distributed CSV Read
            raw_df = (
                spark.read
                .option("header", "true")
                .option("inferSchema", "false")
                .option("mode", "PERMISSIVE")
                .csv(file_path)
            )

            total_uploaded = raw_df.count()
            if total_uploaded == 0:
                raise PySparkExecutionError("Uploaded dataset contains no data rows.")

            col_map = self._match_columns(raw_df.columns, columns)

            if progress_callback:
                progress_callback(35, "Applying Spark DataFrame cleaning & transformation expressions...")

            # 2. Data Cleaning Expressions
            # ID
            id_col = col_map.get('id')
            if id_col:
                clean_id = F.when(
                    F.col(id_col).isNull() | (F.trim(F.col(id_col)) == F.lit("")),
                    F.concat(F.lit("ROW_"), F.monotonically_increasing_id().cast(T.StringType()))
                ).otherwise(F.trim(F.col(id_col)))
                id_missing_expr = F.when(F.col(id_col).isNull() | (F.trim(F.col(id_col)) == F.lit("")), 1).otherwise(0)
            else:
                clean_id = F.concat(F.lit("ROW_"), F.monotonically_increasing_id().cast(T.StringType()))
                id_missing_expr = F.lit(1)

            # Quantity
            qty_col = col_map.get('qty')
            if qty_col:
                raw_qty_clean = F.regexp_replace(F.trim(F.col(qty_col)), ",", "")
                parsed_qty = raw_qty_clean.cast(T.IntegerType())
                clean_qty = F.coalesce(parsed_qty, F.lit(1))
                qty_invalid_expr = F.when(parsed_qty.isNull() & F.col(qty_col).isNotNull() & (F.trim(F.col(qty_col)) != F.lit("")), 1).otherwise(0)
                qty_missing_expr = F.when(F.col(qty_col).isNull() | (F.trim(F.col(qty_col)) == F.lit("")), 1).otherwise(0)
            else:
                clean_qty = F.lit(1)
                qty_invalid_expr = F.lit(0)
                qty_missing_expr = F.lit(1)

            # Revenue / Price
            rev_col = col_map.get('revenue')
            price_col = col_map.get('price')
            if rev_col:
                raw_rev_clean = F.regexp_replace(F.regexp_replace(F.trim(F.col(rev_col)), "\\$", ""), ",", "")
                parsed_rev = raw_rev_clean.cast(T.DoubleType())
                clean_rev = F.coalesce(parsed_rev, F.lit(0.0))
                rev_invalid_expr = F.when(parsed_rev.isNull() & F.col(rev_col).isNotNull() & (F.trim(F.col(rev_col)) != F.lit("")), 1).otherwise(0)
                rev_missing_expr = F.when(F.col(rev_col).isNull() | (F.trim(F.col(rev_col)) == F.lit("")), 1).otherwise(0)
            elif price_col:
                raw_price_clean = F.regexp_replace(F.regexp_replace(F.trim(F.col(price_col)), "\\$", ""), ",", "")
                parsed_price = raw_price_clean.cast(T.DoubleType())
                clean_rev = F.coalesce(parsed_price * clean_qty, F.lit(0.0))
                rev_invalid_expr = F.when(parsed_price.isNull() & F.col(price_col).isNotNull() & (F.trim(F.col(price_col)) != F.lit("")), 1).otherwise(0)
                rev_missing_expr = F.when(F.col(price_col).isNull() | (F.trim(F.col(price_col)) == F.lit("")), 1).otherwise(0)
            else:
                clean_rev = F.lit(0.0)
                rev_invalid_expr = F.lit(0)
                rev_missing_expr = F.lit(1)

            # Date Period
            date_col = col_map.get('date')
            if date_col:
                trimmed_date = F.trim(F.col(date_col))
                # Match YYYY-MM
                period_expr = (
                    F.when(trimmed_date.rlike("^\\d{4}-\\d{1,2}"), F.substring(trimmed_date, 1, 7))
                    .when(trimmed_date.rlike("^\\d{1,2}/\\d{1,2}/\\d{4}"),
                          F.concat(F.substring(trimmed_date, 7, 4), F.lit("-"), F.lpad(F.substring(trimmed_date, 1, 2), 2, "0")))
                    .otherwise(F.lit("Unknown"))
                )
                date_invalid_expr = F.when(period_expr == F.lit("Unknown"), 1).otherwise(0)
                date_missing_expr = F.when(F.col(date_col).isNull() | (F.trim(F.col(date_col)) == F.lit("")), 1).otherwise(0)
            else:
                period_expr = F.lit("Unknown")
                date_invalid_expr = F.lit(1)
                date_missing_expr = F.lit(1)

            # Category Normalization
            cat_col = col_map.get('category')
            if cat_col:
                cat_lower = F.lower(F.trim(F.col(cat_col)))
                cat_norm = (
                    F.when(cat_lower.contains("electronic"), "Electronics")
                    .when(cat_lower.contains("fashion"), "Fashion")
                    .when(cat_lower.contains("appliance"), "Appliances")
                    .when(cat_lower.contains("furniture"), "Furniture")
                    .when(cat_lower.contains("accessori"), "Accessories")
                    .when(cat_lower.contains("bag"), "Bags")
                    .otherwise(F.coalesce(F.initcap(F.trim(F.col(cat_col))), F.lit("General")))
                )
                cat_cleaned_expr = F.when(F.col(cat_col).isNotNull() & (F.col(cat_col) != cat_norm), 1).otherwise(0)
            else:
                cat_norm = F.lit("General")
                cat_cleaned_expr = F.lit(0)

            # Status Normalization
            stat_col = col_map.get('status')
            if stat_col:
                stat_lower = F.lower(F.trim(F.col(stat_col)))
                stat_norm = (
                    F.when(stat_lower.contains("complete"), "Completed")
                    .when(stat_lower.contains("cancel"), "Cancelled")
                    .when(stat_lower.contains("pend"), "Pending")
                    .when(stat_lower.contains("return"), "Returned")
                    .otherwise(F.coalesce(F.initcap(F.trim(F.col(stat_col))), F.lit("Unknown")))
                )
                stat_cleaned_expr = F.when(F.col(stat_col).isNotNull() & (F.col(stat_col) != stat_norm), 1).otherwise(0)
            else:
                stat_norm = F.lit("Unknown")
                stat_cleaned_expr = F.lit(0)

            # Channel Normalization
            chan_col = col_map.get('channel')
            if chan_col:
                chan_lower = F.lower(F.trim(F.col(chan_col)))
                chan_norm = (
                    F.when(chan_lower.contains("online"), "Online")
                    .when(chan_lower.contains("retail"), "Retail")
                    .when(chan_lower.contains("market"), "Marketplace")
                    .when(chan_lower.contains("store"), "Store")
                    .otherwise(F.coalesce(F.initcap(F.trim(F.col(chan_col))), F.lit("General")))
                )
                chan_cleaned_expr = F.when(F.col(chan_col).isNotNull() & (F.col(chan_col) != chan_norm), 1).otherwise(0)
            else:
                chan_norm = F.lit("General")
                chan_cleaned_expr = F.lit(0)

            # Payment Normalization
            pay_col = col_map.get('payment')
            if pay_col:
                pay_lower = F.lower(F.trim(F.col(pay_col)))
                pay_norm = (
                    F.when(pay_lower.contains("net") | pay_lower.contains("banking"), "Net Banking")
                    .when(pay_lower.contains("upi"), "UPI")
                    .when(pay_lower.contains("credit"), "Credit Card")
                    .when(pay_lower.contains("debit"), "Debit Card")
                    .when(pay_lower.contains("cash"), "Cash")
                    .otherwise(F.coalesce(F.initcap(F.trim(F.col(pay_col))), F.lit("General")))
                )
                pay_cleaned_expr = F.when(F.col(pay_col).isNotNull() & (F.col(pay_col) != pay_norm), 1).otherwise(0)
            else:
                pay_norm = F.lit("General")
                pay_cleaned_expr = F.lit(0)

            # Dimensions: Product, City, Salesperson, Customer
            prod_col = col_map.get('product')
            prod_norm = F.coalesce(F.initcap(F.trim(F.col(prod_col))), F.lit("Standard Item")) if prod_col else F.lit("Standard Item")

            city_col = col_map.get('city')
            city_norm = F.coalesce(F.initcap(F.trim(F.col(city_col))), F.lit("General")) if city_col else F.lit("General")

            sp_col = col_map.get('salesperson')
            sp_norm = F.coalesce(F.initcap(F.trim(F.col(sp_col))), F.lit("General")) if sp_col else F.lit("General")

            cust_col = col_map.get('customer')
            cust_norm = F.coalesce(F.initcap(F.trim(F.col(cust_col))), F.lit("General")) if cust_col else F.lit("General")

            # Discount
            disc_col = col_map.get('discount')
            if disc_col:
                raw_disc_clean = F.regexp_replace(F.trim(F.col(disc_col)), "%", "")
                parsed_disc = raw_disc_clean.cast(T.DoubleType())
                clean_disc = F.coalesce(parsed_disc, F.lit(0.0))
            else:
                clean_disc = F.lit(0.0)

            disc_range_expr = (
                F.when(clean_disc > 20, ">20%")
                .when(clean_disc > 10, "11-20%")
                .when(clean_disc > 0, "1-10%")
                .otherwise("0%")
            )

            # Build cleaned DataFrame
            df = raw_df.select(
                clean_id.alias("sale_id"),
                clean_rev.alias("revenue"),
                clean_qty.alias("qty"),
                period_expr.alias("period"),
                cat_norm.alias("category"),
                stat_norm.alias("status"),
                chan_norm.alias("channel"),
                pay_norm.alias("payment"),
                prod_norm.alias("product"),
                city_norm.alias("city"),
                sp_norm.alias("salesperson"),
                cust_norm.alias("customer"),
                clean_disc.alias("discount_pct"),
                disc_range_expr.alias("discount_range"),
                # DQ tracking flags
                (id_missing_expr + qty_missing_expr + rev_missing_expr + date_missing_expr).alias("missing_flag"),
                (qty_invalid_expr + rev_invalid_expr).alias("invalid_num_flag"),
                date_invalid_expr.alias("invalid_date_flag"),
                (cat_cleaned_expr + stat_cleaned_expr + chan_cleaned_expr + pay_cleaned_expr).alias("cleaned_flag")
            )

            if progress_callback:
                progress_callback(55, "Computing distributed PySpark SQL aggregations...")

            # 3. Overall KPI Aggregations (Single tiny row collected)
            kpi_agg = df.select(
                F.sum("revenue").alias("total_revenue"),
                F.sum("qty").alias("total_units"),
                F.countDistinct("sale_id").alias("total_orders"),
                F.count(F.lit(1)).alias("valid_rows"),
                F.avg("discount_pct").alias("avg_discount"),
                F.sum("missing_flag").alias("missing_values"),
                F.sum("invalid_num_flag").alias("invalid_numbers"),
                F.sum("invalid_date_flag").alias("invalid_dates"),
                F.sum("cleaned_flag").alias("cleaned_values")
            ).first()

            total_rev = round(float(kpi_agg["total_revenue"] or 0.0), 2)
            total_units = int(kpi_agg["total_units"] or 0)
            valid_rows = int(kpi_agg["valid_rows"] or 0)
            total_orders = int(kpi_agg["total_orders"] or valid_rows)
            avg_discount = round(float(kpi_agg["avg_discount"] or 0.0), 2)
            avg_order_val = round((total_rev / total_orders), 2) if total_orders > 0 else 0.0

            if progress_callback:
                progress_callback(75, "Collecting small summary metric sets (Top 10s & dimensions)...")

            # 4. Dimensional Aggregations
            # Sales Trend
            trend_rows = (
                df.filter(F.col("period") != "Unknown")
                .groupBy("period")
                .agg(
                    F.sum("revenue").alias("revenue"),
                    F.countDistinct("sale_id").alias("orders"),
                    F.sum("qty").alias("units")
                )
                .orderBy("period")
                .collect()
            )
            sorted_trend = [
                {'period': r["period"], 'revenue': round(float(r["revenue"] or 0.0), 2), 'orders': int(r["orders"] or 0), 'units': int(r["units"] or 0)}
                for r in trend_rows
            ]

            growth_rate = 0.0
            if len(sorted_trend) >= 2:
                last = sorted_trend[-1]['revenue']
                prev = sorted_trend[-2]['revenue']
                if prev > 0:
                    growth_rate = round(((last - prev) / prev) * 100, 2)

            # Top 10 Products
            prod_rows = (
                df.filter(F.col("product") != "Unknown")
                .groupBy("product")
                .agg(
                    F.sum("revenue").alias("revenue"),
                    F.sum("qty").alias("units_sold")
                )
                .orderBy(F.desc("revenue"))
                .limit(10)
                .collect()
            )
            sorted_products = [
                {'product_name': r["product"], 'revenue': round(float(r["revenue"] or 0.0), 2), 'units_sold': int(r["units_sold"] or 0)}
                for r in prod_rows
            ]

            # Category Breakdown
            cat_rows = (
                df.filter(F.col("category") != "Unknown")
                .groupBy("category")
                .agg(
                    F.sum("revenue").alias("revenue"),
                    F.countDistinct("sale_id").alias("orders")
                )
                .orderBy(F.desc("revenue"))
                .collect()
            )
            sorted_categories = [
                {
                    'category': r["category"],
                    'revenue': round(float(r["revenue"] or 0.0), 2),
                    'orders': int(r["orders"] or 0),
                    'percentage': round((float(r["revenue"] or 0.0) / total_rev * 100), 1) if total_rev > 0 else 0.0
                }
                for r in cat_rows
            ]

            # Order Status Breakdown
            stat_rows = (
                df.filter(F.col("status") != "Unknown")
                .groupBy("status")
                .agg(
                    F.countDistinct("sale_id").alias("orders"),
                    F.sum("revenue").alias("revenue")
                )
                .orderBy(F.desc("orders"))
                .collect()
            )
            sorted_statuses = [
                {
                    'status': r["status"],
                    'orders': int(r["orders"] or 0),
                    'revenue': round(float(r["revenue"] or 0.0), 2),
                    'percentage': round((int(r["orders"] or 0) / total_orders * 100), 1) if total_orders > 0 else 0.0
                }
                for r in stat_rows
            ]

            # Sales Channel Breakdown
            chan_rows = (
                df.filter(F.col("channel") != "Unknown")
                .groupBy("channel")
                .agg(
                    F.sum("revenue").alias("revenue"),
                    F.countDistinct("sale_id").alias("orders")
                )
                .orderBy(F.desc("revenue"))
                .collect()
            )
            sorted_channels = [
                {
                    'channel': r["channel"],
                    'revenue': round(float(r["revenue"] or 0.0), 2),
                    'orders': int(r["orders"] or 0),
                    'percentage': round((float(r["revenue"] or 0.0) / total_rev * 100), 1) if total_rev > 0 else 0.0
                }
                for r in chan_rows
            ]

            # Payment Method Breakdown
            pay_rows = (
                df.filter(F.col("payment") != "Unknown")
                .groupBy("payment")
                .agg(
                    F.sum("revenue").alias("revenue"),
                    F.countDistinct("sale_id").alias("orders")
                )
                .orderBy(F.desc("revenue"))
                .collect()
            )
            sorted_payments = [
                {
                    'payment_method': r["payment"],
                    'revenue': round(float(r["revenue"] or 0.0), 2),
                    'orders': int(r["orders"] or 0),
                    'percentage': round((float(r["revenue"] or 0.0) / total_rev * 100), 1) if total_rev > 0 else 0.0
                }
                for r in pay_rows
            ]

            # City Performance (Top 10)
            city_rows = (
                df.filter(F.col("city") != "Unknown")
                .groupBy("city")
                .agg(
                    F.sum("revenue").alias("revenue"),
                    F.countDistinct("sale_id").alias("orders"),
                    F.sum("qty").alias("units_sold")
                )
                .orderBy(F.desc("revenue"))
                .limit(10)
                .collect()
            )
            sorted_cities = [
                {'city': r["city"], 'revenue': round(float(r["revenue"] or 0.0), 2), 'orders': int(r["orders"] or 0), 'units_sold': int(r["units_sold"] or 0)}
                for r in city_rows
            ]

            # Regional Sales from Top 5 Cities
            regional_sales = [
                {
                    'region': c['city'],
                    'revenue': c['revenue'],
                    'orders': c['orders'],
                    'percentage': round(c['revenue'] / total_rev * 100, 1) if total_rev > 0 else 0.0
                }
                for c in sorted_cities[:5]
            ]

            # Salesperson Performance (Top 10)
            sp_rows = (
                df.filter(F.col("salesperson") != "Unknown")
                .groupBy("salesperson")
                .agg(
                    F.sum("revenue").alias("revenue"),
                    F.countDistinct("sale_id").alias("orders"),
                    F.sum("qty").alias("units_sold")
                )
                .orderBy(F.desc("revenue"))
                .limit(10)
                .collect()
            )
            sorted_salespersons = [
                {'salesperson': r["salesperson"], 'revenue': round(float(r["revenue"] or 0.0), 2), 'orders': int(r["orders"] or 0), 'units_sold': int(r["units_sold"] or 0)}
                for r in sp_rows
            ]

            # Customer Analytics (Top 10 & distinct count)
            cust_df = (
                df.filter(F.col("customer") != "Unknown")
                .groupBy("customer")
                .agg(
                    F.sum("revenue").alias("revenue"),
                    F.countDistinct("sale_id").alias("orders")
                )
            )
            unique_cust_count = cust_df.count()
            top_cust_rows = cust_df.orderBy(F.desc("revenue")).limit(10).collect()
            top_customers = [
                {'customer_name': r["customer"], 'revenue': round(float(r["revenue"] or 0.0), 2), 'orders': int(r["orders"] or 0)}
                for r in top_cust_rows
            ]

            # Discount Analytics
            disc_rows = (
                df.groupBy("discount_range")
                .agg(
                    F.sum("revenue").alias("revenue"),
                    F.countDistinct("sale_id").alias("orders")
                )
                .orderBy(F.desc("revenue"))
                .collect()
            )
            discount_ranges = [
                {'range': r["discount_range"], 'revenue': round(float(r["revenue"] or 0.0), 2), 'orders': int(r["orders"] or 0)}
                for r in disc_rows
            ]

            # Data Quality
            duplicate_ids = max(0, valid_rows - total_orders)
            missing_vals = int(kpi_agg["missing_values"] or 0)
            invalid_nums = int(kpi_agg["invalid_numbers"] or 0)
            invalid_dates = int(kpi_agg["invalid_dates"] or 0)
            cleaned_vals = int(kpi_agg["cleaned_values"] or 0)

            elapsed = time.time() - start_time
            logger.info(f"[PYSPARK] Completed DataFrame processing in {elapsed:.2f}s")

            return {
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
                    'missing_values': missing_vals,
                    'invalid_dates': invalid_dates,
                    'invalid_numeric_values': invalid_nums,
                    'normalized_values': cleaned_vals,
                    'rejected_records': max(0, total_uploaded - valid_rows)
                },
                'execution_stats': {
                    'engine': 'Apache PySpark (DataFrame & SQL Engine)',
                    'execution_time_sec': round(elapsed, 3),
                    'data_splits': max(1, valid_rows // 25000),
                    'reducers_completed': self.shuffle_partitions,
                    'hdfs_block_size': 'N/A'
                }
            }

        except Exception as e:
            logger.error(f"[PYSPARK] Execution failed: {e}", exc_info=True)
            raise PySparkExecutionError(f"PySpark job execution failed: {str(e)}")
