import os
import random
import csv
from datetime import datetime, timedelta

def generate_100k_dataset(output_path: str, num_rows: int = 100000):
    print(f"Generating synthetic sales dataset with {num_rows:,} rows at: {output_path}")
    
    regions = ['North America', 'EMEA', 'Asia Pacific', 'LATAM']
    states = {
        'North America': ['California', 'New York', 'Texas', 'Florida', 'Illinois'],
        'EMEA': ['United Kingdom', 'Germany', 'France', 'Spain', 'Italy'],
        'Asia Pacific': ['Japan', 'Australia', 'Singapore', 'India', 'South Korea'],
        'LATAM': ['Brazil', 'Mexico', 'Argentina', 'Chile', 'Colombia']
    }
    
    categories = {
        'Enterprise Software': ['Hadoop Cluster Enterprise', 'Cloud Data Lake License', 'PySpark Processing Suite', 'BI Dashboard Pro'],
        'Hardware & Servers': ['Rack Server Node X1', 'NVMe Array Drive 4TB', 'High-Speed Switch 100G', 'Distributed Node Memory 128GB'],
        'Analytics Services': ['Big Data Migration Consulting', 'MapReduce Optimization Sprint', 'Data Pipeline Managed Support', 'Custom ML Model Deployment'],
        'Cloud Infrastructure': ['Dedicated HDFS Storage Tier', 'YARN Compute Instances', 'Multi-Region Data Relay', 'Real-time Streaming Gateway']
    }

    first_names = ['Alex', 'Jordan', 'Taylor', 'Morgan', 'Sam', 'Chris', 'Pat', 'Riley', 'Casey', 'Jamie']
    last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez']

    start_date = datetime(2025, 1, 1)

    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'Order_ID', 'Order_Date', 'Customer_Name', 'Region', 'State', 
            'Category', 'Product_Name', 'Unit_Price', 'Quantity', 'Discount', 'Total_Revenue'
        ])
        
        for i in range(1, num_rows + 1):
            order_id = f"ORD-2025-{100000 + i}"
            days_offset = random.randint(0, 600)
            order_date = (start_date + timedelta(days=days_offset)).strftime('%Y-%m-%d')
            customer = f"{random.choice(first_names)} {random.choice(last_names)}"
            region = random.choice(regions)
            state = random.choice(states[region])
            category = random.choice(list(categories.keys()))
            product = random.choice(categories[category])
            
            unit_price = round(random.uniform(150.0, 4500.0), 2)
            quantity = random.randint(1, 15)
            discount = round(random.choice([0.0, 0.05, 0.1, 0.15, 0.2]), 2)
            total_revenue = round(unit_price * quantity * (1.0 - discount), 2)

            writer.writerow([
                order_id, order_date, customer, region, state,
                category, product, unit_price, quantity, discount, total_revenue
            ])

    file_size_mb = round(os.path.getsize(output_path) / (1024 * 1024), 2)
    print(f"Successfully created {output_path} ({file_size_mb} MB)")

if __name__ == '__main__':
    target = os.path.join(os.path.dirname(__file__), 'sample_datasets', 'sales_data_100k.csv')
    generate_100k_dataset(target, 100000)
