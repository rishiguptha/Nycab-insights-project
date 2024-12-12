# dags/nyc_taxi_etl_2024.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pendulum
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import pandas as pd
import io
from google.cloud import storage

# Configuration
PROJECT_ID = 'nycab-insights-project'
RAW_BUCKET = 'nycab-insight-bucket' 
PROCESSED_BUCKET = 'nycab-insight-bucket'
DATASET = 'nycab_dwh'

def get_available_months():
    """Get list of available months considering 2-month lag"""
    current_date = pendulum.now()
    latest_available = current_date.subtract(months=2)
    
    months = []
    if latest_available.year == 2024:
        for month in range(1, min(latest_available.month + 1, 13)):
            months.append(f"2024-{month:02d}")
    
    return months

def check_file_availability(year_month):
    """Check if file exists on TLC website"""
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year_month}.parquet'
    try:
        response = requests.head(url, timeout=10)
        available = response.status_code == 200
        print(f"TLC file availability for {year_month}: {'✓' if available else '✗'}")
        return available
    except requests.exceptions.RequestException as e:
        print(f"Error checking file availability: {str(e)}")
        return False

def check_month_processed(**context):
    """Check if data is available and not already processed"""
    execution_date = context.get('execution_date')
    year_month = execution_date.strftime('%Y-%m')
    
    # Get current date and calculate latest available month (2-month lag)
    current_date = pendulum.now()
    latest_available = current_date.subtract(months=2)
    
    print(f"\nProcessing Check for {year_month}")
    print(f"Current date: {current_date.strftime('%Y-%m-%d')}")
    print(f"Latest available month: {latest_available.strftime('%Y-%m')}")
    
    # Convert year_month to pendulum for comparison
    target_date = pendulum.from_format(year_month, 'YYYY-MM')
    
    # Skip if data not yet available
    if target_date > latest_available:
        print(f"⚠️ Skipping {year_month} - Data not yet available (2-month lag rule)")
        context['task_instance'].xcom_push(key='month_exists', value=True)
        return False
    
    # Skip if not 2024 data
    if target_date.year != 2024:
        print(f"⚠️ Skipping {year_month} - not in target year 2024")
        context['task_instance'].xcom_push(key='month_exists', value=True)
        return False
    
    # Check file availability on TLC website
    if not check_file_availability(year_month):
        print(f"⚠️ Source file for {year_month} not available on TLC website")
        context['task_instance'].xcom_push(key='month_exists', value=True)
        return False
        
    # Set values in XCom for other tasks
    context['task_instance'].xcom_push(key='current_year_month', value=year_month)
    
    # Check BigQuery for existing data
    bq_hook = BigQueryHook(gcp_conn_id='google_cloud_default', use_legacy_sql=False)
    
    try:
        fact_query = f"""
            SELECT COUNT(*) as count
            FROM `{PROJECT_ID}.{DATASET}.fact_trips` f
            JOIN `{PROJECT_ID}.{DATASET}.datetime_dim` d
            ON f.datetime_id = d.datetime_id
            WHERE d.year_month = '{year_month}'
        """
        
        fact_result = bq_hook.get_first(fact_query)
        fact_count = fact_result[0] if fact_result else 0
        
        print(f"Found {fact_count} existing records for {year_month}")
        
        month_exists = fact_count > 0
        context['task_instance'].xcom_push(key='month_exists', value=month_exists)
        
        if month_exists:
            print(f"✓ Data for {year_month} already exists. Skipping processing.")
            return False
            
        print(f"➜ No existing data found for {year_month}. Proceeding with processing.")
        return True
        
    except Exception as e:
        print(f"Error checking BigQuery: {str(e)}")
        context['task_instance'].xcom_push(key='month_exists', value=False)
        return True

def extract_data(**context):
    """Download NYC taxi data and upload to GCS"""
    # Get execution date from context
    execution_date = context.get('execution_date')
    year_month = execution_date.strftime('%Y-%m')
    
    # Check if data already exists
    month_exists = context['task_instance'].xcom_pull(key='month_exists')
    
    if month_exists:
        print(f"✓ Skipping extraction for {year_month} as it already exists")
        return None
    
    # Validate year_month
    if not year_month or year_month == 'None':
        raise ValueError(f"Invalid year_month value: {year_month}")
        
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year_month}.parquet'
    print(f"➜ Downloading data from: {url}")
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        # Verify content size
        content_size = len(response.content)
        print(f"Downloaded file size: {content_size / (1024*1024):.2f} MB")
        
        if content_size < 1000:
            raise ValueError("Downloaded file seems too small")
        
        raw_path = f'raw-data/yellow_tripdata_{year_month}.parquet'
        gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
        
        gcs_hook.upload(
            bucket_name=RAW_BUCKET,
            object_name=raw_path,
            data=response.content,
            mime_type='application/octet-stream'
        )
        
        print(f"✓ Successfully uploaded to GCS: gs://{RAW_BUCKET}/{raw_path}")
        context['task_instance'].xcom_push(key='current_year_month', value=year_month)
        return raw_path
        
    except Exception as e:
        print(f"⚠️ Error in extract_data: {str(e)}")
        raise

def transform_data(**context):
    """Transform taxi data into dimensional model"""
    if context['task_instance'].xcom_pull(task_ids='extract_data') is None:
        print("✓ Skipping transformation as extraction was skipped")
        return None
        
    year_month = context['task_instance'].xcom_pull(key='current_year_month')
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
    
    try:
        print(f"➜ Starting transformation for {year_month}")
        
        # Read raw data
        raw_path = f'raw-data/yellow_tripdata_{year_month}.parquet'
        raw_data = gcs_hook.download(
            bucket_name=RAW_BUCKET,
            object_name=raw_path
        )
        df = pd.read_parquet(io.BytesIO(raw_data))
        print(f"✓ Loaded {len(df)} records from raw data")

        # Create datetime dimension
        datetime_dim = pd.DataFrame()
        datetime_dim['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        datetime_dim['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
        datetime_dim['datetime_id'] = datetime_dim.index.astype('int64')
        datetime_dim['year_month'] = pd.Series([year_month] * len(datetime_dim), dtype='string')
        
        # Add datetime attributes
        for prefix in ['pickup', 'dropoff']:
            datetime_field = f'tpep_{prefix}_datetime'
            datetime_dim[f'{prefix}_hour'] = datetime_dim[datetime_field].dt.hour.astype('int64')
            datetime_dim[f'{prefix}_day'] = datetime_dim[datetime_field].dt.day.astype('int64')
            datetime_dim[f'{prefix}_month'] = datetime_dim[datetime_field].dt.month.astype('int64')
            datetime_dim[f'{prefix}_year'] = datetime_dim[datetime_field].dt.year.astype('int64')
            datetime_dim[f'{prefix}_weekday'] = datetime_dim[datetime_field].dt.dayofweek.astype('int64')
            datetime_dim[f'{prefix}_date'] = datetime_dim[datetime_field].dt.date

        # Validate year_month column
        null_count = datetime_dim['year_month'].isnull().sum()
        if null_count > 0:
            print(f"⚠️ WARNING: Found {null_count} null values in year_month column")
            datetime_dim['year_month'] = datetime_dim['year_month'].fillna(year_month)

        # Create dimension tables
        payment_type_dim = pd.DataFrame({
            'payment_type': pd.Series([1, 2, 3, 4, 5, 6], dtype='int64'),
            'payment_name': ["Credit card", "Cash", "No charge", "Dispute", "Unknown", "Voided trip"]
        })
        
        Ratecode_dim = pd.DataFrame({
            'RatecodeID': pd.Series([1, 2, 3, 4, 5, 6], dtype='int64'),
            'ratecode_name': ["Standard rate", "JFK", "Newark", "Nassau or Westchester", 
                            "Negotiated fare", "Group ride"]
        })
        
        vendor_dim = pd.DataFrame({
            'VendorID': pd.Series([1, 2], dtype='int64'),
            'vendor_name': ["Creative Mobile Technologies, LLC", "VeriFone Inc."]
        })
        
        # Process location dimension
        location_data = gcs_hook.download(
            bucket_name=RAW_BUCKET,
            object_name='add-data/taxi_zone_lookup.csv'
        )
        location_dim = pd.read_csv(io.StringIO(location_data.decode('utf-8')))
        location_dim['LocationID'] = location_dim['LocationID'].astype('int64')

        # Create and process fact table
        print("➜ Creating fact table...")
        fact_trips = df.merge(
            datetime_dim[['datetime_id', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']],
            on=['tpep_pickup_datetime', 'tpep_dropoff_datetime']
        )
        
        # Select and cast columns
        fact_columns = [
            'VendorID', 'datetime_id', 'passenger_count', 'trip_distance',
            'PULocationID', 'DOLocationID', 'payment_type', 'RatecodeID',
            'store_and_fwd_flag', 'fare_amount', 'extra', 'mta_tax',
            'tip_amount', 'tolls_amount', 'improvement_surcharge',
            'total_amount', 'congestion_surcharge', 'Airport_fee'
        ]
        fact_trips = fact_trips[fact_columns]
        
        # Cast columns to proper types
        int_columns = ['VendorID', 'datetime_id', 'PULocationID', 'DOLocationID', 'payment_type', 'RatecodeID']
        float_columns = [
            'passenger_count', 'trip_distance', 'fare_amount', 'extra',
            'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge',
            'total_amount', 'congestion_surcharge', 'Airport_fee'
        ]
        
        for col in int_columns:
            fact_trips[col] = fact_trips[col].fillna(0).astype('int64')
        for col in float_columns:
            fact_trips[col] = fact_trips[col].fillna(0.0).astype('float64')
        
        fact_trips['store_and_fwd_flag'] = fact_trips['store_and_fwd_flag'].fillna('N').astype(str)

        # Save all tables
        tables = {
            'datetime_dim': datetime_dim,
            'payment_type_dim': payment_type_dim,
            'Ratecode_dim': Ratecode_dim,
            'location_dim': location_dim,
            'vendor_dim': vendor_dim,
            'fact_trips': fact_trips
        }
        
        print("\n➜ Saving transformed tables:")
        for table_name, df in tables.items():
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer)
            
            processed_path = f'processed-data/{year_month}/{table_name}.parquet'
            gcs_hook.upload(
                bucket_name=PROCESSED_BUCKET,
                object_name=processed_path,
                data=parquet_buffer.getvalue(),
                mime_type='application/octet-stream'
            )
            print(f"✓ Saved {table_name}: {len(df)} records")
            
        print(f"\n✓ Transformation completed for {year_month}")
        return f"processed-data/{year_month}/"
        
    except Exception as e:
        print(f"⚠️ Error in transform_data: {str(e)}")
        raise

def load_to_bigquery(**context):
    """Load tables to BigQuery with appropriate write disposition"""
    if context['task_instance'].xcom_pull(task_ids='transform_data') is None:
        print("✓ Skipping BigQuery load as transformation was skipped")
        return None
        
    year_month = context['task_instance'].xcom_pull(key='current_year_month')
    print(f"\n➜ Starting BigQuery load for {year_month}")
    
    try:
        # Define schemas for all tables
        schemas = {
            'location_dim': [
                {'name': 'LocationID', 'type': 'INTEGER', 'mode': 'REQUIRED'},
                {'name': 'Borough', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'Zone', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'service_zone', 'type': 'STRING', 'mode': 'NULLABLE'}
            ],
            'datetime_dim': [
                {'name': 'datetime_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
                {'name': 'tpep_pickup_datetime', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
                {'name': 'tpep_dropoff_datetime', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
                {'name': 'year_month', 'type': 'STRING', 'mode': 'REQUIRED'},
                {'name': 'pickup_hour', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                {'name': 'pickup_day', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                {'name': 'pickup_month', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                {'name': 'pickup_year', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                {'name': 'pickup_weekday', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                {'name': 'pickup_date', 'type': 'DATE', 'mode': 'NULLABLE'},
                {'name': 'dropoff_hour', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                {'name': 'dropoff_day', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                {'name': 'dropoff_month', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                {'name': 'dropoff_year', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                {'name': 'dropoff_weekday', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                {'name': 'dropoff_date', 'type': 'DATE', 'mode': 'NULLABLE'}
            ],
            'payment_type_dim': [
                {'name': 'payment_type', 'type': 'INTEGER', 'mode': 'REQUIRED'},
                {'name': 'payment_name', 'type': 'STRING', 'mode': 'NULLABLE'}
            ],
            'Ratecode_dim': [
                {'name': 'RatecodeID', 'type': 'INTEGER', 'mode': 'REQUIRED'},
                {'name': 'ratecode_name', 'type': 'STRING', 'mode': 'NULLABLE'}
            ],
            'vendor_dim': [
                {'name': 'VendorID', 'type': 'INTEGER', 'mode': 'REQUIRED'},
                {'name': 'vendor_name', 'type': 'STRING', 'mode': 'NULLABLE'}
            ],
            'fact_trips': [
                {'name': 'VendorID', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                {'name': 'datetime_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                {'name': 'passenger_count', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'trip_distance', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'PULocationID', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                {'name': 'DOLocationID', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                {'name': 'payment_type', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                {'name': 'RatecodeID', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                {'name': 'store_and_fwd_flag', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'fare_amount', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'extra', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'mta_tax', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'tip_amount', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'tolls_amount', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'improvement_surcharge', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'total_amount', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'congestion_surcharge', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'Airport_fee', 'type': 'FLOAT', 'mode': 'NULLABLE'}
            ]
        }

        # Load dimension tables
        dim_tables = [
            'payment_type_dim',
            'Ratecode_dim', 
            'location_dim',
            'vendor_dim'
        ]

        print("\n➜ Loading dimension tables...")
        for table in dim_tables:
            load_task = GCSToBigQueryOperator(
                task_id=f'load_{table}_to_bigquery',
                bucket=PROCESSED_BUCKET,
                source_objects=[f'processed-data/{year_month}/{table}.parquet'],
                destination_project_dataset_table=f'{PROJECT_ID}.{DATASET}.{table}',
                source_format='PARQUET',
                write_disposition='WRITE_TRUNCATE',
                autodetect=False,
                schema_fields=schemas[table],
                dag=context['dag']
            )
            load_task.execute(context)
            print(f"✓ Loaded dimension table: {table}")

        # Load fact tables
        print("\n➜ Loading fact tables...")
        fact_tables = ['datetime_dim', 'fact_trips']
        
        for table in fact_tables:
            load_task = GCSToBigQueryOperator(
                task_id=f'load_{table}_to_bigquery',
                bucket=PROCESSED_BUCKET,
                source_objects=[f'processed-data/{year_month}/{table}.parquet'],
                destination_project_dataset_table=f'{PROJECT_ID}.{DATASET}.{table}',
                source_format='PARQUET',
                write_disposition='WRITE_APPEND',
                autodetect=False,
                schema_fields=schemas[table],
                dag=context['dag']
            )
            load_task.execute(context)
            print(f"✓ Loaded fact table: {table}")

        print(f"\n✓ All tables loaded successfully for {year_month}!")
        return "Load completed"

    except Exception as e:
        print(f"⚠️ Error in load_to_bigquery: {str(e)}")
        raise

# Create DAG
with DAG(
    'nyc_taxi_etl_2024_backfill_updated',
    default_args={
        'owner': 'airflow',
        'depends_on_past': True,
        'start_date': datetime(2024, 1, 15),
        'end_date': datetime(2024, 12, 15),
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='NYC Taxi ETL Pipeline for 2024 Data',
    schedule_interval='0 0 15 * *',  # Run on 15th of each month
    catchup=True,
    max_active_runs=2,
    tags=['2024', 'taxi_data']
) as dag:

    # Define tasks
    check_month_task = PythonOperator(
        task_id='check_month_processed',
        python_callable=check_month_processed,
        provide_context=True
    )

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        provide_context=True
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True
    )
    
    load_task = PythonOperator(
        task_id='load_to_bigquery',
        python_callable=load_to_bigquery,
        provide_context=True
    )

    # Set task dependencies
    check_month_task >> extract_task >> transform_task >> load_task