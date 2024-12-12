# **Execution Manual for NYC Taxi Insights Project**

This manual provides a step-by-step guide to execute the NYC Taxi Insights Project using the provided files and architecture. It covers setting up the ETL pipeline on Cloud Composer, creating tables in BigQuery, running the machine learning pipeline on Google Colab, and deploying the prediction service on Render.

---

## **Prerequisites**
1. **Google Cloud Platform (GCP):**
   - Ensure you have an active GCP account.
   - Enable the following APIs:
     - Cloud Composer
     - Google Cloud Storage
     - BigQuery
   - Create a Cloud Composer environment and configure it.
   - Create a bucket for raw, processed data, and models (e.g., `nycab-insight-bucket`).

2. **Render Account:**
   - Create an account on Render.com to host the prediction API.

3. **Tools:**
   - Python 3.8+ installed locally or Google Colab for model training.
   - GCP Service Account Key file for authentication.

4. **Environment Configuration:**
   - Install required libraries from `requirements.txt` on your local/virtual environment or in Colab:
     ```bash
     pip install -r requirements.txt
     ```

---

## **Step 1: Create Tables in BigQuery**

1. **Set Up BigQuery Dataset:**
   - Open the BigQuery Console in GCP.
   - Create a dataset named `nycab_dwh`.

2. **Run SQL Scripts to Create Tables:**
   - Use the provided SQL scripts (`Table Creation.sql` and `analytics_tbl.sql`) to create required tables for the project.
   - Navigate to the BigQuery console, select the `nycab_dwh` dataset, and execute the SQL commands to create the following tables:
     - **Fact Table:** `fact_trips`
     - **Dimension Tables:** `datetime_dim`, `location_dim`, `vendor_dim`, `Ratecode_dim`, `payment_type_dim`
     - **Analytics Tables:** Tables for aggregations and dashboards.

3. **Example: Creating the Fact Table**
   - Open the BigQuery editor and paste the script for `fact_trips` from `Table Creation.sql`:
     ```sql
     CREATE TABLE `project_id.nycab_dwh.fact_trips` (
         VendorID INT64,
         datetime_id INT64,
         passenger_count FLOAT64,
         trip_distance FLOAT64,
         PULocationID INT64,
         DOLocationID INT64,
         payment_type INT64,
         RatecodeID INT64,
         store_and_fwd_flag STRING,
         fare_amount FLOAT64,
         extra FLOAT64,
         mta_tax FLOAT64,
         tip_amount FLOAT64,
         tolls_amount FLOAT64,
         improvement_surcharge FLOAT64,
         total_amount FLOAT64,
         congestion_surcharge FLOAT64,
         Airport_fee FLOAT64
     );
     ```
   - Run the query to create the table.

4. **Validate the Schema:**
   - After running the scripts, validate that all tables are created with the correct schema by navigating to the dataset in the BigQuery console.

---

## **Step 2: Set Up the ETL Pipeline**

1. **Upload DAG to Cloud Composer:**
   - Open your GCP Console and navigate to the Cloud Composer environment.
   - Locate the DAGs folder linked to the Composer environment.
   - Upload `nycab_insights_3.py` to the DAGs folder.

2. **Verify DAG Deployment:**
   - Open the Airflow UI from the Composer environment.
   - Confirm that the DAG named `nyc_taxi_etl_2024_backfill_updated` appears in the DAGs list.

3. **Run the ETL Pipeline:**
   - Trigger the DAG from the Airflow UI.
   - Monitor task logs in Airflow to ensure successful execution.
   - The pipeline will:
     - Check data availability for NYC Taxi datasets.
     - Extract data, transform it into a star schema, and upload it to GCS.
     - Load the processed data into the tables created in BigQuery.

4. **Schedule the ETL Pipeline:**
   - Confirm that the DAG is scheduled to run automatically on the **15th of every month** for regular updates.

---

## **Step 3: Run the Model Training Pipeline in Google Colab**

1. **Open `Model_Training.ipynb`:**
   - Upload the notebook file to Google Colab.

2. **Set Up Google Cloud Authentication:**
   - Upload your GCP service account key file to Colab.
   - Authenticate with the following code:
     ```python
     from google.colab import auth
     auth.authenticate_user()

     import os
     os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "your-service-account-key.json"
     ```

3. **Install Required Libraries:**
   - Run the following in a Colab cell:
     ```python
     !pip install torch transformers lightgbm pandas scikit-learn google-cloud-storage
     ```

4. **Load Processed Data:**
   - Fetch the processed data from the GCS bucket:
     ```python
     from google.cloud import storage
     client = storage.Client()
     bucket = client.get_bucket('nycab-insight-bucket')
     blobs = bucket.list_blobs(prefix='processed-data/')
     for blob in blobs:
         blob.download_to_filename(blob.name)
     ```

5. **Run Model Training:**
   - Execute cells for:
     - Data preprocessing.
     - Feature extraction using **BERT**.
     - Training models (e.g., LightGBM) for demand and duration prediction.

6. **Save Trained Models:**
   - Save the trained models to the models folder in GCS:
     ```python
     blob = bucket.blob('models/lgb_regressor_demand.pkl')
     blob.upload_from_filename('demand_model.pkl')

     blob = bucket.blob('models/lgb_regressor_duration.pkl')
     blob.upload_from_filename('duration_model.pkl')
     ```

---

## **Step 4: Deploy the Prediction API on Render**

1. **Prepare Files:**
   - Use `main.py` for the API implementation and ensure it is configured with the following:
     - Model paths in GCS: `models/lgb_regressor_demand.pkl` and `models/lgb_regressor_duration.pkl`.
     - Required environment variables:
       - `GOOGLE_APPLICATION_CREDENTIALS_JSON`: GCP service account JSON string.
       - `OPENAI_API_KEY`: Your OpenAI API key.

2. **Set Up a Web Service on Render:**
   - Upload the `main.py` and `requirements.txt` files to a GitHub repository.
   - Connect the repository to Render.
   - Configure environment variables:
     - `GOOGLE_APPLICATION_CREDENTIALS_JSON`: Add the JSON content of your GCP service account.
     - `OPENAI_API_KEY`: Add your OpenAI API key.

3. **Deploy the API:**
   - Render will automatically install dependencies from `requirements.txt` and start the service.
   - Note the public endpoint URL provided after deployment.

4. **Test the API:**
   - Send POST requests using tools like `Postman` or `curl`:
     ```bash
     curl -X POST <Render-API-URL>/predict \
     -H "Content-Type: application/json" \
     -d '{"model": "demand", "features": ["Manhattan", "Morning", "Wednesday"]}'
     ```

---

## **Step 5: Integrate the User Interface**

1. **UI Setup:**
   - The UI interacts with the API using POST requests.
   - Ensure the UI design aligns with the architecture and connects to the Render API endpoint.

2. **Test the UI:**
   - Provide sample inputs for demand and duration prediction.
   - Validate the API response displayed on the UI.

---

## **Step 6: Monitoring and Maintenance**

1. **ETL Pipeline Monitoring:**
   - Periodically check Airflow logs to ensure the pipeline runs successfully.

2. **Model Performance Updates:**
   - Retrain models periodically using updated datasets to maintain accuracy.

3. **API Health Monitoring:**
   - Use Render’s monitoring tools to ensure uptime and diagnose issues promptly.

---

This comprehensive manual ensures smooth execution of the NYC Taxi Insights project for a highly intellectual user. For further assistance, refer to the included source code and dependencies in the provided files.
