# @title Big Query Functions

def execute_query(client, query):
    """
    Execute a SQL query using Google BigQuery and return the results as a DataFrame.

    Args:
    query (str): The SQL query to be executed.
    client: A Google BigQuery client instance.

    Returns:
    DataFrame: The query results as a DataFrame.
    """
    query_job = client.query(query)
    df = query_job.to_dataframe()
    # Remove the index column
    df.reset_index(drop=True, inplace=True)
    return df

def df_to_bq(df, project, dataset, table):
    to_gbq(df, f'{dataset}.{table}', project_id=project, if_exists='replace')

# Create a BigQuery table from a DataFrame with auto-detected schema
def create_bigquery_table(df, project_id, dataset_id, table_id):
    """
    Create a BigQuery table from a Pandas DataFrame with auto-detected schema.

    Args:
        df (pd.DataFrame): The DataFrame to be loaded into BigQuery.
        project_id (str): The Google Cloud project ID.
        dataset_id (str): The BigQuery dataset ID.
        table_id (str): The name for the new BigQuery table.

    Returns:
        None: The function creates the table in BigQuery.

    Example:
        create_bigquery_table_auto_detect(df, 'your-project-id', 'your-dataset-id', 'your-table-id')
    """
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)
    job_config = bigquery.LoadJobConfig(
        autodetect=True  # Enable schema auto-detection
    )
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print(f'Table {project_id}.{dataset_id}.{table_id} created from the DataFrame with auto-detected schema.')

def read_csv_from_gcs(gcs_bucket, file_path):
    """
    Read a CSV file from Google Cloud Storage (GCS) and load it into a Pandas DataFrame.

    Args:
        gcs_bucket (str): The name of the GCS bucket.
        file_path (str): The path to the CSV file in GCS.

    Returns:
        pd.DataFrame: A Pandas DataFrame containing the CSV data.
    """
    # Download the CSV file from GCS
    client = storage.Client()
    bucket = client.get_bucket(gcs_bucket)
    blob = storage.Blob(file_path, bucket)
    content = blob.download_as_text()

    # Create a DataFrame from the CSV data
    df = pd.read_csv(StringIO(content))

    return df

def bigquery_schema(df):
    """
    Generate a BigQuery schema based on the DataFrame's data types.

    Args:
        df (pandas.DataFrame): The DataFrame for which the schema is generated.

    Returns:
        list: A list of dictionaries representing the BigQuery schema with 'name' and 'type' keys.
    """
    dtype_mapping = {
        'object': 'STRING',
        'int64': 'INTEGER',
        'float64': 'FLOAT',
        'bool': 'BOOL',
        'datetime64[ns]': 'TIMESTAMP'
    }

    schema = [{'name': col_name, 'type': dtype_mapping.get(str(col_type), 'UNKNOWN')} for col_name, col_type in df.dtypes.items()]

    return schema

# Function to download a DataFrame as a CSV file
def download(df, fileName):
    """
    Download a DataFrame as a CSV file.

    Args:
    df (DataFrame): The input DataFrame to be downloaded.
    fileName (str): The name of the CSV file (without the .csv extension).

    Returns:
    None
    """
    if df is not None:
      fileName = f'{fileName}.csv'
      filepath = "/content/drive/MyDrive/Colab Downloads/"
      file = filepath + fileName
      df.to_csv(file, index=False)
      files.download(file)