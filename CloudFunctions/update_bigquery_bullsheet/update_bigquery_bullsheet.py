def update_bigquery_bullsheet(data,context):
    from google.cloud import bigquery
    client = bigquery.Client()
    
    # append trades.csv to "trades" table
    uri = "gs://bullsheet/trades.csv"
    table_ref = client.dataset("Bullsheet").table("trades")
    
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND # https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.job.WriteDisposition.html
    job_config.skip_leading_rows = 1
    job_config.source_format = bigquery.SourceFormat.CSV

    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()

    # append buys_realized.csv to "buys_realized" table
    uri = "gs://bullsheet/buys_realized.csv"
    table_ref = client.dataset("Bullsheet").table("buys_realized")
    
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job_config.skip_leading_rows = 1
    job_config.source_format = bigquery.SourceFormat.CSV

    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()

    # append buys_left.csv to "buys_left" table
    uri = "gs://bullsheet/buys_left.csv"
    table_ref = client.dataset("Bullsheet").table("buys_left")
    
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE # truncate to overwrite and remove old unrealized buys that have already been realized
    job_config.skip_leading_rows = 1
    job_config.source_format = bigquery.SourceFormat.CSV

    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()

    # append sells.csv to "sells" table
    uri = "gs://bullsheet/sells.csv"
    table_ref = client.dataset("Bullsheet").table("sells")
    
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job_config.skip_leading_rows = 1
    job_config.source_format = bigquery.SourceFormat.CSV

    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()
