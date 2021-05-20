def update_net(data,context):
    from google.cloud import bigquery
    client = bigquery.Client()
    table_ref = client.dataset("Bullsheet").table("net")
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE # this job will truncate table data and write from the beginning
    job_config.skip_leading_rows = 1
    job_config.source_format = bigquery.SourceFormat.CSV

    uri = "gs://bullsheet/net.csv"
    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()
