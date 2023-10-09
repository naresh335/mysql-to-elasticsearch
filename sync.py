import time
from datetime import timedelta
from elasticsearch import Elasticsearch
import mysql.connector

# MySQL connection configuration
mysql_config = {
    'user': 'mysqluser',
    'password': 'mysqluserpassword',
    'host': 'mysqlhost',
    'database': 'mysqldatabasename',
    'raise_on_warnings': True
}

# Elasticsearch connection configuration
es_config = {
    'host': 'elasticsearch_host',
    'port': 9200,
    'scheme': 'http'
    # 'http_auth': ('elasticsearch_user', 'elasticsearch_password')
}

# Initialize connections
es = Elasticsearch([es_config])
mysql_conn = mysql.connector.connect(**mysql_config)
mysql_cursor = mysql_conn.cursor()

# Define the MySQL table and Elasticsearch index names
mysql_table = 'mysqltable_name'
es_index = 'elasticsearchindex_name'

# Set the batch size and total rows count
batch_size = 1000
total_rows = mysql_table_num_rows

# Calculate the number of batches
num_batches = total_rows // batch_size + (total_rows % batch_size > 0)
for batch in range(num_batches):
    # Calculate the offset for the current batch
    # uncomment next 2 lines to skip batches skip if script breaks for whatever reason
    #if batch < 684:
    #    continue
    offset = batch * batch_size

    # Fetch rows for the current batch from MySQL
    query = f'SELECT * FROM {mysql_table} LIMIT {batch_size} OFFSET {offset}'
    mysql_cursor.execute(query)
    rows = mysql_cursor.fetchall()

    # Process each row and index/update in Elasticsearch
    total_time = 0
    start_time = time.time()  # Start time for the current batch
    for row in rows:
        # Map column names to Elasticsearch indexes
        doc = {mysql_cursor.description[i][0]: row[i] for i in range(len(mysql_cursor.description))}

        # Get the MySQL ID from the row
        mysql_id = row[0]

        # Check if the document with the same ID already exists in Elasticsearch
        if es.exists(index=es_index, id=mysql_id):
            # Update the existing document with the new data
            es.update(index=es_index, id=mysql_id, body={'doc': doc})
        else:
            # Index a new document in Elasticsearch
            es.index(index=es_index, id=mysql_id, body=doc)

    # Calculate time taken for the current batch
    batch_time = time.time() - start_time
    total_time += batch_time

    # Calculate average time per batch
    avg_time_per_batch = total_time / (batch + 1)

    # Calculate remaining time
    remaining_batches = num_batches - (batch + 1)
    remaining_time = avg_time_per_batch * remaining_batches

    # Convert remaining_time to timedelta for formatting
    remaining_time_td = timedelta(seconds=int(remaining_time))

    # Print statistics after every batch update
    print(f'Batch {batch+1}/{num_batches} processed. Rows indexed: {len(rows)}. '
          f'Time taken: {batch_time:.2f} seconds. '
          f'Estimated time remaining: {remaining_time_td}')

# Close connections
mysql_cursor.close()
mysql_conn.close()
