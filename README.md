# Certificate:

Service account for big query which have read access on source project and write access on destination project

  Recommendation using json key format


# Install:

  sudo pip install -r requirement.txt

# Configuration:


table_prefix = ''  # Support with date partitioned tables. https://cloud.google.com/bigquery/docs/partitioned-tables:w

project_id = ''  # project it of the job. It might be the src project
src_project_id = ''  # project id of the source table
src_ds_name = ''  # dataset name of the source table

dest_project_id = ''  # destination project
dest_ds_name = ''  # destination dataset

bigquery_client = bigquery.Client.from_service_account_json('./private_key.json')  # noqa

"""
The secret key with permission to read source big query dataset and write on
the destination.
"""

# Usage:

  python transfer_dataset.py --day=today

or 

  python transfer_dataset.py --day=yesterday

or

  python transfer_dataset.py --date_from=20170501 --date_to=20170505

or 

  python transfer_dataset.py --days_before=2


