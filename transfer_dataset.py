import uuid
import argparse
from google.cloud import bigquery
from extended_copy_job import ExtendedCopyJob
from datetime import date, timedelta, datetime

__author__ = "Le Kien Truc"
__license__ = "GPL"
__email__ = "afterlastangel@gmail.com"


# Instantiates a client
table_prefix = ''  # Support with date partitioned tables.
# https://cloud.google.com/bigquery/docs/partitioned-tables

project_id = ''  # project it of the job. It might be the src project
src_project_id = ''  # project id of the source table
src_ds_name = ''  # dataset name of the source table

dest_project_id = ''  # destination project
dest_ds_name = ''  # destination dataset

bigquery_client = bigquery.Client.from_service_account_json('./private_key.json')  # noqa
# The secret key with permission to read source big query dataset and write on
# the destination.


def do_copy_job(
        project_id, bigquery_client,
        dest_project, dest_ds_name,
        src_project_id, src_ds_name, table_name):
    job_id = str(uuid.uuid4())
    print job_id
    job_data = {
                'jobReference': {
                    'projectId': project_id,
                    'jobId': job_id  # noqa
                },
                'configuration': {
                    'copy': {
                        'createDisposition': 'CREATE_IF_NEEDED',
                        'writeDisposition': 'WRITE_TRUNCATE',
                        'destinationTable': {
                            'projectId': dest_project,
                            'datasetId': dest_ds_name,
                            'tableId': table_name
                        },
                        'sourceTable': {
                            'projectId': src_project_id,
                            'datasetId': src_ds_name,
                            'tableId': table_name
                        },
                    }
                }
            }

    copy_job = ExtendedCopyJob.from_api_repr(job_data, bigquery_client)
    copy_job.create_disposition = 'CREATE_IF_NEEDED'
    copy_job.write_disposition = 'WRITE_TRUNCATE'
    copy_job.begin()
    if copy_job.error_result is not None:
        print copy_job.error_result


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--day')
    parser.add_argument('--days_before')
    parser.add_argument('--date_from')
    parser.add_argument('--date_to')
    args = parser.parse_args()

    days = []
    if args.date_from is not None and args.date_to is not None:
        start = datetime.strptime(args.date_from, "%Y%m%d")
        end = datetime.strptime(args.date_to, "%Y%m%d")
        date_generated = [start + timedelta(days=x) for x in range(0, (end-start).days)]  # noqa
        days = [d.strftime('%Y%m%d') for d in date_generated]

    if args.day == "today":
        days.append(date.today().strftime('%Y%m%d'))
    elif args.day == "yesterday":
        days.append((date.today() - timedelta(1)).strftime('%Y%m%d'))

    if args.days_before is not None:
        days.append(
            (date.today() - timedelta(
                int(args.days_before))).strftime('%Y%m%d'))

    for day in days:
        table_name = table_prefix + day
        print table_name

        do_copy_job(
            project_id, bigquery_client,
            dest_project_id, dest_ds_name,
            src_project_id, src_ds_name, table_name)
