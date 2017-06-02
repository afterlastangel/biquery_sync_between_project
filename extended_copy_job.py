from google.cloud.bigquery.job import CopyJob
from google.cloud.bigquery.dataset import Dataset
from google.cloud.bigquery.table import Table


__author__ = "Le Kien Truc"
__license__ = "GPL"
__email__ = "afterlastangel@gmail.com"


class ExtendedCopyJob(CopyJob):
    """ Big query client has a bug when working with many projects.
    This class purpose is fixing that problem
    """

    @classmethod
    def from_api_repr(cls, resource, client):
        """Factory:  construct a job given its API representation

        .. note:

           This method assumes that the project found in the resource matches
           the client's project.

        :type resource: dict
        :param resource: dataset job representation returned from the API

        :type client: :class:`google.cloud.bigquery.client.Client`
        :param client: Client which holds credentials and project
                       configuration for the dataset.

        :rtype: :class:`google.cloud.bigquery.job.CopyJob`
        :returns: Job parsed from ``resource``.
        """
        name, config = cls._get_resource_config(resource)
        dest_config = config['destinationTable']
        dataset = Dataset(
            dest_config['datasetId'], client,
            project=dest_config['projectId']
        )
        destination = Table(dest_config['tableId'], dataset)
        sources = []
        source_configs = config.get('sourceTables')
        if source_configs is None:
            single = config.get('sourceTable')
            if single is None:
                raise KeyError(
                    "Resource missing 'sourceTables' / 'sourceTable'")
            source_configs = [single]
        for source_config in source_configs:
            dataset = Dataset(
                source_config['datasetId'], client,
                project=source_config['projectId']
            )
            sources.append(Table(source_config['tableId'], dataset))
        job = cls(name, destination, sources, client=client)
        job._set_properties(resource)
        return job
