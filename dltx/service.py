import os
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.sdk import JobsService, DbfsService, DeltaPipelinesService, WorkspaceService


class MockApiClient:
    def __init__(self):
        self.perform_query_response = {}

    def set_perform_query_result(self, res):
        self.perform_query_response = res

    def perform_query(self, **kwargs):
        return self.perform_query_response


class MockService:
    def __init__(self):
        self.api_client = MockApiClient()


class Service:

    def __init__(self):
        self.api_client = ApiClient(
            host=os.getenv('DATABRICKS_HOST'),
            token=os.getenv('DATABRICKS_TOKEN')
        )
        self.jobs = JobsService(self.api_client)
        self.dbfs = DbfsService(self.api_client)
        self.pipelines = DeltaPipelinesService(self.api_client)
        self.workspace = WorkspaceService(self.api_client)

    def pipeline_create(self, data):
        # databricks cli doesn't support photon setting
        return self.api_client.perform_query(
            'POST',
            '/pipelines',
            data=data
        )

    def pipeline_update(self, pipeline_id, data):
        # databricks cli doesn't support photon setting
        return self.api_client.perform_query(
            'PUT',
            '/pipelines/{pipeline_id}'.format(pipeline_id=pipeline_id),
            data=data
        )
