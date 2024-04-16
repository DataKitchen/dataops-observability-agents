from http import HTTPMethod

from framework.core.handles import HTTPAPIRequestHandle


class DatabricksGetRunEndpoint(HTTPAPIRequestHandle):
    path = "api/{jobs_version}/jobs/runs/get"
    method = HTTPMethod.GET


class DatabricksListRunsEndpoint(HTTPAPIRequestHandle):
    path = "api/{jobs_version}/jobs/runs/list"
    method = HTTPMethod.GET
