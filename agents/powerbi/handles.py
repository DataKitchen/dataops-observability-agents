from http import HTTPMethod

from framework.core.handles import HTTPAPIRequestHandle


class PowerBIListGroupsEndpoint(HTTPAPIRequestHandle):
    path = "groups"
    method = HTTPMethod.GET


class PowerBIListDatasetsEndpoint(HTTPAPIRequestHandle):
    path = "groups/{groupId}/datasets"
    method = HTTPMethod.GET


class PowerBIListDatasetRefreshEndpoint(HTTPAPIRequestHandle):
    path = "groups/{groupId}/datasets/{datasetId}/refreshes"
    method = HTTPMethod.GET


class PowerBIListReportsEndpoint(HTTPAPIRequestHandle):
    path = "groups/{groupId}/reports"
    method = HTTPMethod.GET
