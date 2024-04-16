from http import HTTPMethod

from framework.core.handles import HTTPAPIRequestHandle

api_version: str = "api/v1"


class QlikGetReloadsEndpoint(HTTPAPIRequestHandle):
    path = f"{api_version}/reloads"
    method = HTTPMethod.GET


class QlikGetAppsEndpoint(HTTPAPIRequestHandle):
    path = f"{api_version}/apps"
    method = HTTPMethod.GET


class QlikGetCollectionsEndpoint(HTTPAPIRequestHandle):
    path = f"{api_version}/collections"
    method = HTTPMethod.GET
