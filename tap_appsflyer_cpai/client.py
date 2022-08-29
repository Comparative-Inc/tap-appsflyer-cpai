"""REST client handling, including AppsFlyerStream base class."""

from typing import Any, Dict, Iterable, List, Optional, Union

import requests
from memoization import cached
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream


class AppsFlyerStream(RESTStream):
    """AppsFlyer stream class."""

    url_base = "https://hq.appsflyer.com"

    records_jsonpath = "$[*]"  # Or override `parse_response`.

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())
