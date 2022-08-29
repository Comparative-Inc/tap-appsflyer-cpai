"""Stream type classes for tap-appsflyer-cpai."""

from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Union

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_appsflyer_cpai.client import AppsFlyerStream

# Master API, unlike other Appsflyer APIs, return *display* column names, which are not
# SQL-friendly. Need to convert them back to normal appsflyer col names
# See https://support.appsflyer.com/hc/en-us/articles/213223166-Master-API-user-acquisition-metrics-via-API
COL_NAME_MAPPERS = {
    "App ID": "app_id",
    "Media Source": "pid",
    "Campaign": "c",
    "GEO": "geo",
    "Install Time": "install_time",
    "Touch Type": "attributed_touch_type",
}

class MasterAPIStream(AppsFlyerStream):
    name = "master"
    path = "/export/master_report/v4"
    replication_key = None

    schema = th.PropertiesList(
        th.Property("app_id", th.StringType),
        th.Property("pid", th.StringType),
        th.Property("c", th.StringType),
        th.Property("geo", th.StringType),
        th.Property("install_time", th.DateType),
        th.Property("pid", th.StringType),
        th.Property("Impressions", th.IntegerType),
        th.Property("Clicks", th.IntegerType),
        th.Property("Installs", th.IntegerType),
        th.Property("Cost", th.NumberType),
        th.Property("Average eCPI", th.NumberType),
        th.Property("ROI", th.NumberType),
        th.Property("Revenue", th.NumberType),
        th.Property("Conversion Rate", th.NumberType),
        th.Property("Cohort Day 1 - Total Revenue Per User", th.NumberType),
        th.Property("Cohort Day 3 - Total Revenue Per User", th.NumberType),
        th.Property("Cohort Day 7 - Total Revenue Per User", th.NumberType),
        th.Property("Cohort Day 30 - Total Revenue Per User", th.NumberType),
    ).to_dict()

    @property
    def primary_keys(self) -> Optional[List[str]]:
        return self.config.get("groupings", "").split(",")

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {
            "api_token": self.config.get("api_token"),
            "app_id": self.config.get("app_id"),
            "groupings": self.config.get("groupings"),
            "kpis": self.config.get("kpis"),
            "from": self.config.get("from"),
            "to": self.config.get("to"),
            "format": "json",
        }
        return params

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        for k, v in COL_NAME_MAPPERS.items():
            if k in row:
                row[v] = row[k]
                del row[k]
        return row
