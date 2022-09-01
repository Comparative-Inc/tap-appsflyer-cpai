"""Stream type classes for tap-appsflyer-cpai."""

import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests
from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_appsflyer_cpai.client import AppsFlyerStream

# Master API, unlike other Appsflyer APIs, return *display* column names, which are not
# SQL-friendly. Need to convert them back to normal appsflyer col names
# See https://support.appsflyer.com/hc/en-us/articles/213223166-Master-API-user-acquisition-metrics-via-API
COL_NAME_MAPPERS = {
    "App ID": "app_id",
    "Media Source": "pid",
    "Agency": "af_prt",
    "Campaign": "c",
    "Adset": "af_adset",
    "Ad": "af_ad",
    "Channel": "af_channel",
    "Publisher ID": "af_siteid",
    "Keywords": "af_keywords",
    "Is Primary Attribution": "is_primary",
    "Campaign ID": "af_c_id",
    "Adset ID": "af_adset_id",
    "Ad ID": "af_ad_id",
    "Install Time": "install_time",
    "Touch Type": "attributed_touch_type",
    "GEO": "geo",
    "Clicks": "clicks",
    "Installs": "installs",
    "Impressions": "impressions",
    "Average eCPI": "average_ecpi",
    "Conversion Rate": "cr",
    "Cost": "cost",
    "Revenue": "revenue",
    "ROI": "roi",
    "Cohort Day 1 - Total Revenue Per User": "cohort_day_1_total_revenue_per_user",
    "Cohort Day 3 - Total Revenue Per User": "cohort_day_3_total_revenue_per_user",
    "Cohort Day 7 - Total Revenue Per User": "cohort_day_7_total_revenue_per_user",
    "Cohort Day 30 - Total Revenue Per User": "cohort_day_30_total_revenue_per_user",
}

"""
Paginate so that every request takes only 1 days
It's a weird appsflyer master api issue: if the duration is more than SAFE_DATE_RANGE days, some ROAS metrics turn to 0.

Reported the bug to appsflyer
"""
SAFE_DATE_RANGE = 1
class MasterAPIStream(AppsFlyerStream):
    name = "appsflyer_master_api"
    path = "/export/master_report/v4"
    replication_key = None

    schema = th.PropertiesList(
        th.Property("app_id", th.StringType),
        th.Property("pid", th.StringType),
        th.Property("c", th.StringType),
        th.Property("geo", th.StringType),
        th.Property("install_time", th.DateType),
        th.Property("pid", th.StringType),
        th.Property("impressions", th.IntegerType),
        th.Property("clicks", th.IntegerType),
        th.Property("installs", th.IntegerType),
        th.Property("cost", th.NumberType),
        th.Property("average_ecpi", th.NumberType),
        th.Property("roi", th.NumberType),
        th.Property("revenue", th.NumberType),
        th.Property("cr", th.NumberType),
        th.Property("cohort_day_1_total_revenue_per_user", th.NumberType),
        th.Property("cohort_day_3_total_revenue_per_user", th.NumberType),
        th.Property("cohort_day_7_total_revenue_per_user", th.NumberType),
        th.Property("cohort_day_30_total_revenue_per_user", th.NumberType),
    ).to_dict()

    @property
    def primary_keys(self) -> Optional[List[str]]:
        return self.config.get("groupings", "").split(",")

    def get_date_range(self) -> Tuple[datetime.datetime, datetime.datetime]:
        now = datetime.datetime.utcnow()
        to_date = now - datetime.timedelta(days = self.config.get("up_to_days_ago"))
        from_date = to_date - datetime.timedelta(days = self.config.get("date_range") - 1)
        return from_date, to_date

    def get_next_page_token( self, response: requests.Response, previous_token: Optional[datetime.datetime]) -> Optional[datetime.datetime]:
        from_date, to_date = self.get_date_range()
        page_from_date = (previous_token or from_date) + datetime.timedelta(days = SAFE_DATE_RANGE)
        if page_from_date > to_date:
            return None
        return page_from_date

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        from_date, to_date = self.get_date_range()

        from_date = next_page_token or from_date
        to_date = min(to_date, from_date + datetime.timedelta(days = SAFE_DATE_RANGE - 1))

        from_str = from_date.strftime("%Y-%m-%d")
        to_str = to_date.strftime("%Y-%m-%d")

        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {
            "api_token": self.config.get("api_token"),
            "app_id": self.config.get("app_id"),
            "groupings": self.config.get("groupings"),
            "kpis": self.config.get("kpis"),
            "from": from_str,
            "to": to_str,
            "format": "json",
        }
        return params

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        for k, v in COL_NAME_MAPPERS.items():
            if k in row:
                row[v] = row[k]
                del row[k]
        return row
