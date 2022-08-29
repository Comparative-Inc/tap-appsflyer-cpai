"""AppsFlyer tap class."""

from typing import List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_appsflyer_cpai.streams import MasterAPIStream

STREAM_TYPES = [
    MasterAPIStream
]


class TapAppsFlyer(Tap):
    """AppsFlyer tap class."""
    name = "tap-appsflyer-cpai"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_token",
            th.StringType,
            required=True,
            description="The token to authenticate against the API service"
        ),
        th.Property(
            "app_id",
            th.StringType,
            required=True,
            description="App ID(s) to replicate"
        ),
        th.Property(
            "from",
            th.DateType,
            description="The earliest record date to sync"
        ),
        th.Property(
            "to",
            th.DateType,
            description="The latest record date to sync"
        ),
        th.Property(
            "groupings",
            th.StringType,
            description="Groupings",
            default="pid,c,geo,install_time,app_id"
        ),
        th.Property(
            "kpis",
            th.StringType,
            description="KPIs",
            default="impressions,clicks,average_ecpi,installs,cr,cost,revenue,roi,cohort_day_1_total_revenue_per_user,cohort_day_3_total_revenue_per_user,cohort_day_7_total_revenue_per_user,cohort_day_30_total_revenue_per_user"
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
