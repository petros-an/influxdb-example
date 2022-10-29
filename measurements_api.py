from datetime import timedelta, datetime
from typing import Optional, Any

import influxdb_client
import logging

from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.client.write_api import SYNCHRONOUS

logger = logging.getLogger(__name__)


class MeasurementsAPI:
    DEFAULT_ORG = 'dchouse'
    DEFAULT_BUCKET = 'dchouse'
    DEFAULT_URL = 'http://68.183.159.42:8086/'

    def __init__(
        self,
        token: str,
        url: Optional[str] = None,
        org: Optional[str] = None,
        bucket: Optional[str] = None
    ):
        self.token = token
        self.url = url or self.DEFAULT_URL
        self.org = org or self.DEFAULT_ORG
        self.bucket = bucket or self.DEFAULT_BUCKET

        self._client = influxdb_client.InfluxDBClient(
            url=self.url,
            token=self.token,
            org=self.org
        )
        self._write_api = self._client.write_api(write_options=SYNCHRONOUS)
        self._query_api = self._client.query_api()
        self._delete_api = self._client.delete_api()

    def send_measurement(
        self,
        measurement_name: str,
        tags: dict,
        value: float,
    ):

        p = influxdb_client.Point(measurement_name)
        for tag_name, tag_value in tags.items():
            p = p.tag(tag_name, tag_value)

        p = p.field("value", value)

        try:
            self._write_api.write(
                bucket=self.bucket,
                org=self.org,
                record=p,
            )
        except InfluxDBError:
            import traceback
            e = traceback.format_exc()
            logger.error(e)
            
    def read_aggregated_measurements(
        self,
        measurement_name: str,
        start: timedelta,
        location: str,
        aggregate_window: Optional[timedelta] = None,
    ):
        q = """
            from(bucket: bucket_name)
              |> range(start: _start, stop: now())
              |> filter(fn: (r) => r._measurement == measurement_name)
              |> filter(fn: (r) => r["location"] )
              |> aggregateWindow(every: aggrWindow, fn: mean)
        """
        aggregate_window = aggregate_window or timedelta(seconds=1)

        data = self._query_api.query(
            q,
            params={
                'bucket_name': self.bucket,
                'measurement_name': measurement_name,
                'aggrWindow': aggregate_window,
                '_location': location,
                '_start': start,
            }
        )
        return [
            r.values for r in data[0].records
        ]

    def fetch_all_measurement_data(
        self,
        measurement_name: str,
        start: datetime,
        stop: datetime,
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
    ):
        q = """
            from(bucket: bucket_name)
               |> range(start: _start, stop: now())
               |> filter(fn: (r) => r._measurement == measurement_name)
               |> limit(n: _limit, offset: _offset) 
         """
        data = self._query_api.query(
            q,
            params={
                'bucket_name': self.bucket,
                'measurement_name': measurement_name,
                '_start': start,
                '_stop': stop,
                '_limit': limit,
                '_offset': offset,
            }
        )
        return [
            r.values for r in data[0].records
        ] if data else None

    def delete_measurements(
        self,
        measurement_name: str,
        start: datetime,
        stop: datetime,
    ):
        self._delete_api.delete(
            start,
            stop,
            f"_measurement=\"{measurement_name}\"",
            bucket=self.bucket,
            org=self.org,
        )
