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

    def send_measurement(
        self,
        measurement_name: str,
        tags: dict,
        values: Optional[dict] = None,
        value: Optional[Any] = None
    ):
        if value and values or not value and not values:
            raise ValueError("Specify exactly one of 'value' and 'values'")

        p = influxdb_client.Point(measurement_name)
        for tag_name, tag_value in tags.items():
            p = p.tag(tag_name, tag_value)

        if value:
            p = p.field("value", value)
        else:
            for key, value in values.items():
                p = p.field(key, value)

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


