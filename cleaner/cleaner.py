"""
    For every measurement listed in the config file,
    dump from start to end in *measurement*_*start*_*end*.csv
    and then delete that range.
"""
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List
import measurements_api
import configparser
import csv


@dataclass
class Config:
    influxdb_token: str
    measurement_names: List[str]
    start: datetime
    end: datetime
    query_limit: int
    influxdb_url: Optional[str] = None
    influxdb_org: Optional[str] = None
    influxdb_bucket: Optional[str] = None


def load_config() -> Config:
    conf = configparser.ConfigParser()
    conf.read('cleaner/example-config.ini')
    conf = Config(
        influxdb_token=conf['influxdb']['token'],
        influxdb_url=conf['influxdb'].get('url', 'http://68.183.159.42:8086/'),
        influxdb_org=conf['influxdb'].get('org', 'dchouse'),
        influxdb_bucket=conf['influxdb'].get('bucket', 'dchouse'),
        measurement_names=conf['cleaner']['measurement_names'].split(','),
        start=datetime.strptime(
            conf['cleaner']['start'],
            '%Y-%m-%d'
        ),
        end=datetime.strptime(
            conf['cleaner']['end'],
            '%Y-%m-%d'
        ),
        query_limit=int(conf['cleaner'].get('query_limit', '100'))
    )
    return conf


def append_to_csv(
    measurements: List,
    filename: str,
):
    with open(filename, 'a') as f:
        writer = csv.writer(f)
        for m in measurements:
            writer.writerow([
                m['_time'],
                m['_value'],
                m['location']
            ])


def get_output_filename(
    measurement_name: str,
    start: datetime,
    end: datetime,
) -> str:
    return f"{measurement_name}_{start.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}.csv"


def dump_measurement_range(
    api: measurements_api.MeasurementsAPI,
    measurement_name: str,
    start: datetime,
    end: datetime,
    query_limit: int,
):
    filename = get_output_filename(measurement_name, start, end)
    print(f'Downloading {measurement_name}: {start} - {end} to {filename}')
    limit, offset = query_limit, 0
    while measurements := api.fetch_all_measurement_data(
        measurement_name=measurement_name,
        start=start,
        stop=end,
        limit=limit,
        offset=offset
    ):
        print(f'Fetching limit={limit}, offset={offset}')
        offset += limit
        append_to_csv(measurements, filename)


def delete_measurement_range(
    api: measurements_api.MeasurementsAPI,
    measurement_name: str,
    start: datetime,
    end: datetime,
):
    print(f'deleting {measurement_name}')
    api.delete_measurements(measurement_name, start, end)


if __name__ == "__main__":
    config = load_config()
    api = measurements_api.MeasurementsAPI(
        token=config.influxdb_token,
        url=config.influxdb_url,
        org=config.influxdb_org,
        bucket=config.influxdb_bucket,
    )
    for measurement_name in config.measurement_names:
        dump_measurement_range(
            api, measurement_name=measurement_name,
            start=config.start, end=config.end,
            query_limit=config.query_limit,
        )
        # delete_measurement_range(
        #     api, measurement_name=measurement_name,
        #     start=config.start, end=config.end,
        # )
