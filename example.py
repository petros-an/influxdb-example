from datetime import timedelta, datetime

import measurements_api

if __name__ == '__main__':
    api = measurements_api.MeasurementsAPI(
        url='http://68.183.159.42:8086/',
        token='your_token',
    )
    measurements = api.read_measurements(
        measurement_name='B_Temperature',
        start=timedelta(minutes=-41),
        location='Living_Room',
        aggregateWindow=timedelta(minutes=1)
    )
    pass
