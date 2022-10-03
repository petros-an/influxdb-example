import measurements_api

if __name__ == '__main__':
    api = measurements_api.MeasurementsAPI(
        url='http://68.183.159.42:8086/',
        token='your_token',
    )

    # send single value

    api.send_measurement(
        'some_measurement',
        tags={'location': 'perissos', 'sensor': 'petros'},
        value=13,
    )

    """
        Send multiple values for a single measurement
        
       api.send_measurement(
            'some_measurement',
            tags={'location': 'perissos', 'sensor': 'petros'},
            values={'componentX': 1, 'componentY': 2},
       ) 
    
    """
