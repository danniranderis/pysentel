# coding: utf-8
"""
Python script for requesting datapoints from 1-wire-protocol sensors and
report it to an InfluxDB-bucket.
"""
from w1thermsensor import W1ThermSensor, Sensor
import time
from .helpers import InfluxDataIngest


def main():
    """
    Main function. Run continuously with the provided sleep-delay interval.
    """
    # TODO: Move config to a config-file in /etc for package install and get
    #  user to fill in the information
    sensor_names = {
        'SENSOR-ID': 'Human name'}
    delay: int = 10
    url = 'http://localhost:port'
    org = 'org-name'
    bucket = 'bucket-na,e'
    token = 'token'

    # Initialize InfluxDB connection
    influxdb = InfluxDataIngest(url=url, org=org, bucket=bucket, token=token)

    # Run loop as long as this service is running
    while True:
        datapoints = []
        # Initialize 1-wire and get available sensors
        for sensor in W1ThermSensor.get_available_sensors([Sensor.DS18B20]):
            # Append all sensor datapoints to ingest-list with correct
            # informations
            datapoints.append({
                'measurement': 'temperature',
                'tags': {
                    'location': sensor_names[sensor.id],
                    'type': sensor.type.name,
                    'sensor-id': sensor.id},
                'fields': {
                    'value': sensor.get_temperature()}
            })

        # Ingest data
        influxdb.write_points(datapoints)

        time.sleep(delay)


if __name__ == "__main__":
    main()
