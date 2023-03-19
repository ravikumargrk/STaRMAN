# influx stuff

import influxdb_client, os, time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

IFD_TOKN = os.environ.get('IFD_TOKN')
IFD_ORGN = os.environ.get('IFD_ORGN')
IFD_HOST = os.environ.get('IFD_HOST')
IFD_ADDR = f"https://{IFD_HOST}"

write_client = influxdb_client.InfluxDBClient(url=IFD_ADDRS, token=IFD_TOKEN, org=IFD_ORGZN)
write_api = write_client.write_api(write_options=SYNCHRONOUS)

def write(orderId:int, data:dict, metaData:dict, unitConversionReference:dict):
    formattedData = {}

    for standardTag in metaData:
        dataTagId = metaData[standardTag]['dataTagId']
        unitId = metaData[standardTag]['unitId']
        factor = unitConversionReference[unitId]['factor']
        bias = unitConversionReference[unitId]['bias']

        if dataTagId in data:
            value = data[dataTagId]
            value_si = value*factor + bias
            formattedData.update(
                {
                    standardTag: value_si
                }
            )
        
    # if len(formattedData):
    #     dataPoint = (
    #         Point("census")
    #             .tag("meta", )
    #             .field(data[key]["species"], data[key]["count"])
    #     )

    #     write_api.write(bucket=bucket, org=org, record=point)
    pass