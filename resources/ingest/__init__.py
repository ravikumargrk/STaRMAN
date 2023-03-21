# influx stuff
import traceback
import influxdb_client, os
from influxdb_client.client.write_api import SYNCHRONOUS

IFD_TOKN = os.environ.get('IFD_TOKN')
IFD_ORGN = os.environ.get('IFD_ORGN')
IFD_HOST = os.environ.get('IFD_HOST')
IFD_ADDR = f"https://{IFD_HOST}"

write_client = influxdb_client.InfluxDBClient(url=IFD_ADDR, token=IFD_TOKN, org=IFD_ORGN)
write_api = write_client.write_api(write_options=SYNCHRONOUS)

TEMPLATE_PAYLOAD = {
    'orderId'   :   0,
    'key1'      :   'value1',
    'key2'      :   'value2'
}

def write(orderId:int, data:dict, metaData:dict, unitConversionReference:dict):
    formattedData = {}
    status = True
    log = ''
    try:
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
        
        recordDict = {
            'measurement': f'orderId{orderId}',
            'fields'     : formattedData
        }

        write_api.write(bucket='site-data', org=IFD_ORGN, record=recordDict)
        
    except:
        log += traceback.format_exc()
        log += '\n' + str(unitConversionReference)
        status = False

    return {
        'log': log,
        'status': status
    }