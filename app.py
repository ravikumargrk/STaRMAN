from flask import Flask, request
from flask_restful import Resource, Api
import os

from resources import onboardTool
from resources import ingest

app = Flask(__name__)
api = Api(app)


# ENV_LIST = [
#     'PORT',
#     'INSTANCE_CONNECTION_NAME',
#     'DB_USER',
#     'DB_PASS'
# ]

class onboarding(Resource):
    def get(self):
        return {
                'message': 'This endpoint is for onboarding new unit',
                'documentation':'Method for new onboarding: POST\nExample payload is mentioned in the schema',
                'schema': onboardTool.TEMPLATE_PAYLOAD
            }
    
    def post(self):
        # request.json
        data:dict = request.get_json()
        result = onboardTool.processNewOrder(data)
        return result

class dataIngestor(Resource):
    def get(self):
        return {
                'message': 'This endpoint is for storing data',
                'documentation':'Method for sending data: POST\nExample payload is mentioned in the schema',
                'schema': ingest.TEMPLATE_PAYLOAD
            }
    
    def post(self):
        # request.json
        data:dict = request.get_json()
        
        # first get common data.
        result = ingest.getCommonData()
        if not result['status']:
            return {
                'log': result['log'],
                'status': result['status']
            }
        else:
            unitsReference = result['unitsReference']
            timeZonesByOrderId = result['timeZonesByOrderId']
        
        # now get meta data.
        orderId = int(data['orderId'])
        result = ingest.getSiteMeta(orderId)
        if not result['status']:
            return {
                'log': result['log'],
                'status': result['status']
            }
        else:
            siteMetaData = result['siteMetaData']
            metaDataObject = result['metaDataObject']
        
        # now pre - process the data
        result = ingest.dataPreProcessor(data, siteMetaData, orderId, unitsReference, timeZonesByOrderId)
        if not result['status']:
            return {
                'log': result['log'],
                'status': result['status']
            }
        else:
            record = result['data']
        
        # finally write data
        result = ingest.writeSingleRecord(record, metaDataObject, orderId)
        # return result !
        # call other api's from here !
        result.update({'data': str(record)})
        return result

    def patch(self):
        # request.json
        data:dict = request.get_json()
        timeStamp = data['t']
        orderId = int(data['orderId'])

        # now get meta data.
        orderId = int(data['orderId'])
        result = ingest.getSiteMeta(orderId)
        if not result['status']:
            return {
                'log': result['log'],
                'status': result['status']
            }
        else:
            # siteMetaData = result['siteMetaData']
            metaDataObject = result['metaDataObject']

        record = {key: data[key] for key in data if key not in ['t']}
        result = ingest.updateSingleRecord(timeStamp, record, metaDataObject, orderId)
        
        return result
    
api.add_resource(onboarding, '/onboarding')
api.add_resource(dataIngestor, '/ingestion')

if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
