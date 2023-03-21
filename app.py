from flask import Flask, request
from flask_restful import Resource, Api
import os

from resources import onboardTool
from resources import ingest

app = Flask(__name__)
api = Api(app)

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
        result = onboardTool.getUCR()
        if not result['status']:
            return {
                'log': result['log'],
                'status': result['status']
            }
        else:
            unitConversionReference = result['data']
        
        # now get meta data.
        orderId = int(data['orderId'])
        result = onboardTool.getOrderDetails(orderId, ['timeZone', 'metaData'])
        if not result['status']:
            return {
                'log': result['log'],
                'status': result['status']
            }
        else:
            siteTimeZone = result['data']['timeZone'] # will use this when implementing ASYNCHRONOUS write is used.
            metaData = result['data']['metaData']
        
        # finally write data
        result = ingest.write(orderId, data, metaData, unitConversionReference)
        # return result !
        # call other api's from here !
        return result
    
api.add_resource(onboarding, '/onboarding')
api.add_resource(dataIngestor, '/ingestion')

if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
