# This is new re-written version of onboardTool
from datetime import datetime
import os
import pymongo
import traceback

DB_NAME = os.environ.get('DB_NAME')
DB_ADDR = os.environ.get('DB_ADDR')

TEMPLATE_PAYLOAD = {
    'orderId': 1,
    'site': '',
    'unit':'',
    'timeZone':'',
    'metaData': [
        {'standardTag': 'XXXXX1_XXX_XXXXX_XXXX1', 'description': '', 'dataTagId': '', 'unitId': 1},
        {'standardTag': 'XXXXX1_XXX_XXXXX_XXXX2', 'description': '', 'dataTagId': '', 'unitId': 1},
    ],
    'fixedData':[
        {'standardTag': 'XXXXX2_XXX_XXXXX_XXXX1', 'description': '', 'value': 0.00, 'unitId': 1},
        {'standardTag': 'XXXXX2_XXX_XXXXX_XXXX2', 'description': '', 'value': 0.00, 'unitId': 1}
    ]
}

# orders table scehma
# orders
#   orderId
#   dateModified
#   site
#   unit
#   timeZone
#   metaData
#   fixedData

ORDER_DEFAULT_KEYS = ['orderId', 'dateModified', 'site', 'unit', 'timeZone', 'metaData', 'fixedData']

client = pymongo.MongoClient(DB_ADDR)
db = client[DB_NAME]
orders = db['orders']

def processNewOrder(data:dict):
    global db, orders, client
    # set up
    
    if 'orderId' not in data:
        return {
            'log': 'Invalid schema. Refer schema for proper implementation.',
            'schema': TEMPLATE_PAYLOAD
        }

    orderId = data['orderId']
    try:
        orderDetails = {key:data[key] for key in ORDER_DEFAULT_KEYS if key in data}
        orderDetails.update({'dateModified': datetime.now().isoformat()})
        insertResult = orders.replace_one({'orderId': orderId}, orderDetails, upsert=True)
        if insertResult.modified_count < 1:
            log += '\n' + 'Order details not updated.'
            status = False
    except:
        log = '\n' + traceback.format_exc()
        status = False
    
    return {
        'log': log,
        'status': status
    }

def getOrderDetails(orderId:int):
    global db, orders, client
    result = {}
    try:
        insertResult = orders.find_one(filter={'orderId':orderId})
        resultList = [x for x in insertResult]
        data = resultList[0]
        result.update(
            {
                'timeZone'  : data['timeZone'],
                'metaData'  : data['metaData'],
                'fixedData' : data['fixedData'],
            }
        )
        
    except:
        log = '\n' + traceback.format_exc()
        status = False
    
    return {
        'log': log,
        'status': status,
        'data' : data
    }