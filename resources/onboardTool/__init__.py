import sqlalchemy
import pymysql
from datetime import datetime
import traceback
import os

ORDER_DETAILS = ['site', 'unitCode', 'timeZone']
TABLES_LIST = ['META_DATA', 'FIXED_DATA', 'FIXED_DATA']
META_TABLE_COLUMNS = ['standardTag', 'description', 'dataTagId', 'unitId']
FIXED_TABLE_COLUMNS = ['standardTag', 'description','value', 'unitId']

metaObject = sqlalchemy.MetaData()

metaData = sqlalchemy.Table(
	'META_DATA', metaObject,  
	sqlalchemy.Column('standardTag', sqlalchemy.String(30), primary_key = True), 
	sqlalchemy.Column('description', sqlalchemy.TEXT(255)),
	sqlalchemy.Column('dataTagId', sqlalchemy.TEXT(127)),
	sqlalchemy.Column('unitId', sqlalchemy.INTEGER) 
)

fixedData = sqlalchemy.Table(
	'FIXED_DATA', metaObject, 
	sqlalchemy.Column('standardTag', sqlalchemy.String(30), primary_key = True), 
	sqlalchemy.Column('description', sqlalchemy.TEXT(255)),
	sqlalchemy.Column('value', sqlalchemy.REAL),
	sqlalchemy.Column('unitId', sqlalchemy.INTEGER) 
)

def connect_unix_socket(db_name:str) -> sqlalchemy.engine.base.Engine:
    """ Initializes a Unix socket connection pool for a Cloud SQL instance of MySQL. """
    # Note: Saving credentials in environment variables is convenient, but not
    # secure - consider a more secure solution such as
    # Cloud Secret Manager (https://cloud.google.com/secret-manager) to help
    # keep secrets safe.
    db_user = os.environ["DB_USER"]  # e.g. 'my-database-user'
    db_pass = os.environ["DB_PASS"]  # e.g. 'my-database-password'
    # db_name = os.environ["DB_NAME"]  # e.g. 'my-database'
    unix_socket_path = os.environ["INSTANCE_UNIX_SOCKET"]  # e.g. '/cloudsql/project:region:instance'

    pool = sqlalchemy.create_engine(
        # Equivalent URL:
        # mysql+pymysql://<db_user>:<db_pass>@/<db_name>?unix_socket=<socket_path>/<cloud_sql_instance_name>
        sqlalchemy.engine.url.URL.create(
            drivername="mysql+pymysql",
            username=db_user,
            password=db_pass,
            database=db_name,
            query={"unix_socket": unix_socket_path},
        ),
        # ...
    )
    return pool

# data = {
#     'order_id': 1,
#     'site': 'CENTURY ENKA BHOSARI',
#     'unit_code': 'CPRG160/17.5/1001'
# }

TEMPLATE_PAYLOAD = {
    'orderId': '',
    'site': '',
    'unitCode': '',
    'timeZone': '',
    'metaData': [
        {'standardTag': 'XXXXX1_XXX_XXXXX_XXXX1', 'description': '', 'dataTagId': '', 'unitId': 1},
        {'standardTag': 'XXXXX1_XXX_XXXXX_XXXX2', 'description': '', 'dataTagId': '', 'unitId': 1},
    ],
    'fixedData':[
        {'standardTag': 'XXXXX2_XXX_XXXXX_XXXX1', 'description': '', 'value': 0.00, 'unitId': 1},
        {'standardTag': 'XXXXX2_XXX_XXXXX_XXXX2', 'description': '', 'value': 0.00, 'unitId': 1}
    ]
}

def getOrderDetails(orderId:int):
    log = ''
    status = True
    data = {}
    orderDataBaseName = f'ORDER{orderId}'
    dataBaseExists = False
    orderExists = False
    metaDataTableExists = False
    tempDataTableExists = False
    fixedDataTableExists = False
    try:
        # establish connection socket
        common_data_pool = connect_unix_socket("COMMON_DATA")
        # SQL statements
        getOrderDetailsSQL = sqlalchemy.text(f'SELECT * FROM orders WHERE orderId={orderId};')
        listDataBasesSQL = sqlalchemy.text('SHOW DATABASES;')
        # connect 
        with common_data_pool.connect() as db_conn:
            # 
            result = db_conn.execute(getOrderDetailsSQL)
            resultData = result.fetchall()
            resultKeys = result.keys()
            if len(resultData):
                # log += f'\nOrder #{orderId} entry exists in orders list.'
                orderExists = True
                data = dict(zip(resultKeys, list(resultData[0])))
            else:
                data = {'site': 'NULL', 'unitCode': 'NULL', 'timeZone': 'NULL', }
                orderExists = False
                # log += f'\nOrder #{orderId} entry does not exist in orders list.'
        
            result = db_conn.execute(listDataBasesSQL)
            resultData = result.fetchall()

        # check of order's database exists
        if (orderDataBaseName,) not in resultData:
            dataBaseExists = False
        else:
            dataBaseExists = True
            # database exists.
            order_data_pool = connect_unix_socket(orderDataBaseName)
            orderDataBaseMeta = sqlalchemy.MetaData()
            orderDataBaseMeta.reflect(order_data_pool)
            orderTablesList = list(orderDataBaseMeta.tables)
            order_data_pool.dispose()
            
            metaDataTableExists = 'META_DATA' in orderTablesList
            fixedDataTableExists = 'FIXED_DATA' in orderTablesList
            tempDataTableExists = 'TEMP_DATA' in orderTablesList

    except:
        log += '\n' + traceback.format_exc()
        status = False

    return {
        'log': log,
        'status': {
            'queryExecuted'         :   status,
            'orderExists'           :   orderExists,
            'dataBaseExists'        :   dataBaseExists,
            'metaDataTableExists'   :   metaDataTableExists,
            'fixedDataTableExists'  :   fixedDataTableExists,
            'tempDataTableExists'   :   tempDataTableExists
        },
        'orderDetails': { # for future UI
            'site'      :   data['site'],
            'unitCode'  :   data['unitCode'],
            'timeZone'  :   data['timeZone']
        }
    }

def processNewOrder(data:dict):
    global metaObject
    log = ''
    status = True
    addOrderEntry = False
    orderEntryPreExists = False
    orderDataBasePreExists = False
    try:
        orderId = int(data['orderId'])
        orderDataBaseName = f'ORDER{orderId}'
        orderDetailsResult = getOrderDetails(orderId)
        orderDetailsInData = {key:data[key] for key in ORDER_DETAILS if key in data}
        # get what to do
        if not orderDetailsResult['status']['queryExecuted']:
            log += '\nfunction:getOrderDetails():' + orderDetailsResult['log'].replace('\n', '\n\t')
            raise RuntimeError('function:getOrderDetails() did not execute properly. Read logs.')
        else:
            if orderDetailsResult['status']['orderExists']:
                log += f'\nOrder #{orderId} entry pre-exists in the orders list.'
                orderEntryPreExists = True
                if not len(orderDetailsInData):
                    addOrderEntry = False
                else:
                    # orderEntryPreExists = False
                    missingDetails = {key:orderDetailsResult['orderDetails'][key] for key in ORDER_DETAILS if key not in data}
                    orderDetailsInData.update(missingDetails)
                    addOrderEntry = True
            else:
                f'\nOrder #{orderId} entry does not pre-exist in the orders list.'
                addOrderEntry = True
            
            if orderDetailsResult['status']['dataBaseExists']:
                log += f'\nDatabase {orderDataBaseName} already exists.'
                orderDataBasePreExists = True

        # SQL STATEMENTS
        insertOrderSQL = sqlalchemy.text(
            "INSERT INTO orders (orderId, site, unitCode, createDate, timeZone) VALUES (:orderId, :site, :unitCode, :createDate, :timeZone) ON DUPLICATE KEY UPDATE site=:site, unitCode=:unitCode, timeZone=:timeZone;"
        )
        createOrderDataBaseSQL = sqlalchemy.text(f'CREATE DATABASE {orderDataBaseName};')

        common_data_pool = connect_unix_socket("COMMON_DATA")
        with common_data_pool.connect() as db_conn:
            if addOrderEntry: # ADD ORDER ENTRY
                orderDetailsInData.update(
                    {
                        'orderId': orderId,
                        'createDate': datetime.utcnow()
                    }
                )
                db_conn.execute(insertOrderSQL, parameters=orderDetailsInData)
            if not orderDetailsResult['status']['dataBaseExists']: # CREATE DB
                db_conn.execute(createOrderDataBaseSQL)
            db_conn.commit()
            db_conn.close()
        common_data_pool.dispose()

        error = False

        # RECHECK 
        orderDetailsResult = getOrderDetails(orderId)
        
        if not orderDetailsResult['status']['orderExists']:
            log += '\nCould not create order entry in orders table.'
            error = True
        else:
            if orderEntryPreExists:
                if addOrderEntry:
                    log += '\nOrder details have been altered as per request.'
            else:
                log += '\nNew order successfully appended in orders table.'
                
            if not orderDetailsResult['status']['dataBaseExists']:
                log += '\nCould not create new order\'s database.'
                error = True
            else:
                if not orderDataBasePreExists:
                    log += '\nDatabase for new order has been successfully created.'
                # CREATE TABLES
                order_data_pool = connect_unix_socket(orderDataBaseName)
                metaObject.create_all(order_data_pool)

                # RECHECK X 2
                orderDetailsResult = getOrderDetails(orderId)

                if not orderDetailsResult['status']['metaDataTableExists']:
                    log += '\nCould not create table: META_DATA.'
                    error = True
                else:
                    log += '\nTable found: META_DATA.'
                    if 'metaData' in data:
                        result = updateMetaData(data, orderDataBaseName)
                        if not result['status']:
                            log += '\nCould not upload meta data.'
                            log += '\nfunction:updateMetaData' + result['log'].replace('\n', '\n\t')
                            error = True

                if not orderDetailsResult['status']['fixedDataTableExists']:
                    log += '\nCould not create table: FIXED_DATA.'
                    error = True
                else:
                    log += '\nTable found: FIXED_DATA.'
                    if 'fixedData' in data:
                        result = updateFixedData(data, orderDataBaseName)
                        if not result['status']:
                            log += '\nCould not upload fixed data.'
                            log += '\nfunction:updateFixedData' + result['log'].replace('\n', '\n\t')
                            error = True

                # if not orderDetailsResult['status']['tempDataTableExists']:
                #     log += '\ntable: TEMP_DATA not found.'
                #     # error = True
                # else:
                #     log += 'Successfully created table: TEMP_DATA.'
                
        if error:
            raise RuntimeError('Operation unsuccessful. Read logs.')
            
    except:
        log += traceback.format_exc()
        status = False
    
    return {
        'log': log,
        'status': status
    }

def updateMetaData(data:dict, orderDBName:str):
    global metaData
    log = ''
    status = True
    try:
        metaRecords = data['metaData']
        metaTableRecords = [{key: record[key] for key in META_TABLE_COLUMNS} for record in metaRecords]
        
        tempDataColumnsStr = [record['standardTag'] for record in metaTableRecords]
        tempDataColumns = [sqlalchemy.Column(standardTag, sqlalchemy.REAL) for standardTag in tempDataColumnsStr]

        # table Object: TEMP_DATA
        tempMetaObject = sqlalchemy.MetaData()
        tempData = sqlalchemy.Table(
            'TEMP_DATA', tempMetaObject, 
            sqlalchemy.Column('t', sqlalchemy.DATETIME, primary_key=True),
            *tempDataColumns
        )

        order_data_pool = connect_unix_socket(orderDBName)

        # create TEMP_DATA table if not existed previously
        tempMetaObject.create_all(order_data_pool) # 
        
        # updateMetaStatement = updateMetaStatement.
        # upload meta data
        insertMetaDataStatement = sqlalchemy.text('INSERT INTO META_DATA (standardTag, description, dataTagId, unitId) VALUES (:standardTag, :description, :dataTagId, :unitId) ON DUPLICATE KEY UPDATE description=:description, dataTagId=:dataTagId, unitId=:unitId;')
        with order_data_pool.connect() as db_conn:
            # insert into database
            for record in metaTableRecords:
                db_conn.execute(insertMetaDataStatement, parameters=record)
            db_conn.commit()
                
        order_data_pool.dispose()
    
    except Exception as e:
        log += '\n' + traceback.format_exc()
        status = False
    
    return {
        'log': log,
        'status': status
    }

def updateFixedData(data:dict, orderDBName:str):
    global fixedData
    log = ''
    status = True
    try:
        constantsRecords = data['fixedData']
        fixedTableRecords = [{key: record[key] for key in FIXED_TABLE_COLUMNS} for record in constantsRecords]

        # create connection
        order_data_pool = connect_unix_socket(orderDBName)
        
        insertFixedDataStatement = sqlalchemy.text('INSERT INTO FIXED_DATA (standardTag, description, value, unitId) VALUES (:standardTag, :description, :value, :unitId) ON DUPLICATE KEY UPDATE description=:description, value=:value, unitId=:unitId;')
        with order_data_pool.connect() as db_conn:
            # insert into database
            for record in fixedTableRecords:
                db_conn.execute(insertFixedDataStatement, parameters=record)
            db_conn.commit()
                
        order_data_pool.dispose()
    
    except Exception as e:
        log += '\n' + traceback.format_exc()
        status = False
    
    return {
        'log': log,
        'status': status
    }