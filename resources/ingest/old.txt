# data ingestor
import sqlalchemy
import pymysql
import traceback
import os

from datetime import datetime, timedelta
from dateutil.tz import gettz

EWON_TIME_FORMAT = '%d/%m/%Y %H:%M:%S'

TEMPLATE_PAYLOAD = {
    'orderId'   :   0,
    'TIME'      :   datetime.utcnow().strftime(EWON_TIME_FORMAT),
    'key1'      :   'value1',
    'key2'      :   'value2'
}

# function to return the database connection
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

def getCommonData():
    log = ''
    status = True
    # GET units table and timeZone list
    #### sql query to get units table
    show_dbs_statement = sqlalchemy.text('SELECT unitId, factor, bias FROM units;')
    #### sql query to get timezones
    get_timezones_statement = sqlalchemy.text('SELECT orderId, timeZone FROM orders;')
    ## query units table
    try:
        ## establish connection to common_data
        common_data_pool = connect_unix_socket('COMMON_DATA')

        with common_data_pool.connect() as db_conn:
            # query units data
            result = db_conn.execute(show_dbs_statement)
            unitsTableData = result.fetchall()
            ## process data
            unitsReference = {row[0]: {'factor': row[1], 'bias': row[2]} for row in unitsTableData}

            # quert timeZones info
            result = db_conn.execute(get_timezones_statement)
            timeZonesData = result.fetchall()
            ## process data
            timeZonesByOrderId = {row[0]: row[1] for row in timeZonesData}    
    except:
        log += traceback.format_exc()
        status = False
        unitsReference = {}
        timeZonesByOrderId = {}
    return {
        'log': '',
        'status': True,
        'unitsReference': unitsReference,
        'timeZonesByOrderId': timeZonesByOrderId
    }

def getSiteMeta(orderId:int):
    orderDBName = f'ORDER{orderId}'
    log = ''
    status = True
    metaDataObject = sqlalchemy.MetaData()

    try:
        # GET META_DATA
        get_meta_statement = sqlalchemy.text('SELECT standardTag, dataTagId, unitId FROM META_DATA;')
        order_data_pool = connect_unix_socket(orderDBName)
        with order_data_pool.connect() as db_conn:
            result = db_conn.execute(get_meta_statement)
            data = result.fetchall()
            # resultDict = dict(zip(keys, zip(*data)))
        siteMetaData = {metarow[0]: {'dataTagId':metarow[1], 'unitId': metarow[2]} for metarow in data}
        
        metaDataObject.reflect(order_data_pool, only=['TEMP_DATA'])
    except:
        log += traceback.format_exc()
        status = False
        siteMetaData = {}
        
    return {
        'log': log,
        'status': status,
        'siteMetaData': siteMetaData,
        'metaDataObject': metaDataObject
    }

def dataPreProcessor(siteDataRecord: dict, siteMetaData:dict, orderId:int, unitsReference:dict, timeZonesByOrderId:dict):
    log = ''
    status = True
    record = {}
    try:
        # load timeZone
        timezoneStr = timeZonesByOrderId[orderId]

        # calculate & generate record
        for standardTag in siteMetaData:
            dataTagId = siteMetaData[standardTag]['dataTagId']
            unitId = siteMetaData[standardTag]['unitId']

            if dataTagId in siteDataRecord:
                value = siteDataRecord[dataTagId]
                factor = unitsReference[unitId]['factor']
                bias = unitsReference[unitId]['bias']
                valueSI = value*factor + bias
                record.update({standardTag: valueSI})

        # add timeStamp to record
        if 'TIME' in siteDataRecord:
            # add block to validate time!
            value = datetime.strptime(siteDataRecord['TIME'], EWON_TIME_FORMAT) 
            value = value.replace(tzinfo=gettz(timezoneStr))      # attach timezone info 
            utc_value = datetime.utcfromtimestamp(value.timestamp())
        else:
            utc_value = datetime.utcnow()
        record.update({'t': utc_value})

    except Exception as e:
        log += traceback.format_exc()
        status = False

    return {
        'log': log,
        'status': status,
        'data': record
    }

def writeSingleRecord(singleRecord:dict, metaDataObject:sqlalchemy.MetaData, orderId:int):
    orderDBName = f'ORDER{orderId}'
    log = ''
    status = True
    try:
        order_data_pool = connect_unix_socket(orderDBName)
        with order_data_pool.connect() as conn:
            conn.execute(
                metaDataObject.tables['TEMP_DATA'].insert(), # insert at a particular time stamp
                singleRecord
            )
            conn.commit()
            conn.close()

    except Exception as e:
        log += traceback.format_exc()
        status = False

    return {
        'log': log,
        'status': status
    }

def updateSingleRecord(timeStamp:datetime, singleRecord:dict, metaDataObject:sqlalchemy.MetaData, orderId:int):
    orderDBName = f'ORDER{orderId}'
    log = ''
    status = True
    try:
        table = metaDataObject.tables['TEMP_DATA']
        order_data_pool = connect_unix_socket(orderDBName)
        update_statement = sqlalchemy.update(table)
        update_statement = update_statement.values(singleRecord)
        update_statement = update_statement.where(table.c.t == timeStamp)

        with order_data_pool.connect() as conn:
            conn.execute(update_statement)
            conn.commit()
            conn.close()

    except Exception as e:
        log += traceback.format_exc()
        status = False

    return {
        'log': log,
        'status': status
    }