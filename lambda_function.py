import sys
import logging
import rds_config
import pymysql
import json

#Conexión hacia el RDS
rds_host = rds_config.rds_host
name = rds_config.db_username
password = rds_config.db_password
db_name = rds_config.db_name

logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
    conn = pymysql.connect(host=rds_host, user=name, passwd=password, db=db_name, connect_timeout=5)
except pymysql.MySQLError as e:
    logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
    logger.error(e)
    sys.exit()

logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")
def vehiculo_marca(event, context):
    query = ""
    
    if event["queryStringParameters"] != None:
        query = " WHERE id_vehiculo = " + event['queryStringParameters']['id']
        
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM vehiculo" + query)
        
        headers = [x[0] for x in cur.description] 
        rv = cur.fetchall()
        json_data=[]
        for result in rv:
            json_data.append(dict(zip(headers, result)))
        
        return {
            "statusCode": 200,
            "headers": {'Content-Type': 'application/json'},
            "isBase64Encoded": False,
            "body": json.dumps(json_data)
        }

def tasa_interes(event, context):
    query = ""
    
    if event["queryStringParameters"] != None:
        query = " WHERE id_interes = " + event['queryStringParameters']['id']
    
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM interes" + query)
        
        headers = [x[0] for x in cur.description] 
        rv = cur.fetchall()
        json_data=[]
        for result in rv:
            json_data.append(dict(zip(headers, result)))
        
        return {
            "statusCode": 200,
            "headers": {'Content-Type': 'application/json'},
            "isBase64Encoded": False,
            "body": json.dumps(json_data)
        }
        
def linea_serie(event, context):
    query = ""
    
    if event["queryStringParameters"] != None:
        query = " WHERE id_vehiculo = " + event['queryStringParameters']['id_vehiculo']
    
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM serie" + query)
        
        headers = [x[0] for x in cur.description] 
        rv = cur.fetchall()
        json_data=[]
        for result in rv:
            json_data.append(dict(zip(headers, result)))
        
        return {
            "statusCode": 200,
            "headers": {'Content-Type': 'application/json'},
            "isBase64Encoded": False,
            "body": json.dumps(json_data)
        }

def modelo(event, context):
    query = ""
    
    if event["queryStringParameters"] != None:
        query = " WHERE id_serie = " + event['queryStringParameters']['id_serie']
    
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM modelo" + query)
        
        headers = [x[0] for x in cur.description] 
        rv = cur.fetchall()
        json_data=[]
        for result in rv:
            json_data.append(dict(zip(headers, result)))
        
        return {
            "statusCode": 200,
            "headers": {'Content-Type': 'application/json'},
            "isBase64Encoded": False,
            "body": json.dumps(json_data)
        }

def cotiza(event, context, type):
    with conn.cursor() as cur:
        if type == 'GET':
            cur.execute("SELECT * FROM cotiza")
        else:
            cur.execute("INSERT INTO cotiza (id_modelo, id_interes, enganche, total) VALUES(" 
            + event['queryStringParameters']['id_modelo'] + ", "
            + event['queryStringParameters']['id_interes'] + ", "
            + event['queryStringParameters']['enganche'] + ", "
            + "(1 + (SELECT tasa_interes/100 FROM interes WHERE id_interes = " + event['queryStringParameters']['id_interes'] + ")) * ((SELECT precio FROM modelo WHERE id_modelo = " + event['queryStringParameters']['id_modelo'] + ")" + " - " + event['queryStringParameters']['enganche'] + ") + " + event['queryStringParameters']['enganche'] + 
            + ")")
        
        headers = [x[0] for x in cur.description] 
        rv = cur.fetchall()
        json_data=[]
        for result in rv:
            json_data.append(dict(zip(headers, result)))
        
        return {
            "statusCode": 200,
            "headers": {'Content-Type': 'application/json'},
            "isBase64Encoded": False,
            "body": json.dumps(json_data)
        }

def lambda_handler(event, context):
    if event['path'] == '/vehiculo-marca':
        return vehiculo_marca(event, context)
    elif event['path'] == '/tasa-interes':
        return tasa_interes(event, context)
    elif event['path'] == '/linea':
        return linea_serie(event, context)
    elif event['path'] == '/modelo':
        return modelo(event, context)
    elif event['path'] == '/cotiza' and event['httpMethod'] == 'GET':
        return cotiza(event, context, 'GET')
    elif event['path'] == '/cotiza' and event['httpMethod'] == 'POST':
        return cotiza(event, context, 'POST')
        
    return {
            "statusCode": 200,
            "headers": {'Content-Type': 'application/json'},
            "isBase64Encoded": False,
            "body": {}
        }
