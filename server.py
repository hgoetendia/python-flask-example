import requests
import os
import time
import json
import pymysql.cursors
import pymysqlpool
import logging
import warnings

from datetime import datetime
from flask import Flask, url_for
from flask import request
from flask import Response

logger = None

def server(local_port):

    app = Flask(__name__)
    
    @app.route('/', methods = ['POST'])
    def api_root():

        if request.headers['Content-Type'] == 'application/json':
            data = request.get_json();
            try:                
                ret = "{'hola': 'mundo', 'python': 'flask' }"
                
                return Response(json.dumps(ret), mimetype='application/json')
            
            except Exception as e:
                resp =   {"hello": "world", "status": "FAIL" }
                return str(resp)
            
        return 'unknown'
    
    @app.route('/control', methods = ['POST'])
    def api_control():
        try:                
            ret = "{'hola': 'mundo', 'python': 'flask' }"
            
            return Response(json.dumps(ret), mimetype='application/json')
        
        except Exception as e:
            resp =   {"messageid": data['id'], "status": "FAIL" }
            return str(resp)
    
    @app.route('/control/<controlid>')
    def api_article(controlid):
        return 'You are reading ' + controlid

    app.run(port=local_port)

def init_logger(logFile):
    global logger
    logger = logging.getLogger('myapp')
    logger.setLevel(logging.DEBUG)
    # Log to file
    fh = logging.FileHandler(logFile)    
    fh.setLevel(logging.DEBUG)
    # Log to console
    ch = logging.StreamHandler()
    ch.setLevel(logging.ERROR)
    formatter = logging.Formatter('%(asctime)s [%(name)s][%(levelname)s]: %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    # Add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)    
    logger.info('Logger was initialized')
    
def init():

    flag = False
    
    init_logger("example_application.log");
    
    with open('config.json') as config_file:
        conf = json.load(config_file)

    config_pool={'host'            : conf["DB_HOST"],
                 'user'            : conf["DB_USER"],
                 'password'        : conf["DB_PASSWORD"],
                 'database'        : conf["DATABASE"],
                 'autocommit'      : True,
                 'connect_timeout' : 30,
                 'read_timeout'    : 30,
                 'write_timeout'   : 30,
                 'cursorclass'     : pymysql.cursors.DictCursor}

    pymysqlpool.logger.setLevel('DEBUG')

    now = datetime.now()
    logger.debug(now)

    while True:
        try:
            logger.info("Trying to connect to BD");
            pool = pymysqlpool.ConnectionPool(size=conf["DB_POOL_SIZE"], name='pool', **config_pool)
            logger.info("Connected")
            break
        except Exception as e:
            logger.error(str(e))
            logger.error("Can't connect, trying in 5 secs")
            time.sleep(5);
    
    connection = pool.get_connection(timeout=conf["TIMEOUT_DB_CONN"], retry_num=conf["RETRY_DB_CONN"])

    try:

        with connection.cursor() as cursor:
            # Read a single record
            sql = ("SELECT %s AS id_process, %s AS process_type ")
            cursor.execute(sql, ('GRANJA1','GRANJA2',))

            #sql =("(SELECT * FROM foo WHERE %d=%d LIMIT %d)") % (1, 1, 1);

            for row in cursor.fetchall():                
                id_process   = row['id_process']
                process_type = row['process_type']

            logger.info("Data " + id_process + ":" +process_type)

            flag = True

    except Exception as e:
        logger.error(str(e))
        return
    finally:
        connection.close()

    if flag:
        server(conf["SERVER_PORT"])
            
if __name__ == '__main__':
    init()
