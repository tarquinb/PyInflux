import requests as req
import json
import logging
import os

LOGGING = False


def enable_logging():
    #! Change Logging Level to INFO for production

    if not os.path.isdir('LOGS'):
        try:
            os.makedirs('LOGS')
        except OSError as e:
            print(e)
            return 1

    logging.basicConfig(filename=os.path.join('LOGS') + '/influxdb',
                        level=logging.DEBUG, format='%(levelname)-8s : %(asctime)-20s - %(message)s')
    global LOGGING
    LOGGING = True
    return 0


def log(param, line):
    if LOGGING:
        if param == 'w':
            logging.warning(line)
        elif param == 'i':
            logging.info(line)
        elif param == 'e':
            logging.error(line)
        elif param == 'c':
            logging.critical(line)
        else:
            print('Unkown Parameter for logging : {}. Logging failed'.format(param))
            return 1
        return 0
    else:
        if param != 'i':
            print(line)
        return 0


def create_db(db_name):
    try:
        req.post('http://localhost:8086/query',
                 data={'q': 'CREATE DATABASE {}'.format(db_name)})
        log('i', '{20} {}'.format('Database Created', db_name))
        return 0
    except req.exceptions.RequestException as e:
        log('w', 'Exception raised when creating database {} : {}'.format(db_name, e))
        return 1


class Write():
    def __init__(self,  db, consistency=None, p=None, precision=None, rp=None, u=None):
        self.db = db
        self.consistency = consistency
        self.p = p
        self.precision = precision
        self.rp = rp
        self.u = u

    def write(self, data):
        url = 'http://localhost:8086/write'
        params = {'db': self.db, 'consistency': self.consistency, 'p': self.p,
                  'precision': self.precision, 'rp': self.rp, 'u': self.u}
        try:
            req.post(url, params=params, data=data, headers={
                     'Content-Type': 'application/octet-stream'})
            log('i', 'Write successful')
        except req.exceptions.RequestException as e:
            log('w', 'Exception [ {} ] Raised for: {}'.format(e, data))
            return 1

        return 0

    def writes(self, *args):
        return self.write('\n'.join(args))

    def from_file(self, filename):
        with open(filename, 'rb') as f:
            response = self.write(f.read())
        return response


class Query():
    def __init__(self, db, chunked=None, epoch=None, pretty=None, u=None, p=None):
        self. db = db
        self.chunked = chunked
        self.epoch = epoch
        self.pretty = pretty
        self.u = u
        self.p = p

    def query(self, q):
        post_list = ['ALTER', 'CREATE', 'DELETE',
                     'DROP', 'GRANT', 'KILL', 'REVOKE']
        url = 'http://localhost:8086/query'
        params = {'db': self.db, 'q': q, 'chunked': self.chunked,
                  'epoch': self.epoch, 'pretty': self.pretty, 'u': self.u, 'p': self.p}
        if 'SELECT' and 'INTO' in q or any(x in q for x in post_list):
            try:
                r = req.post(url, params=params)
            except req.exceptions.RequestException as e:
                log('w', 'Exception {} raised for query: {}'.format(e, q))
                return 1
        else:
            try:
                r = req.get(url, params=params)
            except req.exceptions.RequestException as e:
                log('w', 'Exception {} raised for query: {}'.format(e, q))
                return 1
        log('i', 'Query successful {}'.format(q))
        return r.text

    # ? Maybe parse queries for clauses
    def queries(self, *args):
        return self.query(';'.join(args))


class Ping():
    def __init__(self):
        self.url = 'http://localhost:8086/ping'

    def ping(self):
        try:
            r = req.get(self.url)
        except req.exceptions.RequestException as e:
            log('w', 'Exception raised for ping endpoint : {}'.format(e))
            return 1
        return r.headers
