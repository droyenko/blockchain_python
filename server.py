from flask import Flask, request
from flask_restful import Resource, Api
from sqlalchemy import create_engine
from flask.ext.jsonpify import jsonify
import hashlib as hasher
import time
import urllib.request

db_connect = create_engine('sqlite:///chinook.db')
app = Flask(__name__)
api = Api(app)


class AddTransaction(Resource):
    def post(self):
        """Adding transaction to SQLite. If number of rows has reached five create block"""
        conn = db_connect.connect()
        print(request.json)
        From = request.json['from']
        To = request.json['to']
        Amount = request.json['amount']
        conn.execute("insert into transactions values(null,'{0}','{1}','{2}')".format(From,To,Amount))
        number_of_data = conn.execute("SELECT COUNT (*) AS number FROM transactions")
        for row in number_of_data:
            if row["number"] % 5 == 0:
                create_block()
        return {'status': 'success'}


class GetBlocks(Resource):
    """Return list of "N" last blocks. Use http://127.0.0.1:5002/last_blocks/N"""
    def get(self, number_of_blocks):
        conn = db_connect.connect()
        query = conn.execute("SELECT * FROM blocks ORDER BY ts DESC limit %d" % int(number_of_blocks))
        result = {'data': [dict(zip(tuple(query.keys()), i)) for i in query.cursor]}
        return jsonify(result)

class ManagementStatus(Resource):
    def get(self):
        result = {'id':80,'name':'DmytroR','last_hash':get_last_hash(),'neighbours':get_neighbours(),'url':'192.168.44.80:5002'}
        return jsonify(result)


class ManagementSync(Resource):
    def get(self):
        conn = db_connect.connect()
        query = conn.execute("SELECT * FROM blocks")
        result = [dict(zip(tuple(query.keys()), i)) for i in query.cursor]
        return jsonify(result)


class ManagementAddLink(Resource):
    def post(self):
        conn = db_connect.connect()
        print(request.json)
        Id = request.json['id']
        Url = request.json['url']
        conn.execute("insert into links values('{0}','{1}')".format(Id, Url))
        return {'status': 'success'}


class BlockchainReceiveUpdate(Resource):
    def post(self):
        print(request.json)
        sender_id = request.json['sender_id']
        block = request.json['block']


def create_block():
    """Create any NOT genesis block"""
    data = "["
    conn = db_connect.connect()
    previous = get_last_hash();
    data_conn = conn.execute(
        "SELECT tx_from, tx_to, tx_amount FROM (SELECT * FROM transactions ORDER BY tx_id DESC limit 5) ORDER BY tx_id ASC")
    for row in data_conn:
        data += "{from:" + row['tx_from'] + ",to:" + row["tx_to"] + ",amount:" + str(row["tx_amount"]) + "},"
    data +="]"
    timestamp = int(time.time())
    hash = hash_block(timestamp, data, previous)
    conn.execute("insert into blocks values('{0}','{1}','{2}','{3}')"
                         .format(previous, hash, timestamp, data))


def get_last_hash():
    conn = db_connect.connect()
    previous_conn = conn.execute("SELECT hash FROM blocks ORDER BY ts DESC limit 1")
    for row in previous_conn:
        previous = row["hash"]
    return previous


def get_neighbours():
    neighbours = []
    conn = db_connect.connect()
    neighbours_conn = conn.execute("SELECT id FROM links")
    for row in neighbours_conn:
        neighbours.append(row["id"])
    return neighbours


def hash_block(timestamp, data, previous):
    """Create hash (sha256) for given block"""
    sha = hasher.sha256()
    sha.update((str(timestamp) + str(data) + str(previous)).encode('utf-8'))
    return sha.hexdigest()

def create_genesis_block():
    """Create initial "genesis" block"""
    conn = db_connect.connect()
    hash = hash_block(int(time.time()), "Genesis Block", "0")
    conn.execute("insert into blocks values('{0}','{1}','{2}','{3}')"
                 .format("0", hash, int(time.time()), "Genesis Block"))


def get_link_url():
    conn = db_connect.connect()
    url_conn = conn.execute("SELECT url FROM links ORDER BY id ASC LIMIT 1")
    for row in url_conn:
        url = row["url"]
    return url


def sync_from():
    url = get_link_url()
    urllib.request.urlopen(url + "/management/sync").read()


api.add_resource(GetBlocks, '/blockchain/get_blocks/<number_of_blocks>')  # Route_1
api.add_resource(AddTransaction, '/management/add_transaction')  # Route_2
api.add_resource(ManagementAddLink, '/management/add_link')  # Route_3
api.add_resource(ManagementStatus, '/management/status')  # Route_4
api.add_resource(ManagementSync, '/management/sync')  # Route_5

# check does any block exist and create genesis block if not
conn = db_connect.connect()
number_of_blocks = conn.execute("SELECT COUNT (*) AS number FROM blocks")
for row in number_of_blocks:
    if row["number"] == 0:
        create_genesis_block()

sync_from

if __name__ == '__main__':
    app.run(host= '0.0.0.0', port=5002)