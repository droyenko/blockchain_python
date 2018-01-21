from flask import Flask, request
from flask_restful import Resource, Api
from sqlalchemy import create_engine
from flask.ext.jsonpify import jsonify
import hashlib as hasher
import time

db_connect = create_engine('sqlite:///chinook.db')
app = Flask(__name__)
api = Api(app)


class AddData(Resource):
    def post(self):
        """Adding data to SQLite. If number of rows has reached five create block"""
        conn = db_connect.connect()
        print(request.json)
        data = request.json['data']
        conn.execute("insert into data values(null,'{0}')".format(data))
        number_of_data = conn.execute("SELECT COUNT (*) AS number FROM data")
        for row in number_of_data:
            if row["number"] % 5 == 0:
                create_block()
        return {'status': 'success'}


class ReturnBlocks(Resource):
    """Return list of "N" last blocks. Use http://127.0.0.1:5002/last_blocks/N"""
    def get(self, number_of_blocks):
        conn = db_connect.connect()
        query = conn.execute("SELECT * FROM blocks ORDER BY timestamp DESC limit %d" % int(number_of_blocks))
        result = {'data': [dict(zip(tuple(query.keys()), i)) for i in query.cursor]}
        return jsonify(result)


def create_block():
    """Create any NOT genesis block"""
    data = ""
    conn = db_connect.connect()
    previous_conn = conn.execute("SELECT block_hash FROM blocks ORDER BY timestamp DESC limit 1")
    for row in previous_conn:
        previous = row["block_hash"]
    data_conn = conn.execute("SELECT  Data FROM (SELECT * FROM data ORDER BY DataId DESC limit 5) ORDER BY DataId ASC")
    for row in data_conn:
        data += row["Data"] + "; "
    timestamp = int(time.time())
    hash = hash_block(timestamp, data, previous)
    conn.execute("insert into blocks values('{0}','{1}','{2}','{3}')"
                         .format(previous, data, timestamp, hash))


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
                 .format("0", "Genesis Block", int(time.time()), hash))

api.add_resource(ReturnBlocks, '/last_blocks/<number_of_blocks>')  # Route_1
api.add_resource(AddData, '/add_data')  # Route_2

# check does any block exist and create genesis block if not
conn = db_connect.connect()
number_of_blocks = conn.execute("SELECT COUNT (*) AS number FROM blocks")
for row in number_of_blocks:
    if row["number"] == 0:
        create_genesis_block()

if __name__ == '__main__':
    app.run(port=5002)