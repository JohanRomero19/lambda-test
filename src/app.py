from flask import Flask, jsonify, request

from commons.helper import Database

db_actions = Database()

app = Flask(__name__)

@app.route('/departments', methods=['POST'])
def departments():
    body = request.get_json()

    response = db_actions.insert_data(body['data'], 'departments')
    
    return jsonify(response), response['status']

@app.route('/jobs', methods=['POST'])
def jobs():
    body = request.get_json()

    response = db_actions.insert_data(body['data'], 'jobs')
    
    return jsonify(response), response['status']

@app.route('/employees', methods=['POST'])
def employees():
    body = request.get_json()

    response = db_actions.insert_data(body['data'], 'employees')
    
    return jsonify(response), response['status']

if __name__ == '__main__':
    app.run(debug=True)
