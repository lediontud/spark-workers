from flask import Flask, request
import requests
import os
import json
from google.auth.transport.requests import Request
from google.auth import exceptions
from google.oauth2 import service_account

app = Flask(__name__)

def get_api_key() -> str:
    secret = os.environ.get("COMPUTE_API_KEY")
    if secret:
        return secret
    else:
        # Local testing
        credentials_path = 'key.json'
        credentials, _ = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        ).with_request(Request())
        return credentials.token

@app.route("/")
def hello():
    return "Add workers to the Spark cluster with a POST request to add"

@app.route("/test")
def test():
    return get_api_key()

@app.route("/add", methods=['GET', 'POST'])
def add():
    if request.method == 'GET':
        return "Use post to add"  # replace with form template
    else:
        token = get_api_key()
        ret = addWorker(token, request.form['num'])
        return ret

def addWorker(token, num):
    with open('payload.json') as p:
        tdata = json.load(p)
    tdata['name'] = 'slave' + str(num)
    data = json.dumps(tdata)
    url = 'https://www.googleapis.com/compute/v1/projects/feisty-tempest-406610/zones/europe-west2-b/instances'
    headers = {"Authorization": "Bearer " + token}
    resp = requests.post(url, headers=headers, data=data)
    if resp.status_code == 200:
        return "Done"
    else:
        print(resp.content)
        return "Error\n" + resp.content.decode('utf-8') + '\n\n\n' + data

if __name__ == "__main__":
    app.run(host='0.0.0.0', port='8080')
