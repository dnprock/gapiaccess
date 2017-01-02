from flask import Flask, request
from flask_restful import abort, Api, Resource
from gcloud import bigquery
import os
import json
from sys import argv

app = Flask(__name__)
api = Api(app)

bq_auth_file = 'bq-auth.json'

@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response

class Shutdown(Resource):
    def post(self):
        func = request.environ.get('werkzeug.server.shutdown')
        if func is None:
            raise RuntimeError('Not running with the Werkzeug Server')
        func()
        return 'Shutdown'

class PyAPI(Resource):
    def get(self):
        return {'services': ['/bigquery']}

class BigQuery(Resource):
    def get(self):
        return {'service': 'bigquery'}

    def post(self):
        try:
            # curl --data "SELECT type, COUNT(*) c FROM [fh-bigquery:hackernews.full_201510] GROUP BY 1 ORDER BY 2 LIMIT 100" localhost:5000/bigquery
            qdata = request.get_data().decode('ascii')
            jsonFormat = True
            jsonData = None
            orig_cred = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
            try:
                jsonData = json.loads(qdata)
            except json.decoder.JSONDecodeError as e:
                jsonFormat = False
            if jsonFormat == True and jsonData["credentials"]:
                # write credentials to a temp file
                cred_file = open(bq_auth_file, 'w')
                cred_file.write(json.dumps(jsonData["credentials"]))
                cred_file.close()
                queryStr = jsonData["query"]
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './' + bq_auth_file
                bqClient = bigquery.Client()
            else:
                queryStr = qdata
                bqClient = bigquery.Client(os.environ['GOOGLE_PROJECT'])

            try:
		            query = bqClient.run_sync_query(queryStr)
		            query.run()
		            fields = []
		            for f in query.schema:
		                fields.append({'name': f.name, 'field_type': f.field_type})

		            if jsonFormat == True and jsonData["credentials"]:
		                os.remove(bq_auth_file)
		                os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = orig_cred

		            return {'error': False, 'rows': query.rows, 'fields': fields}
            except Exception as e:
                print(e)
                return {'error': True, 'code': e.code, 'message': e.message}

        except Exception as e:
            print(e)
            raise(e)

api.add_resource(PyAPI, '/')
api.add_resource(Shutdown, '/shutdown')
api.add_resource(BigQuery, '/bigquery')

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)

# query and credentials
# curl --data '{"credentials": {"type": "service_account","project_id": "master-smithy-633","private_key_id": "295345f9efdbf8f15d47271e7e42360b3faae6c9","private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCDjteWMlp8k36T\n0LBdSmtYnQ5sxItnu6Uv3A0wT+pFxncCpAtUUQidKv+VpIaHhUa5ACjfFTBHtiwb\nEb7wIxRzBAX6+xN/g8DvwFmeQ720uJ2JrkxHMi1bb9JFAjhe65ijruhsYjaNZ7Mf\nyAb+0gFtAE2hXoiBUwJ0A2MtcKVWRNg5U0wKqYzWzoKIRw2hsAUJwhXcSfI++/52\nKvFpJDzzOnZ8dGLr2rhBXTjTg2667NGjQ9sHtM4Q2pecSKpk7pmWB4daBcPlVrnO\nQiz3nBudVoGcOPBIGv84BA/TxN6Z9eBdGshidG0xFOVu7m0fH4NlX6ce6MB1TPTh\n2cGS+IjbAgMBAAECggEAd86G8KZsl1SvCigb5OmX9rwY3d7j4ZUWaw5oCF5OyDpk\nOUhxdNjTEmaNGZcaWKJ79JB7qCUgsb3qTsCLdR9NSS0Z0SYD7LVs6Cnws1uXkNQr\ny62qk4/TlJiyEdxiv7aobDAmREXwzjfo/YFpNXjEiaKkD9gKUlPpQQOwtzoFqBh3\n64bzNw7XGJCnxujrL51UjEnAm3n/mgttFmIAdrgLw0oe2skhI96yTwBaPmCe6Goh\nwqIX4+wmCs7WZmJWpAPLVqcGL58eqDVve8Zm1RTQVjMxThG2m+igKIGmTRh5UNLU\n5F189VwhDudr16ZAGohhxR/lZdsPEqP5u7L76pIymQKBgQDdhi4e+akvZ6xrQJcX\n8cYHV0qL1NJGzWC5thE5+rfmenr1oO4rvaxplxBbNZJQbt5mSdizT3XZFJeZ7VbY\nL7NWt9PkPFogQLFb4OZB1fSPPJ/+s7xY+07OTIgQAqZZRC0bvDbLvP9HdjjV9DVh\n6ildxhArTSfWGv/rBgU1QPE1LQKBgQCYCEn4k+7Vde20MsqBhountNDzX8Tu2+Vd\nu3WCQfqxucdxOStBoRNW6jeNDCBw+C7AZRLEWmHjsOYQ6D4aOWhNSx1ihA4wjOvZ\nuHjdU4p4HWyJEh+ErE2Ess5Wiz0KvHl0kCKD7ZhcaP7nXYMZE0D+lCNiAdomXbPA\n4sEj/AKLJwKBgQCTC9GrsBAVLp1uKFbjhxLrpo8OWPwoEYaGYAv5T6spK95ZFDVl\nBafgA23RDbOM/rtUbOqSxEk3CwtDGx/Nq4RBKQ9XbpRo2mig054kz5BiUFzoJoj9\nC/yYxoZ/EjM4CCBlS7+X6GIQHrLFmzCgSV6iJ6puA+5QmmWMrddhLTWwFQKBgGER\nMcFPGjcZwznBEABg2fuqe5pXtBHZPfl5fY47+AbPGaKVoKOXZxSgF/WrH29hVeAq\n6C+LkWJhiJKU5UmzC4AGDG3g6HphDxiEKk8NTqRgDPL7Kjp4FEo8K3egEMoxCc69\nU/wtj5C4zL8FunUhEOc5x9mEWzcRrnnhjPUfqrQTAoGAFN746AFxsrNtUAWR3h47\nuMYAg5zaeCviSM+bf3qo3JAeUh8MOljStll+bTKkhvqMbcEypVZ53La+K6gL+s0A\nGBdZTeF4q8F6bN3KCdDSbxVKJY/U1QXKIwiI8v4OVl3192XTXYGGchCe9swF733x\n9P6N1VNjfEMSkkgFjCwKkqA=\n-----END PRIVATE KEY-----\n","client_email": "pyapi-578@master-smithy-633.iam.gserviceaccount.com","client_id": "108810066363988524797","auth_uri": "https://accounts.google.com/o/oauth2/auth","token_uri": "https://accounts.google.com/o/oauth2/token","auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs","client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/pyapi-578%40master-smithy-633.iam.gserviceaccount.com"},"query": "SELECT type, COUNT(*) c FROM [fh-bigquery:hackernews.full_201510] GROUP BY 1 ORDER BY 2 LIMIT 100"}' localhost:5000/bigquery