from flask import Flask, request
from flask_restful import abort, Api, Resource
from gcloud import bigquery
import os
import json
from sys import argv
import gspread
from oauth2client.service_account import ServiceAccountCredentials

app = Flask(__name__)
api = Api(app)

auth_file = 'bq-auth.json'

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
        return {'services': ['/bigquery', "/spreadsheet"]}

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
                cred_file = open(auth_file, 'w')
                cred_file.write(json.dumps(jsonData["credentials"]))
                cred_file.close()
                queryStr = jsonData["query"]
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './' + auth_file
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
		                os.remove(auth_file)
		                os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = orig_cred

		            return {'error': False, 'rows': query.rows, 'fields': fields}
            except Exception as e:
                print(e)
                return {'error': True, 'code': e.code, 'message': e.message}

        except Exception as e:
            print(e)
            raise(e)

class Spreadsheet(Resource):
    def get(self):
        return {'service': 'spreadsheet'}

    def get(self):
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/drive.apps.readonly']
        credentials = ServiceAccountCredentials.from_json_keyfile_name(os.environ['GOOGLE_APPLICATION_CREDENTIALS'], scope)
        gc = gspread.authorize(credentials)
        sh = gc.open('Sheet name')
        return str(sh)

api.add_resource(PyAPI, '/')
api.add_resource(Shutdown, '/shutdown')
api.add_resource(BigQuery, '/bigquery')
api.add_resource(Spreadsheet, '/spreadsheet')

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
