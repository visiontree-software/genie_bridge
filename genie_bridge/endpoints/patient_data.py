import flask
import json
from genie_bridge.endpoints import (
    register_endpoint, err_resp, DateTimeFriendlyEncoder, InvalidToken,
    HTTPStatusOk, HTTPStatusClientError, HTTPStatusUnauthenticated
)
from genie_bridge.db import get_db
from datetime import datetime

def register(app):
    @register_endpoint(app, "/patient_data/<since>/<before>", 'usage_patient_data.html')
    def patient_data(since, before):
        req = flask.request
        if not req.is_json:
            return err_resp('request content is not json or content-type not set to "application/json"', HTTPStatusClientError)
        req_body = req.get_json()
        auth_token = req_body["token"]

        try:
            db = get_db(auth_token)
        except InvalidToken as ex:
            return err_resp(str(ex), HTTPStatusUnauthenticated)

        # CreationDate is of Date type, so must be converted
        since_object = datetime.strptime(since, '%Y%m%d%H%M%S')
        since_formatted = since_object.strftime('%Y/%m/%d %H:%M:%S')
        
        before_object = datetime.strptime(before, '%Y%m%d%H%M%S')
        before_formatted = before_object.strftime('%Y/%m/%d %H:%M:%S')

        cursor = db.cursor()
        cols = [
            'id', 'firstname', 'surname', 'dob', 'sex', 'HomePhone', 'EmailAddress',
            'AddressLine1', 'suburb', 'state', 'postcode', 'accounttype',
            'HealthFundName', 'LastUpdated', 'CreationDate',
        ]
        sql = '''
            SELECT {cols}
            FROM Patient
            WHERE CreationDate >= '{since}' AND CreationDate < '{before}'
            ORDER BY CreationDate DESC
        '''.format(cols=', '.join(cols), since=since_formatted, before=before_formatted)
        cursor.execute(sql)
        result = cursor.fetchall()

        data = []
        for r in result:
            dictrow = { cols[i]: r[i] for i in range(len(cols)) }
            data.append(dictrow)

        resultJson = json.dumps(data, cls=DateTimeFriendlyEncoder)
        resp = flask.Response(resultJson)
        resp.headers['Content-Type'] = 'application/json'

        return resp, HTTPStatusOk
