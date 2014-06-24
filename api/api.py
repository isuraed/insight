#!flask/bin/python
from flask import abort, Flask, jsonify
import happybase

app = Flask(__name__)
connection = happybase.Connection('54.183.87.221')

@app.route('/api/v0.1/brand_metrics', methods = ['GET'])
def get_brand_metrics():
    results = []
    table = connection.table('isura_brand_metrics')
    for key, data in table.scan():
        entry = {}
        entry['brand'] = key
        entry['average_score'] = data['cf1:average_score']
        entry['review_count'] = data['cf1:review_count']
        results.append(entry)
    return jsonify( { 'brand_metrics' : results } )


@app.route('/api/v0.1/brand_metrics/<string:brand>', methods = ['GET'])
def get_brand_metrics_for(brand):
    table = connection.table('isura_brand_metrics')
    row = [r for r in table.scan() if r[0] == brand]
    if not row:
        abort(404)
    return jsonify( { 'brand_metrics' : row[0] } )


if __name__ == '__main__':
    app.run(debug = True)
