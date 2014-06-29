#!flask/bin/python
from flask import abort, Flask, jsonify
import happybase
import json
import time

API_VERSION = 'v0.2'

app = Flask(__name__)
connection = happybase.Connection('54.183.87.221')

@app.route('/api/' + API_VERSION + '/brand_metrics', methods = ['GET'])
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


@app.route('/api/' + API_VERSION + '/brand_metrics/<string:brand>', methods = ['GET'])
def get_brand_metrics_for(brand):
    table = connection.table('isura_brand_metrics')
    row = [r for r in table.scan() if r[0] == brand]
    if not row:
        abort(404)
    return jsonify( { 'brand_metrics' : row[0] } )


@app.route('/api/' + API_VERSION + '/reviews/<string:product>', methods = ['GET'])
def get_review_for(product):
    start = time.time()
    table = connection.table('isura_reviews')
    # Keys are stored in lowercase in HBase for more flexible searching.
    row = table.row(product.lower())
    reviews = []
    if not row:
        abort(404)
    for val in row.itervalues():
#        jsonval = json.loads(val)
        reviews.append(val)
    reviews = sorted(reviews, key=lambda k: k['timestamp'])
    reviews = { 'reviews' : reviews }
    reviews['meta'] = { 'count' : len(row), 'responseTime' : time.time() - start }
    return jsonify(reviews)


if __name__ == '__main__':
    app.run(debug = True)
