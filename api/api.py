#!flask/bin/python
from flask import abort, Flask, jsonify
import happybase
import json

app = Flask(__name__)
connection = happybase.Connection('54.183.87.221')

@app.route('/pi/v0.1/brand_metrics', methods = ['GET'])
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


@app.route('/api/v0.1/reviews/<string:product>', methods = ['GET'])
def get_review_for(product):
    print product
    table = connection.table('isura_reviews')
    row = table.row(product)
    reviews = []
    if not row:
        abort(404)
    for key, val in row.iteritems():
        jsonval = json.loads(val)
        jsonval['product'] = product
        reviews.append(jsonval)
    reviews = sorted(reviews, key=lambda k: k['timestamp'])
    return jsonify( { 'reviews' : reviews } )


if __name__ == '__main__':
    app.run(debug = True)
