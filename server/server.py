#!flask/bin/python
import happybase
import json
import re
import time
from collections import OrderedDict
from flask import abort, Flask, jsonify, render_template, request


class Timer:
    def __init__(self):
        self.start_time = time.time()
        self.stop_time = self.start_time

    def start(self):
        self.start_time = time.time()

    def stop(self):
        self.stop_time = time.time()

    def elapsed_time(self):
        return float('{:.3f}'.format(self.stop_time - self.start_time))


API_VERSION = 'v0.3'

app = Flask(__name__)
connection = happybase.Connection('54.183.87.221')

timer = Timer()


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


# v0.2 for backward compatibility for Kelty.
@app.route('/api/v0.2/reviews/<string:product>')
def get_reviews_for_old(product):
    start = time.time()
    table = connection.table('isura_reviews')
    # Keys are stored in lowercase in HBase for more flexible searching.
    row = table.row(product.lower())
    reviews = []
    if not row:
        abort(404)
    for val in row.itervalues():
        reviews.append(json.loads(val))
    reviews = sorted(reviews, key=lambda k: k['timestamp'])
    reviews = { 'reviews' : reviews }
    reviews['meta'] = { 'count' : len(row), 'responseTime' : time.time() - start }
    return jsonify(reviews)


@app.route('/api/' + API_VERSION + '/reviews/id/<string:product_id>')
def get_reviews_for_id(product_id):
    timer.start()
    table = connection.table('isura_reviews_by_product_id')
    row = table.row(product_id)
    reviews = [json.loads(r) for r in row.itervalues()]
    reviews = sorted(reviews, key=lambda k: k['timestamp'])
    timer.stop()
    response = { 'reviews' : reviews }
    response['meta'] = { 'count' : len(row), 'responseTime' : timer.elapsed_time() }
    return jsonify(response)


@app.route('/api/' + API_VERSION + '/reviews/search/<string:query>')
def get_reviews_for(query):
    if not query:
        abort(404)

    timer.start()
    table = connection.table('isura_reviews_by_product_id')
    product_ids = find_products(query)
    all_reviews = []
    review_count = 0
    for pid in product_ids:
        row = table.row(pid)
        reviews = [json.loads(r) for r in row.itervalues()]
        reviews = sorted(reviews, key=lambda k: k['timestamp'])
        entry = { 'productId' : pid, 'count' : len(reviews), 'reviews' : reviews }
        all_reviews.append(entry)
        review_count += len(reviews)
    timer.stop()
    response = { 'reviews' : all_reviews }
    response['meta'] = { 'productCount' : len(product_ids), 'reviewCount' : review_count, 'responseTime' : timer.elapsed_time() }
    return jsonify(response)


@app.route('/products/<string:product_id>', methods = ['GET'])
def get_product_reviews(product_id):
    product_table = connection.table('isura_reviews_by_product_id')
    reviews_row = product_table.row(product_id)
    reviews = [] 
    if reviews_row:
        for r in reviews_row.itervalues():
            reviews.append(json.loads(r))
    return render_template('product.html', reviews=reviews)


@app.route('/')
def get_index():
    query = request.args.get('q')
    if not query:
        return render_template('index.html')

    product_table = connection.table('isura_reviews_by_product_id')
    product_ids = find_products(query)
    results = []
    for pid in product_ids:
        row = product_table.row(pid)
        if row:
            reviews = row.values()
            if reviews:
                first = json.loads(reviews[0])
                results.append(first)

    return render_template('index.html', results=results)


def find_products(query):
    title_table = connection.table('isura_products_by_title')
    keyword_table = connection.table('isura_products_by_keyword')
    result_limit = 25

    found_ids = []
    title_row = title_table.row(query.lower())
    if title_row:
        found_ids.extend(title_row.values())

    if len(found_ids) >= result_limit:
        return found_ids

    keywords = re.split("[^a-z0-9']+", query.lower())
    hit_count = {}
    for kw in keywords:
        keyword_row = keyword_table.row(kw)
        if keyword_row:
            for pid in keyword_row.itervalues():
                if pid in hit_count:
                    hit_count[pid] += 1
                else:
                    hit_count[pid] = 1

    sorted_hits = sorted(hit_count.items(), key=lambda x:x[1], reverse=True)
    found_ids.extend([i[0] for i in sorted_hits])
    found_ids = list(OrderedDict.fromkeys(found_ids))
    return found_ids[:min(len(found_ids), result_limit)]


if __name__ == '__main__':
    app.run(debug = True)
