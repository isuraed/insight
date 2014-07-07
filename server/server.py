#!flask/bin/python
import happybase
import json
import random
import re
from collections import OrderedDict
from flask import abort, Flask, jsonify, render_template, request
from timer import Timer

API_URL = '/api/v0.3/'

app = Flask(__name__)
connection = happybase.Connection('54.183.87.221')
timer = Timer()

# v0.2 for backward compatibility for Kelty.
@app.route('/api/v0.2/reviews/<string:product>', methods = ['GET'])
def get_reviews_for_old(product):
    timer.start()

    table = connection.table('isura_reviews')
    row = table.row(product.lower())
    if not row:
        abort(404)

    reviews = [json.loads(r) for r in row.itervalues()]
    reviews = sorted(reviews, key=lambda k: k['timestamp'])

    timer.stop()

    response = { 'reviews' : reviews }
    response['meta'] = { 'count' : len(row), 'responseTime' : timer.elapsed() }
    return jsonify(response)


@app.route(API_URL + 'reviews/id/<string:product_id>', methods = ['GET'])
def get_reviews_for_id(product_id):
    timer.start()

    table = connection.table('isura_reviews_by_product_id')
    row = table.row(product_id)
    if not row:
        abort(404)

    reviews = [json.loads(r) for r in row.itervalues()]
    reviews = sorted(reviews, key=lambda k: k['timestamp'])

    timer.stop()

    response = { 'reviews' : reviews }
    response['meta'] = { 'count' : len(row), 'responseTime' : timer.elapsed() }
    return jsonify(response)


@app.route(API_URL + 'reviews/search/<string:query>', methods = ['GET'])
def get_reviews_for_query(query):
    if not query:
        abort(404)

    timer.start()

    product_ids = find_products(query)
    table = connection.table('isura_reviews_by_product_id')
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
    response['meta'] = { 'productCount' : len(product_ids), 'reviewCount' : review_count, 'responseTime' : timer.elapsed() }
    return jsonify(response)


@app.route(API_URL + 'brands/top25', methods = ['GET'])
def get_top_brands():
    timer.start()
    table = connection.table('isura_brand_metrics')
    results = []

    for key, data in table.scan():
        entry = {}
        entry['brand'] = key
        entry['average_score'] = data['cf1:average_score']
        entry['review_count'] = data['cf1:review_count']
        results.append(entry)

    results_limited = [r for r in results if int(r['review_count']) >= 500]
    results_sorted = sorted(results_limited, key=lambda k: k['average_score'], reverse=True)
    results_sorted = results_sorted[:25]
    timer.stop()
    
    response = { 'brands-top25' : results_sorted }
    response['meta'] = { 'count' : len(results_sorted), 'responseTime' : timer.elapsed() }
    return jsonify(response)


@app.route(API_URL + 'brands/bottom25', methods = ['GET'])
def get_bottom_brands():
    timer.start()
    table = connection.table('isura_brand_metrics')
    results = []

    for key, data in table.scan():
        entry = {}
        entry['brand'] = key
        entry['average_score'] = data['cf1:average_score']
        entry['review_count'] = data['cf1:review_count']
        results.append(entry)

    results_limited = [r for r in results if int(r['review_count']) >= 500]
    results_sorted = sorted(results_limited, key=lambda k: k['average_score'])
    results_sorted = results_sorted[:25]
    timer.stop()

    response = { 'brands-bottom25' : results_sorted }
    response['meta'] = { 'count' : len(results_sorted), 'responseTime' : timer.elapsed() }
    return jsonify(response)


@app.route('/reviews/<string:product_id>', methods = ['GET'])
def get_reviews(product_id):
    # Can't call the REST API directly from localhost because dev server is
    # single threaded (causes deadlock!)
    response = get_reviews_for_id(product_id) 
    if response.status_code != 200:
        abort(response.status_code)

    response = json.loads(response.data)
    meta = response['meta']
    reviews = response['reviews']
    return render_template('reviews.html', meta=meta, reviews=reviews)

@app.route('/', methods = ['GET'])
def get_index():
    query = request.args.get('q')
    if not query:
        return render_template('index.html')

    # Can't call the REST API directly from localhost because dev server is
    # single threaded (causes deadlock!)
    response = get_reviews_for_query(query.lower()) 
    response = json.loads(response.data)
    product_reviews = response['reviews']

    meta = {}
    meta['count'] = response['meta']['productCount']
    meta['responseTime'] = response['meta']['responseTime']

    results = []
    for prod in product_reviews:
        count = prod['count']
        product_id = prod['productId']
        review = prod['reviews'][random.randrange(count)]
        title = review['title']
        text = review['text']
        results.append( { 'productId' : product_id, 'title' : title, 'count' : count, 'text' : text } )

    return render_template('results.html', meta=meta, results=results)

   
# Returns a list of productId's that match the given query string. First look for
# an exact match then look for partial keyword match to fill the remaining results.
# Use the products_by_title table for exact match and products_by_keyword for the 
# partial matches.
def find_products(query):
    title_table = connection.table('isura_products_by_title')
    keyword_table = connection.table('isura_products_by_keyword')
    result_limit = 50
    found_ids = []

    # Exact matches are first.
    title_row = title_table.row(query.lower())
    if title_row:
        found_ids.extend(title_row.values())

    # There are enough results so skip the keyword search.
    if len(found_ids) >= result_limit:
        return found_ids[:result_limit]

    # Now try for keyword matches. Count how many products have each keyword in
    # their title.
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

    # Rank products by the number of query keywords in their title.
    sorted_hits = sorted(hit_count.items(), key=lambda x:x[1], reverse=True)
    # Put the keyword results after the exact matches.
    found_ids.extend([i[0] for i in sorted_hits])
    # Remove duplicate productId's.
    found_ids = list(OrderedDict.fromkeys(found_ids))

    return found_ids[:min(len(found_ids), result_limit)]


if __name__ == '__main__':
    app.run(debug = True, host='0.0.0.0', port=5000)
