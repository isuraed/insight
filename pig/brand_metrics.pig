set job.name 'isura_brand_metrics';

reviews = load '$reviews' using PigStorage('\t') as (product_id:chararray, title:chararray, price:chararray, user_id:chararray, profile_name:chararray, helpfulness:chararray, score:int, time:long, summary:chararray, text:chararray);

product_scores = foreach reviews generate product_id, score;

brands = load '$brands' using PigStorage('\t') as (product_id:chararray, brand:chararray);

product_scores_brand = join product_scores by product_id, brands by product_id;
describe product_scores_brand;

product_info = foreach product_scores_brand generate product_scores::product_id as product_id, product_scores::score as score, brands::brand as brand;

products_grouped = group product_info by brand;

product_metrics = foreach products_grouped generate group as brand, COUNT(product_info) as review_count, AVG(product_info.score) as average_score;

store product_metrics into '$output' using PigStorage('\t');
