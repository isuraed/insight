REGISTER 'lib/piggybank-0.12.0.jar';

DEFINE ISOToDay org.apache.pig.piggybank.evaluation.datetime.truncate.ISOToDay();
DEFINE UnixToISO org.apache.pig.piggybank.evaluation.datetime.convert.UnixToISO();

set job.name 'isura_test';

reviews = load 'isura/in/reviews/*.tab' using PigStorage('\t') as (product_id:chararray, title:chararray, price:chararray, user_id:chararray, profile_name:chararray, helpfulness:chararray, score:int, time:long, summary:chararray, text:chararray);

scores = foreach reviews generate product_id, time * 1000 as timestamp:long, user_id, score;

store scores into 'isura/out/scores' using PigStorage('\t');
