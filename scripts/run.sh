#!/bin/bash

screen -S isura-pipeline

hdfs_root="isura"
hdfs_input="$hdfs_root"/input
hdfs_output="$hdfs_root"/output
hdfs_reviews_raw="$hdfs_input"/reviews_raw
hdfs_reviews_clean="$hdfs_output"/reviews_clean
hdfs_reviews_unique="$hdfs_output"/reviews_clean_unique
hdfs_product_titles="$hdfs_output"/product_titles
hdfs_reviews_by_product_id="$hdfs_output"/reviews_by_product_id
hdfs_products_by_title="$hdfs_output"/products_by_title
hdfs_products_by_keyword="$hdfs_output"/products_by_keyword

jar_path="../build/libs/insight-1.0.jar"
package_path="com.isuraed.insight"

# Additional Java setup. JDK 1.6/1.7 should already be installed by Cloudera manager.
export HADOOP_CLASSPATH=`hbase classpath`
sudo add-apt-repository ppa:cwchien/gradle
sudo apt-get update
sudo apt-get install gradle

# Build the Hadoop jar.
cd ..
gradle build
cd scripts

hdfs dfs -mkdir $hdfs_root
hdfs dfs -mkdir $hdfs_input
hdfs dfs -mkdir $hdfs_output
hdfs dfs -mkdir $hdfs_reviews_raw

wget http://snap.stanford.edu/data/amazon/all.txt.gz -O snap-reviews.gz

# Parse raw data from SNAP and import into HDFS.
python import_reviews.py snap-reviews.gz $hdfs_reviews_raw

# Save the raw data in HDFS just in case.
hdfs dfs -put snap-reviews.gz $hdfs_input
rm snap-reviews.gz

# Remove anonymous reviews and some othe minor sanity checks.
pig -f clean_reviews.pig -param input=$hdfs_reviews_raw -param output=$hdfs_reviews_clean

# Duplicates are reviews with the same text for different productIds.
pig -f remove_duplicate_reviews.pig -param input=$hdfs_reviews_clean -param output=$hdfs_reviews_unique 

# Extract productId and title for all products.
pig -f extract_product_titles.pig -param input=$hdfs_reviews_clean -param output=$hdfs_product_titles

# MapReduce job that aggregates reviews by productId and packs the reviews into json
# for consumption by HBase.
hadoop jar $jar_path "$package_path".ReviewsByProductId $hdfs_reviews_unique $hdfs_reviews_by_product_id

# MapReduce job that calculates the inverse title to productId mapping.
hadoop jar $jar_path "$package_path".ProductsByTitle $hdfs_product_titles $hdfs_products_by_title

# MapReduce job that builds the reverse keyword index of the product titles.
hadoop jar $jar_path "$package_path".ProductIndexer $hdfs_product_titles $hdfs_products_by_keyword

# Import the results of MapReduce to HBase.
hadoop jar $jar_path "$package_path".ReviewsByProductIdImporter $hdfs_reviews_by_product_id isura_reviews_by_product_id
hadoop jar $jar_path "$package_path".ProductsByTitleImporter $hdfs_products_by_title isura_products_by_title
hadoop jar $jar_path "$package_path".ProductsByKeywordImporter $hdfs_products_by_keyword isura_products_by_keyword

# Finally...Start the thrift server (and pray it doesn't die).
hbase thrift start
