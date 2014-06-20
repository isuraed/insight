import gzip
import logging
import os
import sys

import log_helper

def main():
    input_file = gzip.open(sys.argv[1], 'r')
    output_file = open('brands.tab', 'w')
    hdfs_path = sys.argv[2];
    record_count = 0

    log_helper.init_logging("import_brands.log")
    logging.info("Processing " + input_file.name + "...")

    for idx, line in enumerate(input_file):
        delim_pos = line.find(' ')
        product = line[:delim_pos]
        # strip() to remove newline.
        brand = line[delim_pos+1:].strip()
        if product and brand:
            output_file.write(product + '\t' + brand)
            record_count += 1
        else:
            logging.info("Line " + str(idx + 1) + " was skipped.")

    input_file.close()
    output_file.close()

    os.system("hdfs dfs -put " + output_file.name + " " + hdfs_path)

    logging.info("Wrote " + output_file.name + " to HDFS at path" + hdfs_path+ ".")
    logging.info("Total records written: " + str(record_count))

    os.remove(output_file.name)
    

if __name__ == '__main__':
    main()
