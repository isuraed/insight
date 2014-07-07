import gzip
import logging
import os
import sys

import log_helper

# Parse the SNAP data set. Each record is 9 lines followed by a newline.
def parse(filename):
    record = []
    infile = gzip.open(filename, 'r')

    for line in infile:
        line = line.strip()
        colonpos = line.find(':')
        if colonpos == -1:
            yield record
            record = []
            continue
        val = line[colonpos+1:].strip()
        # '\' in data will wreck havoc with '\t' delimiters.
        val = val.replace('\\', '')
        record.append(val)

    infile.close()
    yield record


def main():
    log_helper.init_logging()

    hdfspath = sys.argv[2]
    recordcount = 0
    filecount = 1
    filename = '{0:04d}'.format(filecount)
    currfile = open(filename + '.tab', 'w')

    # Write the records in tab format to a set of smaller files in HDFS.
    # Choose smaller files so it is easier to work with a subset of the data
    # during testing and adhoc exploration.
    for row, record in enumerate(parse(sys.argv[1])):
        if record:
            if len(record) != 10:
                logging.warning("Skipping record " + str(row) + " because an invalid number of columns were found.")
                continue

            currfile.write('\t'.join(record) + '\n')
            recordcount += 1
            if (recordcount % 100000 == 0):
                currfile.close()
                os.system("hdfs dfs -put " + currfile.name + " " + hdfspath)
                logging.info("Put " + currfile.name + " into HDFS at location " + hdfspath + ".")
                logging.info("Total records written is " + str(recordcount) + ".")
                os.remove(currfile.name)

                filecount += 1
                filename = '{0:04d}'.format(filecount)
                currfile = open(filename + '.tab', 'w')

    currfile.close()
    os.remove(currfile.name)
    logging.info("Done! Wrote " + str(recordcount) + " records in " + str(filecount) + " files.")


if __name__ == '__main__':
    main()
