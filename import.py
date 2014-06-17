import datetime
import gzip
import locale
import os
import sys
import time

def parse(filename):
    infile = gzip.open(filename, 'r')
    record = []
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
    recordcount = 0
    filecount = 1
    filename = '{0:04d}'.format(filecount)
    currfile = open(filename + '.tab', 'w')

    for record in parse(sys.argv[1]):
        if record:
            if len(record) != 10:
                print(str(len(record)) + " is an invalid # of columns.")
                continue

            currfile.write('\t'.join(record) + '\n')
            recordcount += 1
            if (recordcount % 100000 == 0):
                currfile.close()
                os.system("hdfs dfs -put " + currfile.name + " isura/in/reviews")
                os.remove(currfile.name)

                print("Processed " + str(recordcount) + " records.")

                filecount += 1
                filename = '{0:04d}'.format(filecount)
                currfile = open(filename + '.tab', 'w')

    currfile.close()
    os.remove(currfile.name)
    print("Done. Total record count is " + str(recordcount) + ".")


if __name__ == '__main__':
    main()
