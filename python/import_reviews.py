import gzip
import logging
import os
import sys

def init_logging():
    rootlogger = logging.getLogger()
    logformatter = logging.Formatter("%(asctime)s [%(levelname)s]  %(message)s")

    filehandler = logging.FileHandler("import.log")
    filehandler.setFormatter(logformatter)
    rootlogger.addHandler(filehandler)

    consolehandler = logging.StreamHandler()
    consolehandler.setFormatter(logformatter)
    rootlogger.addHandler(consolehandler)
    
    rootlogger.setLevel(logging.DEBUG)


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
        # Escape quotes because the text will be stored in json at some point.
        val = val.replace('"', '\\"')
        record.append(val)

    infile.close()
    yield record


def main():
    init_logging()

    hdfspath = argv[2]
    recordcount = 0
    filecount = 1
    filename = '{0:04d}'.format(filecount)
    currfile = open(filename + '.tab', 'w')

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
