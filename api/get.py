import os
import sys
import urllib

def main():
    baseurl = 'http://localhost:5000/api/v0.1/'
    requesturl = baseurl + sys.argv[1] + '/' + urllib.quote(sys.argv[2])
    os.system("curl " + requesturl)


if __name__ == '__main__':
    main()
