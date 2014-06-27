import os
import sys
import urllib

def main():
    baseurl = 'http://localhost:5000/api/v0.1/reviews/'
    requesturl = baseurl + urllib.quote(sys.argv[1])
    os.system("curl " + requesturl)


if __name__ == '__main__':
    main()
