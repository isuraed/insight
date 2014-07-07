import sys

activate_this = '/home/ubuntu/.virtualenvs/flask/bin/activate_this.py'
execfile(activate_this, dict(__file__=activate_this))

sys.path.insert(0, '/home/ubuntu/insight/server/')
from server import app as application
