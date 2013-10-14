import json
import urllib
import urllib2

ADDRESS = 'http://localhost:8000/update'

def call(cmd, param):
    q = dict(cmd=cmd, param=json.dumps(param))
    p = urllib2.urlopen(ADDRESS, urllib.urlencode(q))
    p.read()

def drawLine(coords, opts=None):
    if opts is None:
        opts = {}
    call('drawLine', [[(lat, lng) for lng, lat in coords], opts])

def drawPoint(coord, opts=None):
    if opts is None:
        opts = {}
    lng, lat = coord
    call('drawPoint', [[lat, lng], opts])

def drawPoints(coords, opts=None):
    if opts is None:
        opts = {}
    call('drawPoints', [[(lat, lng) for lng, lat in coords], opts])

