import sys
import re
import math

import psycopg2

DB_CONFIG = dict(dbname='mm',
                 user='angkot',
                 host='localhost',
                 password='angkot')
import plot

class Lines(object):
    """
    Store the closest road segments and also plot them
    """
    items_unique = set()
    items = []

    def __init__(self, c):
        self.c = c

    def append(self, item):
        segment_id, osm_id = item
        if segment_id not in self.items_unique:
            self.items.append(item)
            self.items_unique.add(segment_id)

            self.draw(segment_id)

    def draw(self, segment_id):
        self.c.execute('''
            SELECT ST_AsText(geometry)
            FROM mm_segment
            WHERE id=%s
            ''', (segment_id,))
        g = self.c.fetchone()[0]
        coords = map(lambda x: map(float, x.split()),
                     re.sub(r'^.+\((.+)\).*$', r'\1', g).split(','))

        plot.drawLine(coords, dict(color='red'))
        plot.drawPoints(coords, dict(color='green', radius=2))

class DownSampler(object):
    """
    GPS data down sampler.

    This will reduce the density of the GPS data. The resulting
    GPS data points will have distance distance at least 100 meter
    for every two consecutive points.
    """

    MIN_DISTANCE = 100 # M

    def __init__(self):
        self.lng = None
        self.lat = None
        self.angle = None

    def is_next(self, lng, lat):
        if None in [self.lng, self.lat]:
            self.lng = lng
            self.lat = lat
            return True

        distance = self.get_distance(lng, lat)
        if distance >= self.MIN_DISTANCE:
            self.lng = lng
            self.lat = lat
            return True

        return False

    def get_distance(self, lng, lat):
        # From http://www.movable-type.co.uk/scripts/latlong.html

        R = 6371 # KM

        lat1, lng1, lat2, lng2 = map(math.radians,
                                     (self.lat, self.lng, lat, lng))
        x = (lng2-lng1) * math.cos((lat1+lat2)/2.0)
        y = lat2-lat1
        d = math.sqrt(x*x + y*y) * R
        return d * 1000 # M

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    c = conn.cursor()

    coords = []
    # Input is a file containing lines of longitude, latitude number pairs
    for line in open(sys.argv[1]):
        coords.append(map(float, line.split()))

    ds = DownSampler()

    lines = Lines(c)
    total = len(coords)
    idx = 0

    for lng, lat in coords:
        if not ds.is_next(lng, lat):
            continue

        plot.drawPoint((lng, lat),
                       dict(radius=7, color='blue', weight=1,
                            fillOpacity=0.4))

        # Get the closest road segment
        c.execute('''
            SELECT id, osm_id, ST_Distance(geometry, ST_GeomFromText('POINT(%s %s)', 4326)) AS distance
            FROM mm_segment
            ORDER BY distance ASC
            LIMIT 1
            ''', (lng, lat))
        segment_id, osm_id = c.fetchone()[0:2]
        lines.append((segment_id, osm_id))

        # Plot the segment
        c.execute('''
            SELECT ST_AsText(
                     ST_ClosestPoint(
                        geometry,
                        ST_GeomFromText('POINT(%s %s)', 4326)))
            FROM mm_segment
            WHERE id=%s
            ''', (lng, lat, segment_id))
        p = c.fetchone()[0]
        clng, clat = map(float, re.sub(r'^.+\((.+)\).*$', r'\1', p).split())
        plot.drawLine([[lng, lat], [clng, clat]],
                      dict(weight=5, color='blue'))

        idx += 1
        print idx, '/', total, '=>', lng, lat, '=>', osm_id

    c.execute('''
        SELECT id, osm_id, ST_AsText(geometry)
        FROM mm_segment
        WHERE id IN ( %s )
        ''' % ', '.join([str(segment_id) for segment_id, _ in lines.items]))
    for row in c:
        print row

if __name__ == '__main__':
    main()

