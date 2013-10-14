from collections import defaultdict
from datetime import datetime
import sys

from imposm.parser import OSMParser
import psycopg2

DB_CONFIG = dict(host='localhost',
                 user='angkot',
                 password='angkot',
                 dbname='mm')

class TimeIt(object):
    def __init__(self, name):
        self.name = name

    def __enter__(self):
        self.start = datetime.now()
        print '[T] %s :: begin' % self.name

    def __exit__(self, *args):
        end = datetime.now()
        print '[T] %s :: end -> %s' % (self.name, end - self.start)

class Collector(object):
    coords = {}
    highway_refs = {}
    highway_tags = {}

    def collect_coords(self, coords):
        for osm_id, lng, lat in coords:
            self.coords[osm_id] = (lng, lat)

    def collect_highways(self, ways):
        for osm_id, tags, refs in ways:
            if 'highway' not in tags:
                continue
            self.highway_refs[osm_id] = refs
            self.highway_tags[osm_id] = tags

    def clean(self):
        """
        Remove invalid coords and highways.

        Some highways use unknown coords and not all coords are
        used for highways.
        """

        print 'Before:'
        print '- coords:', len(self.coords)
        print '- highways:', len(self.highway_refs)

        ref_coords = []
        for osm_id, refs in self.highway_refs.iteritems():
            ref_coords += refs
        ref_coords = set(ref_coords)

        available_coords = set(self.coords.keys())
        invalid_coords = ref_coords - available_coords
        valid_coords = ref_coords - invalid_coords
        unused_coords = available_coords - ref_coords

        for osm_id in unused_coords:
            del self.coords[osm_id]

        incomplete_highways = []
        for osm_id, refs in self.highway_refs.iteritems():
            if any((ref in invalid_coords for ref in refs)):
                incomplete_highways.append(osm_id)

        for osm_id in incomplete_highways:
            del self.highway_refs[osm_id]
            del self.highway_tags[osm_id]

        print 'Cleaning up:'
        print '- available coords:', len(available_coords)
        print '- referenced coords:', len(ref_coords)
        print '- invalid coords:', len(invalid_coords)
        print '- valid coords:', len(valid_coords)
        print '- unused coords:', len(unused_coords)
        print '- incomplete highways:', len(incomplete_highways)

        print 'After:'
        print '- coords:', len(self.coords)
        print '- highways:', len(self.highway_refs)

    def split(self):
        """
        Split highways at their intersections.

        Let say there are 2 highways that share 1 intersection:

            [o]aaaa[o]aaaa[o]
                    b
                    b
                   [o]

        This function will split the highway a into two segments
        at the intersection with highway b:

            [a]aaaa[o]cccc[o]
                    b
                    b
                   [o]

        """

        coord_count = defaultdict(int)
        for _, refs in self.highway_refs.iteritems():
            for coord_id in refs:
                coord_count[coord_id] += 1

        shared_coord = set([coord_id
                            for coord_id, count in coord_count.iteritems()
                            if count > 1])

        highway_points = defaultdict(list)
        for way_id, refs in self.highway_refs.iteritems():
            last_index = len(refs) - 1
            for index, coord_id in enumerate(refs):
                if index == 0 or index == last_index:
                    continue
                if coord_id in shared_coord:
                    highway_points[way_id].append((coord_id, index))

        print 'Shared coords:', len(shared_coord)
        print 'Shared highways:', len(highway_points)

        segments = defaultdict(list)
        for way_id, points in highway_points.iteritems():
            refs = self.highway_refs[way_id]
            last = 0

            for _, index in points:
                segments[way_id].append(refs[last:index+1])
                last = index
            segments[way_id].append(refs[last:])

        self.highway_segments = segments

class DB(object):
    def connect(self):
        self.conn = psycopg2.connect(**DB_CONFIG)

    def close(self):
        self.conn.commit()

    def init(self):
        cur = self.conn.cursor()

        # Coord

        cur.execute('''
            CREATE TABLE mm_coord (
                osm_id BIGINT PRIMARY KEY
            );
        ''')

        cur.execute('''
            SELECT AddGeometryColumn('mm_coord', 'geometry', 4326, 'POINT', 2);
        ''')

        # Highway

        cur.execute('''
            CREATE TABLE mm_highway (
                osm_id BIGINT PRIMARY KEY,
                highway VARCHAR(128),
                name VARCHAR(1024),
                oneway BOOLEAN
            );
        ''')

        cur.execute('''
            SELECT AddGeometryColumn('mm_highway', 'geometry', 4326, 'LINESTRING', 2);
        ''')

        # Segmented highway

        cur.execute('''
            CREATE TABLE mm_segment (
                id BIGSERIAL,
                osm_id BIGINT,
                highway VARCHAR(128),
                name VARCHAR(1024),
                oneway BOOLEAN
            );
        ''')

        cur.execute('''
            SELECT AddGeometryColumn('mm_segment', 'geometry', 4326, 'LINESTRING', 2);
        ''')

        # TODO add index to osm_id

    def save(self, c):
        cur = self.conn.cursor()

        # Save coords

        with TimeIt('Save coords'):
            sql = '''
                INSERT INTO mm_coord (osm_id, geometry)
                VALUES (%s, ST_GeomFromText(%s, 4326))
            '''
            data = []
            for osm_id, coord in c.coords.iteritems():
                data.append((osm_id, 'POINT(%f %f)' % coord))
            cur.executemany(sql, data)

        # Save highways

        with TimeIt('Save highways'):
            sql = '''
                INSERT INTO mm_highway (osm_id, highway, name, oneway, geometry)
                VALUES (%s, %s, %s, %s, ST_GeomFromText(%s, 4326))
            '''
            data = []
            for osm_id, refs in c.highway_refs.iteritems():
                tags = c.highway_tags[osm_id]
                name = tags.get('name', None)
                highway = tags.get('highway', None)
                oneway = tags.get('oneway', '') == 'yes'

                coords = ['%f %f' % c.coords[ref] for ref in refs]
                geometry = 'LINESTRING(%s)' % ', '.join(coords)

                data.append((osm_id, highway, name, oneway, geometry))

            cur.executemany(sql, data)

        # Save segments

        with TimeIt('Save segments'):
            sql = '''
                INSERT INTO mm_segment (osm_id, highway, name, oneway, geometry)
                VALUES (%s, %s, %s, %s, ST_GeomFromText(%s, 4326))
            '''
            data = []
            for osm_id, refs in c.highway_refs.iteritems():
                tags = c.highway_tags[osm_id]
                name = tags.get('name', None)
                highway = tags.get('highway', None)
                oneway = tags.get('oneway', '') == 'yes'

                if osm_id in c.highway_segments:
                    for segment in c.highway_segments[osm_id]:
                        coords = ['%f %f' % c.coords[ref] for ref in segment]
                        geometry = 'LINESTRING(%s)' % ', '.join(coords)

                        data.append((osm_id, highway, name, oneway, geometry))

                else:
                    coords = ['%f %f' % c.coords[ref] for ref in refs]
                    geometry = 'LINESTRING(%s)' % ', '.join(coords)

                    data.append((osm_id, highway, name, oneway, geometry))

            cur.executemany(sql, data)

def main():
    c = Collector()
    p = OSMParser(concurrency=4,
                  coords_callback=c.collect_coords,
                  ways_callback=c.collect_highways)

    with TimeIt('Parsing data'):
        p.parse(sys.argv[1])

    with TimeIt('Cleaning data'):
        c.clean()

    with TimeIt('Split segment'):
        c.split()

    with TimeIt('Store to database'):
        db = DB()
        db.connect()
        db.init()
        db.save(c)
        db.close()

if __name__ == '__main__':
    main()

