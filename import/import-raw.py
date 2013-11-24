import sys
from collections import defaultdict
from datetime import datetime

from imposm.parser import OSMParser
import psycopg2

DB_CONFIG = dict(host='localhost',
                 user='angkot',
                 password='angkot',
                 dbname='angkot_osm_jakarta')

class TimeIt(object):
    def __init__(self, name):
        self.name = name

        if not hasattr(TimeIt, '_LEVEL'):
            TimeIt._LEVEL = 0

    def __enter__(self):
        level = TimeIt._LEVEL
        TimeIt._LEVEL += 1
        self.indent = '    ' * level

        self.start = datetime.now()
        print '%s[T] %s :: begin' % (self.indent, self.name)

    def __exit__(self, *args):
        TimeIt._LEVEL -= 1
        end = datetime.now()
        print '%s[T] %s :: end -> %s' % (self.indent, self.name, end - self.start)

class Collector(object):
    nodes = {}
    way_refs = {}
    way_tags = {}

    def collect_nodes(self, nodes):
        for osm_id, lng, lat in nodes:
            self.nodes[osm_id] = (lng, lat)

    def collect_ways(self, ways):
        for osm_id, tags, refs in ways:
            if 'highway' not in tags:
                continue
            self.way_refs[osm_id] = refs
            self.way_tags[osm_id] = tags

    def clean(self):
        """
        Remove invalid nodes and ways.

        Some ways use unknown nodes and not all nodes are
        used for ways.
        """

        print 'Before:'
        print '- nodes:', len(self.nodes)
        print '- ways:', len(self.way_refs)

        ref_nodes = []
        for osm_id, refs in self.way_refs.iteritems():
            ref_nodes += refs
        ref_nodes = set(ref_nodes)

        available_nodes = set(self.nodes.keys())
        invalid_nodes = ref_nodes - available_nodes
        valid_nodes = ref_nodes - invalid_nodes
        unused_nodes = available_nodes - ref_nodes

        for osm_id in unused_nodes:
            del self.nodes[osm_id]

        incomplete_ways = []
        way_nodes_count = 0
        for osm_id, refs in self.way_refs.iteritems():
            if any((ref in invalid_nodes for ref in refs)) or len(refs) <= 1:
                incomplete_ways.append(osm_id)
            way_nodes_count += len(refs)

        for osm_id in incomplete_ways:
            del self.way_refs[osm_id]
            del self.way_tags[osm_id]

        print 'Cleaning up:'
        print '- available nodes:', len(available_nodes)
        print '- referenced nodes:', len(ref_nodes)
        print '- invalid nodes:', len(invalid_nodes)
        print '- valid nodes:', len(valid_nodes)
        print '- unused nodes:', len(unused_nodes)
        print '- incomplete ways:', len(incomplete_ways)
        print '- way nodes count:', way_nodes_count

        print 'After:'
        print '- nodes:', len(self.nodes)
        print '- ways:', len(self.way_refs)


class DB(object):
    def connect(self):
        self.conn = psycopg2.connect(**DB_CONFIG)

    def close(self):
        self.conn.commit()

    def init(self):
        cur = self.conn.cursor()

        # Nodes

        cur.execute('''
            CREATE TABLE new_osm_node (
                id       BIGSERIAL,
                osm_id   BIGINT PRIMARY KEY,

                created  TIMESTAMP DEFAULT NOW(),
                updated  TIMESTAMP DEFAULT NOW()
            );
        ''')

        cur.execute('''
            SELECT AddGeometryColumn('new_osm_node', 'coord', 4326, 'POINT', 2);
        ''')

        # Way

        cur.execute('''
            CREATE TABLE new_osm_way (
                id       BIGSERIAL,
                osm_id   BIGINT PRIMARY KEY,

                name     VARCHAR(1024),
                highway  VARCHAR(128),
                oneway   BOOLEAN DEFAULT FALSE,

                created  TIMESTAMP DEFAULT NOW(),
                updated  TIMESTAMP DEFAULT NOW()
            );
        ''')

        cur.execute('''
            SELECT AddGeometryColumn('new_osm_way', 'path', 4326, 'LINESTRING', 2);
        ''')

        # Way nodes

        cur.execute('''
            CREATE TABLE new_osm_waynode (
                id       BIGSERIAL,
                way_id   BIGINT,
                node_id  BIGINT,
                index    INT,
                size     INT
            );
        ''')

        # TODO add index to osm_id

    def save(self, c):
        from psycopg2.extensions import adapt
        BATCH = 10000

        node_id_map = {}
        way_id_map = {}

        cur = self.conn.cursor()

        # Save nodes

        with TimeIt('Save nodes'):
            sql = '''
                INSERT INTO new_osm_node (osm_id, coord)
                VALUES %s
                RETURNING id
            '''

            def flush(osm_ids, data):
                if len(data) == 0:
                    return [], []

                params = ['(%s, ST_GeomFromText(%s, 4326))' % tuple([adapt(v).getquoted() for v in values])
                          for values in data]
                cur.execute(sql % ', '.join(params))

                node_ids = []
                for row in cur:
                    node_ids.append(row[0])
                node_id_map.update(dict(zip(osm_ids, node_ids)))

                return [], []

            data = []
            osm_ids = []
            for osm_id, coord in c.nodes.iteritems():
                data.append((osm_id, 'POINT(%f %f)' % coord))
                osm_ids.append(osm_id)

                if len(data) > BATCH:
                    osm_ids, data = flush(osm_ids, data)
            flush(osm_ids, data)

        # Save ways

        with TimeIt('Save ways'):
            sql = '''
                INSERT INTO new_osm_way (osm_id, name, highway, oneway, path)
                VALUES %s
                RETURNING id
            '''

            def flush(osm_ids, data):
                if len(data) == 0:
                    return [], []

                params = ['(%s, %s, %s, %s, ST_GeomFromText(%s, 4326))' % tuple([adapt(v).getquoted() for v in values])
                          for values in data]
                cur.execute(sql % ', '.join(params))

                way_ids = []
                for row in cur:
                    way_ids.append(row[0])
                way_id_map.update(dict(zip(osm_ids, way_ids)))

                return [], []

            data = []
            osm_ids = []
            for osm_id, refs in c.way_refs.iteritems():
                tags = c.way_tags[osm_id]
                name = tags.get('name', None)
                highway = tags.get('highway', None)
                oneway = tags.get('oneway', '') == 'yes'

                coords = ['%f %f' % c.nodes[ref] for ref in refs]
                geometry = 'LINESTRING(%s)' % ', '.join(coords)

                data.append((osm_id, name, highway, oneway, geometry))
                osm_ids.append(osm_id)

                if len(data) > BATCH:
                    osm_ids, data = flush(osm_ids, data)
            flush(osm_ids, data)

        # Save way nodes

        with TimeIt('Save way nodes'):
            sql = '''
                INSERT INTO new_osm_waynode (way_id, node_id, index, size)
                VALUES %s
            '''

            def flush(data):
                if len(data) == 0:
                    return []

                params = ['(%s, %s, %s, %s)' % values
                          for values in data]
                cur.execute(sql % ', '.join(params))

                return []

            data = []
            for osm_id, refs in c.way_refs.iteritems():
                way_id = way_id_map[osm_id]
                size = len(refs)
                for index, ref in enumerate(refs):
                    node_id = node_id_map[ref]
                    data.append((way_id, node_id, index, size))

                if len(data) > BATCH:
                    data = flush(data)
            flush(data)

def main():
    c = Collector()
    p = OSMParser(concurrency=4,
                  coords_callback=c.collect_nodes,
                  ways_callback=c.collect_ways)

    with TimeIt('Parsing data'):
        p.parse(sys.argv[1])

    with TimeIt('Cleaning data'):
        c.clean()

    with TimeIt('Store to database'):
        db = DB()
        db.connect()
        db.init()
        db.save(c)
        db.close()

if __name__ == '__main__':
    main()

