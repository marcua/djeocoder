import sys
import psycopg2

import djeocoder
import postgis


# Example usage: 
#
# import postgis
# import psycopg2
# conn = psycopg2.connect('dbname=openblock user=...')
# s = postgis.PostgisBlockSearcher(conn)
# points = [(x[1], x[2]) for x in s.search('Tobin', 25)]
# s.close()

def test_PostgisAddressGeocoder(cxn):
    g = djeocoder.PostgisAddressGeocoder(cxn)
    locstring = '32 Vassar Street'
    results = g.geocode(locstring)
    print results

def test_PostgisBlockSearcher():
    s = postgis.PostgisBlockSearcher()
    results = s.search('Tobin', 25)
    assert len(results) > 0, 'No results returned from PostgisBlockSearcher'
    print results

def test_PostgisIntersectionSearcher():
    s = postgis.PostgisIntersectionSearcher()
    results = s.search(street_a='MALVERN')
    assert len(results) > 0, 'No results returned from PostgisIntersectionSearcher'
    print results

def main(argv):
    test_PostgisBlockSearcher()
    test_PostgisIntersectionSearcher()
    #test_PostgisAddressGeocoder(cxn)

if __name__ == "__main__":
    main(sys.argv[1:])
    
