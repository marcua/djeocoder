import psycopg2

from db import DbTables
from parser.parsing import normalize, parse, ParsingError
from results import BlockResult, IntersectionResult, parse_point
from sqlalchemy import create_engine, MetaData, select,and_, or_, not_, Integer, cast
from sqlalchemy.sql import func

class Correction:
    def __init__(self, incorrect, correct):
        self.incorrect = incorrect
        self.correct = correct

class SpellingCorrector: 
    def correct(self, incorrect):
        # by default, correct nothing.
        return Correction(incorrect, incorrect)

# TODO: There's also a GeocoderException class in djeocoder.py
# -- these should probably be merged.
class GeocodingException(Exception):
    pass

class DoesNotExist(GeocodingException):
    pass

class PointParsingException(Exception):
    def __init__(self, str):
        self.str = str
    def __repr__(self):
        return 'String \'%s\' could not be parsed into points.' % self.str


class PostgisBlockSearcher:
    """
    Replaces the everyblock class \"BlockManager\".
    Handles interaction with the underlying database, taking a call to the search() method, converting it into a query,
    and then forming the response rows into BlockResult objects.
    """
    def __init__(self): 
        self.db_tables = DbTables()
        
    def close(self):
        pass

    def contains_number(self, number, from_num, to_num, left_from_num, left_to_num, right_from_num, right_to_num):
        """
        Copied almost verbatim from the corresponding EveryBlock class.
        
        Attempts to discover whether a particular triple of ranges
          [ (from_num, to_num), (left_from_num, left_to_num), (right_from_num, right_to_num) ]
        contains the given number.  The trick is that the number's parity may not match the parity of either the corresponding
        left or right range...
        """
        if not number: return True, from_num, to_num
        
        parity = int(number) % 2
        if left_from_num and right_from_num:
            left_parity = left_from_num % 2
            # If this block's left side has the same parity as the right side,
            # all bets are off -- just use the from_num and to_num.
            if right_to_num % 2 == left_parity or left_to_num % 2 == right_from_num % 2:
                from_num, to_num = from_num, to_num
            elif left_parity == parity:
                from_num, to_num = left_from_num, left_to_num
            else:
                from_num, to_num = right_from_num, right_to_num
        elif left_from_num:
            from_parity, to_parity = left_from_num % 2, left_to_num % 2
            from_num, to_num = left_from_num, left_to_num
            # If the parity is equal for from_num and to_num, make sure the
            # parity of the number is the same.
            if (from_parity == to_parity) and from_parity != parity:
                return False, from_num, to_num
            else:
                from_parity, to_parity = right_from_num % 2, right_to_num % 2
                from_num, to_num = right_from_num, right_to_num
                # If the parity is equal for from_num and to_num, make sure the
                # parity of the number is the same.
                if (from_parity == to_parity) and from_parity != parity:
                    return False, from_num, to_num
        return (from_num <= number <= to_num), from_num, to_num

    def search(self,street,number=None,pre_dir=None,suffix=None,post_dir=None,city=None,state=None,zip=None,left_city=None,right_city=None):

        blocks = self.db_tables.blocks.c
        select_clause = [blocks.id, blocks.pretty_name, blocks.from_num,
                         blocks.to_num, blocks.left_from_num, blocks.left_to_num,
                         blocks.right_from_num, blocks.right_to_num,
                         func.ST_AsEWKT(blocks.geom)]

        where_clause = [blocks.street == street.upper()]
        if pre_dir:
            where_clause.append(blocks.predir == pre_dir.upper())
        if suffix: 
            where_clause.append(blocks.suffix == suffix.upper())
        if post_dir: 
            where_clause.append(blocks.postdir == post_dir.upper())
        if city: 
            cu = city.upper()
            where_clause.append(or_(blocks.left_city == cu, blocks.right_city == cu))
        if state: 
            su = state.upper()
            where_clause.append(or_(blocks.left_state == su, blocks.right_state == su))
        if zip: 
            where_clause.append(or_(blocks.left_zip == zip, blocks.right_zip == zip))
        if number: 
            where_clause.append(or_(blocks.from_num <= number, blocks.to_num >= number))
        
        conn = self.db_tables.engine.connect()
        query = select(select_clause, and_(*where_clause))
        cursor = conn.execute(query)

        blocks = []
        for block in cursor.fetchall(): 
            containment = self.contains_number(number, block[2], block[3], block[4], block[5], block[6], block[7])
            if containment[0]: blocks.append([block, containment[1], containment[2]])
            
        final_blocks = []
        
        for b in blocks: 
            block = b[0]
            from_num = b[1]
            to_num = b[2]
            try:
                fraction = (float(number) - from_num) / (to_num - from_num)
            except TypeError:
                # TODO: revisit this clause.  We're getting here because the 'number' field was zero.  What do
                # we do in this case?  What does the original code do? 
                fraction = 0.5
            except ZeroDivisionError:
                fraction = 0.5

            # TODO: when we want to extract the geocoder from dependence on
            # Postgis, this is one of the main dependencies: we'll need to introduce
            # a new GIS library, so that we can do this interpolation "in code" -TWD
            select_clause = [func.ST_AsEWKT(func.line_interpolate_point(block[8], fraction))]
            cursor = conn.execute(select(select_clause))
            wkt_str = cursor.fetchone()[0]
            
            x,y = parse_point(wkt_str)
            final_blocks.append(BlockResult(block, wkt_str))
            
        cursor.close()
        conn.close()
        return final_blocks

class PostgisIntersectionSearcher:
    """
    Replaces the IntersectionManager clmass.
    """
    def __init__(self):
        self.db_tables = DbTables()

    def close(self):
        pass
    
    def search(self, predir_a=None, street_a=None, suffix_a=None, postdir_a=None, predir_b=None, street_b=None, suffix_b=None, postdir_b=None):
        ixns = self.db_tables.intersections.c
        select_clause = [ixns.id, ixns.pretty_name, func.ST_ASEWKT(ixns.location)]

        where_clause = []
        if predir_a: 
            where_clause.append(or_(ixns.predir_a == predir_a, ixns.predir_b == predir_a))
        if predir_b: 
            where_clause.append(or_(ixns.predir_a == predir_b, ixns.predir_b == predir_b))
        if street_a: 
            where_clause.append(or_(ixns.street_a == street_a, ixns.street_b == street_a))
        if street_b: 
            where_clause.append(or_(ixns.street_a == street_b, ixns.street_b == street_b))
        if suffix_a: 
            where_clause.append(or_(ixns.suffix_a == suffix_a, ixns.suffix_b == suffix_a))
        if suffix_b: 
            where_clause.append(or_(ixns.suffix_a == suffix_b, ixns.suffix_b == suffix_b))
        if postdir_a: 
            where_clause.append(or_(ixns.postdir_a == postdir_a, ixns.postdir_b == postdir_a))
        if postdir_b: 
            where_clause.append(or_(ixns.postdir_a == postdir_b, ixns.postdir_b == postdir_b))

        # This line is in IntersectionManager
        #   qs = qs.extra(select={"point": "AsText(location)"})
        # ... not sure exactly what it does here, 
        # but I'm grabbing 'location' as an WKT, so I'm assuming that this qualification 
        # doesn't matter. -TWD
        
        # TODO: can we replace these print statements with some sort of logging? 
        # print query
        # print filters

        conn = self.db_tables.engine.connect()
        query = select(select_clause, and_(*where_clause))
        cursor = conn.execute(query)
        results = cursor.fetchall()
        cursor.close()
        conn.close()

        return [IntersectionResult(res) for res in results]
