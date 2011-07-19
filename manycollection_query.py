"""
   File  : manycollection_query.py
   Author: Raymond Tay
   Date  : 15 July 2011

   Description:
   =================
 
   This module will support a simple multi-collection join query in MongoDB.
   In SQL parlance, a 'table' is native to SQL whereas its equivalent is 
   the term 'collection' in NoSQL databases e.g. MongoGB.

   Currently, we have plans to only support queries of this simple form:
   
   SELECT A.COL1, A.COL2, B.COL1, B.COL2 from A,B where A.COL3 == B.COL3

   The 'where' clause will support relational operators that will return
   a Boolean i.e. True or False.
   
   Caveats:
   ========
   Don't use database aliases as its not supported yet.


   Technical Details:
   ==================
   This section highlights the implementation:
   (1) Parse the SQL-SELECT query and it looks for the following 
       (1.1) column selectors e.g. SELECT A.<col1>, A.<col2>, B.<col1> etc
       (1.2) comparison expressions in the 'WHERE' clause e.g. SELECT .... where A.<col> <binop> B.<col>
             or A.<col> <binop> 'string|number'
       (1.3) range expressions in the 'WHERE' clause e.g. SELECT ... FROM A,B where A in (val1, val2)
   (2) A data structure will be kept s.t. column selectors and comparison expressions are tagged for
       their appropriate table. The idea is to be able to lookup via the table you're interested in.
   (3) The data is retrieved by firing a MongoDB query which combines the range expressions, comparison expressions
       (excluding those cross-table queries) for each table. Each table's data will be stored in RAM (Yeah it won't scale)
   (4) Then, each cross-table query will be applied to the tables involved. (Its space/runtime is potentially O(n^n) where
       'n' is the number of tables - yeah it's going to suck bigtime) 

   TODO:
   ======
   (1) Optimize the querying s.t. its no longer O(n^n)

"""
import simpleSQL
import re
from pymongo import *
from pprint import *

test1 = "select A.Title, A.author, A.date, B.Title, B.author, B.date from A, B where A.name = B.name"
test2 = 'select A.Title, A.author from A'
test3 = 'select A.Title, A.author, B.Title, B.author from A,B'
test4 = "select A.Title, A.author, A.date, B.Title, B.author, B.date from A, B where A.author = B.author and A.author in (Raymond)"
test5 = 'select A.author, A.Title, B.author, B.Title from A, B where A.author = B.author'
test6 = 'select A.author, A.Title, A.date, B.author, B.Title, B.date from A, B where A.author = B.author and A.date = B.date'
test7 = 'select A.author, B.author, C.author from A, B, C where A.author = B.author and B.author = C.author'
test8 = 'select A.Title, A.author from A limit (4,8)'
test9 = 'select A.Title, A.author from A limit (4)'

DEBUG = False

def parseSQL(query):
    """
       Only 'SELECT' is supported; even that is 'bare' minimum.
    """
    tokens = simpleSQL.parse(query)
    if DEBUG: print('\nparseSQL:tokens are %s\n' % tokens)

    return tokens

def constructMongoQuery(tokens):
    """
       Using the information in the tokens, construct the Mapper, Reducer
       and Query functions as defined in the MongoDB shell command
    """

    # construct the mapper, reducer and query
    # for each table in the query, the corresponding query string
    # will be generated and stored/ref via the indexes of 'mappers'
    selectors = {}
    queries = {}
    for tbl in tokens.tables:
        selectors[tbl] = buildSelectors(tbl, tokens.columns)

        # construct the filter a.k.a 'WHERE'
        queries[tbl] = buildFilters(tbl, tokens.where)

    return selectors,queries

def buildSelectors(tableName, columns):
    """ 
       The col names should be like <table name>.<col name>
       Locate all of them for their appropriate tables and group them.
       Currently, you can only select the columns and not able to perform
       any computation on the column values
    """
    s = [] 
    for col in columns:
        cols = re.findall('%s\.([a-zA-Z0-9]+)' % tableName, col) 
        for c in cols:
            s.append(c)

    if DEBUG: print('\nbuildSelectors:%s\n' % s )

    return s

def buildFilters(tablename, where):
    """
       The idea is to fish out the clauses that reference a single table or cross tables
       Think of them as the 'query' in the MapReduce in MongoDB.
       Currently, only the following operators  are supported
       - AND
       - < <= > >= = !=
       - IN
    """
    filters = {tablename:{}} # this string will contain strings of the format "filter1: {'$eq', 12}"
    # flag out if multi-tables are referenced
    # LHS - the format should be <tablename>.<col name>
    # RHS - the format should be <tablename>.<col name> OR a number, a char, a string
    idxs = range(1, len(where[0]), 2)
    counter = 0
    for i in idxs:
        indicator, res = findRangeOps(tablename, where[0][i])
        if indicator: filters[tablename].setdefault('rangeop%d'%counter,res)

        indicator, res = findComparisonOps(tablename, where[0][i])
        if indicator: filters[tablename].setdefault('cmpop%d'%counter, res)
        counter += 1

    if DEBUG: print('\nbuildFilters:%s' % filters)

    return filters

def findRangeOps(tablename, tokens):
    """
       Search/Populate filter templates for expressions like
       LHS 'in' (val1, val2, val3, ... valn) where LHS is <table>.<col>
    """
    filters = {}
    res = [i for i in tokens if i == 'in' or len(re.findall('(%s)\.'% tablename,i))]
    # The list, res, should only have 2 elements 'in' & 'table.col'
    if len(res) == 2:
        d = {}
        d['$in'] = [i for i in tokens[3:len(tokens)-1]]
        key = re.findall('%s\.([a-zA-Z0-9]+)' %  tablename, tokens[0])[0]
        filters[key] = d
        return tablename, filters
    return '',filters

def findLimit(tablename, tokens):
    """
       Locate the expression that does corresponds to LIMIT (start, stop)
    """
    limitFound = re.findall('limit|LIMIT', tokens)

def findAggFunctions():
    """
       Locate the expressions that does AVG, COUNT, MAX, MIN
    """
    pass

def findComparisonOps(tablename, tokens):
    """
       Search/Populate filter templates for expressions like
       LHS <bin op> RHS where <bin op> = {<, <=, >, >=, !=, =}
       [0]..[1].....[2]
    """
    filters = {}
    res = [i for i in tokens if i in ['<', '<=', '>', '>=', '!=', '='] or len(re.findall('(%s)\.'% tablename,i))]
    if len(res) == 2:
        try:
            LHS, binOp, RHS = tokens
            if binOp in ['=','eq']:
                filters[LHS] = { '$eq': RHS }
            if binOp in ['!=','neq']:
                filters[LHS] = { '$ne': RHS}
            if binOp in ['<', 'lt']:
                filters[LHS] = { '$lt': RHS}
            if binOp in ['>', 'gt']:
                filters[LHS] = { '$gt': RHS}
            if binOp in ['<=', 'lte']:
                filters[LHS] = { '$lte': RHS}
            if binOp in ['>=', 'gte']:
                filters[LHS] = { '$gte': RHS}
        except ValueError, e:
            print('\nfindComparisonOps: error msg: %s' % e)
        return tablename, filters
    return '',filters 

def runQuery(query, database, username=None, output=None, password=None, port=27017):
    """
       username/password - is not done yet.
       tokens            - query string as parsed by simpleSQL
       mapred_fns        - list of fns ('map','reduce','query' etc supported 
                           in the mongodb shell command)
       database          - name of database to connect to
    """
    tokens = parseSQL(query)
    print "\nTOKENS-> %s\n" % tokens
    selectors,queries = constructMongoQuery(tokens) 

    #
    # for each table we detect in the 'FROM' clause, we'll construct a 
    # query and store the results. For multiple-table matches, 
    #
    # Due to persistence, i've decided to ignore the notion of 'collection'
    # and use 'table' instead. This may/may not be removed in the future
    #
    # Owing to the fact that field selection in MongoDB is case sensitive,
    # the case has to be preserved. See simpleSQL.py
    #
    records = {}
    
    for tablename in tokens.tables:
        conn = Connection('localhost', port)
        try:
            db = conn['%s'% database]
            table = db['%s'% tablename]
            #
            # sieve out the queries of the <table.col> <cmp> <table.col>
            # 
            c = table.find(spec=getQuery(queries, tablename, tokens.tables), fields=selectors[tablename])
            records.setdefault(tablename, [rec for rec in c])
        except Exception, e:
            print "\nrunQuery: Error caught while in MongoDB, msg %s" % e
        conn.disconnect()
    final = computeJoin(records, queries, tokens.tables, tokens.limit)
    #writeToDisk(final, 'raymond', 'raymond_datafile') if final else None
    writeToDisk(final, username, output) if final else None

    return final

def writeToDisk(records, username, output):
    """
       Write to Disk. Assuming this is the master_node in DCA
    """
    f = open('/tmp/' + output, 'a')
    try:
        keys = records[0].keys()
        header = ''
        for k in keys: 
            print '^%s' % k
            if k != '_id': header += k +','
        header = header.strip(',')
        f.write('%s\n' % header)
        for rec in records:
            line = ''
            for k in rec.keys():
                if k != '_id': line += rec[k] + ','
            line = line.strip(',')
            f.write('%s\n' % line) 
        f.close() 
    except Exception, e:
        print '\nwriteToDisk:%s' % e

def computeJoin(records, queries, tables, limit):
    """
       Return a collection of records where the 'JOIN'
       query a.k.a <table>.<col> binop <table2>.<col>
       makes sense.
	   
	   Now supports the notation LIMIT <range> or LIMIT <num>
       where <range> = (i, j) and <num> returns the first 'num' records
    """
    final = [] # will contain all documents that matched across the tables
  
    tablerange = range(0, len(tables))
    for i in tablerange: 
        query = queries[tables[i]][tables[i]]
        if not query:
           # this happens when queries of the form select ... from table1, table2
           # with no 'where' clause
           table1 = records[tables[i]] 
           for rec in table1: final.append(rec)
        else:
            for opType in query:
                if re.findall('cmpop', opType): 
                    # find the x-tbl query 
                    # parse the query of the form {'table1.col': {'binop': 'table2.col'}}
                    binop = query[opType].values()[0].keys()[0]
                    lhs = re.findall('([a-zA-Z0-9]+)\.([a-zA-Z0-9]+)', query[opType].keys()[0]) 
                    rhs = re.findall('([a-zA-Z0-9]+)\.([a-zA-Z0-9]+)', query[opType].values()[0][binop]) 
                    if lhs and rhs:
                        tname1 = lhs[0][0]
                        tname2 = rhs[0][0]
                        col1 = lhs[0][1]
                        col2 = rhs[0][1]
                        # At this point, we've found both tables in the LHS & RHS
                        # and we can proceed to do a comparison against the records
                        # after determining which comparison operator to use
                        table1 = records[tname1] 
                        table2 = records[tname2] 
                        if binop == '$eq': cmpFn = lambda x,y : x == y
                        if binop == '$neq': cmpFn = lambda x,y : x != y
                        if binop == '$lt': cmpFn = lambda x,y : x < y
                        if binop == '$lte': cmpFn = lambda x,y : x <= y
                        if binop == '$gt': cmpFn = lambda x,y : x > y
                        if binop == '$gte': cmpFn = lambda x,y : x >= y
                        final = [rec1 for rec1 in table1 for rec2 in table2 if cmpFn(rec1[col1],rec2[col2])]       
    final = computeLimit(final, limit) if limit else final
    #print 'final: %s' % final
    return final

def computeLimit(records, limit):
    """
	   Now supports the notation LIMIT <range> or LIMIT <num>
       where <range> = (i, j) and <num> returns the first 'num' records
    """
    start = 0
    end   = 0
	if len(limit) == 4: start = limit[2]
    start, end = limit[2], limit[4] if len(limit) == 6 else None

	if not end:
        return records[start-1:end-1]
    else:
        return records[0:start-1]
        
def getQuery(queries,tablename, tables):
    """
       Return True if queries of the form <table.col> <cmp> <table.col>
       are found else False
    """
    # collect the rangeops and place them into a dict
    # collect the cmpops and place them into a dict
    # sieve thru for cross table queries and do not include 
    # them
    d = [] 
    query = queries[tablename][tablename]
    for opType in query:
        if re.findall('rangeop', opType): 
            d.append(query[opType].keys()[0])
            d.append(query[opType].values()[0])
        if re.findall('cmpop', opType): 
            # find the x-tbl query 
            # parse the query of the form {'table1.col': {'binop': 'table2.col'}}
            binop = query[opType].values()[0].keys()[0]
            lhs = re.findall('([a-zA-Z0-9]+)\.', query[opType].keys()[0]) 
            rhs = re.findall('([a-zA-Z0-9]+)\.', query[opType].values()[0][binop]) 

            # Conduct an extra check cos if the table names are in lhs[0] &
            # rhs[0] that would mean that we've got a x-table query & we skip it.
            if lhs and rhs:
                if lhs[0] in tables and rhs[0] in tables:
                    continue
            else:
                d.append(query[opType].keys()[0])   
                d.append(query[opType].values()[0])   
            
    ret = {}
    i = 0
    if not d: return ret
    for dummy in range(0,len(d)%2+1):
        ret.setdefault(d[i], d[i+1])
        i += 2
    return ret

if __name__ == '__main__':
   runQuery(test9, 'testing')
#   simpleSQL.test(test9)
