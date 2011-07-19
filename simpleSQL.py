# simpleSQL.py
#
from pyparsing import Literal, CaselessLiteral, Word, Upcase, delimitedList, Optional, \
    Combine, Group, alphas, nums, alphanums, ParseException, Forward, oneOf, quotedString, \
    ZeroOrMore, restOfLine, Keyword

def test( str ):
    print str,"->"
    try:
        tokens = simpleSQL.parseString( str )
        print "tokens = ",        tokens
        print "tokens.columns =", tokens.columns
        print "tokens.tables =",  tokens.tables
        print "tokens.where =", tokens.where
        print "tokens.limit =", tokens.limit
    except ParseException, err:
        print " "*err.loc + "^\n" + err.msg
        print err
    print

def parse(str):
    """
       Parse the query string cos we're gonna do some magic 
       with them. You should capture the return object and query
       its attributes 'where', 'columns', 'tables'
    """
    try:
        tokens = simpleSQL.parseString(str)
    except ParseException, err:
        print " "*err.loc + "^\n" + err.msg
        print err
    return tokens

#
# SQL keywords supported 'SELECT', 'FROM'
#
selectStmt   = Forward()
selectToken  = Keyword("select", caseless=True)
fromToken    = Keyword("from", caseless=True)
limitToken   = Keyword("limit", caseless=True)

#
# Grammar for capturing IDENTIFIERS
#
ident          = Word( alphas, alphanums + "_$" ).setName("identifier")
columnName     = delimitedList( ident, ".", combine=True )
columnNameList = Group( delimitedList( columnName ) )
tableName      = delimitedList( ident, ".", combine=True )
tableNameList  = Group( delimitedList( tableName ) )

#
# Grammer to capture the conditionals within the SQL query
#
whereExpression = Forward()
and_ = Keyword("and", caseless=True)
or_ = Keyword("or", caseless=True)
in_ = Keyword("in", caseless=True)

#
#
#
E = CaselessLiteral("E")
binop = oneOf("= != < > >= <= eq ne lt le gt ge", caseless=True)
arithSign = Word("+-",exact=1)
realNum = Combine( Optional(arithSign) + ( Word( nums ) + "." + Optional( Word(nums) )  | ( "." + Word(nums) ) ) + Optional( E + Optional(arithSign) + Word(nums) ) )
intNum = Combine( Optional(arithSign) + Word( nums ) + Optional( E + Optional("+") + Word(nums) ) )

columnRval = realNum | intNum | quotedString | columnName # need to add support for alg expressions
whereCondition = Group(
    ( columnName + binop + columnRval ) |
    ( columnName + in_ + "(" + delimitedList( columnRval ) + ")" ) |
    ( columnName + in_ + "(" + selectStmt + ")" ) |
    ( "(" + whereExpression + ")" ) 
    )
whereExpression << whereCondition + ZeroOrMore( ( and_ | or_ ) + whereExpression )

# define the grammar
selectStmt      << ( selectToken + 
                   ( '*' | columnNameList ).setResultsName( "columns" ) + 
                   fromToken + 
                   tableNameList.setResultsName( "tables" ) + 
                   Optional( Group( CaselessLiteral("where") + whereExpression ), "" ).setResultsName("where") + 
                   Optional( limitToken + "(" + Word(nums) + Optional("," + Word(nums) ) + ")" ).setResultsName("limit") )

simpleSQL = selectStmt

# define Oracle comment format, and ignore them
oracleSqlComment = "--" + restOfLine
simpleSQL.ignore( oracleSqlComment )

