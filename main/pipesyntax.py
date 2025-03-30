from enum import Enum
# -------------------------------------------
#                 Enumeration
# -------------------------------------------


class Aggregate(Enum):
    COUNT = "COUNT",
    SUM = "SUM",
    AVG = "AVG",
    MIN = "MIN",
    MAX = "MAX",
    FIRST = "FIRST",
    LAST = "LAST",
    MEDIAN = "MEDIAN",
    MODE = "MODE"

    def __str__(self):
        return self.name

    @classmethod
    def _missing_(cls, value: object):
        if isinstance(value, str):
            value = value.lower()
            for member in cls:
                if member.name.lower() == value:
                    return member
        return None


class Operator(Enum):
    EQUAL = "=",
    NOTEQUAL = "!-",
    LESSTHAN = "<",
    GREATERTHAN = ">",
    LESSTHANOREQUAL = "<=",
    GREATERTHANOREQUAL = ">=",
    AND = "AND",
    OR = "OR"


class QueryType(Enum):
    SELECT = "INDEX ONLY SCAN",
    FROM = "FROM",
    JOIN = "JOIN",
    WHERE = "SEQ SCAN",
    ORDER = "SORT",
    GROUP = "GROUP",
    LIMIT = "LIMIT",
    AGGREGATE = "AGGREGATE",

    def __str__(self):
        return self.name

    @classmethod
    def _missing_(cls, value: object):
        if isinstance(value, str):
            value = value.lower()
            for member in cls:
                if value.find(member.value[0].lower()) != -1:
                    return member
        return None


# -------------------------------------------
#      Parse Queries and helper methods
# -------------------------------------------

def parse_query(query_dict: dict):
    '''
    Parse Query parses the sanitized dictionary of queries and returns the pipe syntax
    :param query_dict:
    :return:
    '''
    order = []
    aggregate_dict = {}
    for query_string in query_dict:
        query, aggregate_query = sanitize_query(query_string)
        print(query, aggregate_query)
        if aggregate_query is not None:
            aggregate_dict.update({aggregate_query: None})
        match query:
            case QueryType.SELECT:
                continue
            case QueryType.FROM:
                continue
            case QueryType.JOIN:
                continue
            case QueryType.LIMIT:
                continue
            case QueryType.AGGREGATE:
                order.extend(match_aggregate(aggregate_dict))
            case _:
                continue


def sanitize_query(query_string):
    '''
    Sanitize the query and force the enumeration into the respective query type
    :param query_string: String of the query
    :return: A tuple of enum QueryType and enum Aggregate
    '''
    try:
        aggregate_query = Aggregate(query_string)
    except ValueError:
        aggregate_query = None
    if aggregate_query is not None:
        query_string = QueryType.AGGREGATE
    query = QueryType(query_string)
    return {query, aggregate_query}


def match_aggregate(aggregate_dict):
    '''

    :param aggregate_dict:
    :return:
    '''
    aggregate_list = []
    for aggregate in aggregate_dict:
        match aggregate:
            case Aggregate.COUNT:
                continue
            case Aggregate.SUM:
                continue
            case Aggregate.AVG:
                continue
            case Aggregate.MIN:
                continue
            case Aggregate.MAX:
                continue
            case Aggregate.FIRST:
                continue
            case Aggregate.LAST:
                continue
            case Aggregate.MEDIAN:
                continue
            case Aggregate.MODE:
                continue
            case _:
                continue
    return aggregate_list

# -------------------------------------------
#                 Pipe Syntax
# -------------------------------------------



