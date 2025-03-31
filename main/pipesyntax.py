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
    SELECT = "SCAN",
    JOIN = "JOIN",
    ORDER = "SORT",
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


class Parser:

    __default_syntax = "|>"


    @staticmethod
    def parse_query(query_list: list) -> list:
        '''
        Parse Query parses the sanitized dictionary of queries and returns the pipe syntax
        :param query_list:
        :return:
        '''
        order = []
        for query in query_list:
            order.append(Parser.sanitize_query(query))
        return order

    @staticmethod
    def sanitize_query(query_dict: dict) -> str:
        '''
        Sanitize the query and force the enumeration into the respective query type
        :param query_dict: Dictionary which contains the variables and dictionary
        :return: A string which output the parsed statement
        '''
        # Retrieve the key
        query_key = next(iter(query_dict))
        query = QueryType(query_key)
        query_params = query_dict.get(query_key)
        pipe_syntax = ""
        match query:
            case QueryType.SELECT:
                pipe_syntax = Parser.__parse_select_statement(query_params)
            case QueryType.JOIN:
                pipe_syntax = Parser.__parse_join_statement(query_params)
            case QueryType.ORDER:
                pipe_syntax = Parser.__parse_order_statement(query_params)
            case QueryType.LIMIT:
                pipe_syntax = Parser.__parse_limit_statement(query_params)
            case QueryType.AGGREGATE:
                pipe_syntax = Parser.__parse_aggregate_statement(query_params)
        return pipe_syntax

    @classmethod
    def __parse_select_statement(cls, query_params) -> str:
        return Parser.__default_syntax

    @classmethod
    def __parse_join_statement(cls, query_params: dict) -> str:
        return Parser.__default_syntax

    @classmethod
    def __parse_order_statement(cls, query_params: dict) -> str:
        return Parser.__default_syntax

    @classmethod
    def __parse_limit_statement(cls, query_params: dict) -> str:
        return Parser.__default_syntax

    @classmethod
    def __parse_aggregate_statement(cls, query_params: dict) -> str:
        return Parser.__default_syntax
