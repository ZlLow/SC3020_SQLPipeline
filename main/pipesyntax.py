import enum
# -------------------------------------------
#                 Enumeration
# -------------------------------------------


class EnumMeta(enum.EnumMeta):
    def __contains__(cls, item):
        """

        Check whether Values in Enum is a substring of item

        :param item: The value of string to be checked
        :return: Boolean condition whether the value exist in Enum
        """
        return isinstance(item, cls) or any(str(v.value).lower() in item for v in cls.__members__.values())


class Aggregate(enum.Enum, metaclass=EnumMeta):
    COUNT = "COUNT"
    SUM = "SUM"
    AVG = "AVG"
    MIN = "MIN"
    MAX = "MAX"
    FIRST = "FIRST"
    LAST = "LAST"
    MEDIAN = "MEDIAN"
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


class Operator(enum.Enum, metaclass=EnumMeta):
    EQUAL = "="
    NOTEQUAL = "!="
    LESS_THAN = "<"
    GREATER_THAN = ">"
    NOT_LIKE = "!~~"
    NOT = "<>"
    LESS_THAN_OR_EQUAL = "<="
    GREATER_THAN_OR_EQUAL = ">="
    AND = "AND"
    OR = "OR"

    def __str__(self):
        return self.name

    @classmethod
    def to_string(cls, value: str):
        return next(iter(list(k.replace("_", " ") for k, v in cls.__members__.items() if str(v.value) == value)), "None")


class QueryType(enum.Enum):
    SELECT = "SELECT"
    FROM = "SCAN"
    WHERE = "WHERE"
    JOIN = "JOIN"
    ORDER = "SORT"
    LIMIT = "LIMIT"
    AGGREGATE = "AGGREGATE"
    WINDOWAGG = "WINDOWAGG"
    UPDATE = "MODIFYTABLE"
    SET = "SET"

    def __str__(self):
        return self.name

    @classmethod
    def _missing_(cls, value: object):
        if isinstance(value, str):
            value = value.lower()
            for member in cls:
                if value.find(member.value.lower()) != -1:
                    return member
        return None


# -------------------------------------------
#      Parse Queries and helper methods
# -------------------------------------------


class Parser:
    __default_syntax = "|>"

    @staticmethod
    def parse_query(query_list: list):
        """"
        Parse Query parses the sanitized dictionary of queries and returns the pipe syntax
        :param query_list:
        :return: tuple(str,float)
        """

        order = []
        for qep in query_list:
            order.append(Parser.sanitize_query(qep))
        output = ""
        order.reverse()
        for o in order:
            output += o
        return output

    @staticmethod
    def sanitize_query(query_dict: dict) -> str:
        """
        Sanitize the query and force the enumeration into the respective query type
        :param query_dict: Dictionary which contains the variables and dictionary
        :return: A string which output the parsed statement
        """
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
            case QueryType.FROM:
                pipe_syntax = Parser.__parse_from_statement(query_params)
            case QueryType.WHERE:
                pipe_syntax = Parser.__parse_where_statement(query_params)
            case QueryType.ORDER:
                pipe_syntax = Parser.__parse_order_statement(query_params)
            case QueryType.LIMIT:
                pipe_syntax = Parser.__parse_limit_statement(query_params)
            case QueryType.AGGREGATE:
                pipe_syntax = Parser.__parse_aggregate_statement(query_params)
            case QueryType.WINDOWAGG:
                pipe_syntax = Parser.__parse_window_aggregate_statement(query_params)
            case QueryType.UPDATE:
                pipe_syntax = Parser.__parse_update_statement(query_params)
            case QueryType.SET:
                pipe_syntax = Parser.__parse_set_statement(query_params)
        return pipe_syntax

    @classmethod
    def __parse_select_statement(cls, query_params: dict) -> str:
        return f"{Parser.__default_syntax} SELECT {query_params['Index Name']} \n"

    @classmethod
    def __parse_from_statement(cls, query_params: dict) -> str:
        return f"{Parser.__default_syntax} FROM {query_params['Relation Name']} \n Total Time: {query_params['Actual Total Time']} \n"

    @classmethod
    def __parse_join_statement(cls, query_params: dict) -> str:
        condition = next(iter(map(query_params.get,filter(lambda item: "Cond" in item, query_params))),None)
        output = f"{Parser.__default_syntax} {query_params['Join Type']} JOIN ON {condition}"
        if query_params.get("Filter", None) is not None:
            output += f" AND {query_params['Filter']}"
        return output + f"\n Total Time: {query_params['Actual Total Time']} \n"

    @classmethod
    def __parse_where_statement(cls, query_params: dict) -> str:
        return f"{Parser.__default_syntax} WHERE {query_params['Index Name']} \n"

    @classmethod
    def __parse_order_statement(cls, query_params: dict) -> str:
        return f"{Parser.__default_syntax} ORDER BY {query_params['Sort Key']} \n Total Time: {query_params['Actual Total Time']} \n"

    @classmethod
    def __parse_limit_statement(cls, query_params: dict) -> str:
        return f"{Parser.__default_syntax} LIMIT {query_params['Plan Rows']} \n Total Time: {query_params['Actual Total Time']} \n"

    @classmethod
    def __parse_aggregate_statement(cls, query_params: dict) -> str:
        having_clause = ''
        if 'Filter' in query_params:
            having_clause = f"HAVING {query_params['Filter']}"

        out =  f"{Parser.__default_syntax} AGGREGATE {query_params['Index Name']} GROUP BY {query_params['Group Key']} {having_clause}\n Total Time: {query_params['Actual Total Time']} \n"
        return out 
    
    @classmethod
    def __parse_window_aggregate_statement(cls, query_params: dict) -> str:
        return f"{Parser.__default_syntax} WINDOWAGG \n Total Time: {query_params['Actual Total Time']} \n"
    
    @classmethod
    def __parse_update_statement(cls, query_params: dict) -> str:
        return f"{Parser.__default_syntax} UPDATE {query_params['Relation Name']} \n Total Time: {query_params['Actual Total Time']} \n"
    
    @classmethod
    def __parse_set_statement(cls, query_params: dict) -> str:
        return f"{Parser.__default_syntax} SET {query_params['Set Statement']} \n"