import copy
import queue
import sys
from time import sleep
import re

import psycopg2
import sqlglot
from psycopg2._psycopg import QueryCanceledError
from psycopg2.errors import UndefinedTable, UndefinedColumn
from sqlglot import Expression

from pipesyntax import QueryType, Aggregate, Parser, Operator

class DBConnection:
    """
    .. _db_label:

    Database Object used to connect to Postgres and execute the queries.

    """
    _DEFAULT_DBNAME = "TPC-H"
    _DEFAULT_USERNAME = "postgres"
    _DEFAULT_PASSWORD = "password"
    _DEFAULT_PORT = 5432
    _DEFAULT_TIMEOUT = 5000

    def __init__(self, dbname: str = _DEFAULT_DBNAME, username: str = _DEFAULT_USERNAME,
                 password: str = _DEFAULT_PASSWORD, port: int = _DEFAULT_PORT, options: int = _DEFAULT_TIMEOUT):
        self._dbname = dbname
        self._username = username
        self._password = password
        self._port = port
        self._options = options
        self._conn = self._connect(self._dbname, self._username, self._password, self._port, self._options)

    def __del__(self):
        self.close()

    def _connect(self, dbname: str, username: str, password: str, port: int, options: int):
        """
        Private method which connects to postgres SQL

        Notes:
        Does not retry and will throw exception if the database does not connect
        Initialized when creating DBConnection object

        :param dbname: Name of the database connected (Not the schema)
        :param username: Name of the username which is used to log in into Postgres
        :param password: Password which is used to log in into Postgres
        :param port: Port number which is used to host the database
        :param options: The option for timeout
        :return: A Connect object ..function:: psycopg2.connect()
        Example

        >>> db = DBConnection(dbname="TPC-H", username="postgres", password="SECRET", port=5432, options=5000)

        """
        conn = psycopg2.connect(dbname=dbname, user=username, password=password, port=port,
                                options=f"-c statement_timeout={options}")
        return conn

    def execute(self, query, times=3):
        """
        Executes the query of the SQL into Postgres
        :param query: The SQL query which is used to execute
        :param times: Number of times to execute the query when the query fails. The default is 3 times
        """
        while times > 0:
            try:
                with self._conn.cursor() as cur:
                    cur.execute(query)
                    return cur.fetchall()
            except QueryCanceledError:
                error = QueryCanceledError("Invalid SQL. Please ensure that the SQL is valid.")
                print("Unable to execute in time. Retrying execution...")
                self._conn.rollback()
                sleep(10)
                times -= 1
            except (UndefinedTable, UndefinedColumn):
                error = sqlglot.TokenError("Missing SQL statement")
                print("Error in executing statement. Retrying execution...")
                self._conn.rollback()
                sleep(10)
                times -= 1
        print("Please ensure that the query is a valid!")
        raise error

    def close(self):
        """
        Closes the connection to the database
        """
        self._conn.close()


class QEP:
    """
    # Explanation of the variables
    # ============================
    # Hash Cond = The condition which is used for join condition (Only in Aggregated)
    # Merge Cond = The condition which is used for join condition
    # Partial Mode = Condition used to determine whether aggregate statement is partial (might require duplication, or finalized)
    # Filter = Where Statement that evaluates condition that returns multiple statement
    # Relation Name = Name of the table used
    # Index Name = Name of index
    # Index Cond = Where Statement which only evaluates boolean condition
    # Scan Direction = Direction of the select query. (Is used only when 1 column is sorted)
    # Join Filter = Join condition when the condition is RIGHT JOIN
    # Join Type = Direction of the Join (Left, Right, Full)
    # Actual Total Time = Execution Time for each statement
    """
    _conditions = ["Hash Cond", "Merge Cond", "Partial Mode", "Filter", "Relation Name", "Index Name", "Index Cond",
                   "Scan Direction",
                   "Join Filter", "Join Type", "Actual Total Time"]

    @classmethod
    def unwrap(cls, query: str, db: DBConnection) -> tuple[list, float]:
        """
        .. _unwrap_label:

        Unwraps the Query Execution Plan from the SQL

        Note:
        Does reshape and flatten the JSON into a list

        :param query: A string which is used to represent SQL query
        :param db: The database object which is used to connect to the Postgres database :ref: `db_label`
        :return: Returns a tuple which contains the query list and the total execution time as a float

        Example
        >>> (qep_example, example_execution_time) = QEP.unwrap("SELECT * from customer LIMIT 100", db)

        >>> print(qep_example)
        [
            { LIMIT: { "Plan Row": 100, "Actual Total Time": 0.55 }
            },
            { FROM: {"Index Name": "cust_pkey", "Relation Name": "customer",
                      "Scan Direction": "Forward", "Actual Total Time": 1.205}
            }
        ]
        >>> print(example_execution_time)
        1.755

        Coupled Functions:
        - __unwrap_QEP(..) :ref: `unwrap_internal_label`
        - __clean_and_replace_variables(..) :ref: `clean_label`
        - __inject_queries(..) :ref: `inject_label`
        - __inject_where_condition(..) :ref: `inject_where_label`
        - __deduplicate(..) :ref: `deduplicate_label`

        """
        parsed_query = validate_query(query)
        alias = cls.__retrieve_alias(parsed_query)
        query_plan_json = get_qep(query, db)
        query_plan_json = flatten(query_plan_json)
        execution_time = query_plan_json[0]["Execution Time"]
        query_list = cls.__unwrap_QEP(query_plan_json[0])
        # This section modifies the QEP dictionary object
        # ====================================
        # Inject the column alias from the most recent query line
        cls.__clean_and_replace_variables(query_list, alias)
        # Inject select into aggregate if any and aggregation
        cls.__inject_queries(query, query_list, alias)
        # Inject set if any update
        cls.__inject_set_statement(query, query_list, alias)
        # Inject where condition
        cls.__inject_where_condition(query_list)
        return query_list, execution_time

    @classmethod
    def __unwrap_QEP(cls, query_plan: dict) -> list[dict]:
        """
        .. ref::_unwrap_internal_label:


        An internal method used to disassemble the QEP and retrieve the relevant fields from _conditions

        Notes:
        Reshape the QEP into an array of dictionary
        Simulates a JSON dictionary with nested dictionary within a dictionary

        :param query_plan: The JSON dictionary which contains the nested statements
        :return: List of nested dictionary. View example to see the structure of the output.

        Example

        >>> print(cls.__unwrap_QEP(
        >>> {"Plan": {"Actual Total Time": 1.275, "Plans":
        >>>     [{"Node Type": "Limit", "Actual Total Time": 0.55, "Plan Row": 100, "Plans":
        >>>         [{"Node Type": "Seq Index Scan", "Actual Total Time": 1}]}]}})

        [
            { LIMIT: { "Plan Row": 100, "Actual Total Time": 0.55 }
            },
            { FROM: {"Index Name": "cust_pkey", "Relation Name": "customer",
                      "Scan Direction": "Forward", "Actual Total Time": 1.205}
            }
        ]

        Query Execution Plan

        .. code-block:: text

            LIMIT
            ├── AGGREGATE <- Finalized
            │   ├── SORT
            │   ├── AGGREGATE <-- Partial
            │   │   ├── JOIN
            │   │   │   ├── FROM
            │   │   │   └── FROM

        Expected Output:

        .. code-block:: text

            LIMIT
            ├── AGGREGATE
            ├── SELECT <-- Inserted
            │   ├── SORT
            │   │   ├── JOIN
            │   │   │   ├── FROM
            │   │   │   └── FROM
        """

        query_plan = query_plan["Plan"]
        query_list = []
        plan_queue = queue.Queue()
        plan_queue.put(query_plan)
        while not plan_queue.empty():
            plans = plan_queue.get()
            try:
                query = QueryType(plans["Node Type"])
            except ValueError:
                # Skip non-related Queries
                query = None
            variable_queries = dict()
            for query_key in filter(lambda key: "Key" in key, plans):
                variable_queries.update({query_key: plans.get(query_key)})
            if "Plans" in plans:
                for plan in plans["Plans"]:
                    plan_queue.put(plan)
            for query_key in filter(lambda key: key in cls._conditions, plans):
                variable_queries.update({query_key: plans.get(query_key)})
            # Only specific for limit to retrieve the condition (Plan rows)
            # Retrieve Limit Rows
            if query is not None and query is QueryType.LIMIT:
                variable_queries.update({"Plan Rows": plans.get("Plan Rows", None)})
            if query is not None and query is QueryType.AGGREGATE:
                # Skips Partial Aggregation
                if plans.get("Partial Mode") == "Partial":
                    continue
            if query is not None:
                query_list.append({query: variable_queries})
        return query_list

    @classmethod
    def __retrieve_alias(cls, query: Expression) -> dict:
        """
        Parse the SQL Query and retrieve the relevant column alias with the attribute

        :param query: SQL query
        :return: A dictionary which maps the attribute to its alias

        Example
            >>> retrieve = cls.__retrieve_alias(query="SELECT c_count, count(*) AS custdist FROM"
            >>>                                      "(SELECT c_custkey, count(o_orderkey) FROM customer"
            >>>                                      "LEFT OUTER JOIN orders ON c_custkey = o_custkey"
            >>>                                      "AND o_comment not like '%unusual%packages%' "
            >>>                                      "GROUP BY c_custkey ) as c_orders (c_custkey, c_count) "
            >>>                                      "GROUP BY c_count "
            >>>                                      "ORDER BY custdist DESC, c_count DESC;")
            >>> print(retrieve)
            {"c_count": "count(o_orderkey)", "custdist": "count(*)", "c_custkey": "c_custkey" }

        """

        # Sanitize the attribute and the alias
        # will keep the column name if alias is empty
        aliases = {str(i.alias).lower() if i.alias != "" else str(i).split()[0].lower(): str(i).lower() if re.search(r'over\s*\(', str(i).lower()) else str(i).split()[0].lower()
                for i in query.expressions}
        
        subquery_alias = cls.__retrieve_subquery_alias(query)
        aliases.update(subquery_alias)
        return aliases

    @classmethod
    def __clean_and_replace_variables(cls, query_list: list[dict], aliases: dict) -> None:
        """
        .. ref::clean_label:

        Combination of removing tables from columns and adding column alias into the variable

        Notes:

        This method updates the object in the parameters (Not a good practice)

        :param query_list: List of query which simulates a JSON list
        :param aliases: A dictionary of items which contains the alias (key) and the column name (value)

        Example
        >>> cls.__clean_and_replace_variables(query_list=[{"JOIN":{...}}, {"FROM": {...}}, {"FROM": {...}}],
        >>>                                 aliases={"c_count": "count(o_orderkey)", "custdist": "count(*)", "c_custkey": "c_custkey" })

        """

        for query in query_list:
            # Deconstructing the nested dictionary
            query_variables = next(iter(query.values()))
            for variable in list(filter(lambda items: "Key" in items, query_variables.keys())):
                variable_list = []
                for variable_string in query_variables.get(variable):
                    temp_values = cls.__remove_tables_from_variables(variable_string)
                    temp_values = cls.__add_table_alias(aliases, temp_values)
                    variable_list.append(temp_values)

                temp_string = ",".join(variable_list)
                query_variables.update({variable: temp_string})
            if query_variables.get("Filter", None) is not None:
                temp_values = cls.__convert_operation_and_clean_variables(query_variables.get("Filter"))
                query_variables.update({"Filter": temp_values})

    @classmethod
    def __add_table_alias(cls, aliases: dict, variable_string: str) -> str:
        """
        .. ref::_add_table_label:

        Add Alias into the respective column name based on the aliases

        :param aliases: A dictionary of items which contains the alias (key) and the column name (value)
        :param variable_string: The variable name that is obtained from QEP
        :return: The string which is replaced by the alias or the original value

        Example:
        >>> print(cls.__add_table_alias(aliases={"custdist": "count(*)"}, variable_string="count(*)"))
        custdist
        """
        temp_var = next((items for items in aliases.items() if items[1] in variable_string), None)
        return variable_string.replace(temp_var[1], temp_var[0]) if temp_var is not None else variable_string

    @classmethod
    def __remove_tables_from_variables(cls, variable_string: str) -> str:
        """
        .. ref:_remove_table_label:

        Removes the additional tables from the column names

        Notes:
        This function is simplistic in its implementation.
        It does not check whether it is necessary to remove the table
        It checks for 2 conditions:
         - Whether a full stop is found
         - Whether a bracket is found before the full stop
        If it satisfies the two conditions:
            - Remove any substring from ( and .
        else If it satisfies the . condition
            - Remove any substring before .
        else
            - Retains the original value

        :param variable_string: Value which is used to check whether the value contains any of the above condition
        :return: A string value

        Example
        >>> print(cls.__remove_tables_from_variables("customer.c_custkey"))
        c_custkey
        >>> print(cls.__remove_tables_from_variables("count(customer.c_acct_bal)"))
        count(c_acct_bal)

        """
        full_stop_index = variable_string.find(".")
        bracket_index = variable_string.rfind("(", 0, full_stop_index)
        # TODO
        # Janky solution to finding and removing table from columns
        # Assumed that full stop (.) is the main separator
        if bracket_index < full_stop_index and bracket_index != -1:
            temp_values = variable_string[:bracket_index] + "(" + variable_string[full_stop_index + 1:]
        elif bracket_index == -1:
            temp_values = variable_string[full_stop_index + 1:]
        else:
            temp_values = variable_string
        return temp_values

    @classmethod
    def __convert_operation_and_clean_variables(cls, variable_string: str) -> str:
        """

        Converts the operation into the Enumeration and remove any type fo variable

        :param variable_string: The string type of the variable which needs to be converted
        :return: A string output of the sanitized query
        """
        result_string = re.sub(r'::[^) ]*[) ]',  '', variable_string)
        return result_string

    @classmethod
    def __inject_queries(cls, query: str, query_list: list, alias: dict):
        """
        Inject AS into AGGREGATE function and INJECT SELECT statement

        Notes:
        This injection is sensitive to the query and will place

        :param query: The SQL query that is executed
        :param query_list: An arraylist of Query Execution Plan
        :param alias: A dictionary of that contains column name alias and column names

        """
        parsed = sqlglot.parse_one(query)
        temp_alias = copy.deepcopy(alias)
        subquery_alias = cls.__retrieve_subquery_alias(parsed)
        filtered_subquery = {k: v for k, v in subquery_alias.items() if k == v}
        remove_key_list = alias.keys() & filtered_subquery.keys()
        for remove_key in remove_key_list:
            temp_alias.pop(remove_key, None)
        alias_list = list((k, v) for k, v in temp_alias.items() if v in Aggregate)
        alias_list.reverse()
        aggregate_list = list(filter(lambda item: QueryType.AGGREGATE in item, query_list))
        select_list = [v for k, v in temp_alias.items() if v not in Aggregate]
        if aggregate_list:
            for i in range(len(aggregate_list)):
                if i < len(alias_list):
                    if alias_list[i][0] != alias_list[i][1]:
                        aggregate_list[i][QueryType.AGGREGATE].update(
                            {"Index Name": f"{alias_list[i][1]} AS {alias_list[i][0]}"})
                    else:
                        aggregate_list[i][QueryType.AGGREGATE].update({"Index Name": f"{alias_list[i][1]}"})
        # TODO
        # ===============================================
        # This solution might not be optimal as it will always insert the select statement next to the FROM/JOIN statement
        # This might not be true if extend if inserted
        if select_list:
            query_list.insert(0, {QueryType.SELECT: {"Index Name": f"{','.join(select_list)}"}})

    @classmethod
    def __retrieve_subquery_alias(cls, parsed: Expression) -> dict:
        """

        Retrieve all subqueries from the query

        Note:
        Have not tested out with a nested subquery

        :param parsed: An SQL Query which is parsed into sqlglot library and transformed into Expression object
        :return: A dictionary which contains all the subqueries
        """
        # Retrieve all alias from the subquery
        # TODO
        # Will have to check whether alias is declared outside of subquery or within subquery
        # Probably can ignore internal usage of column alias (Assumption)
        sub_query_alias = dict()
        for subquery in parsed.find_all(sqlglot.exp.Subquery):
            # Alias declared externally
            alias_column_names = tuple(subquery.alias_column_names)
            # Get all the columns from the select
            key_column_names = tuple(str(s).lower() for s in subquery.selects)
            # Will ignore all keys that does not have an alias
            sub_query_alias.update(dict(zip(alias_column_names, key_column_names)))
        return sub_query_alias

    @classmethod
    def __inject_where_condition(cls, query_list: list):
        """
        Injects Where condition into the query list.

        Notes:
        This func should always be inserted into the query list after the insertion of SELECT statement.
        This statment will always be at the second position
        It will ensure that the order of pipe syntax remains the same
        where the row will be filtered before retrieving from the columns
        :param query_list: List of query
        """
        from_range_filter = list(i for i, item in enumerate(query_list) if item.get(QueryType.FROM, None) is not None and item.get(QueryType.FROM).get("Filter", None) is not None)
        for index in reversed(from_range_filter):
            where_condition = query_list[index][QueryType.FROM].get("Filter", None)
            if where_condition is not None:
                query_list.insert(index, {QueryType.WHERE: {"Index Name": where_condition}})
        
        from_range_index = list(i for i, item in enumerate(query_list) if item.get(QueryType.FROM, None) is not None and item.get(QueryType.FROM).get("Index Cond", None) is not None)
        for index in reversed(from_range_index):
            where_condition = query_list[index][QueryType.FROM].get("Index Cond", None)
            if where_condition is not None:
                query_list.insert(index, {QueryType.WHERE: {"Index Name": where_condition}})
            
    @classmethod   
    def __inject_set_statement(cls, query: str, query_list: list, alias: dict):
        """
        Injects SET statememt where UPDATE exists
        """
        update_index = next((i for i, query in enumerate(query_list) if QueryType.UPDATE in query), -1)
        
        if update_index != -1:
            
            match = re.search(r'\bSET\b\s+(.*?)(?=\bFROM\b|\bWHERE\b|;|$)', query, re.IGNORECASE | re.DOTALL)
            set_statement = match.group(1).strip()
            
            if set_statement is not None:
                for a in alias:
                    set_statement = re.sub(rf'\b{re.escape(a)}\b', a.lower(), set_statement, flags=re.IGNORECASE)
                query_list.insert(update_index, {QueryType.SET: {"Set Statement": set_statement}})

                if update_index - 1 >= 0:
                    if QueryType.SELECT in query_list[update_index - 1]:
                        query_list.pop(update_index - 1)
                        

def flatten(nested_list: list) -> list:
    return [item for sublist in nested_list for item in sublist[0]]


def get_qep(query: str, db: DBConnection):
    qep_query = "EXPLAIN (ANALYZE, FORMAT JSON)" + query
    return db.execute(qep_query)


def validate_query(query: str):
    try:
        transpiled = \
        sqlglot.transpile(query, write="postgres", read="postgres", pretty=True, error_level=sqlglot.ErrorLevel.RAISE)[
            0]
        parsed = sqlglot.parse_one(transpiled)
        return parsed
    except sqlglot.ParseError:
        error_output = "Error in parsing SQL. Please ensure that the query is valid!"
        raise sqlglot.ParseError(error_output)


def get_system_args():
    '''
    Retrieve variables from command line
    Note:

    Require precise number of variables (4)

    :return: None when the variables do not match the number of variables

    Example

    >>> print(get_system_args())
    ("TPC-H", "username", "password", "5124", 1000)


    '''
    print("Retrieving variables from command line")
    if len(sys.argv) != 5:
        print("Unable to retrieve any values from command line. Retrieving default settings.")
        return None
    else:
        return sys.argv[0], sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])


def compare_str(s1: any, s2: any) -> bool:
    return str(s1).lower() == str(s2).lower()


def example():
    sys_arg = get_system_args()
    db = DBConnection() if sys_arg is None else DBConnection(sys_arg[0], sys_arg[1], sys_arg[2], int(sys_arg[3]))

    # Standard SQL
    #query = "SELECT c_count, count(*) AS custdist FROM (SELECT c_custkey, count(o_orderkey) FROM customer INNER JOIN orders ON c_custkey = o_custkey AND o_comment not like '%unusual%packages%' GROUP BY c_custkey) as c_orders (c_custkey, c_count) GROUP BY c_count ORDER BY custdist DESC, c_count DESC;"
    # Simple SQL
    #query = "SELECT c_custkey, sum(c_acctbal) FROM customer where c_acctbal > 100 GROUP BY c_custkey, c_acctbal ORDER BY c_acctbal LIMIT 100"
    # Unoptimized SQL
    #query = "SELECT c_custkey, SUM(c_acctbal) as total_bal FROM customer LEFT JOIN orders on customer.c_custkey = orders.o_custkey WHERE c_acctbal > 100 GROUP BY c_custkey ORDER BY c_custkey LIMIT 100"
    # Invalid SQL
    # query = "SELECT c_count, count(*) AS custdist FROM (SELECT c_custkey, count(o_orderkey) FROM customer INNER JOIN orders ON c_custkey = o_custkey OR o_comment not like '%unusual%packages%' GROUP BY c_custkey) as c_orders (c_custkey, c_count) GROUP BY c_count ORDER BY custdist DESC, c_count DESC;"
    # Broken SQL
    # query = "SELECT * from t"
    # Duplicate Keys
    #query = "SELECT customer.c_custkey, orders.o_custkey as c_custkey FROM customer LEFT JOIN orders ON orders.o_custkey = customer.c_custkey WHERE c_acctbal > 100  GROUP BY orders.o_custkey, customer.c_custkey ORDER BY customer.c_custkey"
    
    
    #query = "SELECT c_custkey FROM customer LEFT JOIN orders on customer.c_custkey = orders.o_custkey WHERE c_acctbal > 100 and o_totalprice > 100"
    # Aggregate + HAVING
    #query="""SELECT o_custkey, COUNT(o_orderkey) AS order_count, AVG(o_totalprice) AS avg_order_price, MIN(o_orderdate) AS first_order_date, MAX(o_orderdate) AS last_order_date FROM orders WHERE o_orderdate >= '1995-01-01' AND o_orderpriority LIKE '%5%' GROUP BY o_custkey HAVING COUNT(o_orderkey) >= 3 ORDER BY avg_order_price DESC LIMIT 20;"""
    # Window function
    query = """
    SELECT c.c_name, o.o_orderkey, o.o_orderdate, o.o_totalprice, RANK() OVER (PARTITION BY c.c_custkey ORDER BY o.o_totalprice DESC) AS price_rank FROM public.customer c JOIN public.orders o ON c.c_custkey = o.o_custkey WHERE o.o_orderdate BETWEEN '1995-01-01' AND '1995-12-31' ORDER BY c.c_name,price_rank;"""

    # UPDATE SQL statements
    #query = "UPDATE customer SET c_comment = 'Preferred', c_acctbal = c_acctbal * 1.1 WHERE c_mktsegment = 'FURNITURE';"

    (qep_list, execution_time) = QEP.unwrap(query, db)
    print(Parser.parse_query(qep_list))


# Remove the comment below the run the code
example()
