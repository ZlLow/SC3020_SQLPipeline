import copy
import queue
import sys
import psycopg2
import sqlglot
from time import sleep

from sqlglot import Expression

from pipesyntax import QueryType, Parser, Aggregate, Operator


class DBConnection:
    """
    .. _db_label:

    Database Object used to connect to Postgres and execute the queries.

    """
    _DEFAULT_DBNAME = "TPC-H"
    _DEFAULT_USERNAME = "postgres"
    _DEFAULT_PASSWORD = "password"
    _DEFAULT_PORT = 5432

    def __init__(self, dbname: str = _DEFAULT_DBNAME, username: str = _DEFAULT_USERNAME,
                 password: str = _DEFAULT_PASSWORD, port: int = _DEFAULT_PORT):
        self._dbname = dbname
        self._username = username
        self._password = password
        self._port = port
        self._conn = self._connect(self._dbname, self._username, self._password, self._port)

    def __del__(self):
        self.close()

    def _connect(self, dbname: str, username: str, password: str, port: int):
        """
        Private method which connects to postgres SQL

        Notes:
        Does not retry and will throw exception if the database does not connect
        Initialized when creating DBConnection object

        :param dbname: Name of the database connected (Not the schema)
        :param username: Name of the username which is used to login into Postgres
        :param password: Password which is used to login into Postgres
        :param port: Port number which is used to host the database
        :return: A Connect object ..function:: psycopg2.connect()
        Example

        >>> db = DBConnection(dbname="TPC-H", username="postgres", password="SECRET", port=5432)

        """
        conn = psycopg2.connect(dbname=dbname, user=username, password=password, port=port)
        return conn

    def execute(self, query, times=3):
        """
        Executes the query of the SQL into Postgres
        :param query: The SQL query which is used to execute
        :param times: Number of times to execute the query when the query fails. The default is 3 times
        """
        i = 0
        while i < times:
            try:
                with self._conn.cursor() as cur:
                    cur.execute(query)
                    return cur.fetchall()
            except:
                print("Unable to execute")
                sleep(10)
                times += 1

    def close(self):
        """
        Closes the connection to the database
        """
        self._conn.close()


class QEP:
    """
    # Explanation of the variables
    # ============================
    # Hash Cond = The condition which is used for join condition
    # Filter = Where Statement that evaluates condition that returns multiple statement
    # Relation Name = Name of the table used
    # Index Name = Name of index
    # Index Cond = Where Statement which only evaluates boolean condition
    # Scan Direction = Direction of the select query. (Is used only when 1 column is sorted)
    # Join Filter = Join condition when the condition is RIGHT JOIN
    # Join Type = Direction of the Join (Left, Right, Full)
    # Actual Total Time = Execution Time for each statement
    """
    _conditions = ["Hash Cond", "Filter", "Relation Name", "Index Name", "Index Cond", "Scan Direction",
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
        - _unwrap_QEP(..) :ref: `unwrap_internal_label`
        - _merge_queries(..) :ref: `merge_label`
        - _clean_and_replace_variables(..) :ref: `clean_label`
        - _inject_queries(..) :ref: `inject_label`

        """

        query = sqlglot.transpile(query, write="postgres", read="postgres", pretty=True)[0]
        parsed_query = sqlglot.parse_one(query)
        alias = cls._retrieve_alias(parsed_query)
        qep_query = "EXPLAIN (ANALYZE, FORMAT JSON)" + query
        query_plan_json = db.execute(qep_query)
        execution_time = query_plan_json[0][0][0]["Execution Time"]
        query_list = cls._unwrap_QEP(query_plan_json[0][0][0])
        # This section modifies the QEP dictionary object
        # ====================================
        # Merge Join queries with Select
        cls._merge_queries(query_list)
        # Inject the column alias from the most recent query line
        cls._clean_and_replace_variables(query_list, alias)
        # Inject select into aggregate if any and aggregation
        cls._inject_queries(query, query_list, alias)
        return query_list, execution_time

    @classmethod
    def _unwrap_QEP(cls, query_plan: dict) -> list[dict]:
        """
        .. ref::_unwrap_internal_label:


        An internal method used to disassemble the QEP and retrieve the relevant fields from _conditions

        Notes:
        Reshape the QEP into an array of dictionary
        Simulates a JSON dictionary with nested dictionary within a dictionary

        :param query_plan: The JSON dictionary which contains the nested statements
        :return: List of nested dictionary. View example to see the structure of the output.

        Example

        >>> print(cls._unwrap_QEP(
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

        """

        query_plan = query_plan["Plan"]
        query_list = []
        plan_queue = queue.Queue()
        plan_queue.put(query_plan)
        while not plan_queue.empty():
            plans = plan_queue.get()
            try:
                query = QueryType(plans["Node Type"])
            except:
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
            if query is not None:
                query_list.append({query: variable_queries})
        return query_list

    @classmethod
    def _merge_queries(cls, query_list: list[dict]) -> None:
        """
        .. ref::merge_label:

        Merges the query of any JOIN condition with the closest FROM statement

        Notes:

        A simple implementation that checks whether there is a JOIN statement.
        It will merge with the next statement (Does not check for the closest FROM statement)
        Lazily assume that the next statemnet is a FROM statement

        :param query_list:
        :return:

        Example
        >>> cls._merge_queries(query_list=[{"JOIN":{...}}, {"FROM": {...}}, {"FROM": {...}}])

        TODO:
        should check for the closest FROM statement. Will implement ltr
        Might case Error Statement in two scenarios
            - When JOIN statement is the last statement (i.e.: [LIMIT, SORT, JOIN])
            - When next statement is not a FROM statement (i.e [JOIN, LIMIT])

        """

        remove_join_list = []
        for i in range(len(query_list)):
            query = query_list[i].get(QueryType.JOIN, None)
            if query is not None:
                if i + 1 <= len(query_list):
                    remove_join_list.append(i + 1)
                    query_select = list(query_list[i + 1].values())[0]
                    query["Actual Total Time"] = query["Actual Total Time"] + query_select["Actual Total Time"]
                    query.update(query_select)
        for i in remove_join_list:
            query_list.pop(i)

    @classmethod
    def _retrieve_alias(cls, query: Expression) -> dict:
        """
        Parse the SQL Query and retrieve the relevant column alias with the attribute

        :param query: SQL query
        :return: A dictionary which maps the attribute to its alias

        Example
            >>> retrieve = cls._retrieve_alias(query="SELECT c_count, count(*) AS custdist FROM"
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
        aliases = {str(i.alias).lower() if i.alias != "" else str(i).split()[0].lower(): str(i).split()[0].lower()
                   for i in query.expressions}
        subquery_alias = cls._retrieve_subquery_alias(query)
        aliases.update(subquery_alias)
        return aliases

    @classmethod
    def _clean_and_replace_variables(cls, query_list: list[dict], aliases: dict) -> None:
        """
        .. ref::clean_label:

        Combination of removing tables from columns and adding column alias into the variable

        Notes:

        This method updates the object in the parameters (Not a good practice)

        :param query_list: List of query which simulates a JSON list
        :param aliases: A dictionary of items which contains the alias (key) and the column name (value)

        Example
        >>> cls._clean_and_replace_variables(query_list=[{"JOIN":{...}}, {"FROM": {...}}, {"FROM": {...}}],
        >>>                                 aliases={"c_count": "count(o_orderkey)", "custdist": "count(*)", "c_custkey": "c_custkey" })

        """

        for query in query_list:
            # Deconstructing the nested dictionary
            query_variables = next(iter(query.values()))
            for variable in list(filter(lambda items: "Key" in items, query_variables.keys())):
                variable_list = []
                for variable_string in query_variables.get(variable):
                    temp_values = cls._remove_tables_from_variables(variable_string)
                    temp_values = cls._add_table_alias(aliases, temp_values)
                    variable_list.append(temp_values)

                temp_string = ",".join(variable_list)
                query_variables.update({variable: temp_string})
            if query_variables.get("Filter", None) is not None:
                temp_values = cls._convert_operation_and_clean_variables(query_variables.get("Filter"))
                query_variables.update({"Filter": temp_values})


    @classmethod
    def _add_table_alias(cls, aliases: dict, variable_string: str) -> str:
        """
        .. ref::_add_table_label:

        Add Alias into the respective column name based on the aliases

        :param aliases: A dictionary of items which contains the alias (key) and the column name (value)
        :param variable_string: The variable name that is obtained from QEP
        :return: The string which is replaced by the alias or the original value

        Example:
        >>> print(cls._add_table_alias(aliases={"custdist": "count(*)"}, variable_string="count(*)"))
        custdist
        """
        temp_var = next((items for items in aliases.items() if items[1] in variable_string), None)
        return variable_string.replace(temp_var[1], temp_var[0]) if temp_var is not None else variable_string

    @classmethod
    def _remove_tables_from_variables(cls, variable_string: str) -> str:
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

        """
        full_stop_index = variable_string.find(".")
        bracket_index = variable_string.rfind("(", 0, full_stop_index)
        # TODO
        # Janky solution to finding and removing table from columns
        if bracket_index < full_stop_index and bracket_index != -1:
            temp_values = variable_string[:bracket_index] + "(" + variable_string[full_stop_index + 1:]
        elif bracket_index == -1:
            temp_values = variable_string[full_stop_index + 1:]
        else:
            temp_values = variable_string
        return temp_values

    @classmethod
    def _convert_operation_and_clean_variables(cls, variable_string: str) -> str:
        """

        Converts the operation into the Enumeration and remove any type fo variable

        :param variable_string: The string type of the variable which needs to be converted
        :return: A string output of the sanitized query
        """
        string_list = [string for string in variable_string.split(" ")]
        result_string = []
        for string in string_list:
            if string.find("::") != -1:
                result_string.append(string.split("::")[0])
            elif string in Operator:
                result_string.append(Operator.to_string(string))
        return " ".join(result_string)

    @classmethod
    def _inject_queries(cls, query: str, query_list: list, alias: dict):
        """

        Inject AS into AGGREGATE function and INJECT SELECT statement

        :param query: The SQL query that is executed
        :param query_list: An arraylist of Query Execution Plan
        :param alias: A dictionary of that contains column name alias and column names

        TODO
        """
        parsed = sqlglot.parse_one(query)
        temp_alias = copy.deepcopy(alias)
        subquery_alias = cls._retrieve_subquery_alias(parsed)
        filtered_subquery = {k: v for k, v in subquery_alias.items() if k == v}
        remove_key_list = alias.keys() & filtered_subquery.keys()
        for remove_key in remove_key_list:
            temp_alias.pop(remove_key, None)
        alias_list = list((k, v) for k, v in temp_alias.items() if v in Aggregate)
        alias_list.reverse()
        aggregate_list = list(filter(lambda item: QueryType.AGGREGATE in item, query_list))
        select_list = [k for k, v in temp_alias.items() if v not in Aggregate or v == "*"]
        if aggregate_list:
            for i in range(len(aggregate_list)):
                if i > len(alias_list):
                    break
                aggregate_list[i][QueryType.AGGREGATE]["Index Name"] = f"{alias_list[i][1]} AS {alias_list[i][0]}"
        if select_list:
            query_list.insert(-1, {QueryType.SELECT:
                                       {"Index Names": ",".join([k for k, v in temp_alias.items() if v not in Aggregate])}})

    @classmethod
    def _retrieve_subquery_alias(cls, parsed: Expression) -> dict:
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


def get_system_args():
    '''
    Retrieve variables from command line
    Note:

    Require precise number of variables (4)

    :return: None when the variables do not match the number of variables

    Example

    >>> print(get_system_args())
    ("TPC-H", "username", "password", "5124")


    '''
    print("Retrieving variables from command line")
    if len(sys.argv) != 4:
        print("Unable to retrieve any values from command line. Retrieving default settings.")
        return None
    else:
        return sys.argv[0], sys.argv[1], sys.argv[2], sys.argv[3]


def example():
    sys_arg = get_system_args()
    db = DBConnection() if sys_arg is None else DBConnection(sys_arg[0], sys_arg[1], sys_arg[2], int(sys_arg[3]))
    (qep_list, execution_time) = QEP.unwrap(
        "SELECT c_count, count(*) AS custdist FROM (SELECT c_custkey, count(o_orderkey) FROM customer "
        "INNER JOIN orders ON c_custkey = o_custkey AND o_comment not like '%unusual%packages%' "
        "GROUP BY c_custkey ) as c_orders (c_custkey, c_count) "
        "GROUP BY c_count "
        "ORDER BY custdist DESC, c_count DESC;"
        , db)
    # (qep_list, execution_time) = QEP.unwrap(
    #     "SELECT c_custkey FROM customer "
    #     "WHERE c_acctbal > 100 OR c_custkey = 1 "
    #     "ORDER BY c_custkey DESC, c_acctbal "
    #     "LIMIT 100;", db)
    print(qep_list)
    print(execution_time)
    print(Parser.parse_query(qep_list))


# Remove the comment below the run the code
example()
