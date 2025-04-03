import queue
import sys
import psycopg2
import sqlglot
from time import sleep
from pipesyntax import QueryType, Parser


class DBConnection:
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

    def _connect(self, dbname: str, username: str, password: str, port: int):
        """
        Connects to Postgres SQL database
        :param dbname:
        :param username:
        :param password:
        :param port:
        :return:
        """
        conn = psycopg2.connect(dbname=dbname, user=username, password=password, port=port)
        return conn

    def execute(self, query, times=0):
        """

        :param query:
        :param times:
        :return:
        """
        while times < 3:
            try:
                with self._conn.cursor() as cur:
                    cur.execute(query)
                    return cur.fetchone()
            except:
                print("Unable to execute")
                sleep(10)
                times += 1

    def close(self):
        self._conn.close()


class QEP:
    """
    # Explanation of the variables
    # ============================
    # Hash Cond =
    # Filter = Where Statement that evaluates condition that returns multiple statement
    # Relation Name = Name of the table used
    # Index Name = Name of index
    # Index Cond = Where Statement which only evaluates boolean condition
    # Scan Direction = Direction for the
    # Join Filter = Join condition
    # Join Type = Direction of the Join (Left, Right, Full)
    """
    _conditions = ["Hash Cond", "Filter", "Relation Name", "Index Name", "Index Cond", "Scan Direction",
                   "Join Filter", "Join Type", "Actual Total Time"]

    @classmethod
    def get_QEP(cls, query: str, db: DBConnection) -> tuple[list, float]:
        """

        :param query:
        :param db:
        :param times:
        :return:
        """
        query = sqlglot.transpile(query, write="postgres", read="postgres", pretty=True)[0]
        alias = cls._retrieve_alias(query)
        qep_query = "EXPLAIN (ANALYZE, FORMAT JSON)" + query
        query_plan_json = db.execute(qep_query)
        execution_time = query_plan_json[0][0]["Execution Time"]
        query_list = cls._unwrap_QEP(query_plan_json[0][0])
        # Inject the column alias from the most recent query line
        cls._clean_and_replace_variables(query_list, alias)
        # Merge Join queries with Select
        cls._merge_queries(query_list)
        return query_list, execution_time

    @classmethod
    def _unwrap_QEP(cls, query_plan: dict) -> list:
        """

        :param query_plan:
        :return: List of nested dictionary. View example to see the structure of the output.

        Example

        >>>
        Expected Output
        >>>
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
                variable_queries.update({"Plan Rows": plans.get("Plan Rows")})
            if query is not None:
                query_list.append({query: variable_queries})
        return query_list

    @classmethod
    def _merge_queries(cls, query_list: list[dict]) -> None:
        """

        :param query_list:
        :return:
        """
        remove_join_list = []
        for i in range(len(query_list)):
            if query_list[i].get(QueryType.JOIN, None) is not None:
                remove_join_list.append(i)
                if i + 1 <= len(query_list):
                    query_select = [*query_list[i].values()][0]
                    query_list[i+1].update(query_select)
        for i in remove_join_list:
            query_list.pop(i)


    @classmethod
    def _retrieve_alias(cls, query: str) -> dict:
        """
        Parse the SQL Query and retrieve the relevant column alias with the attribute

        :param query: SQL query
        :return: A dictionary which maps the attribute to its alias

        Example
            >>> retrieve_alias(query="SELECT COUNT(c_key) as c FROM customer")
            Expected Output
            >>> {"c": "count(c_key)"}
        """
        # Sanitize the attribute and the alias
        # will keep the column name if alias is empty
        parsed = sqlglot.parse_one(query)
        aliases = {str(i.alias).lower() if i.alias != "" else str(i).split()[0].lower(): str(i).split()[0].lower()
                   for i in parsed.expressions}
        # Retrieve all alias from the subquery
        # TODO
        # Will have to check whether alias is declared outside of subquery or within subquery
        # Probably can ignore internal usage of column alias (Assumption)
        for subquery in parsed.find_all(sqlglot.exp.Subquery):
            # Alias declared externally
            alias_column_names = tuple(subquery.alias_column_names)
            # Get all the columns from the select
            key_column_names = tuple(str(s).lower() for s in subquery.selects)
            aliases.update(dict(zip(alias_column_names, key_column_names)))
        print(aliases)
        return aliases

    @classmethod
    def _clean_and_replace_variables(cls, query_list: list[dict[dict]], aliases: dict) -> None:
        """

        :param query_list:
        :param aliases:
        :return:
        """
        for query in query_list:
            # Deconstructing the nested dictionary
            query_variables = list(query.values())[0]
            for variable in list(filter(lambda items: "Key" in items, query_variables.keys())):
                variable_list = []
                for variable_string in query_variables.get(variable):
                    temp_values = cls._remove_tables_from_variables(variable_string)
                    temp_values = cls._add_table_alias(aliases, temp_values)
                    variable_list.append(temp_values)

                temp_string = " ".join(variable_list)
                query_variables.update({variable: temp_string})

    @classmethod
    def _add_table_alias(cls, aliases: dict, variable_string: str) -> str:
        '''

        :param aliases:
        :param variable_string:
        :return:
        '''
        temp_var = next((items for items in aliases.items() if items[1] in variable_string), None)
        return variable_string.replace(temp_var[1], temp_var[0]) if temp_var is not None else variable_string

    @classmethod
    def _remove_tables_from_variables(cls, variable_string: str) -> str:
        """

        :param variable_string:
        :return:
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


def get_system_args():
    '''
    Retrieve variables from command line
    :return: Default value of db connection or variables from command line
    '''
    print("Retrieving variables from command line")
    if len(sys.argv) != 4:
        print("Unable to retrieve any values from command line. Retrieving default settings.")
        return None
    else:
        return sys.argv[0], sys.argv[1], sys.argv[2], sys.argv[3]


def example():
    sys_arg = get_system_args()
    db = DBConnection() if sys_arg is None else DBConnection(sys_arg[0], sys_arg[1], sys_arg[2], sys_arg[3])
    (qep_list, execution_time) = QEP.get_QEP(
        "SELECT c_count, count(*) AS custdist FROM (SELECT c_custkey, count(o_orderkey) FROM customer "
        "LEFT OUTER JOIN orders ON c_custkey = o_custkey AND o_comment not like '%unusual%packages%' "
        "GROUP BY c_custkey ) as c_orders (c_custkey, c_count) "
        "GROUP BY c_count "
        "ORDER BY custdist DESC, c_count DESC;"
        , db)
    # qep_list = get_QEP(
    #     "SELECT c_custkey FROM customer "
    #     "WHERE c_acctbal > 100 "
    #     "ORDER BY c_custkey DESC, c_acctbal "
    #     "LIMIT 100;", cur)
    print(qep_list)
    print(execution_time)
    print(Parser.parse_query(qep_list))


# Remove the comment below the run the code
example()
