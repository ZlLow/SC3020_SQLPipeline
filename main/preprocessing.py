import queue
import psycopg2
import sys
from time import sleep
from pipesyntax import QueryType, Parser

DEFAULT_DBNAME = "TPC-H"
DEFAULT_USERNAME = "postgres"
DEFAULT_PASSWORD = "password"
DEFAULT_PORT = "5432"

"""
# Explanation of the variables 
# ============================
# Hash Cond = 
# Filter = Where Statement that evaluates condition that returns multiple statement 
# Relation Name = Name of the table used
# Index Name = Name of index
# Index Cond = Where Statement which only evaluates boolean condition
"""
variables = ["Hash Cond", "Filter", "Relation Name", "Index Name", "Index Cond", "Scan Direction"]


def connect(dbname, username, password, port):
    '''
    Connects to Postgres SQL database
    :param dbname:
    :param username:
    :param password:
    :param port:
    :return:
    '''
    conn = psycopg2.connect(dbname=dbname, user=username, password=password, port=port)
    return conn.cursor()


def get_system_args():
    '''
    Retrieve variables from command line
    :return: Default value of db connection or variables from command line
    '''
    print("Retrieving variables from command line")
    if len(sys.argv) != 4:
        print("Unable to retrieve any values from command line. Retrieving default settings.")
        return DEFAULT_DBNAME, DEFAULT_USERNAME, DEFAULT_PASSWORD, DEFAULT_PORT
    else:
        return sys.argv[0], sys.argv[1], sys.argv[2], sys.argv[3]


def get_QEP(query, cur, times=1):
    '''

    :param query:
    :param cur:
    :param times:
    :return:
    '''
    query_list = []
    while times < 3:
        try:
            cur.execute("EXPLAIN (FORMAT JSON) " + query)
            query_plan_json = cur.fetchone()
            query_list = unwrap_QEP(query_plan_json[0][0])
            break
        except:
            print("Unable to execute")
            sleep(10)
            times += 1
    return query_list


def unwrap_QEP(query_plan):
    '''

    :param query_plan:
    :return: list
    '''
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
        for query_key in filter(lambda key: key in variables, plans):
            variable_queries.update({query_key: plans.get(query_key)})
        if query is not None:
            query_list.append({query: variable_queries})
    query_list.reverse()
    return query_list


def example():
    dbname, username, password, port = get_system_args()
    cur = connect(dbname, username, password, port)
    qep_list = get_QEP(
        "SELECT c_count, count(*) AS custdist FROM (SELECT c_custkey, count(o_orderkey) FROM customer LEFT OUTER JOIN "
        "orders ON c_custkey = o_custkey AND o_comment not like '%unusual%packages%' GROUP BY c_custkey) as c_orders "
        "(c_custkey, c_count) GROUP BY c_count ORDER BY custdist DESC, c_count DESC ;",cur)
    print(qep_list)
    print(Parser.parse_query(qep_list))


# Remove the comment below the run the code
example()
