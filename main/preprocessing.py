import queue
import psycopg2
import sys
from time import sleep
from pipesyntax import QueryType


DEFAULT_DBNAME = "TPC-H"
DEFAULT_USERNAME = "postgres"
DEFAULT_PASSWORD = "password"
DEFAULT_PORT = "5432"

variables = ["Hash Cond", "Filter", "Relation Name", "Index Name"]


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


def get_QEP(query, times=1):
    query_dict = dict()
    while times < 3:
        try:
            cur.execute("EXPLAIN (FORMAT JSON) " + query)
            query_plan_json = cur.fetchone()
            query_dict = unwrap_QEP(query_plan_json[0][0])
            break
        except:
            print("Unable to execute")
            sleep(10)
            times += 1
    return query_dict


def unwrap_QEP(query_plan):
    query_plan = query_plan["Plan"]
    query_dict = dict()
    plan_queue = queue.Queue()
    plan_queue.put(query_plan)
    while not plan_queue.empty():
        plans = plan_queue.get()
        try:
            query = QueryType(plans["Node Type"])
        except ValueError:
            query = None
        print(query)
        print(list(map(plans.get, filter(lambda key: "Key" in key, plans))))
        if "Plans" in plans:
            for plan in plans["Plans"]:
                plan_queue.put(plan)
        print(list(map(plans.get, filter(lambda key: key in variables, plans))))
    return query_dict





dbname, username, password, port = get_system_args()
cur = connect(dbname, username, password, port)
get_QEP(
    "SELECT c_count, count(*) AS custdist FROM (SELECT c_custkey, count(o_orderkey) FROM customer LEFT OUTER JOIN "
    "orders ON c_custkey = o_custkey AND o_comment not like '%unusual%packages%' GROUP BY c_custkey) as c_orders ("
    "c_custkey, c_count) GROUP BY c_count ORDER BY custdist DESC, c_count DESC ;")
