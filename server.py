from flask import Flask, request, jsonify
import pymysql
import traceback
import pandas as pd

from dotenv import load_dotenv
import os

import datetime
import time
import threading
import queue

import ast

load_dotenv()
current_node = os.getenv('current_node')

node1_host = "10.2.0.204"
node1_port = 3306
node1_user = "user"
node1_password = "password"

node2_host = "10.2.0.205"
node2_user = "user"
node2_port = 3306
node2_password = "password"

node3_host = "10.2.0.206"
node3_user = "user"
node3_port = 3306
node3_password = "password"
##### Connections
def get_node1_connection():

    try:
        connection1 = pymysql.connect(
            host=node1_host,
            user=node1_user,
            port=node1_port,
            password=node1_password,
            database='games',
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False
        )
        return connection1
    except Exception as e:
        print(f"Error connecting to node 1 database: {e}")
        return None
    
def get_node2_connection():

    try:
        connection2 = pymysql.connect(
            host=node2_host,
            user=node2_user,
            password=node2_password,
            port=node2_port,
            database='games',
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False
        )
        return connection2
    except Exception as e:
        print(f"Error connecting to node 2 database: {e}")
        return None
    
def get_node3_connection():
    try:
        connection3 = pymysql.connect(
            host=node3_host,
            user=node3_user,
            password=node3_password,
            port=node3_port,
            database='games',
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False)
        return connection3
    except Exception as e:
        print(f"Error connecting to node 3 database: {e}")
        return None

##### Locking logic
def acquire_lock(timeout, connection):

    start_time = time.time()
    lock = "database_lock"
    select_lock = """
    SELECT locked_by, lock_time FROM distributed_lock
    WHERE lock_name = %s
    """
    insert_lock = """
    INSERT INTO distributed_lock (lock_name, locked_by)
    VALUES (%s, %s)
    """
    update_lock = """
    UPDATE distributed_lock
    SET locked_by = %s, lock_time = NOW()
    WHERE lock_name = %s        
    """

    while True:
        try:
           cursor = connection.cursor()

           cursor.execute(select_lock, (lock,))
           result = cursor.fetchone()

           if result is None:
               cursor.execute(insert_lock, (lock, current_node))
               connection.commit()
               print(f"Lock acquired by {current_node}")
               return True
           else:
               locked_by = result['locked_by']
               timestamp = result['lock_time']

               if (time.time() - timestamp.timestamp()) > timeout:
                   cursor.execute(update_lock, (current_node, lock))
                   connection.commit()
                   cursor.execute(select_lock, (lock,))
                   result = cursor.fetchone()

                   # Check if we actually acquired the lock
                   if current_node == result['locked_by']:
                       print(f"Lock expired and acquired by {current_node}")
                       return True

           if (time.time() - timestamp) > timeout:
               return False

           time.sleep(1)
           
        except Exception as e:
            print(f"Failed to acquire lock: {e}")
            
            if time.time() - start_time > timeout:
                return False
            
        finally:
            cursor.close()
            connection.close()

def release_lock(connection):

    lock = "database_lock"
    delete_lock = """
    DELETE from distributed_lock
    WHERE lock_name = %s  
    """

    print("trying to release lock")
    cursor = connection.cursor()

    cursor.execute(delete_lock, ("database_lock",))
    connection.commit()

    print(f"Lock released by {current_node}")

def check_lock(conn):

    lock = "database_lock"
    select_lock = """
    SELECT locked_by, lock_time FROM distributed_lock
    WHERE lock_name = %s
    """
    print("checking lock")
    try:
        cursor = conn.cursor()

        cursor.execute(select_lock, (lock,))
        result = cursor.fetchone()

        if result is None:
            return True
        else:
            return False
    except Exception as e:
        print(f"Error: {e}")
        return False

##### Transaction logic    

def check_us_lock(conn):
    lock = "database_lock"
    select_lock = """
    SELECT locked_by, lock_time FROM distributed_lock
    WHERE lock_name = %s
    """
    print("checking lock")
    try:
        cursor = conn.cursor()

        cursor.execute(select_lock, (lock,))
        result = cursor.fetchone()

        if result['locked_by'] == current_node:
            return True
        else:
            return False
    except Exception as e:
        print(f"Error: {e}")
        return False


def recover():
    #lock the three nodes
    if current_node == 'node1':
        self_connection = get_node1_connection()
        other_connection_1 = get_node2_connection()
        other_connection_2 = get_node3_connection()
    elif current_node == 'node2':
        self_connection = get_node2_connection()
        other_connection_1 = get_node1_connection()
        other_connection_2 = get_node3_connection()
    else:
        self_connection = get_node3_connection()
        other_connection_1 = get_node1_connection()
        other_connection_2 = get_node2_connection()

    while True:
        acquire_lock(30, self_connection)
        acquire_lock(30, other_connection_1)
        acquire_lock(30, other_connection_2)

        #if all three nodes have the lock, recover
        if check_us_lock(self_connection) and check_us_lock(other_connection_1) and check_us_lock(other_connection_2):
            #get self.timestamp
            cursor_self = self_connection.cursor()
            cursor_self.execute("SELECT timestamp FROM checkpoint")
            timestamp = cursor_self.fetchone()['timestamp']

            #query other updates
            query_other_logs = "SELECT * FROM checkpoint WHERE timestamp > %s"
            cursor_other_1 = other_connection_1.cursor()
            cursor_other_2 = other_connection_2.cursor()
            cursor_other_1.execute(query_other_logs, (timestamp,))
            cursor_other_2.execute(query_other_logs, (timestamp,))

            other_logs_1 = cursor_other_1.fetchall()
            other_logs_2 = cursor_other_2.fetchall()

            #insert logs
            update_log_query = "INSERT INTO distributed_log (node, timestamp, query, params) VALUES (%s, %s, %s, %s)"
            set_logs = set(other_logs_1 + other_logs_2)
            for log in set_logs:
                cursor_self.execute(update_log_query, (log['node'], log['timestamp'], log['query'], log['params']))
                cursor_self.execute(log['query'], ast.literal_eval(log['params']))

            #commit everything
            self_connection.commit()
            other_connection_1.commit()
            other_connection_2.commit()

            #close everything needed to be closed
            cursor_self.close()
            cursor_other_1.close()
            cursor_other_2.close()
            self_connection.close()
            other_connection_1.close()
            other_connection_2.close()

            #release all locks
            release_lock(self_connection)
            release_lock(other_connection_1)
            release_lock(other_connection_2)

            print("Recovered from failure")
            break




def execute_transaction_down_master(slave_connection, slave_other, transaction):    
    update_log_query = "INSERT INTO distributed_log (node, timestamp, query, params) VALUES (%s, %s, %s, %s)"
    update_checkpoint_query = "UPDATE checkpoint SET timestamp = %s"
    cursor_slave = slave_connection.cursor()
    cursor_other = slave_other.cursor()
    try:

        timestamp = datetime.datetime.now()
        cursor_slave.execute(update_log_query, (current_node, timestamp, transaction['query'], str(tuple(transaction['params'].values()))))
        cursor_other.execute(update_log_query, (current_node, timestamp, transaction['query'], str(tuple(transaction['params'].values()))))

        cursor_slave.execute(transaction['query'], tuple(transaction['params'].values()))

        cursor_slave.execute(update_checkpoint_query, timestamp)
    except Exception as e:
        slave_connection.rollback()
        slave_other.rollback()
        print(f"Transaction failed: {transaction}")
        return False
    

    cursor_slave.close()
    cursor_other.close()
    slave_connection.commit()
    slave_other.commit()
    release_lock(slave_connection)
    release_lock(slave_other)
    print(f"Transaction succeeded: {transaction}")
    return True

def execute_transaction_down_one_slave(master_connection, slave_connection, transaction):
    update_log_query = "INSERT INTO distributed_log (node, timestamp, query, params) VALUES (%s, %s, %s, %s)"
    update_checkpoint_query = "UPDATE checkpoint SET timestamp = %s"
    cursor_slave = slave_connection.cursor()
    cursor_master = master_connection.cursor()
    try:
        timestamp = datetime.datetime.now()
        cursor_slave.execute(update_log_query, (current_node, timestamp, transaction['query'], str(tuple(transaction['params'].values()))))
        cursor_master.execute(update_log_query, (current_node, timestamp, transaction['query'], str(tuple(transaction['params'].values()))))

        cursor_slave.execute(transaction['query'], tuple(transaction['params'].values()))
        cursor_master.execute(transaction['query'], tuple(transaction['params'].values()))
        cursor_slave.execute(update_checkpoint_query, timestamp)
    except Exception as e:
        slave_connection.rollback()
        cursor_master.rollback()
        print(f"Transaction failed: {transaction}")
        return False
    
    cursor_slave.close()
    cursor_master.close()
    slave_connection.commit()
    master_connection.commit()
    release_lock(slave_connection)
    release_lock(master_connection)

    print(f"Transaction succeeded: {transaction}")
    return True

def execute_transaction(transaction):
        #if node 1 is down, commit to both node 2 or 3.
        #if node 2 is down, commit to both node 1 and node 3
        # if node 3 id down, commit to both node 1 and node 2
    try:
        #transaction['target_node'] -- 2 or 3
        # lock master and target node
        master_connection = get_node1_connection()
        if transaction['target_node'] == "node2":
            slave_connection = get_node2_connection()
            slave_other = get_node3_connection()
        elif transaction['target_node'] == "node3":
            slave_connection = get_node3_connection()
            slave_other = get_node2_connection()


        locked_master = acquire_lock(3, master_connection)
        if not locked_master:
            return execute_transaction_down_master(slave_connection, slave_other, transaction)
             
        locked_slave = acquire_lock(3, slave_connection)
        if not locked_slave:
            return execute_transaction_down_one_slave(master_connection, slave_other, transaction)
        
        locked_other = acquire_lock(3, slave_other)
        if not locked_other:
            return execute_transaction_down_one_slave(master_connection, slave_connection, transaction)
    
        update_log_query = "INSERT INTO distributed_log (node, timestamp, query, params) VALUES (%s, %s, %s, %s)"
        update_checkpoint_query = "UPDATE checkpoint SET timestamp = %s"


        cursor_master = master_connection.cursor()
        cursor_slave = slave_connection.cursor()
        cursor_other = slave_other.cursor()
        
        try:

            timestamp = datetime.datetime.now()
            cursor_master.execute(update_log_query, (transaction['target_node'], timestamp, transaction['query'], str(tuple(transaction['params'].values()))))
            cursor_slave.execute(update_log_query, (current_node, timestamp, transaction['query'], str(tuple(transaction['params'].values()))))
            cursor_other.execute(update_log_query, (current_node, timestamp, transaction['query'], str(tuple(transaction['params'].values()))))

            cursor_master.execute(transaction['query'], tuple(transaction['params'].values()))
            cursor_slave.execute(transaction['query'], tuple(transaction['params'].values()))

            cursor_master.execute(update_checkpoint_query, timestamp)
            cursor_slave.execute(update_checkpoint_query, timestamp)
            cursor_other.execute(update_checkpoint_query, timestamp)
        except Exception as e:
            master_connection.rollback()
            slave_connection.rollback()
            slave_other.rollback()
            print(f"Transaction failed: {transaction}")
            return False
        

        cursor_master.close()
        cursor_slave.close()
        cursor_other.close()
        master_connection.commit()
        slave_connection.commit()
        slave_other.commit()
        release_lock(master_connection)
        release_lock(slave_connection)
        release_lock(slave_other)

     
        print(f"Transaction succeeded: {transaction}")
        return True
        
    except Exception as e:
        print(f"Transaction failed: {transaction}, Error: {e}")
        return False
    
def execute_query(connection, query, params=()):
    try:
        if not check_lock(connection):
            raise Exception("Table locked!")
        cursor = connection.cursor()
        # Params is a single value that we tuple
        if params == ():
            cursor.execute(query)
        else:
            cursor.execute(query, (params,))
            
        results = cursor.fetchall()
        cursor.close()
        connection.close()
        return {"status": "success", "results": results}

    except Exception as e:
        print(f"Query failed: {query}, Error: {e}")
        return {"status": "error", "error": str(e)}

##### Flask server
app = Flask(__name__)
@app.route('/write', methods=['POST'])
def add_transaction():

        data = request.json
        transaction = data.get('transaction')
        result = execute_transaction(transaction)
        
        return jsonify({"status": result, "transaction": transaction}), 200


@app.route('/select', methods=['POST'])
def run_query():
    data = request.json
    query = data.get('query')
    params = data.get('params', ())

    if not query:
        return jsonify({"status": "error", "message": "Query required"}), 400

    try:
        
        result = execute_query(get_node1_connection(), query, params)
        if result['status'] == 'success':
            return jsonify(result), 200    
        else:
            result2 = None
            result3 = None
            to_return = []
            result2 = execute_query(get_node2_connection(), query, params)
            result3 = execute_query(get_node3_connection(), query, params)
            if result2['status'] == 'success':
                print(type(result2['results']))
                to_return = to_return + result2['results']
            elif result3['status'] == 'success':
                print(type(result3['results']))
                to_return = to_return + result3['results']
            print(to_return)
            if result2['status'] == 'success' or result3['status'] == 'success':
                return jsonify({ "status": "warning", "results": to_return}), 200  
            else:
                return jsonify({"status": "error", "message": "Unable to connect to any of the databases. Try again later"}), 500
    except Exception as e:
        return jsonify({"status": "error", "message": "Database is currently syncing"}), 500
    
if __name__ == '__main__':
    recover()
    app.run(host='0.0.0.0', port=5000)
