from flask import Flask, request, jsonify
import pymysql
import traceback
import pandas as pd

from dotenv import load_dotenv
import os

import datetime
import time
import random

import ast

load_dotenv()
current_node = os.getenv('current_user')

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
    if not connection:
        print("Tried to acquire lock on empty connection")
        return False
    elif not connection.open:
        print("Tried to acquire lock on closed connection")
        return False

    # Loop until we acquire the lock until timeout
    # There is a flaw where if a query takes more than 30 seconds to finish,
    # the lock may be acquired by another transaction
    while True:
        try:
           cursor = connection.cursor()

           cursor.execute(select_lock, (lock,))
           result = cursor.fetchone()

           if result is None:
               cursor.execute(insert_lock, (lock, current_node))
               connection.commit()
               cursor.execute(select_lock, (lock,))
               result = cursor.fetchone()
               # We check if we acquired the lock, this should be always true anyways
               if current_node == result['locked_by']:
                   print(f"Lock acquired by {current_node} on {connection.host}:{connection.port}")
                   return True
           else:
               locked_by = result['locked_by']
               timestamp = result['lock_time']

               # We are the one currently locking the node so we refresh the timestamp
               if locked_by == current_node:
                   cursor.execute(update_lock, (current_node, lock))
                   connection.commit()
                   cursor.execute(select_lock, (lock,))
                   result = cursor.fetchone()

                   # We check if we acquired the lock, this should be always true anyways
                   # This also guards against race conditions, I think -- Boris 
                   if current_node == result['locked_by']:
                       print(f"Lock acquired by {current_node} on {connection.host}:{connection.port}")
                       return True
                   
               elif time.time() > timestamp.timestamp() + 30:
                   # We get the lock when it hasn't been released in 30 secs. 
                   cursor.execute(update_lock, (current_node, lock))
                   connection.commit()
                   cursor.execute(select_lock, (lock,))
                   result = cursor.fetchone()

                   # Check if we actually acquired the lock
                   if current_node == result['locked_by']:
                       print(f"Lock expired and acquired by {current_node} on {connection.host}:{connection.port}")
                       return True

           # If we fail to acquire the lock during the specified time, we fail
           if (time.time() - start_time) > timeout:
               return False

           time.sleep(1)
           
        except Exception as e:
            print(f"Failed to acquire lock on {connection.host}:{connection.port}: {e}")
            
            if not connection.open:
                print("Tried to acquire lock on closed connection")
                return False
            
            # See above
            if (time.time() - start_time) > timeout:
                return False

def release_lock(connection):

    lock = "database_lock"
    delete_lock = """
    DELETE from distributed_lock
    WHERE lock_name = %s  
    """
    if not connection:
        print("Tried to release lock on empty connection")
        return
    elif not connection.open:
        print("Tried to release lock on closed connection")
        return

    print(f"trying to release lock on {connection.host}:{connection.port}")
    try:
        cursor = connection.cursor()

        if check_us_lock(connection):
            cursor.execute(delete_lock, ("database_lock",))
            connection.commit()
            print(f"Released lock by {current_node} on {connection.host}:{connection.port}")

    except Exception as e:
        print(f"Failed to release lock on {connection.host}:{connection.port}: {e}")

def check_lock(conn):

    lock = "database_lock"
    select_lock = """
    SELECT locked_by, lock_time FROM distributed_lock
    WHERE lock_name = %s
    """
    print(f"checking if lock exists on {conn.host}:{conn.port}")
    try:
        cursor = conn.cursor()

        cursor.execute(select_lock, (lock,))
        result = cursor.fetchone()

        if not result:
            return True
        else:
            print(f"Is locked by {result['locked_by']}")
            return False
    except Exception as e:
        print(f"Error checking lock on {conn.host}:{conn.port}: {e}")
        return False

##### Transaction logic    

def check_us_lock(conn):
    lock = "database_lock"
    select_lock = """
    SELECT locked_by, lock_time FROM distributed_lock
    WHERE lock_name = %s
    """
    print(f"checking if we {current_node} are the locker on {conn.host}:{conn.port}")
    try:
        cursor = conn.cursor()

        cursor.execute(select_lock, (lock,))
        result = cursor.fetchone()

        if result['locked_by'] == current_node:
            print("We are the locker")
            return True
        else:
            print(f"Node is not locked by us {current_node} but by: {result['locked_by']}")
            return False
    except Exception as e:
        print(f"Error: {e}")
        return False

def recover():

    # The general logic should work even if all databases come down
    # We cannot become online without recovering, we loop until we succeed
    while True:
        # Wrap in try since we shouldn't exit prematurely
        try:
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
            
            acquire_lock(3, self_connection)
            acquire_lock(3, other_connection_1)
            acquire_lock(3, other_connection_2)

            if not self_connection or not other_connection_1 or not other_connection_2:
                raise Exception("All nodes must be online before recovery can begin")

            # If we locked all three nodes, we can recover
            if check_us_lock(self_connection) and check_us_lock(other_connection_1) and check_us_lock(other_connection_2):

                try:
                    # Get our timestamp
                    cursor_self = self_connection.cursor()
                    cursor_self.execute("SELECT timestamp FROM checkpoint")
                    row = cursor_self.fetchone()
                    # On first run
                    if row is None:
                        print("Timestamp is none, inserting first timestamp, this must be the first run")
                        cursor_self.execute("INSERT INTO checkpoint (timestamp) VALUES (%s)",(datetime.datetime.now(),))
                        print("First timestamp inserted")
                        cursor_self.execute("SELECT timestamp FROM checkpoint")
                        row = cursor_self.fetchone()

                    timestamp = row['timestamp']
                    print(f"Node timestamp: {timestamp}")
                    # Query other updates
                    query_other_logs = "SELECT node, timestamp, query, params FROM distributed_log WHERE timestamp > %s"
                    cursor_other_1 = other_connection_1.cursor()
                    cursor_other_2 = other_connection_2.cursor()
                    cursor_other_1.execute(query_other_logs, (timestamp,))
                    cursor_other_2.execute(query_other_logs, (timestamp,))

                    other_logs_1 = cursor_other_1.fetchall()
                    other_logs_2 = cursor_other_2.fetchall()
                    print("Logs fetched")
                    # Insert logs
                    update_log_query = "INSERT INTO distributed_log (node, timestamp, query, params) VALUES (%s, %s, %s, %s)"
                    update_checkpoint_query = "UPDATE checkpoint SET timestamp = %s"

                    # Combine the logs
                    print("Combining logs")
                    combined_logs = other_logs_1 + other_logs_2
                    
                    # Use set comprehension to dedupe the logs
                    unique_logs = {tuple(row.items()) for row in combined_logs}

                    # Convert back to list of dictionaries
                    set_logs = [dict(row) for row in unique_logs]
                    print(f"Logs to query: {set_logs}")
                
                    for log in set_logs:
                        # Insert the logs to our own database
                        print(f"Log to insert: {log}")
                        cursor_self.execute(update_log_query, (log['node'], log['timestamp'], log['query'], log['params']))
                        print(f"Log inserted")
                        # We are master node so we execute every query regardless
                        if current_node == "node1":
                            cursor_self.execute(log['query'], ast.literal_eval(log['params']))
                            print(f"Transaction done")
                            curr_time = datetime.datetime().now()
                            cursor_self.execute(update_checkpoint_query, (curr_time,))
                            print(f"Checkpoint updated")
                        elif log['node'] == current_node:
                            cursor_self.execute(log['query'], ast.literal_eval(log['params']))
                            print(f"Transaction done")
                            curr_time = datetime.datetime().now()
                            cursor_self.execute(update_checkpoint_query, (curr_time,))
                            print(f"Checkpoint updated")

                    # We succeeded and can commit everything YAY!!!
                    self_connection.commit()
                    other_connection_1.commit()
                    other_connection_2.commit()

                    # Release all locks
                    print("All logs transacted releasing locks...")
                    release_lock(self_connection)
                    release_lock(other_connection_1)
                    release_lock(other_connection_2)
                
                    # Close everything needed to be closed
                    cursor_self.close()
                    cursor_other_1.close()
                    cursor_other_2.close()
                    self_connection.close()
                    other_connection_1.close()
                    other_connection_2.close()

                    # Exit from recovery 
                    print("Recovered successfully")
                    break
            
                except Exception as e:
                    print(f"Error occurred while recovering, trying again: {e}")
                    release_lock(self_connection)
                    release_lock(other_connection_1)
                    release_lock(other_connection_2)
                    self_connection.rollback()
                    other_connection_1.rollback()
                    other_connection_2.rollback()
                    seconds = random.uniform(1,12)
                    print(f"Sleeping for {seconds} before trying again")
                    time.sleep(seconds)
            else:
                # We didn't acquire all the locks
                print(f"Not all locks acquired, releasing all locks then waiting")
                release_lock(self_connection)
                release_lock(other_connection_1)
                release_lock(other_connection_2)
                # We have to sleep for a random time to avoid deadlocks
                seconds = random.uniform(1,12)
                print(f"Sleeping for {seconds} before trying again")
                time.sleep(seconds)

        except Exception as e:
            print(f"Error occurred while recovering, trying again: {e}")
            release_lock(self_connection)
            release_lock(other_connection_1)
            release_lock(other_connection_2)
            seconds = random.uniform(1,12)
            print(f"Sleeping for {seconds} before trying again")
            time.sleep(seconds)
    
def execute_transaction_down_one_committing(master_connection, slave_other, transaction):
    update_log_query = "INSERT INTO distributed_log (node, timestamp, query, params) VALUES (%s, %s, %s, %s)"
    update_checkpoint_query = "UPDATE checkpoint SET timestamp = %s"
    
    locked_master = acquire_lock(3, master_connection)
    locked_other = acquire_lock(3, slave_other)

    # We can only commit if a lock has been acquired on both nodes
    if not locked_master or not locked_other:
        # Release any lock we acquired
        if locked_master:
            release_lock(master_connection)
        if locked_other:
            release_lock(slave_other)
        print(f"Transaction failed, failed to acquire lock on both nodes: {transaction}")
        return False

    try:   
        cursor_master = master_connection.cursor()
        cursor_other = slave_other.cursor()

        try:    
            # Update logs
            timestamp = datetime.datetime.now()
            cursor_master.execute(update_log_query, (transaction['target_node'], timestamp, transaction['query'], str(tuple(transaction['params'].values()))))
            print("Logs updated on primary")
            cursor_other.execute(update_log_query, (transaction['target_node'], timestamp, transaction['query'], str(tuple(transaction['params'].values()))))
            print("Logs updated on other")
            # Execute transaction
            cursor_master.execute(transaction['query'], tuple(transaction['params'].values()))
            print("Transaction done on primary")
            # Update timestamp
            cursor_master.execute(update_checkpoint_query, timestamp)
            print("Checkpoint updated on primary")
            cursor_other.execute(update_checkpoint_query, timestamp)
            print("Checkpoint updated on other")
        except Exception as e:
            # Rollback partial transaction
            release_lock(master_connection)
            release_lock(slave_other)
            if master_connection.open:
                master_connection.rollback()
            if slave_other.open:
                slave_other.rollback()
            print(f"Transaction failed: {transaction}")
            return False

        cursor_master.close()
        cursor_other.close()
        master_connection.commit()
        slave_other.commit()
        release_lock(master_connection)
        release_lock(slave_other)
        print(f"Transaction succeeded: {transaction}")
        return True

    except Exception as e:
        # Rollback partial transaction
        release_lock(master_connection)
        release_lock(slave_other)
        if master_connection.open:
            master_connection.rollback()
        if slave_other.open:
            slave_other.rollback()
        print(f"Transaction failed: {transaction}")
        return False

def execute_transaction_down_non_committing(master_connection, slave_connection, transaction):
    update_log_query = "INSERT INTO distributed_log (node, timestamp, query, params) VALUES (%s, %s, %s, %s)"
    update_checkpoint_query = "UPDATE checkpoint SET timestamp = %s"
    
    locked_master = acquire_lock(3, master_connection)
    locked_slave = acquire_lock(3, slave_connection)

    # We can only commit if a lock has been acquired on both nodes
    if not locked_master or not locked_slave:
        # Release any lock we acquired
        if locked_master:
            release_lock(master_connection)
        if locked_slave:
            release_lock(slave_connection)
        print(f"Transaction failed, failed to acquire lock on both nodes: {transaction}")
        return False
    
    try:
        cursor_slave = slave_connection.cursor()
        cursor_master = master_connection.cursor()
        
        try:
            # Update logs
            timestamp = datetime.datetime.now()
            cursor_slave.execute(update_log_query, (transaction['target_node'], timestamp, transaction['query'], str(tuple(transaction['params'].values()))))
            print("Logs updated on slave")
            cursor_master.execute(update_log_query, (transaction['target_node'], timestamp, transaction['query'], str(tuple(transaction['params'].values()))))
            print("Logs updated on master")
            # Execute transactions
            cursor_slave.execute(transaction['query'], tuple(transaction['params'].values()))
            print("Transaction done on slave")
            cursor_master.execute(transaction['query'], tuple(transaction['params'].values()))
            print("Transaction done on master")
            # Update timestamp
            cursor_slave.execute(update_checkpoint_query, timestamp)
            print("Checkpoint updated on slave")
            cursor_master.execute(update_checkpoint_query, timestamp)
            print("Checkpoint updated on master")
            
        except Exception as e:
            # Rollback partial transaction
            release_lock(slave_connection)
            release_lock(master_connection)
            if master_connection.open:
                master_connection.rollback()
            if slave_connection.open:
                slave_connection.rollback()
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
        
    except Exception as e:
        # We somehow failed during the commit
        release_lock(slave_connection)
        release_lock(master_connection)
        if master_connection.open:
            master_connection.rollback()
        if slave_connection.open:
            slave_connection.rollback()
        print(f"Transaction failed: {transaction}")
        return False

def execute_transaction(transaction):
        # If node 1 is down, commit to both node 2 or 3.
        # If node 2 is down, commit to both node 1 and node 3
        # If node 3 id down, commit to both node 1 and node 2
    try:
        # transaction['target_node'] -- node2 or node3
        # Lock master and target node
        master_connection = None
        slave_connection = None
        slave_other = None
        
        master_connection = get_node1_connection()
        if transaction['target_node'] == "node2":
            slave_connection = get_node2_connection()
            slave_other = get_node3_connection()
        elif transaction['target_node'] == "node3":
            slave_connection = get_node3_connection()
            slave_other = get_node2_connection()

        # Check if database is online
        if not master_connection: 
            print("Master is not online, committing only on slave")
            return execute_transaction_down_one_committing(slave_connection, slave_other, transaction)

        if not slave_connection:          
            print("Slave is not online, committing only on master")
            return execute_transaction_down_one_committing(master_connection, slave_other, transaction)
        
        if not slave_other:
            print("Other is not online, proceeding with transaction")
            return execute_transaction_down_non_committing(master_connection, slave_connection, transaction)

        # We have to lock all three nodes for the transaction to proceed
        # If this fails, another transaction acquired a lock
        # All transactions must either lock all or two nodes to proceed otherwise, 
        # -- they will fail
        # If two transactions proceed at the same time, both will fail 
        # -- if each transaction locks a node each

        locked_master = acquire_lock(3, master_connection)
        locked_slave = acquire_lock(3, slave_connection)
        locked_other = acquire_lock(3, slave_other)

        if not locked_master or not locked_slave or locked_other:            
            release_lock(master_connection)
            release_lock(slave_connection)
            release_lock(slave_other)
            print(f"Transaction failed, not all nodes have been locked: {transaction}")
            return False
    
        update_log_query = "INSERT INTO distributed_log (node, timestamp, query, params) VALUES (%s, %s, %s, %s)"
        update_checkpoint_query = "UPDATE checkpoint SET timestamp = %s"

        cursor_master = master_connection.cursor()
        cursor_slave = slave_connection.cursor()
        cursor_other = slave_other.cursor()
        
        try:
            # Update the logs of all nodes with the transaction
            timestamp = datetime.datetime.now()
            cursor_master.execute(update_log_query, (transaction['target_node'], timestamp, transaction['query'], str(tuple(transaction['params'].values()))))
            print("Logs updated on master")
            cursor_slave.execute(update_log_query, (transaction['target_node'], timestamp, transaction['query'], str(tuple(transaction['params'].values()))))
            print("Logs updated on slave")
            cursor_other.execute(update_log_query, (transaction['target_node'], timestamp, transaction['query'], str(tuple(transaction['params'].values()))))
            print("Logs updated on other")

            # We execute the transaction on the master node and the target node
            cursor_master.execute(transaction['query'], tuple(transaction['params'].values()))
            print("Transaction done on master")
            cursor_slave.execute(transaction['query'], tuple(transaction['params'].values()))
            print("Transaction done on slave")
            # We update the timestamp as a committed transaction counter so --
            # -- we only need to get the transactions after the timestamp during --
            # -- recovery.
            cursor_master.execute(update_checkpoint_query, timestamp)
            print("Checkpoint updated on master")
            cursor_slave.execute(update_checkpoint_query, timestamp)
            print("Checkpoint updated on slave")
            cursor_other.execute(update_checkpoint_query, timestamp)
            print("Checkpoint updated on other")
        except Exception as e:
            # We rollback then return failure on transaction.
            # We failed to commit a part of the transaction so everything must be rolled back.
            release_lock(master_connection)
            release_lock(slave_connection)
            release_lock(slave_other)
            if master_connection.open:
                master_connection.rollback()
            if slave_connection.open:
                slave_connection.rollback()
            if slave_other.open:
                slave_other.rollback()
            print(f"Transaction failed: {transaction}")
            return False

        # We succeeded on the transactions
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
        # We somehow failed during the commit
        release_lock(master_connection)
        release_lock(slave_connection)
        release_lock(slave_other)
        if master_connection.open:
            master_connection.rollback()
        if slave_connection.open:
            slave_connection.rollback()
        if slave_other.open:
            slave_other.rollback()
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
    query = data.get('query')
    params = data.get('params')
    target_node = data.get('target_node')

    transaction = {
        "query": query,
        "params": params,
        "target_node": target_node,
    }
    print(f"Transaction: {transaction}")
        
    result = execute_transaction(transaction)

    if result: 
        return jsonify({"status": success, "transaction": transaction}), 200
    else:
        return jsonify({"status": success, "message": "Database is currently busy"}), 200

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
    # We need to stagger the recovery or this will deadlock
    # We do time.sleep() on the recovery function, 
    # time.sleep() here would help on the first run
    seconds = random.uniform(1,20)
    print(f"We are {current_node}")
    print(f"Starting up and waiting for {seconds}")
    time.sleep(seconds)
    print("Beginning database sync")
    recover()
    app.run(host='0.0.0.0', port=5000)
