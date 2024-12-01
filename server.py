from flask import Flask, request, jsonify
import pymysql
import traceback

from dotenv import load_dotenv
import os

import datetime
import time
import threading
import queue

load_dotenv()
current_node = os.getenv('current_node')

node1_host = "ccscloud.dlsu.edu.ph"
node1_port = 22042
node1_user = "user"
node1_password = "password"

node2_host = "ccscloud.dlsu.edu.ph"
node2_user = "user"
node2_port = 22052
node2_password = "password"

node3_host = "ccscloud.dlsu.edu.ph"
node3_user = "user"
node3_port = 22062
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
            password=node2_user,
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
            password=node3_user,
            database='games',
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False)
        return connection3
    except Exception as e:
        print(f"Error connecting to node 3 database: {e}")
        return None

##### Locking logic
def acquire_lock(timeout):

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
           connection = get_node1_connection()
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

def release_lock():

    lock = "database_lock"
    delete_lock = """
    DELETE from distributed_lock
    WHERE lock_name = %s  
    """
    
    connection = get_node1_connection()
    cursor = connection.cursor()

    cursor.execute(delete_lock, ("database_lock",))
    connection.commit()

    print(f"Lock released by {current_node}")

def check_lock():
    
    lock = "database_lock"
    select_lock = """
    SELECT locked_by, lock_time FROM distributed_lock
    WHERE lock_name = %s
    """

    try:
        connection = get_node1_connection()
        cursor = connection.cursor()

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
transaction_queue = queue.Queue()

is_processing = False
is_processing_lock = threading.Lock()

def process_queue():
    global is_processing
    global is_processing_lock
    print(transaction_queue.size())
    while True:
        with is_processing_lock:
            is_processing = False if transaction_queue.empty() else True

        if is_processing and not transaction_queue.empty():
           
            transaction = transaction_queue.get()
            if transaction is None:
                release_lock()
                continue

            success = execute_transaction(transaction)
            if not success:
                # Re-enqueue if transaction failed
                transaction['retries'] += 1
                if transaction['retries'] <= 3:  # To improve: rollback all transca
                    transaction_queue.put(transaction)
                else:
                    print(f"Transaction failed after retries: {transaction}")
            transaction_queue.task_done()

        with is_processing_lock:
            is_processing = not transaction_queue.empty()  
        
        with is_processing_lock:
            if not is_processing:
                release_lock()


    
def execute_transaction(transaction):
    try:
        if transaction['target_node'] == "node1":
            connection = get_node1_connection()
        elif transaction['target_node'] == "node2":
            connection = get_node2_connection()
        else:
            connection = get_node3_connection()
        print(transaction)
        cursor = connection.cursor()    
        cursor.execute(transaction['query'], tuple(transaction['params'].values()))
        connection.commit()
        cursor.close()
        connection.close()
        print(f"Transaction succeeded: {transaction}")
        return True
        
    except Exception as e:
        print(f"Transaction failed: {transaction}, Error: {e}")
        return False
    
def execute_query(node, query, params=()):
    try:
        if node == "node1":
            connection = get_node1_connection()
        elif node == "node2":
            connection = get_node2_connection()
        else:
            connection = get_node3_connection()

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
    global is_processing

    with is_processing_lock:
        if is_processing:
            return jsonify({"status": "busy", "message": "Server is processing transactions. Try again later."}), 503

    try:
        if not acquire_lock(timeout=20):
            return jsonify({"status": "error", "message": "Unable to acquire lock."}), 503

        data = request.json
        transactions = data

        for transaction in transactions:
            tx = {
                "query": transaction['query'],
                "params": transaction['params'],
                "target_node": transaction['target_node'],
                "retries": 0
            }
            transaction_queue.put(tx)
            
        return jsonify({"status": "queued", "transaction": transaction}), 200
    except Exception as e:
        print(traceback.format_exc())
        return jsonify({"status": "error", "message": "Failed to acquire lock. Please try again later."}), 503

@app.route('/select', methods=['POST'])
def run_query():
    print(transaction_queue.qsize())
    global is_processing

    with is_processing_lock:
        print(is_processing)
        if is_processing:
            return jsonify({"status": "busy", "message": "Server is processing transactions. Try again later."}), 503

    data = request.json
    query = data.get('query')
    params = data.get('params', ())
    target_node = data.get('target_node')

    if not query or not target_node:
        return jsonify({"status": "error", "message": "Query and target node are required"}), 400

    if check_lock():
        print(params)
        result = execute_query(target_node, query, params)
        return jsonify(result), 200 if result['status'] == 'success' else 500
    else:
        return jsonify({"status": "error", "message": "Database is currently syncing"}), 500

if __name__ == '__main__':
    worker_thread = threading.Thread(target=process_queue, daemon=True)
    worker_thread.start()

    app.run(host='0.0.0.0', port=5000)