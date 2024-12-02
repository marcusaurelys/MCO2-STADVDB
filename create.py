CREATE TABLE distributed_lock (
    lock_name VARCHAR(255) PRIMARY KEY, -- Unique identifier for the lock
    locked_by VARCHAR(255),            -- App or process that acquired the lock
    lock_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Timestamp of lock acquisition
);

CREATE TABLE distributed_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT, -- Id of distributed log entry
    node INTEGER NOT NULL, -- Relevant slave node for the query
    timestamp TIMESTAMP NOT NULL, -- Timestamp of the transaction
    query VARCHAR(255) NOT NULL, -- query string
    params VARCHAR(255) NOT NULL -- parameters
);