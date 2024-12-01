CREATE TABLE distributed_lock (
    lock_name VARCHAR(255) PRIMARY KEY, -- Unique identifier for the lock
    locked_by VARCHAR(255),            -- App or process that acquired the lock
    lock_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Timestamp of lock acquisition
);
