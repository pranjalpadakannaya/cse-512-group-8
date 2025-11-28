"""
Database Connection Manager for CockroachDB
Handles connection pooling and retry logic
"""

import psycopg2
from psycopg2 import pool
import json
import time
from typing import Optional, Dict, Any

class CockroachDBConnection:
    def __init__(self, config_file='config/cluster_config.json'):
        """Initialize connection pool to CockroachDB cluster"""
        with open(config_file, 'r') as f:
            self.config = json.load(f)
        
        self.primary_node = self.config['primary_node']
        host, port = self.primary_node.split(':')
        
        # Create connection pool
        self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=20,
            host=host,
            port=port,
            database='ecommerce',
            user='root',
            # No password in insecure mode
        )
        
        print(f"✓ Connection pool created to {self.primary_node}")
    
    def get_connection(self):
        """Get a connection from the pool"""
        return self.connection_pool.getconn()
    
    def release_connection(self, conn):
        """Release connection back to pool"""
        self.connection_pool.putconn(conn)
    
    def execute_query(self, query: str, params: tuple = None, fetch: bool = True):
        """
        Execute a query with automatic retry logic
        
        Args:
            query: SQL query string
            params: Query parameters
            fetch: Whether to fetch results
        
        Returns:
            Query results or None
        """
        max_retries = 3
        retry_delay = 0.1  # seconds
        
        for attempt in range(max_retries):
            conn = None
            try:
                conn = self.get_connection()
                cursor = conn.cursor()
                
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                if fetch:
                    results = cursor.fetchall()
                    conn.commit()
                    return results
                else:
                    conn.commit()
                    return None
                    
            except psycopg2.Error as e:
                if conn:
                    conn.rollback()
                
                # Check if it's a serialization error (retry-able)
                if '40001' in str(e):  # Serialization failure
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay * (2 ** attempt))  # Exponential backoff
                        continue
                
                print(f"Database error: {e}")
                raise
            
            finally:
                if conn:
                    self.release_connection(conn)
        
        raise Exception(f"Query failed after {max_retries} retries")
    
    def execute_transaction(self, operations: list):
        """
        Execute multiple operations in a single transaction
        
        Args:
            operations: List of (query, params) tuples
        """
        max_retries = 3
        
        for attempt in range(max_retries):
            conn = None
            try:
                conn = self.get_connection()
                cursor = conn.cursor()
                
                # Begin transaction
                cursor.execute("BEGIN")
                
                # Execute all operations
                for query, params in operations:
                    cursor.execute(query, params if params else ())
                
                # Commit transaction
                cursor.execute("COMMIT")
                return True
                
            except psycopg2.Error as e:
                if conn:
                    conn.rollback()
                
                if '40001' in str(e) and attempt < max_retries - 1:
                    time.sleep(0.1 * (2 ** attempt))
                    continue
                
                print(f"Transaction error: {e}")
                raise
            
            finally:
                if conn:
                    self.release_connection(conn)
        
        return False
    
    def close_all(self):
        """Close all connections in the pool"""
        self.connection_pool.closeall()
        print("✓ All connections closed")


# Convenience functions
def get_db_connection():
    """Get a global database connection instance"""
    return CockroachDBConnection()