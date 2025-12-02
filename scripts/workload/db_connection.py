"""
Database Connection Manager for CockroachDB
FIXED: Proper transaction management to prevent "transaction in progress" errors
"""

import psycopg2
from psycopg2 import pool
import time

class CockroachDBConnection:
    def __init__(self, config_file='config/cluster_config.json'):
        """Initialize connection pool"""
        import json
        
        with open(config_file, 'r') as f:
            config = json.load(f)
        
        # Get primary node for connection
        primary_node = config['primary_node']
        host, port = primary_node.split(':')
        
        # Connection parameters
        self.conn_params = {
            'host': host,
            'port': int(port),
            'database': 'tpch',
            'user': 'root',
            'sslmode': 'disable',
            'application_name': 'ecommerce_workload'
        }
        
        # Create connection pool (important for multi-threaded workloads)
        self.pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=5,
            maxconn=50,
            **self.conn_params
        )
    
    def execute_query(self, query, params=None, fetch=True, max_retries=3):
        """
        Execute a query with proper transaction management
        CRITICAL: Always commits or rolls back to prevent hanging transactions
        """
        conn = None
        cursor = None
        
        for attempt in range(max_retries):
            try:
                # Get connection from pool
                conn = self.pool.getconn()
                
                # IMPORTANT: Set autocommit for simple queries
                # Or explicitly manage transactions for complex operations
                if fetch and not params:
                    # Read-only query - use autocommit
                    conn.autocommit = True
                else:
                    # Write query - manage transaction explicitly
                    conn.autocommit = False
                
                cursor = conn.cursor()
                
                # Execute query
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                # Fetch results if needed
                result = None
                if fetch:
                    result = cursor.fetchall()
                
                # Commit if not autocommit
                if not conn.autocommit:
                    conn.commit()
                
                return result
                
            except psycopg2.OperationalError as e:
                # Retry on operational errors (network issues, etc.)
                if attempt < max_retries - 1:
                    time.sleep(0.1 * (attempt + 1))
                    if conn:
                        try:
                            conn.rollback()
                        except:
                            pass
                    continue
                else:
                    print(f"Query failed after {max_retries} attempts: {e}")
                    raise
                    
            except Exception as e:
                # Rollback on any error
                if conn and not conn.autocommit:
                    try:
                        conn.rollback()
                    except:
                        pass
                print(f"Error executing query: {e}")
                raise
                
            finally:
                # CRITICAL: Always close cursor and return connection to pool
                if cursor:
                    try:
                        cursor.close()
                    except:
                        pass
                
                if conn:
                    # Return connection to pool
                    self.pool.putconn(conn)
    
    def execute_transaction(self, operations, max_retries=3):
        """
        Execute multiple operations in a single transaction
        IMPORTANT: Properly commits or rolls back the entire transaction
        
        Args:
            operations: List of (query, params) tuples
            max_retries: Number of retry attempts
        """
        conn = None
        cursor = None
        
        for attempt in range(max_retries):
            try:
                # Get connection from pool
                conn = self.pool.getconn()
                conn.autocommit = False  # Explicit transaction management
                
                cursor = conn.cursor()
                
                # Execute all operations
                for query, params in operations:
                    cursor.execute(query, params)
                
                # Commit transaction
                conn.commit()
                return True
                
            except psycopg2.extensions.TransactionRollbackError as e:
                # CockroachDB serialization error - retry
                if attempt < max_retries - 1:
                    if conn:
                        try:
                            conn.rollback()
                        except:
                            pass
                    time.sleep(0.1 * (attempt + 1))
                    continue
                else:
                    print(f"Transaction failed after {max_retries} attempts: {e}")
                    if conn:
                        try:
                            conn.rollback()
                        except:
                            pass
                    return False
                    
            except Exception as e:
                # Rollback on error
                if conn:
                    try:
                        conn.rollback()
                    except:
                        pass
                print(f"Transaction error: {e}")
                return False
                
            finally:
                # CRITICAL: Always close cursor and return connection
                if cursor:
                    try:
                        cursor.close()
                    except:
                        pass
                
                if conn:
                    self.pool.putconn(conn)
        
        return False
    
    def close_all(self):
        """Close all connections in the pool"""
        if self.pool:
            self.pool.closeall()


def test_connection():
    """Test database connection"""
    try:
        db = CockroachDBConnection()
        result = db.execute_query("SELECT version()")
        if result:
            print("✓ Connection successful!")
            print(f"CockroachDB version: {result[0][0]}")
        db.close_all()
        return True
    except Exception as e:
        print(f"✗ Connection failed: {e}")
        return False


if __name__ == "__main__":
    test_connection()