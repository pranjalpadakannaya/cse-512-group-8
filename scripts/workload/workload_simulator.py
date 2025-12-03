"""
Workload Simulator - Generate realistic e-commerce traffic
FIXED: Accepts CRUD instance to share metrics with dashboard
"""

import random
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from scripts.workload.db_connection import CockroachDBConnection
from scripts.workload.crud_operations import EcommerceCRUD
import json

class WorkloadSimulator:
    def __init__(self, crud_or_db):
        """
        Initialize simulator with either CRUD instance or DB connection
        
        Args:
            crud_or_db: Either EcommerceCRUD instance (from dashboard) 
                       or CockroachDBConnection (standalone)
        """
        if isinstance(crud_or_db, EcommerceCRUD):
            # Dashboard passed CRUD instance - use it directly (shares metrics!)
            self.crud = crud_or_db
            self.db = crud_or_db.db
        else:
            # Standalone mode - create own CRUD instance
            self.db = crud_or_db
            self.crud = EcommerceCRUD(crud_or_db)
        
        self.is_running = False
        
        # Workload distribution (percentage)
        self.workload_mix = {
            'create_order': 30,
            'read_order': 40,
            'update_order': 20,
            'analytics': 10
        }
    
    def _get_random_operation(self):
        """Select random operation based on workload mix"""
        rand = random.randint(1, 100)
        cumulative = 0
        
        for operation, percentage in self.workload_mix.items():
            cumulative += percentage
            if rand <= cumulative:
                return operation
        
        return 'read_order'
    
    def _ensure_customer_exists(self, custkey):
        """Create customer if doesn't exist (fixes foreign key violation)"""
        try:
            # Check if customer exists
            check_query = "SELECT C_CUSTKEY FROM tpch.CUSTOMER WHERE C_CUSTKEY = %s"
            result = self.db.execute_query(check_query, (custkey,))
            
            if not result:
                # Customer doesn't exist - create it
                self.crud.create_customer(custkey)
            
            return True
        except Exception as e:
            print(f"Error ensuring customer exists: {e}")
            return False
    
    def _ensure_part_supplier_exists(self, partkey, suppkey):
        """Create part, supplier, and partsupp if they don't exist"""
        try:
            # Check/create part
            part_check = "SELECT P_PARTKEY FROM tpch.PART WHERE P_PARTKEY = %s"
            if not self.db.execute_query(part_check, (partkey,)):
                # Create part
                part_query = """
                INSERT INTO tpch.PART (P_PARTKEY, P_NAME, P_MFGR, P_BRAND, P_TYPE, 
                                      P_SIZE, P_CONTAINER, P_RETAILPRICE, P_COMMENT)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                name = f"Part#{partkey:09d}"
                mfgr = random.choice(['Manufacturer#1', 'Manufacturer#2', 'Manufacturer#3'])
                brand = f"Brand#{random.randint(1, 5)}{random.randint(1, 5)}"
                ptype = random.choice(['STANDARD', 'SMALL', 'MEDIUM', 'LARGE', 'ECONOMY'])
                size = random.randint(1, 50)
                container = random.choice(['SM CASE', 'SM BOX', 'SM PACK', 'LG CASE', 'LG BOX'])
                price = round(random.uniform(100.0, 2000.0), 2)
                comment = f"Part {partkey}"
                
                self.db.execute_query(part_query, (partkey, name, mfgr, brand, ptype, 
                                                   size, container, price, comment), fetch=False)
            
            # Check/create supplier
            supp_check = "SELECT S_SUPPKEY FROM tpch.SUPPLIER WHERE S_SUPPKEY = %s"
            if not self.db.execute_query(supp_check, (suppkey,)):
                # Create supplier
                supp_query = """
                INSERT INTO tpch.SUPPLIER (S_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY, 
                                          S_PHONE, S_ACCTBAL, S_COMMENT)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                name = f"Supplier#{suppkey:09d}"
                address = f"{random.randint(1, 999)} Supply St"
                nationkey = random.randint(0, 24)
                phone = f"{random.randint(10,99)}-{random.randint(100,999)}-{random.randint(1000,9999)}"
                acctbal = round(random.uniform(-999.99, 9999.99), 2)
                comment = f"Supplier {suppkey}"
                
                self.db.execute_query(supp_query, (suppkey, name, address, nationkey, 
                                                   phone, acctbal, comment), fetch=False)
            
            # Check/create partsupp
            ps_check = "SELECT PS_PARTKEY FROM tpch.PARTSUPP WHERE PS_PARTKEY = %s AND PS_SUPPKEY = %s"
            if not self.db.execute_query(ps_check, (partkey, suppkey)):
                # Create partsupp
                ps_query = """
                INSERT INTO tpch.PARTSUPP (PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, 
                                          PS_SUPPLYCOST, PS_COMMENT)
                VALUES (%s, %s, %s, %s, %s)
                """
                availqty = random.randint(100, 10000)
                supplycost = round(random.uniform(1.0, 1000.0), 2)
                comment = f"Part {partkey} from supplier {suppkey}"
                
                self.db.execute_query(ps_query, (partkey, suppkey, availqty, 
                                                supplycost, comment), fetch=False)
            
            return True
        except Exception as e:
            print(f"Error ensuring part/supplier exists: {e}")
            return False
    
    def _execute_create_order(self):
        """Execute create order operation with foreign key handling"""
        try:
            # Get random customer and ensure it exists
            custkey = random.randint(1, 1000)
            if not self._ensure_customer_exists(custkey):
                return ('create_order', False)
            
            # Generate random items and ensure they exist
            num_items = random.randint(1, 5)
            items = []
            for _ in range(num_items):
                partkey = random.randint(1, 1000)
                suppkey = random.randint(1, 100)  # Smaller range for suppliers
                
                # Ensure part/supplier/partsupp exist
                if not self._ensure_part_supplier_exists(partkey, suppkey):
                    continue
                
                quantity = random.randint(1, 50)
                price = round(random.uniform(10.0, 1000.0), 2)
                items.append((partkey, suppkey, quantity, price))
            
            if not items:
                return ('create_order', False)
            
            orderkey = self.crud.create_order(custkey, items)
            return ('create_order', orderkey is not None)
        
        except Exception as e:
            print(f"Error creating order: {e}")
            return ('create_order', False)
    
    def _execute_read_order(self):
        """Execute read order operation"""
        orderkey = random.randint(1, 5000)
        result = self.crud.get_order_details(orderkey)
        return ('read_order', result is not None)
    
    def _execute_update_order(self):
        """Execute update order operation"""
        orderkey = random.randint(1, 5000)
        new_status = random.choice(['P', 'F'])
        result = self.crud.update_order_status(orderkey, new_status)
        return ('update_order', result)
    
    def _execute_analytics(self):
        """Execute analytics query"""
        choice = random.choice(['top_customers', 'revenue_by_region'])
        
        if choice == 'top_customers':
            result = self.crud.get_top_customers(limit=10)
        else:
            result = self.crud.get_revenue_by_region()
        
        return ('analytics', result is not None)
    
    def run_single_transaction(self, max_retries=3):
        """
        Execute a single random transaction with retry logic
        Handles connection failures and transient errors gracefully
        """
        operation = self._get_random_operation()
        
        for attempt in range(max_retries):
            try:
                if operation == 'create_order':
                    return self._execute_create_order()
                elif operation == 'read_order':
                    return self._execute_read_order()
                elif operation == 'update_order':
                    return self._execute_update_order()
                elif operation == 'analytics':
                    return self._execute_analytics()
            
            except Exception as e:
                error_msg = str(e).lower()
                
                # Check if it's a retryable error (connection issues, timeouts)
                retryable = any(keyword in error_msg for keyword in [
                    'connection', 'timeout', 'broken pipe', 'reset by peer',
                    'connection refused', 'no route to host', 'temporary failure',
                    'deadlock', 'serialization', 'restart transaction'
                ])
                
                if retryable and attempt < max_retries - 1:
                    # Retryable error and we have retries left
                    print(f"Retryable error on attempt {attempt + 1}/{max_retries}: {e}")
                    time.sleep(0.1 * (attempt + 1))  # Exponential backoff
                    continue
                else:
                    # Non-retryable error or out of retries
                    if attempt == max_retries - 1:
                        print(f"Transaction failed after {max_retries} attempts: {e}")
                    else:
                        print(f"Non-retryable transaction error: {e}")
                    return (operation, False)
        
        # Should never reach here, but just in case
        return (operation, False)
    
    def run_workload(self, num_transactions: int = 1000, num_threads: int = 10):
        """
        Run workload with multiple concurrent threads
        
        Args:
            num_transactions: Total number of transactions to execute
            num_threads: Number of concurrent threads
        """
        print(f"\n{'='*60}")
        print(f"Starting workload simulation")
        print(f"Transactions: {num_transactions}")
        print(f"Threads: {num_threads}")
        print(f"Workload mix: {self.workload_mix}")
        print(f"{'='*60}\n")
        
        self.is_running = True
        start_time = time.time()
        
        results = {
            'total': 0,
            'success': 0,
            'failed': 0,
            'by_type': {}
        }
        
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(self.run_single_transaction) 
                      for _ in range(num_transactions)]
            
            for i, future in enumerate(as_completed(futures), 1):
                try:
                    op_type, success = future.result()
                    results['total'] += 1
                    
                    if success:
                        results['success'] += 1
                    else:
                        results['failed'] += 1
                    
                    if op_type not in results['by_type']:
                        results['by_type'][op_type] = {'success': 0, 'failed': 0}
                    
                    if success:
                        results['by_type'][op_type]['success'] += 1
                    else:
                        results['by_type'][op_type]['failed'] += 1
                    
                    # Progress update
                    if i % 100 == 0:
                        elapsed = time.time() - start_time
                        tps = i / elapsed
                        print(f"Progress: {i}/{num_transactions} ({i/num_transactions*100:.1f}%) "
                              f"| TPS: {tps:.2f} | Success: {results['success']} | Failed: {results['failed']}")
                
                except Exception as e:
                    print(f"Future error: {e}")
                    results['failed'] += 1
        
        total_time = time.time() - start_time
        self.is_running = False
        
        # Print summary
        print(f"\n{'='*60}")
        print(f"WORKLOAD SUMMARY")
        print(f"{'='*60}")
        print(f"Total transactions: {results['total']}")
        print(f"Successful: {results['success']} ({results['success']/results['total']*100:.1f}%)")
        print(f"Failed: {results['failed']} ({results['failed']/results['total']*100:.1f}%)")
        print(f"Total time: {total_time:.2f} seconds")
        print(f"Average TPS: {results['total']/total_time:.2f}")
        print(f"\nResults by operation type:")
        for op_type, counts in results['by_type'].items():
            total_op = counts['success'] + counts['failed']
            print(f"  {op_type}: {counts['success']}/{total_op} successful")
        
        # Get detailed metrics from CRUD operations
        print(f"\n{'='*60}")
        print(f"DETAILED PERFORMANCE METRICS")
        print(f"{'='*60}")
        metrics = self.crud.get_performance_metrics()
        for op_type, stats in metrics.items():
            if stats['count'] > 0:
                print(f"\n{op_type.upper()} Operations:")
                print(f"  Count: {stats['count']}")
                print(f"  Avg Latency: {stats['avg_latency']*1000:.2f} ms")
                print(f"  Min Latency: {stats['min_latency']*1000:.2f} ms")
                print(f"  Max Latency: {stats['max_latency']*1000:.2f} ms")
                print(f"  P95 Latency: {stats['p95_latency']*1000:.2f} ms")
        
        print(f"{'='*60}\n")
        
        return results


def main():
    """Main function to run workload simulator"""
    print("Initializing database connection...")
    db = CockroachDBConnection()
    
    print("Starting workload simulator...")
    simulator = WorkloadSimulator(db)
    
    # Run workload
    results = simulator.run_workload(
        num_transactions=1000,
        num_threads=10
    )
    
    # Save results to file
    with open('logs/workload/workload_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    print("Results saved to logs/workload/workload_results.json")
    
    db.close_all()


if __name__ == "__main__":
    main()