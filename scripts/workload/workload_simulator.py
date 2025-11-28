"""
Workload Simulator - Generate realistic e-commerce traffic
"""

import random
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from db_connection import CockroachDBConnection
from crud_operations import EcommerceCRUD
import json

class WorkloadSimulator:
    def __init__(self, db_connection: CockroachDBConnection):
        self.db = db_connection
        self.crud = EcommerceCRUD(db_connection)
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
    
    def _execute_create_order(self):
        """Execute create order operation"""
        # Get random customer
        custkey = random.randint(1, 1000)
        
        # Generate random items
        num_items = random.randint(1, 5)
        items = []
        for _ in range(num_items):
            partkey = random.randint(1, 1000)
            suppkey = random.randint(1, 1000)
            quantity = random.randint(1, 50)
            price = round(random.uniform(10.0, 1000.0), 2)
            items.append((partkey, suppkey, quantity, price))
        
        orderkey = self.crud.create_order(custkey, items)
        return ('create_order', orderkey is not None)
    
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
    
    def run_single_transaction(self):
        """Execute a single random transaction"""
        operation = self._get_random_operation()
        
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
            print(f"Error in transaction {operation}: {e}")
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