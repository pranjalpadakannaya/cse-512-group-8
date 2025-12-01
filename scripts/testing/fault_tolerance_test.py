"""
Automated Fault Tolerance Testing
Tests cluster behavior under node failures
"""

import time
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from cluster.node_controller import NodeController
from cluster.cluster_monitor import ClusterMonitor
from workload.workload_simulator import WorkloadSimulator
from workload.db_connection import CockroachDBConnection
import threading
import json

class FaultToleranceTest:
    def __init__(self):
        self.controller = NodeController()
        self.monitor = ClusterMonitor()
        self.results = []
    
    def test_single_node_failure(self, node_id, workload_duration=30):
        """
        Test cluster behavior when a single node fails
        
        Args:
            node_id: Node to fail
            workload_duration: How long to run workload (seconds)
        """
        print(f"\n{'='*70}")
        print(f"TEST: Single Node Failure (Node {node_id})")
        print(f"{'='*70}\n")
        
        test_result = {
            'test': 'single_node_failure',
            'node_id': node_id,
            'start_time': time.time(),
            'workload_duration': workload_duration
        }
        
        # Step 1: Verify cluster is healthy
        print("Step 1: Verifying cluster health...")
        self.monitor.print_cluster_summary()
        time.sleep(2)
        
        # Step 2: Start workload in background
        print(f"\nStep 2: Starting workload (duration: {workload_duration}s)...")
        db = CockroachDBConnection()
        simulator = WorkloadSimulator(db)
        
        workload_complete = threading.Event()
        workload_results = {}
        
        def run_workload():
            nonlocal workload_results
            # Calculate number of transactions based on duration
            # Assume ~50 TPS with 10 threads
            num_transactions = workload_duration * 50
            workload_results = simulator.run_workload(
                num_transactions=num_transactions,
                num_threads=10
            )
            workload_complete.set()
        
        workload_thread = threading.Thread(target=run_workload)
        workload_thread.start()
        
        time.sleep(5)  # Let workload start
        
        # Step 3: Kill the node
        print(f"\nStep 3: Killing node {node_id}...")
        failure_time = time.time()
        self.controller.kill_node(node_id)
        
        test_result['failure_time'] = failure_time
        
        # Step 4: Monitor cluster during failure
        print("\nStep 4: Monitoring cluster with failed node...")
        time.sleep(5)
        self.monitor.print_cluster_summary()
        
        # Step 5: Wait for workload to complete
        print("\nStep 5: Waiting for workload to complete...")
        workload_complete.wait()
        
        test_result['workload_results'] = workload_results
        test_result['end_time'] = time.time()
        
        # Step 6: Restart the failed node
        print(f"\nStep 6: Restarting node {node_id}...")
        recovery_start = time.time()
        self.controller.start_node(node_id)
        
        # Wait for node to rejoin
        time.sleep(10)
        
        test_result['recovery_time'] = time.time() - recovery_start
        
        # Step 7: Verify cluster recovered
        print("\nStep 7: Verifying cluster recovery...")
        self.monitor.print_cluster_summary()
        
        # Print test summary
        self._print_test_summary(test_result)
        
        db.close_all()
        self.results.append(test_result)
        
        return test_result
    
    def test_multiple_node_failure(self, node_ids, workload_duration=30):
        """
        Test cluster behavior when multiple nodes fail
        
        Args:
            node_ids: List of node IDs to fail
            workload_duration: How long to run workload (seconds)
        """
        print(f"\n{'='*70}")
        print(f"TEST: Multiple Node Failure (Nodes {node_ids})")
        print(f"{'='*70}\n")
        
        test_result = {
            'test': 'multiple_node_failure',
            'node_ids': node_ids,
            'start_time': time.time(),
            'workload_duration': workload_duration
        }
        
        # Verify cluster health
        print("Step 1: Verifying cluster health...")
        self.monitor.print_cluster_summary()
        time.sleep(2)
        
        # Start workload
        print(f"\nStep 2: Starting workload...")
        db = CockroachDBConnection()
        simulator = WorkloadSimulator(db)
        
        workload_complete = threading.Event()
        workload_results = {}
        
        def run_workload():
            nonlocal workload_results
            num_transactions = workload_duration * 50
            workload_results = simulator.run_workload(
                num_transactions=num_transactions,
                num_threads=10
            )
            workload_complete.set()
        
        workload_thread = threading.Thread(target=run_workload)
        workload_thread.start()
        
        time.sleep(5)
        
        # Kill nodes sequentially
        print(f"\nStep 3: Killing {len(node_ids)} nodes...")
        for i, node_id in enumerate(node_ids, 1):
            print(f"  Killing node {node_id} ({i}/{len(node_ids)})...")
            self.controller.kill_node(node_id)
            time.sleep(2)
        
        # Monitor
        print("\nStep 4: Monitoring cluster...")
        time.sleep(5)
        self.monitor.print_cluster_summary()
        
        # Wait for workload
        print("\nStep 5: Waiting for workload...")
        workload_complete.wait()
        
        test_result['workload_results'] = workload_results
        
        # Restart nodes
        print(f"\nStep 6: Restarting {len(node_ids)} nodes...")
        recovery_start = time.time()
        for node_id in node_ids:
            print(f"  Restarting node {node_id}...")
            self.controller.start_node(node_id)
            time.sleep(2)
        
        test_result['recovery_time'] = time.time() - recovery_start
        
        # Verify recovery
        time.sleep(10)
        print("\nStep 7: Verifying recovery...")
        self.monitor.print_cluster_summary()
        
        self._print_test_summary(test_result)
        
        db.close_all()
        self.results.append(test_result)
        
        return test_result
    
    def _print_test_summary(self, result):
        """Print test summary"""
        print(f"\n{'='*70}")
        print("TEST SUMMARY")
        print(f"{'='*70}")
        
        if 'workload_results' in result:
            wr = result['workload_results']
            print(f"Total Transactions: {wr['total']}")
            print(f"Successful: {wr['success']} ({wr['success']/wr['total']*100:.1f}%)")
            print(f"Failed: {wr['failed']} ({wr['failed']/wr['total']*100:.1f}%)")
        
        if 'recovery_time' in result:
            print(f"Recovery Time: {result['recovery_time']:.2f} seconds")
        
        print(f"{'='*70}\n")
    
    def save_results(self, filename='logs/workload/fault_tolerance_results.json'):
        """Save all test results to file"""
        with open(filename, 'w') as f:
            json.dump(self.results, f, indent=2)
        print(f"Results saved to {filename}")


def main():
    """Run fault tolerance tests"""
    tester = FaultToleranceTest()
    
    print("\nFAULT TOLERANCE TESTING SUITE")
    print("This will test cluster resilience under node failures\n")
    
    # Test 1: Single node failure
    input("Press Enter to start Test 1: Single Node Failure...")
    tester.test_single_node_failure(node_id=2, workload_duration=30)
    
    time.sleep(10)  # Cool down
    
    # Test 2: Multiple node failure
    input("\nPress Enter to start Test 2: Multiple Node Failure...")
    tester.test_multiple_node_failure(node_ids=[1, 2], workload_duration=30)
    
    # Save results
    tester.save_results()
    
    print("\n✓ All fault tolerance tests complete!")


if __name__ == "__main__":
    main()