"""
Custom workload scenarios for specific testing
"""

from workload_simulator import WorkloadSimulator
from .db_connection import CockroachDBConnection
import time

def scenario_high_read(db):
    """Simulate high-read workload (90% reads, 10% writes)"""
    simulator = WorkloadSimulator(db)
    simulator.workload_mix = {
        'create_order': 5,
        'read_order': 70,
        'update_order': 5,
        'analytics': 20
    }
    
    print("\n=== SCENARIO: High-Read Workload ===\n")
    return simulator.run_workload(num_transactions=500, num_threads=15)

def scenario_high_write(db):
    """Simulate high-write workload (70% writes, 30% reads)"""
    simulator = WorkloadSimulator(db)
    simulator.workload_mix = {
        'create_order': 50,
        'read_order': 20,
        'update_order': 20,
        'analytics': 10
    }
    
    print("\n=== SCENARIO: High-Write Workload ===\n")
    return simulator.run_workload(num_transactions=500, num_threads=10)

def scenario_analytics_heavy(db):
    """Simulate analytics-heavy workload"""
    simulator = WorkloadSimulator(db)
    simulator.workload_mix = {
        'create_order': 10,
        'read_order': 30,
        'update_order': 10,
        'analytics': 50
    }
    
    print("\n=== SCENARIO: Analytics-Heavy Workload ===\n")
    return simulator.run_workload(num_transactions=200, num_threads=5)

def run_all_scenarios():
    """Run all test scenarios"""
    db = CockroachDBConnection()
    
    results = {}
    
    results['high_read'] = scenario_high_read(db)
    time.sleep(5)  # Cool down period
    
    results['high_write'] = scenario_high_write(db)
    time.sleep(5)
    
    results['analytics_heavy'] = scenario_analytics_heavy(db)
    
    db.close_all()
    
    return results

if __name__ == "__main__":
    run_all_scenarios()