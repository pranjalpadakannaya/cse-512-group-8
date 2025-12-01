"""
Performance Benchmarking Script
Measure cluster performance under various conditions
"""

import time
import json
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from workload.db_connection import CockroachDBConnection
from workload.workload_simulator import WorkloadSimulator

class PerformanceBenchmark:
    def __init__(self):
        self.results = []
    
    def benchmark_concurrency(self, thread_counts=[1, 5, 10, 20, 50]):
        """Benchmark performance with different thread counts"""
        print(f"\n{'='*70}")
        print("BENCHMARK: Concurrency Scaling")
        print(f"{'='*70}\n")
        
        db = CockroachDBConnection()
        
        for threads in thread_counts:
            print(f"\nTesting with {threads} threads...")
            
            simulator = WorkloadSimulator(db)
            result = simulator.run_workload(
                num_transactions=500,
                num_threads=threads
            )
            
            result['thread_count'] = threads
            self.results.append({
                'benchmark': 'concurrency',
                'threads': threads,
                'result': result
            })
            
            time.sleep(5)  # Cool down
        
        db.close_all()
    
    def benchmark_workload_types(self):
        """Benchmark different workload types"""
        print(f"\n{'='*70}")
        print("BENCHMARK: Workload Types")
        print(f"{'='*70}\n")
        
        db = CockroachDBConnection()
        
        workload_types = {
            'read_heavy': {'create_order': 10, 'read_order': 70, 'update_order': 10, 'analytics': 10},
            'write_heavy': {'create_order': 50, 'read_order': 20, 'update_order': 20, 'analytics': 10},
            'balanced': {'create_order': 30, 'read_order': 40, 'update_order': 20, 'analytics': 10},
            'analytics_heavy': {'create_order': 10, 'read_order': 20, 'update_order': 10, 'analytics': 60}
        }
        
        for workload_name, mix in workload_types.items():
            print(f"\nTesting {workload_name} workload...")
            
            simulator = WorkloadSimulator(db)
            simulator.workload_mix = mix
            
            result = simulator.run_workload(
                num_transactions=500,
                num_threads=10
            )
            
            self.results.append({
                'benchmark': 'workload_type',
                'type': workload_name,
                'mix': mix,
                'result': result
            })
            
            time.sleep(5)
        
        db.close_all()
    
    def save_results(self, filename='logs/workload/performance_benchmark.json'):
        """Save benchmark results"""
        with open(filename, 'w') as f:
            json.dump(self.results, f, indent=2)
        print(f"\nResults saved to {filename}")


def main():
    """Run all benchmarks"""
    benchmark = PerformanceBenchmark()
    
    print("\nPERFORMANCE BENCHMARKING SUITE")
    print("This will measure cluster performance\n")
    
    input("Press Enter to start concurrency benchmark...")
    benchmark.benchmark_concurrency()
    
    input("\nPress Enter to start workload type benchmark...")
    benchmark.benchmark_workload_types()
    
    benchmark.save_results()
    
    print("\n✓ All benchmarks complete!")


if __name__ == "__main__":
    main()