"""
CockroachDB Cluster Health Monitor
Monitors node status, replication, and cluster health
"""

import requests
import json
import time
from datetime import datetime

class ClusterMonitor:
    def __init__(self, config_file='config/cluster_config.json'):
        """Initialize cluster monitor"""
        with open(config_file, 'r') as f:
            self.config = json.load(f)
        
        self.nodes = self.config['nodes']
        self.primary_node = self.config['primary_node']
        
        # Get Admin UI URL from primary node
        primary_host = self.primary_node.split(':')[0]
        primary_http = next(n['http_port'] for n in self.nodes 
                           if f"{n['host']}:{n['port']}" == self.primary_node)
        self.admin_url = f"http://{primary_host}:{primary_http}"
    
    def get_node_status(self):
        """Get status of all nodes in the cluster"""
        try:
            response = requests.get(f"{self.admin_url}/_status/nodes", timeout=5)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Error getting node status: {response.status_code}")
                return None
        except Exception as e:
            print(f"Error connecting to cluster: {e}")
            return None
    
    def get_cluster_metrics(self):
        """Get cluster-wide metrics"""
        try:
            response = requests.get(f"{self.admin_url}/_status/vars", timeout=5)
            if response.status_code == 200:
                return response.json()
            else:
                return None
        except Exception as e:
            print(f"Error getting metrics: {e}")
            return None
    
    def get_replication_status(self):
        """Get replication status for all ranges"""
        try:
            response = requests.get(f"{self.admin_url}/_status/ranges/local", timeout=5)
            if response.status_code == 200:
                return response.json()
            else:
                return None
        except Exception as e:
            print(f"Error getting replication status: {e}")
            return None
    
    def print_cluster_summary(self):
        """Print a human-readable cluster summary"""
        print(f"\n{'='*70}")
        print(f"COCKROACHDB CLUSTER STATUS - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*70}\n")
        
        node_status = self.get_node_status()
        
        if node_status and 'nodes' in node_status:
            nodes = node_status['nodes']
            
            print(f"Total Nodes: {len(nodes)}")
            print(f"\nNode Details:")
            print(f"{'ID':<5} {'Address':<25} {'Status':<10} {'Uptime':<15}")
            print(f"{'-'*70}")
            
            for node in nodes:
                node_id = node['desc']['node_id']
                address = node['desc']['address']['address_field']
                is_live = 'LIVE' if node.get('liveness', {}).get('is_live', False) else 'DEAD'
                started_at = node.get('started_at', '')
                
                if started_at:
                    start_time = datetime.fromisoformat(started_at.replace('Z', '+00:00'))
                    uptime = datetime.now(start_time.tzinfo) - start_time
                    uptime_str = str(uptime).split('.')[0]  # Remove microseconds
                else:
                    uptime_str = 'Unknown'
                
                print(f"{node_id:<5} {address:<25} {is_live:<10} {uptime_str:<15}")
        
        else:
            print("❌ Unable to retrieve node status")
        
        print(f"\n{'='*70}\n")
    
    def monitor_continuously(self, interval=10):
        """Monitor cluster status continuously"""
        print(f"Starting continuous monitoring (interval: {interval} seconds)")
        print("Press Ctrl+C to stop\n")
        
        try:
            while True:
                self.print_cluster_summary()
                time.sleep(interval)
        except KeyboardInterrupt:
            print("\n\nMonitoring stopped by user")


def main():
    """Main function"""
    monitor = ClusterMonitor()
    
    # Print initial status
    monitor.print_cluster_summary()
    
    # Ask if user wants continuous monitoring
    choice = input("\nStart continuous monitoring? (y/n): ")
    if choice.lower() == 'y':
        interval = input("Enter monitoring interval in seconds (default 10): ")
        interval = int(interval) if interval.isdigit() else 10
        monitor.monitor_continuously(interval)


if __name__ == "__main__":
    main()