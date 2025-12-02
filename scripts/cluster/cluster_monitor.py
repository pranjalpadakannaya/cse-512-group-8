"""
CockroachDB Cluster Health Monitor
Monitors node status, replication, and cluster health
UPDATED FOR COCKROACHDB v25.3 API FORMAT
"""

import requests
import json
import time
from datetime import datetime, timezone

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
    
    def _is_node_live(self, updated_at_ns, threshold_seconds=600):
        """
        Determine if node is live based on updatedAt timestamp
        CockroachDB v25.3 doesn't have explicit 'liveness' field in /_status/nodes
        """
        if not updated_at_ns:
            return False
        
        try:
            # Convert nanoseconds to seconds
            updated_at_seconds = int(updated_at_ns) / 1_000_000_000
            current_time = datetime.now(timezone.utc).timestamp()
            
            # If updated within threshold (default 10 minutes), consider live
            age_seconds = current_time - updated_at_seconds
            return age_seconds < threshold_seconds
        except:
            return False
    
    def get_node_status(self):
        """Get status of all nodes in the cluster"""
        try:
            response = requests.get(f"{self.admin_url}/_status/nodes", timeout=5)
            if response.status_code == 200:
                data = response.json()
                
                # Parse and enrich with liveness information
                if 'nodes' in data:
                    for node in data['nodes']:
                        # Add computed liveness based on updatedAt
                        updated_at = node.get('updatedAt')
                        node['is_live'] = self._is_node_live(updated_at)
                
                return data
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
            
            # Count live vs dead nodes
            live_count = sum(1 for n in nodes if n.get('is_live', False))
            dead_count = len(nodes) - live_count
            
            print(f"Total Nodes: {len(nodes)}")
            print(f"Live Nodes: {live_count}")
            print(f"Dead Nodes: {dead_count}")
            print(f"\nNode Details:")
            print(f"{'ID':<5} {'Address':<30} {'Status':<10} {'Last Updated':<25}")
            print(f"{'-'*70}")
            
            for node in nodes:
                # v25.3 API format: desc.nodeId and desc.address.addressField
                node_id = node['desc']['nodeId']
                address = node['desc']['address']['addressField']
                is_live = 'LIVE' if node.get('is_live', False) else 'DEAD'
                
                # Convert nanosecond timestamp to readable format
                updated_at_ns = node.get('updatedAt', '')
                if updated_at_ns:
                    try:
                        updated_at_seconds = int(updated_at_ns) / 1_000_000_000
                        last_updated = datetime.fromtimestamp(updated_at_seconds).strftime('%Y-%m-%d %H:%M:%S')
                    except:
                        last_updated = 'Unknown'
                else:
                    last_updated = 'Unknown'
                
                print(f"{node_id:<5} {address:<30} {is_live:<10} {last_updated:<25}")
        
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