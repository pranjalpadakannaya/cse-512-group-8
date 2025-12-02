"""
Node Controller - Start, stop, and restart nodes for fault tolerance testing
FIXED: Returns all nodes from cluster_config.json for dashboard
"""

import subprocess
import time
import requests
import json

class NodeController:
    def __init__(self, config_file='config/cluster_config.json'):
        """Initialize node controller"""
        with open(config_file, 'r') as f:
            self.config = json.load(f)
        
        self.nodes = self.config['nodes']
        self.primary_node = self.config['primary_node']
    
    def get_local_nodes(self):
        """
        Get ALL nodes from cluster configuration
        FIXED: Returns all nodes so dashboard can display them
        
        Note: For actual control, only nodes on this laptop can be controlled
        """
        # Return all configured nodes for display
        return self.nodes
    
    def _is_node_local(self, node):
        """Check if a node is running on this laptop"""
        import socket
        try:
            local_ip = socket.gethostbyname(socket.gethostname())
            local_ips = [local_ip, 'localhost', '127.0.0.1', '192.168.0.140']  # Add known local IPs
            
            return node['host'] in local_ips
        except:
            return False
    
    def stop_node(self, node_id):
        """Stop a specific node (local nodes only) - graceful shutdown"""
        node = next((n for n in self.nodes if n['id'] == node_id), None)
        
        if not node:
            print(f"Node {node_id} not found in configuration")
            return False
        
        # Check if node is local
        if not self._is_node_local(node):
            print(f"Node {node_id} is on a different laptop - cannot control remotely")
            return False
        
        print(f"Stopping node {node_id} ({node['host']}:{node['port']})...")
        
        try:
            # Find process by port
            find_cmd = f"lsof -ti :{node['port']}"
            result = subprocess.run(find_cmd, shell=True, capture_output=True, text=True)
            
            if result.stdout.strip():
                pids = result.stdout.strip().split('\n')
                # Use first PID (cockroach process)
                pid = pids[0]
                
                # Graceful shutdown with SIGTERM
                kill_cmd = f"kill -TERM {pid}"
                subprocess.run(kill_cmd, shell=True)
                print(f"✓ Node {node_id} stopped gracefully (PID: {pid})")
                return True
            else:
                print(f"No process found on port {node['port']}")
                return False
        
        except Exception as e:
            print(f"Error stopping node: {e}")
            return False
    
    def start_node(self, node_id):
        """Start a specific node (local nodes only)"""
        node = next((n for n in self.nodes if n['id'] == node_id), None)
        
        if not node:
            print(f"Node {node_id} not found in configuration")
            return False
        
        # Check if node is local
        if not self._is_node_local(node):
            print(f"Node {node_id} is on a different laptop - cannot control remotely")
            return False
        
        print(f"Starting node {node_id} ({node['host']}:{node['port']})...")
        
        # Build join addresses
        join_addrs = ','.join([f"{n['host']}:{n['port']}" for n in self.nodes])
        
        # Build start command
        cmd = [
            'cockroach', 'start',
            '--insecure',
            f"--store=nodes/node{node_id}",
            f"--listen-addr={node['host']}:{node['port']}",
            f"--http-addr={node['host']}:{node['http_port']}",
            f"--join={join_addrs}",
            '--background'
        ]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                print(f"✓ Node {node_id} started successfully")
                time.sleep(2)  # Wait for node to start
                return True
            else:
                print(f"✗ Failed to start node {node_id}: {result.stderr}")
                return False
        
        except Exception as e:
            print(f"Error starting node: {e}")
            return False
    
    def restart_node(self, node_id):
        """Restart a specific node"""
        print(f"Restarting node {node_id}...")
        
        if self.stop_node(node_id):
            time.sleep(2)  # Wait before restart
            return self.start_node(node_id)
        
        return False
    
    def kill_node(self, node_id):
        """Forcefully kill a node process (simulates crash)"""
        node = next((n for n in self.nodes if n['id'] == node_id), None)
        
        if not node:
            print(f"Node {node_id} not found")
            return False
        
        # Check if node is local
        if not self._is_node_local(node):
            print(f"Node {node_id} is on a different laptop - cannot control remotely")
            return False
        
        print(f"Forcefully killing node {node_id}...")
        
        try:
            # Find process by port
            find_cmd = f"lsof -ti :{node['port']}"
            result = subprocess.run(find_cmd, shell=True, capture_output=True, text=True)
            
            if result.stdout.strip():
                pids = result.stdout.strip().split('\n')
                # Kill first PID (cockroach main process)
                pid = pids[0]
                
                kill_cmd = f"kill -9 {pid}"
                kill_result = subprocess.run(kill_cmd, shell=True, capture_output=True, text=True)
                
                if kill_result.returncode == 0:
                    print(f"✓ Node {node_id} process killed (PID: {pid})")
                    return True
                else:
                    print(f"✗ Failed to kill process: {kill_result.stderr}")
                    return False
            else:
                print(f"✗ No process found on port {node['port']}")
                return False
        
        except Exception as e:
            print(f"Error killing node: {e}")
            return False


def interactive_menu():
    """Interactive menu for node control"""
    controller = NodeController()
    all_nodes = controller.get_local_nodes()
    
    while True:
        print(f"\n{'='*60}")
        print("NODE CONTROLLER - FAULT INJECTION TOOL")
        print(f"{'='*60}")
        print("\nAll Cluster Nodes:")
        for node in all_nodes:
            laptop = node.get('laptop', 'Unknown')
            print(f"  Node {node['id']}: {node['host']}:{node['port']} ({laptop})")
        
        print("\nOptions:")
        print("1. Stop a node (graceful shutdown)")
        print("2. Start a node")
        print("3. Restart a node")
        print("4. Kill a node (simulate crash)")
        print("5. Exit")
        
        choice = input("\nEnter choice (1-5): ")
        
        if choice == '5':
            break
        
        if choice in ['1', '2', '3', '4']:
            node_id = input("Enter node ID: ")
            
            try:
                node_id = int(node_id)
                
                if choice == '1':
                    controller.stop_node(node_id)
                elif choice == '2':
                    controller.start_node(node_id)
                elif choice == '3':
                    controller.restart_node(node_id)
                elif choice == '4':
                    controller.kill_node(node_id)
            
            except ValueError:
                print("Invalid node ID")


if __name__ == "__main__":
    interactive_menu()