"""
CockroachDB Cluster Monitoring Dashboard
Real-time visualization of cluster health, performance metrics, and node control
"""

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import sys
import os
import time
import json
from datetime import datetime

# Add parent directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from scripts.cluster.cluster_monitor import ClusterMonitor
from scripts.cluster.node_controller import NodeController
from scripts.workload.db_connection import CockroachDBConnection
from scripts.workload.crud_operations import EcommerceCRUD

# Page configuration
st.set_page_config(
    page_title="CockroachDB Cluster Monitor",
    page_icon="🪳",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1E88E5;
        text-align: center;
        padding: 1rem 0;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    .status-live {
        color: #4CAF50;
        font-weight: bold;
    }
    .status-dead {
        color: #F44336;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'monitor' not in st.session_state:
    st.session_state.monitor = ClusterMonitor()
if 'controller' not in st.session_state:
    st.session_state.controller = NodeController()
if 'db' not in st.session_state:
    st.session_state.db = CockroachDBConnection()
if 'crud' not in st.session_state:
    st.session_state.crud = EcommerceCRUD(st.session_state.db)

# Header
st.markdown('<div class="main-header">🪳 CockroachDB Cluster Monitor</div>', unsafe_allow_html=True)
st.markdown("---")

# Sidebar
with st.sidebar:
    st.header("⚙️ Dashboard Controls")
    
    # Refresh controls
    st.subheader("Refresh Settings")
    auto_refresh = st.checkbox("Auto-refresh", value=True)
    refresh_interval = st.slider("Refresh interval (seconds)", 5, 60, 10)
    
    if st.button("🔄 Refresh Now"):
        st.rerun()
    
    st.markdown("---")
    
    # Node control section
    st.subheader("🎛️ Node Control")
    local_nodes = st.session_state.controller.get_local_nodes()
    
    if local_nodes:
        node_options = {f"Node {n['id']} ({n['host']}:{n['port']})": n['id'] 
                       for n in local_nodes}
        selected_node = st.selectbox("Select Node", list(node_options.keys()))
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🟢 Start"):
                node_id = node_options[selected_node]
                with st.spinner(f"Starting node {node_id}..."):
                    if st.session_state.controller.start_node(node_id):
                        st.success(f"Node {node_id} started!")
                    else:
                        st.error(f"Failed to start node {node_id}")
        
        with col2:
            if st.button("🔴 Stop"):
                node_id = node_options[selected_node]
                with st.spinner(f"Stopping node {node_id}..."):
                    if st.session_state.controller.stop_node(node_id):
                        st.success(f"Node {node_id} stopped!")
                    else:
                        st.error(f"Failed to stop node {node_id}")
        
        if st.button("⚠️ Kill (Force)", type="secondary"):
            node_id = node_options[selected_node]
            if st.button("⚠️ Confirm Kill"):
                with st.spinner(f"Killing node {node_id}..."):
                    if st.session_state.controller.kill_node(node_id):
                        st.warning(f"Node {node_id} killed!")
                    else:
                        st.error(f"Failed to kill node {node_id}")
    else:
        st.info("No local nodes detected")

# Main content
tab1, tab2, tab3, tab4 = st.tabs(["📊 Cluster Overview", "📈 Performance Metrics", 
                                   "🔍 Query Console", "🧪 Testing"])

# Tab 1: Cluster Overview
with tab1:
    st.header("Cluster Status")
    
    # Get node status
    node_status = st.session_state.monitor.get_node_status()
    
    if node_status and 'nodes' in node_status:
        nodes = node_status['nodes']
        
        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)
        
        live_nodes = sum(1 for n in nodes if n.get('liveness', {}).get('is_live', False))
        total_nodes = len(nodes)
        
        with col1:
            st.metric("Total Nodes", total_nodes)
        with col2:
            st.metric("Live Nodes", live_nodes, delta=live_nodes - total_nodes if live_nodes < total_nodes else None)
        with col3:
            st.metric("Dead Nodes", total_nodes - live_nodes)
        with col4:
            health_pct = (live_nodes / total_nodes * 100) if total_nodes > 0 else 0
            st.metric("Health", f"{health_pct:.0f}%")
        
        st.markdown("---")
        
        # Node details table
        st.subheader("Node Details")
        
        node_data = []
        for node in nodes:
            node_id = node['desc']['node_id']
            address = node['desc']['address']['address_field']
            is_live = node.get('liveness', {}).get('is_live', False)
            started_at = node.get('started_at', '')
            
            if started_at:
                start_time = datetime.fromisoformat(started_at.replace('Z', '+00:00'))
                uptime = datetime.now(start_time.tzinfo) - start_time
                uptime_str = str(uptime).split('.')[0]
            else:
                uptime_str = 'Unknown'
            
            node_data.append({
                'Node ID': node_id,
                'Address': address,
                'Status': '🟢 LIVE' if is_live else '🔴 DEAD',
                'Uptime': uptime_str
            })
        
        df_nodes = pd.DataFrame(node_data)
        st.dataframe(df_nodes, use_container_width=True, hide_index=True)
        
        # Node status visualization
        st.subheader("Node Status Visualization")
        
        fig = go.Figure(data=[go.Pie(
            labels=['Live Nodes', 'Dead Nodes'],
            values=[live_nodes, total_nodes - live_nodes],
            marker=dict(colors=['#4CAF50', '#F44336']),
            hole=.4
        )])
        fig.update_layout(
            title="Node Health Distribution",
            height=400
        )
        st.plotly_chart(fig, use_container_width=True)
    
    else:
        st.error("❌ Unable to retrieve cluster status")
        st.info("Please check your cluster configuration and network connectivity")

# Tab 2: Performance Metrics
with tab2:
    st.header("Performance Metrics")
    
    # Get performance data from CRUD operations
    metrics = st.session_state.crud.get_performance_metrics()
    
    if any(m['count'] > 0 for m in metrics.values()):
        # Operation counts
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Create Operations", metrics.get('create', {}).get('count', 0))
        with col2:
            st.metric("Read Operations", metrics.get('read', {}).get('count', 0))
        with col3:
            st.metric("Update Operations", metrics.get('update', {}).get('count', 0))
        with col4:
            st.metric("Delete Operations", metrics.get('delete', {}).get('count', 0))
        
        st.markdown("---")
        
        # Latency charts
        st.subheader("Operation Latency (ms)")
        
        latency_data = []
        for op_type, stats in metrics.items():
            if stats['count'] > 0:
                latency_data.append({
                    'Operation': op_type.upper(),
                    'Average': stats['avg_latency'] * 1000,
                    'Min': stats['min_latency'] * 1000,
                    'Max': stats['max_latency'] * 1000,
                    'P95': stats['p95_latency'] * 1000
                })
        
        if latency_data:
            df_latency = pd.DataFrame(latency_data)
            
            fig = px.bar(df_latency, x='Operation', y=['Average', 'P95', 'Max'],
                        title="Latency Comparison by Operation Type",
                        barmode='group',
                        labels={'value': 'Latency (ms)', 'variable': 'Metric'})
            st.plotly_chart(fig, use_container_width=True)
            
            # Latency table
            st.dataframe(df_latency, use_container_width=True, hide_index=True)
        
        # Reset metrics button
        if st.button("🔄 Reset Metrics"):
            st.session_state.crud.reset_metrics()
            st.success("Metrics reset!")
            st.rerun()
    
    else:
        st.info("📊 No performance data available yet. Run some workload operations to see metrics.")

# Tab 3: Query Console
with tab3:
    st.header("Query Console")
    
    st.markdown("""
    Execute queries against the cluster and see real-time results.
    """)
    
    # Quick query templates
    st.subheader("Quick Queries")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("Show All Tables"):
            query = "SHOW TABLES FROM ecommerce;"
            st.code(query, language="sql")
            result = st.session_state.db.execute_query(query)
            if result:
                df = pd.DataFrame(result, columns=['Schema', 'Table', 'Type', 'Owner', 'Est. Rows', 'Locality'])
                st.dataframe(df, use_container_width=True)
        
        if st.button("Recent Orders"):
            query = "SELECT * FROM ecommerce.ORDERS ORDER BY O_ORDERDATE DESC LIMIT 10;"
            st.code(query, language="sql")
            result = st.session_state.db.execute_query(query)
            if result:
                st.dataframe(pd.DataFrame(result), use_container_width=True)
    
    with col2:
        if st.button("Top Customers"):
            query = """
            SELECT c.C_NAME, COUNT(o.O_ORDERKEY) as num_orders, SUM(o.O_TOTALPRICE) as total_spent
            FROM ecommerce.CUSTOMER c
            JOIN ecommerce.ORDERS o ON c.C_CUSTKEY = o.O_CUSTKEY
            GROUP BY c.C_CUSTKEY, c.C_NAME
            ORDER BY total_spent DESC
            LIMIT 10;
            """
            st.code(query, language="sql")
            result = st.session_state.db.execute_query(query)
            if result:
                df = pd.DataFrame(result, columns=['Customer', 'Orders', 'Total Spent'])
                st.dataframe(df, use_container_width=True)
        
        if st.button("Table Row Counts"):
            query = """
            SELECT 'ORDERS' as table_name, COUNT(*) as row_count FROM ecommerce.ORDERS
            UNION ALL SELECT 'LINEITEM', COUNT(*) FROM ecommerce.LINEITEM
            UNION ALL SELECT 'CUSTOMER', COUNT(*) FROM ecommerce.CUSTOMER
            UNION ALL SELECT 'PART', COUNT(*) FROM ecommerce.PART;
            """
            st.code(query, language="sql")
            result = st.session_state.db.execute_query(query)
            if result:
                df = pd.DataFrame(result, columns=['Table', 'Row Count'])
                st.dataframe(df, use_container_width=True)
    
    st.markdown("---")
    
    # Custom query
    st.subheader("Custom Query")
    custom_query = st.text_area("Enter SQL query:", height=150,
                                placeholder="SELECT * FROM ecommerce.orders LIMIT 10;")
    
    if st.button("▶️ Execute Query"):
        if custom_query:
            try:
                with st.spinner("Executing query..."):
                    result = st.session_state.db.execute_query(custom_query)
                    if result:
                        st.success(f"Query returned {len(result)} rows")
                        st.dataframe(pd.DataFrame(result), use_container_width=True)
                    else:
                        st.success("Query executed successfully (no results)")
            except Exception as e:
                st.error(f"Query error: {e}")
        else:
            st.warning("Please enter a query")

# Tab 4: Testing
with tab4:
    st.header("Fault Tolerance Testing")
    
    st.markdown("""
    Test cluster resilience by simulating node failures while running workloads.
    """)
    
    # Workload configuration
    st.subheader("Workload Configuration")
    
    col1, col2 = st.columns(2)
    with col1:
        num_transactions = st.number_input("Number of transactions", 
                                          min_value=10, max_value=10000, 
                                          value=500, step=50)
    with col2:
        num_threads = st.number_input("Concurrent threads", 
                                     min_value=1, max_value=50, 
                                     value=10, step=1)
    
    # Workload mix
    st.subheader("Workload Mix (%)")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        create_pct = st.slider("Create", 0, 100, 30)
    with col2:
        read_pct = st.slider("Read", 0, 100, 40)
    with col3:
        update_pct = st.slider("Update", 0, 100, 20)
    with col4:
        analytics_pct = st.slider("Analytics", 0, 100, 10)
    
    total_pct = create_pct + read_pct + update_pct + analytics_pct
    if total_pct != 100:
        st.warning(f"⚠️ Workload percentages must sum to 100% (current: {total_pct}%)")
    
    # Run workload
    if st.button("▶️ Run Workload", disabled=(total_pct != 100)):
        from scripts.workload.workload_simulator import WorkloadSimulator
        
        simulator = WorkloadSimulator(st.session_state.db)
        simulator.workload_mix = {
            'create_order': create_pct,
            'read_order': read_pct,
            'update_order': update_pct,
            'analytics': analytics_pct
        }
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        with st.spinner("Running workload..."):
            # We can't easily show real-time progress, so just run it
            results = simulator.run_workload(
                num_transactions=num_transactions,
                num_threads=num_threads
            )
            
            progress_bar.progress(100)
            status_text.success("✅ Workload complete!")
            
            # Show results
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Transactions", results['total'])
            with col2:
                st.metric("Successful", results['success'], 
                         delta=f"{results['success']/results['total']*100:.1f}%")
            with col3:
                st.metric("Failed", results['failed'])

# Auto-refresh
if auto_refresh:
    time.sleep(refresh_interval)
    st.rerun()

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: gray; padding: 1rem 0;'>
    <p>CockroachDB Distributed Order Processing System | CSE 512 Project</p>
    <p>Team 8 | Fall 2024</p>
</div>
""", unsafe_allow_html=True)