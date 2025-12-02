"""
CRUD Operations for E-Commerce Order Processing
UPDATED FOR CORRECT TPC-H COLUMN NAMES
"""

import random
import time
from datetime import datetime, timedelta
from decimal import Decimal
from scripts.workload.db_connection import CockroachDBConnection

class EcommerceCRUD:
    def __init__(self, db: CockroachDBConnection):
        self.db = db
        self.metrics = {
            'create': [],
            'read': [],
            'update': [],
            'delete': []
        }
    
    # ==================== CREATE Operations ====================
    
    def create_order(self, custkey: int, items: list) -> int:
        """
        Create a new order with line items
        
        Args:
            custkey: Customer key (C_CUSTKEY)
            items: List of (partkey, suppkey, quantity, price) tuples
        
        Returns:
            O_ORDERKEY of created order
        """
        start_time = time.time()
        
        # Generate order data
        orderkey = random.randint(100000, 999999)
        orderstatus = 'O'  # Open
        totalprice = sum(item[3] * item[2] for item in items)
        orderdate = datetime.now().strftime('%Y-%m-%d')
        orderpriority = random.choice(['1-URGENT', '2-HIGH', '3-MEDIUM', '4-NOT SPECIFIED', '5-LOW'])
        clerk = f"Clerk#{random.randint(1, 1000):09d}"
        shippriority = random.randint(0, 1)
        comment = f"Order created at {datetime.now()}"
        
        operations = []
        
        # Insert order (using O_* columns)
        order_query = """
        INSERT INTO ORDERS (O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, 
                          O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        operations.append((order_query, (orderkey, custkey, orderstatus, totalprice, 
                                        orderdate, orderpriority, clerk, shippriority, comment)))
        
        # Insert line items (using L_* columns)
        for i, (partkey, suppkey, quantity, price) in enumerate(items, 1):
            lineitem_query = """
            INSERT INTO LINEITEM (L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY,
                                L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS,
                                L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            extendedprice = price * quantity
            discount = round(random.uniform(0.0, 0.1), 2)
            tax = round(random.uniform(0.0, 0.08), 2)
            returnflag = random.choice(['R', 'A', 'N'])
            linestatus = 'O'
            shipdate = (datetime.now() + timedelta(days=random.randint(1, 30))).strftime('%Y-%m-%d')
            commitdate = (datetime.now() + timedelta(days=random.randint(10, 40))).strftime('%Y-%m-%d')
            receiptdate = (datetime.now() + timedelta(days=random.randint(15, 50))).strftime('%Y-%m-%d')
            shipinstruct = random.choice(['DELIVER IN PERSON', 'COLLECT COD', 'NONE', 'TAKE BACK RETURN'])
            shipmode = random.choice(['AIR', 'MAIL', 'SHIP', 'TRUCK', 'RAIL', 'REG AIR', 'FOB'])
            lineitem_comment = f"LineItem {i}"
            
            operations.append((lineitem_query, (orderkey, partkey, suppkey, i, quantity,
                                               extendedprice, discount, tax, returnflag, linestatus,
                                               shipdate, commitdate, receiptdate, shipinstruct, 
                                               shipmode, lineitem_comment)))
        
        try:
            self.db.execute_transaction(operations)
            elapsed = time.time() - start_time
            self.metrics['create'].append(elapsed)
            return orderkey
        except Exception as e:
            print(f"Error creating order: {e}")
            return None
    
    def create_customer(self, custkey: int = None) -> int:
        """Create a new customer (using C_* columns)"""
        start_time = time.time()
        
        if custkey is None:
            custkey = random.randint(200000, 999999)
        
        name = f"Customer#{custkey:09d}"
        address = f"{random.randint(1, 999)} Main St"
        nationkey = random.randint(0, 24)
        phone = f"{random.randint(10,99)}-{random.randint(100,999)}-{random.randint(1000,9999)}"
        acctbal = round(random.uniform(-999.99, 9999.99), 2)
        mktsegment = random.choice(['AUTOMOBILE', 'BUILDING', 'FURNITURE', 'MACHINERY', 'HOUSEHOLD'])
        comment = f"Customer created {datetime.now()}"
        
        query = """
        INSERT INTO CUSTOMER (C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, C_COMMENT)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        try:
            self.db.execute_query(query, (custkey, name, address, nationkey, phone, 
                                         acctbal, mktsegment, comment), fetch=False)
            elapsed = time.time() - start_time
            self.metrics['create'].append(elapsed)
            return custkey
        except Exception as e:
            print(f"Error creating customer: {e}")
            return None
    
    # ==================== READ Operations ====================
    
    def get_order_details(self, orderkey: int):
        """Get full order details including line items (using O_* and C_* columns)"""
        start_time = time.time()
        
        query = """
        SELECT o.*, c.C_NAME as customer_name, c.C_MKTSEGMENT
        FROM ORDERS o
        JOIN CUSTOMER c ON o.O_CUSTKEY = c.C_CUSTKEY
        WHERE o.O_ORDERKEY = %s
        """
        
        try:
            order = self.db.execute_query(query, (orderkey,))
            
            # Get line items (using L_*, P_*, S_* columns)
            lineitem_query = """
            SELECT l.*, p.P_NAME as part_name, s.S_NAME as supplier_name
            FROM LINEITEM l
            JOIN PART p ON l.L_PARTKEY = p.P_PARTKEY
            JOIN SUPPLIER s ON l.L_SUPPKEY = s.S_SUPPKEY
            WHERE l.L_ORDERKEY = %s
            """
            lineitems = self.db.execute_query(lineitem_query, (orderkey,))
            
            elapsed = time.time() - start_time
            self.metrics['read'].append(elapsed)
            
            return {
                'order': order,
                'lineitems': lineitems
            }
        except Exception as e:
            print(f"Error reading order: {e}")
            return None
    
    def get_customer_orders(self, custkey: int, limit: int = 10):
        """Get recent orders for a customer (using O_* and C_* columns)"""
        start_time = time.time()
        
        query = """
        SELECT * FROM ORDERS
        WHERE O_CUSTKEY = %s
        ORDER BY O_ORDERDATE DESC
        LIMIT %s
        """
        
        try:
            result = self.db.execute_query(query, (custkey, limit))
            elapsed = time.time() - start_time
            self.metrics['read'].append(elapsed)
            return result
        except Exception as e:
            print(f"Error reading customer orders: {e}")
            return None
    
    def search_parts(self, part_type: str = None, max_price: float = None):
        """Search for parts by type and price (using P_* columns)"""
        start_time = time.time()
        
        conditions = []
        params = []
        
        if part_type:
            conditions.append("P_TYPE LIKE %s")
            params.append(f"%{part_type}%")
        
        if max_price:
            conditions.append("P_RETAILPRICE <= %s")
            params.append(max_price)
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        query = f"""
        SELECT * FROM PART
        WHERE {where_clause}
        LIMIT 100
        """
        
        try:
            result = self.db.execute_query(query, tuple(params) if params else None)
            elapsed = time.time() - start_time
            self.metrics['read'].append(elapsed)
            return result
        except Exception as e:
            print(f"Error searching parts: {e}")
            return None
    
    # ==================== UPDATE Operations ====================
    
    def update_order_status(self, orderkey: int, new_status: str):
        """Update order status (O -> P -> F)"""
        start_time = time.time()
        
        query = """
        UPDATE ORDERS
        SET O_ORDERSTATUS = %s
        WHERE O_ORDERKEY = %s
        """
        
        try:
            self.db.execute_query(query, (new_status, orderkey), fetch=False)
            elapsed = time.time() - start_time
            self.metrics['update'].append(elapsed)
            return True
        except Exception as e:
            print(f"Error updating order status: {e}")
            return False
    
    def update_customer_balance(self, custkey: int, amount: float):
        """Update customer account balance (using C_* columns)"""
        start_time = time.time()
        
        query = """
        UPDATE CUSTOMER
        SET C_ACCTBAL = C_ACCTBAL + %s
        WHERE C_CUSTKEY = %s
        """
        
        try:
            self.db.execute_query(query, (amount, custkey), fetch=False)
            elapsed = time.time() - start_time
            self.metrics['update'].append(elapsed)
            return True
        except Exception as e:
            print(f"Error updating customer balance: {e}")
            return False
    
    def update_inventory(self, partkey: int, suppkey: int, quantity_delta: int):
        """Update part inventory (using PS_* columns)"""
        start_time = time.time()
        
        query = """
        UPDATE PARTSUPP
        SET PS_AVAILQTY = PS_AVAILQTY + %s
        WHERE PS_PARTKEY = %s AND PS_SUPPKEY = %s
        """
        
        try:
            self.db.execute_query(query, (quantity_delta, partkey, suppkey), fetch=False)
            elapsed = time.time() - start_time
            self.metrics['update'].append(elapsed)
            return True
        except Exception as e:
            print(f"Error updating inventory: {e}")
            return False
    
    # ==================== DELETE Operations ====================
    
    def delete_order(self, orderkey: int):
        """Delete an order and its line items (using O_* and L_* columns)"""
        start_time = time.time()
        
        operations = [
            ("DELETE FROM LINEITEM WHERE L_ORDERKEY = %s", (orderkey,)),
            ("DELETE FROM ORDERS WHERE O_ORDERKEY = %s", (orderkey,))
        ]
        
        try:
            self.db.execute_transaction(operations)
            elapsed = time.time() - start_time
            self.metrics['delete'].append(elapsed)
            return True
        except Exception as e:
            print(f"Error deleting order: {e}")
            return False
    
    # ==================== Analytics Queries ====================
    
    def get_top_customers(self, limit: int = 10):
        """Get customers with highest total order value (using C_* and O_* columns)"""
        start_time = time.time()
        
        query = """
        SELECT c.C_CUSTKEY, c.C_NAME, c.C_MKTSEGMENT, 
               COUNT(o.O_ORDERKEY) as num_orders,
               SUM(o.O_TOTALPRICE) as total_spent
        FROM CUSTOMER c
        JOIN ORDERS o ON c.C_CUSTKEY = o.O_CUSTKEY
        GROUP BY c.C_CUSTKEY, c.C_NAME, c.C_MKTSEGMENT
        ORDER BY total_spent DESC
        LIMIT %s
        """
        
        try:
            result = self.db.execute_query(query, (limit,))
            elapsed = time.time() - start_time
            self.metrics['read'].append(elapsed)
            return result
        except Exception as e:
            print(f"Error getting top customers: {e}")
            return None
    
    def get_revenue_by_region(self):
        """Get total revenue by region (using R_*, N_*, C_*, O_* columns)"""
        start_time = time.time()
        
        query = """
        SELECT r.R_NAME as region, 
               COUNT(DISTINCT o.O_ORDERKEY) as num_orders,
               SUM(o.O_TOTALPRICE) as total_revenue
        FROM REGION r
        JOIN NATION n ON r.R_REGIONKEY = n.N_REGIONKEY
        JOIN CUSTOMER c ON n.N_NATIONKEY = c.C_NATIONKEY
        JOIN ORDERS o ON c.C_CUSTKEY = o.O_CUSTKEY
        GROUP BY r.R_REGIONKEY, r.R_NAME
        ORDER BY total_revenue DESC
        """
        
        try:
            result = self.db.execute_query(query)
            elapsed = time.time() - start_time
            self.metrics['read'].append(elapsed)
            return result
        except Exception as e:
            print(f"Error getting revenue by region: {e}")
            return None
    
    # ==================== Metrics ====================
    
    def get_performance_metrics(self):
        """Get performance metrics for all operations"""
        metrics_summary = {}
        
        for op_type, times in self.metrics.items():
            if times:
                metrics_summary[op_type] = {
                    'count': len(times),
                    'avg_latency': sum(times) / len(times),
                    'min_latency': min(times),
                    'max_latency': max(times),
                    'p95_latency': sorted(times)[int(len(times) * 0.95)] if len(times) > 20 else max(times)
                }
            else:
                metrics_summary[op_type] = {
                    'count': 0,
                    'avg_latency': 0,
                    'min_latency': 0,
                    'max_latency': 0,
                    'p95_latency': 0
                }
        
        return metrics_summary
    
    def reset_metrics(self):
        """Reset performance metrics"""
        for key in self.metrics:
            self.metrics[key] = []