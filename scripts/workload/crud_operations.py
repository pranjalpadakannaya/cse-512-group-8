"""
CRUD Operations for E-Commerce Order Processing
"""

import random
import time
from datetime import datetime, timedelta
from decimal import Decimal
from db_connection import CockroachDBConnection

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
            custkey: Customer key
            items: List of (partkey, suppkey, quantity, price) tuples
        
        Returns:
            orderkey of created order
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
        
        # Insert order
        order_query = """
        INSERT INTO orders (orderkey, custkey, orderstatus, totalprice, orderdate, 
                          orderpriority, clerk, shippriority, comment)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        operations.append((order_query, (orderkey, custkey, orderstatus, totalprice, 
                                        orderdate, orderpriority, clerk, shippriority, comment)))
        
        # Insert line items
        for i, (partkey, suppkey, quantity, price) in enumerate(items, 1):
            lineitem_query = """
            INSERT INTO lineitem (orderkey, partkey, suppkey, linenumber, quantity,
                                extendedprice, discount, tax, returnflag, linestatus,
                                shipdate, commitdate, receiptdate, shipinstruct, shipmode, comment)
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
        
        # Execute transaction
        try:
            self.db.execute_transaction(operations)
            elapsed = time.time() - start_time
            self.metrics['create'].append(elapsed)
            return orderkey
        except Exception as e:
            print(f"Error creating order: {e}")
            return None
    
    def create_customer(self, custkey: int = None) -> int:
        """Create a new customer"""
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
        INSERT INTO customer (custkey, name, address, nationkey, phone, acctbal, mktsegment, comment)
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
        """Get full order details including line items"""
        start_time = time.time()
        
        query = """
        SELECT o.*, c.name as customer_name, c.mktsegment
        FROM orders o
        JOIN customer c ON o.custkey = c.custkey
        WHERE o.orderkey = %s
        """
        
        try:
            order = self.db.execute_query(query, (orderkey,))
            
            # Get line items
            lineitem_query = """
            SELECT l.*, p.name as part_name, s.name as supplier_name
            FROM lineitem l
            JOIN part p ON l.partkey = p.partkey
            JOIN supplier s ON l.suppkey = s.suppkey
            WHERE l.orderkey = %s
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
        """Get recent orders for a customer"""
        start_time = time.time()
        
        query = """
        SELECT * FROM orders
        WHERE custkey = %s
        ORDER BY orderdate DESC
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
        """Search for parts by type and price"""
        start_time = time.time()
        
        conditions = []
        params = []
        
        if part_type:
            conditions.append("type LIKE %s")
            params.append(f"%{part_type}%")
        
        if max_price:
            conditions.append("retailprice <= %s")
            params.append(max_price)
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        query = f"""
        SELECT * FROM part
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
        UPDATE orders
        SET orderstatus = %s
        WHERE orderkey = %s
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
        """Update customer account balance"""
        start_time = time.time()
        
        query = """
        UPDATE customer
        SET acctbal = acctbal + %s
        WHERE custkey = %s
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
        """Update part inventory (increase/decrease available quantity)"""
        start_time = time.time()
        
        query = """
        UPDATE partsupp
        SET availqty = availqty + %s
        WHERE partkey = %s AND suppkey = %s
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
        """Delete an order and its line items"""
        start_time = time.time()
        
        operations = [
            ("DELETE FROM lineitem WHERE orderkey = %s", (orderkey,)),
            ("DELETE FROM orders WHERE orderkey = %s", (orderkey,))
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
        """Get customers with highest total order value"""
        start_time = time.time()
        
        query = """
        SELECT c.custkey, c.name, c.mktsegment, 
               COUNT(o.orderkey) as num_orders,
               SUM(o.totalprice) as total_spent
        FROM customer c
        JOIN orders o ON c.custkey = o.custkey
        GROUP BY c.custkey, c.name, c.mktsegment
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
        """Get total revenue by region"""
        start_time = time.time()
        
        query = """
        SELECT r.name as region, 
               COUNT(DISTINCT o.orderkey) as num_orders,
               SUM(o.totalprice) as total_revenue
        FROM region r
        JOIN nation n ON r.regionkey = n.regionkey
        JOIN customer c ON n.nationkey = c.nationkey
        JOIN orders o ON c.custkey = o.custkey
        GROUP BY r.regionkey, r.name
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