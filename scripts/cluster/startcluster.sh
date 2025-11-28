#!/bin/bash
LAPTOP1_IP="192.168.0.140"
LAPTOP2_IP="1192.168.0.61"
LAPTOP3_IP="192.168.0.74"
LAPTOP4_IP="192.168.0.54"

JOIN_ADDRS="${LAPTOP1_IP}:26257,${LAPTOP1_IP}:26258,${LAPTOP2_IP}:26257,${LAPTOP2_IP}:26258,${LAPTOP3_IP}:26257,${LAPTOP3_IP}:26258"

# Start Node 1
cockroach start --insecure --store=nodes/node1 \
  --listen-addr=${LAPTOP1_IP}:26257 --http-addr=${LAPTOP1_IP}:8080 \
  --join=${JOIN_ADDRS} --background

# Start Node 2
cockroach start --insecure --store=nodes/node2 \
  --listen-addr=${LAPTOP1_IP}:26258 --http-addr=${LAPTOP1_IP}:8081 \
  --join=${JOIN_ADDRS} --background

echo "Laptop 1 nodes started"
cockroach node status --insecure --host=${LAPTOP1_IP}:26257