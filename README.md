# cse-512-group-8
This repo is dedicated to the course project of CSE-512 (Distributed Database Systems) @ ASU for Fall 2025 semester

## Node Startup Details

### Pranjal (Host) Nodes 1 and 2

```
cockroach start --insecure --store=node1 --listen-addr=192.168.0.140:26257 --http-addr=192.168.0.140:8080
```

```
cockroach start --insecure --store=node2 --listen-addr=192.168.0.140:26258 --http-addr=192.168.0.140:8081 --join=192.168.0.140:26257
```

Then run the below only once all other primary nodes have run

```
cockroach init --insecure --host=192.168.0.140:26257
```

### Samarth Nodes 3 and 4

```
cockroach start --insecure --store=node3 --listen-addr=192.168.0.61:26257 --http-addr=192.168.0.61:8080 --join=192.168.0.140:26257,192.168.0.74:26257
```

```
cockroach start --insecure --store=node4 --listen-addr=192.168.0.61:26258 --http-addr=192.168.0.61:8081 --join=192.168.0.140:26257,192.168.0.61:26257,192.168.0.74:26257
```

### Sachin Nodes 5 and 6

```
cockroach start --insecure --store=node5 --listen-addr=192.168.0.74:26257 --http-addr=192.168.0.74:8080 --join=192.168.0.140:26257,192.168.0.61:26257
```

```
cockroach start --insecure --store=node6 --listen-addr=192.168.0.74:26258 --http-addr=192.168.0.74:8081 --join=192.168.0.140:26257,192.168.0.61:26257,192.168.0.74:26257
```

### Smaran Nodes 7 and 8

```
cockroach start --insecure --store=node7 --listen-addr=192.168.0.54:26257 --http-addr=192.168.0.54:8080 --join=192.168.0.140:26257,192.168.0.61:26257,192.168.0.74:26257
```

```
cockroach start --insecure --store=node8 --listen-addr=192.168.0.54:26258 --http-addr=192.168.0.54:8081 --join=192.168.0.140:26257,192.168.0.61:26257,192.168.0.74:26257
```

### To check node status at any time 

```
cockroach node status --insecure --host=192.168.0.140:26257
```
### To check dashboard

```
http://192.168.0.140:8080
```
### To access CockroachSQL

```
cockroach sql --insecure --host=192.168.0.140:26257
```


