# Key-Value Server with Raft

## Overview
This project is a **Key-Value Server** built on top of the **Raft consensus algorithm**, ensuring **consistency** and **high availability**. It was developed as part of the MIT Distributed Systems lab and includes an implementation of Raft from scratch, as well as the client-side components and clerks required for interaction with the server.

## Features
- **Fully Implemented Raft Protocol**: Includes leader election, log replication, and fault tolerance.
- **Key-Value Store Operations**:
  - `put(key, value)`: Stores the given value under the specified key.
  - `get(key)`: Retrieves the value associated with the key.
  - `append(key, value)`: Appends the given value to the existing value for the specified key.
- **Optimizations for Performance**:
  - Snapshotting to reduce log size and improve efficiency.
- **Robust Testing**:
  - Successfully passes MIT's premade tests for correctness and fault tolerance.

## Implementation Details
- **Raft Algorithm**:
  - Follows the Raft consensus model to maintain consistency across distributed nodes.
  - Handles leader election, log replication, and state persistence.
- **Key-Value Server**:
  - Built on top of the Raft layer to provide a consistent distributed storage system.
  - Supports basic operations (`put`, `get`, `append`) via client requests.
- **Client and Clerk**:
  - A client library is implemented to facilitate communication with the Key-Value Server.
  - Clerks abstract interaction with the server, ensuring seamless request handling.

