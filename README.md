![Go](https://img.shields.io/badge/Go-%2300ADD8.svg?&logo=go&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=fff)
![Kubernetes](https://img.shields.io/badge/Kubernetes-326CE5?logo=kubernetes&logoColor=fff)
![Helm](https://img.shields.io/badge/Helm-0F1689?logo=helm&logoColor=fff)

# Distributed Raft Key-Value Store

![Gantt](/misc/project-gantt-chart.png)

## 📝 Description
This project implements a highly available and fault-tolerant **Go distributed key-value store** with **Raft**, a consensus algorithm. 

Deployed with **Docker** and **Kubernetes**, it utilizes **Helm charts** for templating & packaging manifests and **Longhorn** for the distributed persistent storage.

![](/misc/architecture_diagram.png)

## ⚙️ Installation
Prerequisites
- Kubectl CLI, Helm CLI, Kubernetes Cluster (K3s), Longhorn Storage

1. Clone and move to repository
    ```bash
    git clone https://github.com/jjiang0595/Distributed_KV_Store.git
    ```

2. Configure ImagePullSecret (Docker credentials)
    ```bash
    kubectl create secret docker-registry my-registry-secret \
    --docker-server=https://index.docker.io/v1/ \
    --docker-username=<DOCKER_HUB_USERNAME> \
    --docker-password=<DOCKER_HUB_PASSWORD> \
    --docker-email=<DOCKER_HUB_EMAIL> \
    -n default
    ```
3. Build and push Docker image
    ```bash
    docker build <DOCKER_HUB_USERNAME>/<DOCKER_IMAGE_NAME>:<DOCKER_TAG> .
    docker push <DOCKER_HUB_USERNAME>/<DOCKER_IMAGE_NAME>:<DOCKER_TAG>
    ```
4. Deploy Raft KV Store
    ```bash
    helm install raft-kv-store-release ./ \
    --namespace default \
    --set node.imagePullSecret=my-registry-secret \
    --set image.repository=<DOCKER_HUB_USERNAME>/<DOCKER_IMAGE_NAME>
    --set image.tag=<DOCKER_TAG>
    ```
   
## 🌐 API Reference
```markdown
|--------------------|--------|--------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------|
| Endpoint           | Method | Body                                                   | Description                                                                                                                            |
|--------------------|--------|--------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------|
| `/key/{id}`        | PUT    | -                                                      | Creates a key with an associated value in the database                                                                                 |
| `/key/{id}         | GET    | `{ "body": "..." }`                                    | Returns the value associated with the key if available                                                                                 |
| `/status`          | GET    | `{ "leaderID": "...", "isLead": "..."}`                | Returns the leader node's status                                                                                                       |
|--------------------|--------|--------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------|
```

## 🧪 Tests
More than 20 comprehensive and thorough tests spanning from core raft logic to HTTP methods
  - Core Raft Logic
    - Leader Election
    - Log Replication
    - Node Partition
  - HTTP PUT/GET
    - Error injection
    - Maximum retries 
    - Transient errors
    - Redirect handling

## 🚧 Project Learnings & Obstacles
Throughout this project, I faced many difficult problems and challenges, allowing me to deepen my fundamental understanding of distributed systems and Kubernetes.

**Raft Consensus Debugging**
- Initially struggled with mutexes and WaitGroups. Not knowing the difference between `defer mu.Unlock()` and `mu.Unlock()` caused lock contention
  - Resolved a critical concurrency error involving mutexes in the leader's replication logic, alongside improving defensive programming techniques such as timer draining
- Faced Raft election storms and split-brain situations due to election timeouts being too strict and not accounting for real world latencies

**Raft Testing**
- Improved testing process by transitioning from an inefficient for loop to a more robust ticker-based approach, allowing greater control over test conditions
- Encountered node startup time mismatch during testing, which was solved through the usage of `runtime.gosched()`, which allows other goroutines to run after incrementing every x amount of milliseconds

**Kubernetes & Longhorn**
- Transitioned to a distributed block-storage system using Longhorn upon realising that problem that using Docker Desktop's Kubernetes was inefficient as data was stored on hostPath, leading to data loss on node crash.
- Diagnosed a problem with Raft node's file permission conflict resulting in Raft not being able to write to data persistence file, which was resolved by applying Kubernetes security context configuration
- Struggled with a rare Kubernetes StatefulSet volume binding problem with Longhorn's storage provisioner, necessitating a viable suboptimal alternative tradeoff approach