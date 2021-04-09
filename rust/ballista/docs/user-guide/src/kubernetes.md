<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->
# Deploying Ballista with Kubernetes

Ballista can be deployed to any Kubernetes cluster using the following instructions. These instructions assume that
you are already comfortable with managing Kubernetes deployments.

The k8s deployment consists of:

- k8s stateful set for one or more scheduler processes
- k8s stateful set for one or more executor processes
- k8s service to route traffic to the schedulers
- k8s persistent volume and persistent volume claims to make local data accessible to Ballista

## Limitations

Ballista is at an early stage of development and therefore has some significant limitations:

- There is no support for shared object stores such as S3. All data must exist locally on each node in the 
  cluster, including where any client process runs  (until 
  [#473](https://github.com/ballista-compute/ballista/issues/473) is resolved).
- Only a single scheduler instance is currently supported unless the scheduler is configured to use `etcd` as a 
  backing store.

## Create Persistent Volume and Persistent Volume Claim 

Copy the following yaml to a `pv.yaml` file and apply to the cluster to create a persistent volume and a persistent 
volume claim so that the specified host directory is available to the containers. This is where any data should be 
located so that Ballista can execute queries against it.

```yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: data-pv
  labels:
    type: local
spec:
  storageClassName: manual
  capacity:
    storage: 10Gi
  accessModes:
    - ReadWriteOnce
  hostPath:
    path: "/mnt"
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: data-pv-claim
spec:
  storageClassName: manual
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 3Gi
```

To apply this yaml:

```bash
kubectl apply -f pv.yaml
```

You should see the following output:

```bash
persistentvolume/data-pv created
persistentvolumeclaim/data-pv-claim created
```

## Deploying Ballista Scheduler and Executors

Copy the following yaml to a `cluster.yaml` file.

```yaml
apiVersion: v1
kind: Service
metadata:
  name: ballista-scheduler
  labels:
    app: ballista-scheduler
spec:
  ports:
    - port: 50050
      name: scheduler
  clusterIP: None
  selector:
    app: ballista-scheduler
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: ballista-scheduler
spec:
  serviceName: "ballista-scheduler"
  replicas: 1
  selector:
    matchLabels:
      app: ballista-scheduler
  template:
    metadata:
      labels:
        app: ballista-scheduler
        ballista-cluster: ballista
    spec:
      containers:
      - name: ballista-scheduler
        image: ballistacompute/ballista-rust:0.4.2-SNAPSHOT
        command: ["/scheduler"]
        args: ["--port=50050"]
        ports:
          - containerPort: 50050
            name: flight
        volumeMounts:
          - mountPath: /mnt
            name: data
      volumes:
      - name: data
        persistentVolumeClaim:
          claimName: data-pv-claim
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: ballista-executor
spec:
  serviceName: "ballista-scheduler"
  replicas: 2
  selector:
    matchLabels:
      app: ballista-executor
  template:
    metadata:
      labels:
        app: ballista-executor
        ballista-cluster: ballista
    spec:
      containers:
        - name: ballista-executor
          image: ballistacompute/ballista-rust:0.4.2-SNAPSHOT
          command: ["/executor"]
          args: ["--port=50051", "--scheduler-host=ballista-scheduler", "--scheduler-port=50050", "--external-host=$(MY_POD_IP)"]
          env:
            - name: MY_POD_IP
              valueFrom:
                fieldRef:
                  fieldPath: status.podIP            
          ports:
            - containerPort: 50051
              name: flight
          volumeMounts:
            - mountPath: /mnt
              name: data
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: data-pv-claim
```

```bash
$ kubectl apply -f cluster.yaml
```

This should show the following output:

```
service/ballista-scheduler created
statefulset.apps/ballista-scheduler created
statefulset.apps/ballista-executor created
```

You can also check status by running `kubectl get pods`:

```bash
$ kubectl get pods
NAME                   READY   STATUS    RESTARTS   AGE
busybox                1/1     Running   0          16m
ballista-scheduler-0   1/1     Running   0          42s
ballista-executor-0    1/1     Running   2          42s
ballista-executor-1    1/1     Running   0          26s
```

You can view the scheduler logs with `kubectl logs ballista-scheduler-0`:

```
$ kubectl logs ballista-scheduler-0
[2021-02-19T00:24:01Z INFO  scheduler] Ballista v0.4.2-SNAPSHOT Scheduler listening on 0.0.0.0:50050
[2021-02-19T00:24:16Z INFO  ballista::scheduler] Received register_executor request for ExecutorMetadata { id: "b5e81711-1c5c-46ec-8522-d8b359793188", host: "10.1.23.149", port: 50051 }
[2021-02-19T00:24:17Z INFO  ballista::scheduler] Received register_executor request for ExecutorMetadata { id: "816e4502-a876-4ed8-b33f-86d243dcf63f", host: "10.1.23.150", port: 50051 }
```

## Deleting the Ballista cluster

Run the following kubectl command to delete the cluster.

```bash
kubectl delete -f cluster.yaml
```