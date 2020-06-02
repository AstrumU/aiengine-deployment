# aiengine-deployment
## Contianerize notebook in docker in kubernetes
## Workflow: From demo to production about AI translation engine
 
## 1.Background
AI translation engine demo Currently running as off-line pattern: Data prepare, analysis, model  building as notebook running against  databricks  cluster using PySpark. Data mainly loaded from offline data sources like delta lake.
Incoming AI translation engine production is real-time running pattern as HTTP API server on azure cloud.
## 2. Deployment Workflow
2.1 Containerized notebooks from databricks cluster 
Two components in the databricks cluster need to be deployed.
First of all, all notebooks including feature space building, Hard skill matching and weight GPA matching calculating need to be wrapped in a Docker container and pushed to the Azure Container Registry. Additionally, Some libraries are required when we initialise Spark. So, we will install required dependencies into this environment.

Secondly, pre-trained models, such as NLP and prediction Machine Learning Model are built into model docker image using azure machine learning engine and workspace.

Required Azure Resource: Azure container registry

2.2 Store and retrieve data in Kubernetes cluster
Tables of precalculated student profile and vector, job role vector need to be stored as HDFS on Kubernetes cluster. We need to establish an HDFS namenode pod on the same node as the Spark driver pod, as well as one datanode pod per node to be able to store files on the same machines as the executor pods using those files.
 
Lookup tables and real time generated recommendation results need to be stored as Kubernetes volume. When we create a pod definition, the persistent volume claim is specified to request the desired storage. It can be mounted on a container at a particular path.

Required Azure Resource: Azure Kubernetes Cluster, and kubeflow, helm
 
2.3 API Gateway integrated multiple microservices in Kubernetes cluster
There are multiple microservices triggering and integration based on campaign entity information inputs of API calling. We need to send different portions of  campaign entity information as input of different microservices. Hence we  will use  ingress gateway  to route traffic and applied cluster/microservies access rules, as well as definition Input/Output of all internal MicroService API.

Required Azure Resource: Azure Kubernetes Cluster, and Ingress
