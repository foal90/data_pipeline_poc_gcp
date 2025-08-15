# **Creating a Data Pipeline in GCP**

A data pipeline is a set of processes that move, transform, and load data from a source to a destination. On Google Cloud Platform (GCP), you can build robust and scalable pipelines using a variety of services that integrate natively with one another.

### **Tools Used**

* **Google Composer / Apache Airflow**: Used for pipeline **orchestration** and scheduling. It allows you to define, schedule, and monitor complex workflows (DAGs).  
* **Google Cloud Storage**: Acts as a data **storage** layer. It is an ideal place to store raw data (source files), transformation scripts (PySpark), and temporarily processed data.  
* **Google Dataproc**: Used to create a **PySpark** cluster environment for processing large volumes of data. You can launch on-demand clusters, run your Spark jobs, and then shut them down to optimize costs.  
* **Google BigQuery**: A scalable, serverless data warehouse. It is used as the final destination where the processed data is loaded for subsequent analysis.

### **Pipeline Workflow**

This process ensures that the pipeline is automated, scalable, and efficient, leveraging the pay-as-you-go nature of GCP services.

\<br\>

A typical pipeline workflow in Airflow, as visualized in the provided image, would follow a sequence of tasks:

1. **Begin**: The DAG starts.  
2. **Create Cluster**: A Dataproc cluster is provisioned.  
3. **PySpark Task**: The PySpark job is submitted and executed on the newly created cluster.  
4. **Transfer Data to BigQuery**: The processed data is loaded from Cloud Storage into a BigQuery table.  
5. **Delete Cluster**: The Dataproc cluster is shut down to save costs.  
6. **End**: The DAG successfully finishes its execution.

\<br\>