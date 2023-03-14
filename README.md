# STaRMAN
Single Time stAmp Records Manager.

This code base is sufficient to build and deploy a REST API microservice container which can ingest IoT asset data and store it in a MySQL database instance.

Data is converted into specified unit-system with each record having only time stamp, using which you would query the database.

## Configuration
### Deployment
This service is made to run in google's cloud-run service with google's cloud-sql instance. 
Cloud run creates a unix socket which points directly to all cloud-sql instances (this has to be enabled in cloud-run)

Environmental Variables needed to be defined on your cloud-run service are:
```
INSTANCE_UNIX_SOCKET = '/cloudsql/<project>:<region>:<instance>'
DB_USER              = '<username>'
DB_PASS              = '<password>'
```

These variables should be at your cloud-sql-instance's dashboard.

For coding examples: [Tutorial -- Connect cloud run to cloud sql through unix sockets](https://cloud.google.com/sql/docs/mysql/connect-run#connect-unix-socket)

### Build
To build this, create a branch of this repository that connects to CI/CD pipeline from GCP.

Github Repository -> Google Cloud Build -> Google Container Registry -> Google Cloud Run

## Data Model