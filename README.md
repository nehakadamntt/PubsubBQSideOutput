# PubsubBQApacheBeam


## ABOUT
This is Simple Apache Beam Pipeline written using Java SDK which takes pubsub message in the json format and parse it using Gson and then store it into the Bigquery Table.It also handle the erraneous data and store it in DLQ topic.
In this Maven Project  Main.java file contains the entry point function from where program starts.

## Dependencies
1.Java SDK 
2.An IDE (optional)
3.Maven Package Manager
4.run ```mvn clean dependency:resolve``` to fetch the apache beam dependency

## Running
Use following command to run the pipeline

```mvn compile exec:java -Dexec.mainClass={MAIN_CLASS_NAME} -Dexec.args="--project={project_name} --jobName={job_name} --region=europe-west4 --serviceAccount={service account name}   --runner=DataflowRunner  --outputTableName={OUTPUT_TABLE_NAME} --subscription={input_topic_subscriptionId} --streaming=true"```
 
