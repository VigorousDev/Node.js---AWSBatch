# Social Analytics
This script is used to generate the user statistics (likes, listens, gender, etc) used to create reports for Brands and 
Events. It runs as a batch job on the AWS Batch service. The job automatically downloads a bundled zip from S3 and 
executes the `social-analytics-job.sh` shell script to generate statistics for each event and brand, and then uploads 
them to a Cloudant Database which is used by the frontend to generate graphs and reports.

**Note** Even though links in this document generally point to resources in the development account, always remember 
that batch jobs should be tested and debugged on the Development AWS account (139331702337) but deployed on the 
production account (691589232377)

## Job Definition
The Batch job definition is named [social-analytics](https://us-west-2.console.aws.amazon.com/batch/home?region=us-west-2#/job-definitions)
It is currently set to execute jobs on a queue comprised of a single m4.large instance but it can be scaled up if
necessary. The m4.large instance contains 2 vCPUs so the batch is executed as two simultaneous jobs, each with 1 vCPU and 
3500MB or RAM allocated. Sharding of the job is handled by the social-analytics-job.sh script, dependent on the
environment variables that are passed in when each job is submitted. Two shards are currently used but that can be 
easily increased.

## Job Execution
There are three components that facilitate the automation of the batch job. 
1. A generic "fetch and run" docker image that is hosted on the [EC2 Container Registry](https://us-west-2.console.aws.amazon.com/ecs/home?region=us-west-2#/repositories/awsbatch:fetch_and_run#images;tagStatus=ALL).
Sources and more details about the image can be found in the [DevOps repo](https://github.com/theticketfairy/devops/tree/master/aws-batch).

2. A generic "submitBatchJob" Lambda function that is used to start the batch job. Until Batch jobs can be executed
directly from a Cloudwatch Scheduled Rule, the Lambda function has to be used as an intermediary step.

3. A CloudWatch Events rule which triggers the Lambda function according to a fixed schedule. The rule contains the
JSON object that is passed to the Lambda function as input (see below). The JSON object contains the jobName, 
jobDefinition, jobQueue, and any container overrides (e.g. environment variables) that are to be passed to the job.


## Environment Variables
The default values for the environment variables used by the batch job are defined in the [Batch Job Definition](https://us-west-2.console.aws.amazon.com/batch/home?region=us-west-2#/job-definitions).

Here are the current environment variables and their defaults (for the development account)

* BATCH_FILE_S3_URL=s3://theticketfairy-dev/aws-batch/social-analytics/batch-job.zip
* CACHE_FILE_S3_URL=s3://theticketfairy-dev/aws-batch/social-analytics/leveldb-cache-1.zip
* NUM_SHARDS=2
* CURRENT_SHARD=1
* BATCH_FILE_TYPE=zip
* EVENT_ID_START=1300
* BRAND_ID_START=500

Any environment variable can be overridden by the command that starts the batch. Here is the JSON event submitted 
by the CloudWatch event. The rule currently triggers two batch jobs in parallel and overrides the CACHE_FILE_S3_URL
and CURRENT_SHARD environment variables accordingly
```
{
  "jobs": [
    {
      "jobName": "social-analytics-1",
      "jobDefinition": "social-analytics",
      "jobQueue": "m4-large-queue",
      "containerOverrides": {
        "environment": [
          {
            "name": "CACHE_FILE_S3_URL",
            "value": "s3://theticketfairy-dev/aws-batch/social-analytics/leveldb-cache-1.zip"
          },
          {
            "name": "CURRENT_SHARD",
            "value": "1"
          }
        ]
      }
    },
    {
      "jobName": "social-analytics-2",
      "jobDefinition": "social-analytics",
      "jobQueue": "m4-large-queue",
      "containerOverrides": {
        "environment": [
          {
            "name": "CACHE_FILE_S3_URL",
            "value": "s3://theticketfairy-dev/aws-batch/social-analytics/leveldb-cache-2.zip"
          },
          {
            "name": "CURRENT_SHARD",
            "value": "2"
          }
        ]
      }
    }
  ]
}
```

## Updating/Deployment
To deploy any changes made to app.js, the relevant files must be zipped and uploaded to S3 so that they can be picked 
up by the next execution of the batch job. Run the publish-batch-job script to zip up the relevant files and upload them
to S3 from the command line. This uses the AWS Node SDK to upload to S3 so it assumes you have [configured you credentials](http://docs.aws.amazon.com/sdk-for-javascript/v2/developer-guide/setting-credentials-node.html)
accordingly. 
```sh
node publish-batch-job
```
The publish-batch-job.js script uploads the job zip file to the **development** S3 bucket so that changes can be 
tested on the dev account before they are rolled out in production. Once the testing is complete the zip file must then
be manually moved to the [production S3 bucket](https://s3.console.aws.amazon.com/s3/buckets/theticketfairy-batch/social-analytics/?region=us-east-1&tab=overview)


## Debugging
In order to simulate the batch environment so that you can test code changes without running a new batch each time, 
you can spin up an EC2 instance, install docker and build the "fetch and run" image. Once that is done you can run 
the following docker command (adjusting the env variables accordingly) to test your changes. 
**Note that you must deploy your changes to S3 first.**
```
sudo docker run -i --memory-reservation 3500M \
-e BATCH_FILE_TYPE='zip' -e BATCH_FILE_S3_URL='s3://theticketfairy-dev/aws-batch/social-analytics/batch-job.zip' \
-e CACHE_FILE_S3_URL='s3://theticketfairy-dev/aws-batch/social-analytics/leveldb-cache-1.zip' \
-e EVENT_ID_START='623' -e EVENT_ID_END='623' -e BRAND_ID_START='5' -e BRAND_ID_END='5' \
-e NUM_SHARDS='1' -e CURRENT_SHARD='1' \
awsbatch/fetch_and_run social-analytics-job.sh
```

**Note** Even though links in this document generally point to resources in the development account, always remember 
that batch jobs should be tested and debugged on the Development AWS account (139331702337) but deployed on the 
production account (691589232377)