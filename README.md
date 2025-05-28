# Task 2. Scaling distributed Systems in the Cloud

## How to prepare the environment

### Centralized Configuration
All global parameters for RabbitMQ, AWS Lambda, S3, and the list of insults are defined in the `conf/conf.py` file. If you need to change any of these settings (e.g., RabbitMQ host, AWS region, S3 bucket name, Lambda function name), please modify this central configuration file.

### Preparing the lambda
First you have to create a lambda in your AWS Environment. Name it according to the `LAMBDA_FUNCTION_NAME` variable in `conf/conf.py` (default is `InsultFilterWorkerLambda`).
Choose the Runtime "Python 3.13" when creating the Lambda.
Use an existing role in Execution role.
Then you have to paste the code from the file `Exercice1/insult_filter_lambda.py` into the created lambda.

### Preparing the S3 bucket
Create an S3 bucket and set its name according to the `S3_BUCKET_NAME` variable in `conf/conf.py`, it must be a unique name. You also have to create two folders inside the bucket, corresponding to `S3_INPUT_PREFIX` (default: "input/") and `S3_OUTPUT_PREFIX` (default: "output/").
You can use the provided examples inside the folder `test_files`, and upload them into the S3 bucket inside the input folder (e.g., `input/example1.txt`).

## Executing the exercices
### Exercice 1
This exercise demonstrates a basic message queuing setup with RabbitMQ and AWS Lambda for text filtering. 
- The `text_publisher.py` script sends individual sentences as messages to a RabbitMQ queue (`texts_to_filter_queue` by default, configured in `conf/conf.py`).
- The `rabbitmq_to_lambda_trigger.py` script consumes messages from this queue.
- For each message received, it triggers an AWS Lambda function (`InsultFilterWorkerLambda` by default, configured in `conf/conf.py`).
- The Lambda function (`Exercice1/insult_filter_lambda.py`) processes the text to censor predefined insults (list in `conf/conf.py`) and logs the original and censored text.

Start the `rabbitmq_to_lambda_trigger.py` file first to listen for messages.
```
python rabbitmq_to_lambda_trigger.py
```

Start the text_publisher.py file. You can give it a text to filter if you want, if not it will use the default ones.
```
python text_publisher.py Example text without insults.
```

### Exercice 2
This exercise demonstrates a stream processing primitive. It consumes messages from a RabbitMQ queue and invokes a Lambda function for each message, managing concurrency.
- The `Exercice2/stream_processor.py` script consumes messages from a specified RabbitMQ queue.
- It invokes an AWS Lambda function (specified by the `--function` argument or `LAMBDA_FUNCTION_NAME` in `conf/conf.py`) for each message.
- Concurrency of Lambda invocations is managed by the `--maxfunc` argument.
- Messages are expected to be published to the RabbitMQ queue (specified by `--queue` or `RABBITMQ_QUEUE` in `conf/conf.py`), for example, by `Exercice1/text_publisher.py`.

To run Exercise 2, execute the `stream_processor.py` script from the `Exercice2` folder. You can specify the Lambda function name, maximum concurrent invokers, and queue name as command-line arguments. If not provided, it will use the defaults defined in `conf/conf.py`.

```
python Exercice2/stream_processor.py --function YourLambdaFunctionName --maxfunc 5 --queue your_queue_name
```

Or, using default values from `conf/conf.py`:
```
python Exercice2/stream_processor.py
```

Publish messages to the specified RabbitMQ queue (e.g., using `Exercice1/text_publisher.py`) to see the stream processor in action.

### Exercice 3
This exercise uses Lithops to perform a map-reduce operation on files stored in S3.
- The `Exercice3/lithops_insult_filter.py` script orchestrates the map-reduce job using Lithops.
- The `map_censor_and_count` function (defined within the script) is the map function. It's applied to each object found in the S3 input path (`S3_BUCKET_NAME` + `S3_INPUT_PREFIX` from `conf/conf.py`). It reads the file, censors insults (using `INSULTS_LIST` from `conf/conf.py`), writes the censored content to the S3 output path (`S3_BUCKET_NAME` + `S3_OUTPUT_PREFIX`), and returns the count of insults found in that file.
- The `reduce_sum_insults` function (defined within the script) is the reduce function. It sums the counts returned by all map function instances.
- Lithops handles the execution of these functions, potentially in parallel on a serverless backend.

Ensure your Lithops environment is configured correctly to access AWS services (S3 and Lambda for execution, if applicable based on your Lithops backend configuration).

To run Exercise 3, execute the `lithops_insult_filter.py` script from the `Exercice3` folder. It will use the S3 bucket and prefixes defined in `conf/conf.py`.

```
python Exercice3/lithops_insult_filter.py
```

Make sure you have uploaded files to the S3 input path (e.g., `s3://<S3_BUCKET_NAME>/<S3_INPUT_PREFIX>/`) before running.

### Exercice 4
This exercise implements a basic batch processing operation using Lithops. It processes multiple files from S3 concurrently, applying a censoring function to each, and then aggregates the results.
- The `Exercice4/batch.py` script orchestrates the batch job.
- It lists files from an S3 input path (specified by `--bucket` or `S3_BUCKET_NAME` from `conf/conf.py`, combined with `S3_INPUT_PREFIX`).
- For each file, it invokes a processing function (default `censor_file_task`, defined within the script) using Lithops. This function reads the file, censors insults (using `INSULTS_LIST` from `conf/conf.py`), and saves the censored version to an S3 output path (`--bucket`/`S3_BUCKET_NAME` + `S3_OUTPUT_PREFIX`). It returns the count of insults.
- The number of concurrent Lithops function executions is controlled by the `--maxfunc` argument.
- After all files are processed, the script uses the `reduce_sum_insults` function to sum the individual insult counts.

Similar to Exercise 3, ensure your Lithops environment is configured.

To run Exercise 4, execute the `batch.py` script from the `Exercice4` folder. You can specify the S3 bucket name and maximum concurrent functions (maxfunc) as command-line arguments. If not provided, it will use the defaults from `conf/conf.py`.

```
python Exercice4/batch.py --bucket your_s3_bucket_name --maxfunc 10
```

Or, using default values from `conf/conf.py` for the bucket (maxfunc still defaults to 2 unless specified):
```
python Exercice4/batch.py
```
