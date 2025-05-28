# Task 2. Scaling distributed Systems in the Cloud

## How to prepare the environment
### Preparing the lambda
First you have to create a lambda in your AWS Environment, name it the same as the constant LAMBDA_FUNCTION_NAME.
Then you have to paste the code from the file insult_filter_lambda.py which is inside the folder Exercice1 to the created lambda.

### Preparing the S3 bucket
Create an S3 bucket and set its name to the constant S3_BUCKET_NAME. You also have to create two folders inside the bucket, an "input" folder, and an "output" folder.
You can use the provided examples inside the folder test_files, and upload them into the S3 bucket inside the "input" folder.

## Executing the exercices
### Exercice 1
Start the rabbitmq_to_lambda_trigger.py file.
```
python rabbitmq_to_lambda_trigger.py
```

Start the text_publisher.py file. You can give it a text to filter if you want, if not it will use the default ones.
```
python text_publisher.py Example text without insults.
```

### Exercice 2
