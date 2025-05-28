import argparse
import lithops
import re
import concurrent.futures
import sys
import os

# Add the parent directory of 'conf' to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from conf import conf

# S3 Configuration from conf.py
S3_INPUT_BUCKET_NAME = conf.S3_BUCKET_NAME
S3_INPUT_PREFIX = conf.S3_INPUT_PREFIX
S3_OUTPUT_BUCKET_NAME = conf.S3_BUCKET_NAME # Assuming same bucket for output
S3_OUTPUT_PREFIX = conf.S3_OUTPUT_PREFIX

# Insults List from conf.py
INSULTS_LIST = conf.INSULTS_LIST

def reduce_sum_insults(list_of_counts):
    print(f"[ReduceTask] Starting reduction with results: {list_of_counts}")
    total_insults = sum(list_of_counts)
    print(f"[ReduceTask] Total censored insults calculated: {total_insults}")
    return total_insults

def censor_file_task(data, storage):
    """
    Processes a text file from S3:
    1. Reads the content.
    2. Filters insults and counts them.
    3. Saves the censored version to S3 (in another location).
    4. Returns the number of censored insults in this file.
    
    :param data: A dictionary containing:
                 'input_bucket': S3 input bucket name.
                 'input_key': S3 input object key.
                 'output_bucket': S3 output bucket name.
                 'output_prefix': S3 prefix for output files.
                 'insults_list': List of insults to censor.
    :param storage: Instance of 'lithops.storage.Storage' injected by Lithops.
    :return: Number of insults censored in the file.
    """
    input_bucket = data['input_bucket']
    input_key = data['input_key']
    output_bucket = data['output_bucket']
    output_prefix = data['output_prefix']
    insults_list_to_use = data['insults_list']

    print(f"[CensorTask] Starting processing for: s3://{input_bucket}/{input_key}")

    try:
        file_content = storage.get_object(bucket=input_bucket, key=input_key).decode('utf-8')
        print(f"[CensorTask] Content read from s3://{input_bucket}/{input_key}")
    except Exception as e:
        print(f"[CensorTask] ERROR reading s3://{input_bucket}/{input_key}: {e}")
        return 0

    censored_words = []
    insult_count_for_file = 0
    for word in file_content.split():
        # Separate word from punctuation
        match = re.match(r"^([^\w]*)([\wÀ-ÿ]+)([^\w]*)$", word)
        if match:
            prefix, core_word, suffix = match.groups()
            clean_word = core_word.lower()
        else:
            prefix, core_word, suffix = '', word, ''
            clean_word = word.lower()
        if clean_word in insults_list_to_use:
            censored_words.append(f"{prefix}CENSORED{suffix}")
            insult_count_for_file += 1
        else:
            censored_words.append(word)
    censored_content = " ".join(censored_words)

    original_filename = input_key.split('/')[-1]
    output_key = f"{output_prefix.rstrip('/')}/{original_filename}"

    try:
        storage.put_object(bucket=output_bucket,
                           key=output_key,
                           body=censored_content.encode('utf-8'))
        print(f"[CensorTask] Censored content saved to: s3://{output_bucket}/{output_key}")
    except Exception as e:
        print(f"[CensorTask] ERROR saving to s3://{output_bucket}/{output_key}: {e}")

    print(f"[CensorTask] Processing finished for s3://{input_bucket}/{input_key}. Insults found: {insult_count_for_file}")
    return insult_count_for_file


def basic_batch_operation(processing_function, 
                          max_concurrent_executions, 
                          input_bucket, 
                          input_prefix,
                          output_bucket,
                          output_prefix,
                          insults_list_param):
    """
    Implements the basic batch operation.
    :param processing_function: The function to execute for each file (e.g., censor_file_task).
    :param max_concurrent_executions: (maxfunc) Maximum number of concurrent executions of the function.
    :param input_bucket: S3 bucket containing the input files.
    :param input_prefix: S3 prefix for input files.
    :param output_bucket: S3 bucket for output files.
    :param output_prefix: S3 prefix for output files.
    :param insults_list_param: List of insults to pass to the processing_function.
    :return: List of results returned by each execution of processing_function.
    """
    fexec = lithops.FunctionExecutor()
    storage = lithops.Storage() # Needed to list keys

    print(f"Listing objects in s3://{input_bucket}/{input_prefix}")
    object_keys = storage.list_keys(bucket=input_bucket, prefix=input_prefix)
    
    files_to_process = [key for key in object_keys if not key.endswith('/')]
    print(f"{len(files_to_process)} files will be processed.")
    print(f"Files to process: {files_to_process}")

    lithops_futures = []
    results = []

    # ThreadPoolExecutor to control the number of *simultaneous* invocations to fexec.call_async
    # This limits how many Lithops tasks (Lambdas) are started at once from this script.
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrent_executions) as executor:
        # This list will contain the 'futures' returned by executor.submit(),
        # each representing a task to invoke fexec.call_async().
        submitted_tasks_futures = []

        for key in files_to_process:
            data_for_function = {
                'input_bucket': input_bucket,
                'input_key': key,
                'output_bucket': output_bucket,
                'output_prefix': output_prefix,
                'insults_list': insults_list_param
            }

            payload_for_call_async = {'data': data_for_function}
            # Submit the task (which is to call fexec.call_async) to the thread pool.
            # The executor will ensure that no more than 'max_concurrent_executions'
            # of these lambdas (calls to call_async) run at the same time.
            task_future = executor.submit(lambda payload=payload_for_call_async: fexec.call_async(processing_function, payload))
            submitted_tasks_futures.append(task_future)

        # Wait for each task in the pool (call to call_async) to complete
        # and collect the Lithops future returned by call_async.
        for task_future in concurrent.futures.as_completed(submitted_tasks_futures):
            try:
                lithops_future_from_call_async = task_future.result()
                lithops_futures.append(lithops_future_from_call_async)
            except Exception as e:
                print(f"Error sending the task to Lithops or getting the call_async future: {e}")
    
    print(f"{len(lithops_futures)} tasks have been started in Lithops. Waiting for results...")

    # Now, get the results from all Lithops tasks (Lambdas)
    for lf in lithops_futures:
        try:
            results.append(lf.result())
        except Exception as e:
            print(f"Error getting the result from a Lithops task: {e}")
            results.append(0) # Add a default value or handle the error

    fexec.clean() # Clean up Lithops temporary resources
    return results

if __name__ == '__main__':
    print("Starting script for Exercise 4: Batch Operation with Lithops...")

    parser = argparse.ArgumentParser(description="Basic batch operation with Lithops")
    parser.add_argument('--function', help='Function name, default is censor_file_task', default='censor_file_task')
    parser.add_argument('--maxfunc', type=int, help='Max parallel message processing, default is 2', default=2)
    parser.add_argument('--bucket', help=f'S3 bucket name, default is {conf.S3_BUCKET_NAME}', default=conf.S3_BUCKET_NAME)

    args = parser.parse_args()

    print(f"Configuration: maxfunc = {args.maxfunc}")
    # Use S3_INPUT_BUCKET_NAME and S3_OUTPUT_BUCKET_NAME directly as they are now from conf
    print(f"Input bucket: s3://{args.bucket}/{S3_INPUT_PREFIX}") 
    print(f"Output bucket: s3://{args.bucket}/{S3_OUTPUT_PREFIX}")

    # Map function names to actual function references
    function_map = {
        'censor_file_task': censor_file_task,
    }

    try:
        # Get the function reference from the string
        processing_function = function_map.get(args.function)
        if processing_function is None:
            raise ValueError(f"Function '{args.function}' not found. Available: {list(function_map.keys())}")

        list_of_insult_counts = basic_batch_operation(
            processing_function=processing_function,
            max_concurrent_executions=args.maxfunc,
            input_bucket=args.bucket, # This will use the argument or default from conf
            input_prefix=S3_INPUT_PREFIX,
            output_bucket=args.bucket, # This will use the argument or default from conf
            output_prefix=S3_OUTPUT_PREFIX,
            insults_list_param=INSULTS_LIST
        )

        print(f"\nInsult counts per file: {list_of_insult_counts}")

        total_censored_insults = reduce_sum_insults(list_of_insult_counts)

        print("\n--- Final Result of Exercise 4 ---")
        print(f"The total number of censored insults in all files is: {total_censored_insults}")

        print("Exercise 4 script finished.")
    except Exception as e:
        print(e)