import lithops
import re


S3_INPUT_BUCKET_NAME = 'xavi-task2'
S3_INPUT_PREFIX = 'input/'

S3_OUTPUT_BUCKET_NAME = 'xavi-task2'
S3_OUTPUT_PREFIX = 'output/'


INSULTS_LIST = [
    "tonto", "lleig", "boig", "idiota", "estúpid", "inútil", "desastre",
    "fracassat", "covard", "mentider", "beneit", "capsigrany", "ganàpia",
    "nyicris", "gamarús", "bocamoll", "murri", "dropo", "bleda", "xitxarel·lo"
]

def map_censor_and_count(obj, storage):
    input_bucket = obj.bucket
    input_key = obj.key
    print(f"[MapTask] Starting processing for: s3://{input_bucket}/{input_key}")
    try:
        file_content = storage.get_object(bucket=input_bucket, key=input_key).decode('utf-8')
        print(f"[MapTask] Content read from s3://{input_bucket}/{input_key}")
    except Exception as e:
        print(f"[MapTask] ERROR reading s3://{input_bucket}/{input_key}: {e}")
        return 0
    censored_words = []
    insult_count_for_file = 0
    for word in file_content.split():
        # Separate word from punctuation
        match = re.match(r"^([\"'(\[{<]*)(\w+)([\"')\]}>.,!?;:]*)$", word)
        if match:
            prefix, core_word, suffix = match.groups()
            clean_word = core_word.lower()
        else:
            prefix, core_word, suffix = '', word, ''
            clean_word = word.lower()
        if clean_word in INSULTS_LIST:
            censored_words.append(f"{prefix}CENSORED{suffix}")
            insult_count_for_file += 1
        else:
            censored_words.append(word)
    censored_content = " ".join(censored_words)
    original_filename = input_key.split('/')[-1]
    output_key = f"{S3_OUTPUT_PREFIX.rstrip('/')}/{original_filename}"
    try:
        storage.put_object(bucket=S3_OUTPUT_BUCKET_NAME,
                           key=output_key,
                           body=censored_content.encode('utf-8'))
        print(f"[MapTask] Censored content saved to: s3://{S3_OUTPUT_BUCKET_NAME}/{output_key}")
    except Exception as e:
        print(f"[MapTask] ERROR saving to s3://{S3_OUTPUT_BUCKET_NAME}/{output_key}: {e}")
    print(f"[MapTask] Processing finished for s3://{input_bucket}/{input_key}. Insults found: {insult_count_for_file}")
    return insult_count_for_file

def reduce_sum_insults(list_of_counts):
    print(f"[ReduceTask] Starting reduction with results: {list_of_counts}")
    total_insults = sum(list_of_counts)
    print(f"[ReduceTask] Total censored insults calculated: {total_insults}")
    return total_insults


if __name__ == '__main__':
    print("Starting insult filtering script with Lithops...")

    fexec = lithops.FunctionExecutor() # Will use the default configuration

    # Define the iteration data for the map.
    iterdata_s3_prefix = f's3://{S3_INPUT_BUCKET_NAME}/{S3_INPUT_PREFIX}'

    print(f"Running map_reduce on data from: {iterdata_s3_prefix}")
    
    # Run the map_reduce
    try:
        result = fexec.map_reduce(
            map_function=map_censor_and_count,
            map_iterdata=iterdata_s3_prefix, # Lithops will iterate over the objects in this prefix
            reduce_function=reduce_sum_insults
        )

        print("\n--- Final Result ---")
        print(f"The total number of censored insults in all files is: {result.get_result()}")

    except Exception as e:
        print(f"There was an error during Lithops execution: {e}")
        import traceback
        traceback.print_exc()
    finally:
        fexec.clean()

    print("Lithops script finished.")