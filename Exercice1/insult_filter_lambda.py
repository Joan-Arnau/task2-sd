import json
import re
import sys
import os

# Add the parent directory of 'conf' to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from conf import conf

# Initial list of insults - now from conf.py
INITIAL_INSULTS = conf.INSULTS_LIST

def filter_text_logic(text_to_filter, insults_list):
    censored_words = []
    words_censored_count = 0
    for word in text_to_filter.split():
        match = re.match(r"^([^\w]*)([\wÀ-ÿ]+)([^\w]*)$", word)
        if match:
            prefix, core_word, suffix = match.groups()
            clean_word = core_word.lower()
        else:
            prefix, core_word, suffix = '', word, ''
            clean_word = word.lower()
        if clean_word in insults_list:
            censored_words.append(f"{prefix}CENSORED{suffix}")
            words_censored_count += 1
        else:
            censored_words.append(word)
    return " ".join(censored_words), words_censored_count

def lambda_handler(event, context):
    try:
        # The EC2 script will send a payload: {'text_to_filter': 'some text'}
        text_to_filter = event.get('text_to_filter')

        if not text_to_filter:
            print("Error: No 'text_to_filter' provided in the event.")
            return {
                'statusCode': 400,
                'body': json.dumps({'error': "No 'text_to_filter' provided."})
            }

        print(f"Received text for filtering: '{text_to_filter}'")
        
        censored_version, count = filter_text_logic(text_to_filter, INITIAL_INSULTS)
        
        print(f"Original: '{text_to_filter}' -> Censored: '{censored_version}' (Words censored: {count})")

        # What Lambda returns can be useful for logs or if the invoker was synchronous.
        # For asynchronous invocation ('Event'), this return does not go directly to the RabbitMQ invoker.
        return {
            'statusCode': 200,
            'body': json.dumps({
                'original_text': text_to_filter,
                'censored_text': censored_version,
                'words_censored': count,
                'message': 'Text filtered successfully by Lambda.'
            })
        }
    except Exception as e:
        print(f"Error in lambda_handler: {str(e)}")
        # Error details for CloudWatch Logs
        import traceback
        traceback.print_exc()
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }