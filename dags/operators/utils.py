import json
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def get_object_from_s3(key, bucket_name):
    """
    Load S3 object as JSON
    """

    hook = S3Hook()
    content_object = hook.get_key(key=key, bucket_name=bucket_name)
    file_content = content_object.get()["Body"].read().decode("utf-8")
    try:
        result = json.loads(file_content)
    except:
        result = file_content
    finally:
        return result


def add_to_dictionary(dictionary, name, value):
    # if name not in dictionary, add
    if dictionary.get(name) is None:
        dictionary[name] = value
    else:
        if isinstance(value, str):
            dictionary[name] = value
        if isinstance(value, list):
            dictionary[name] += value
        # if dictionary contains dictionary, iterate
        if isinstance(value, dict):
            for n2, v2 in value.items():
                add_to_dictionary(dictionary[name], n2, v2)
