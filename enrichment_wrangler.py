import json
import traceback
import boto3
import pandas as pd
import os
import random


def _get_traceback(exception):
    """
    Given an exception, returns the traceback as a string.
    :param exception: Exception object
    :return: string
    """

    return ''.join(
        traceback.format_exception(
            etype=type(exception), value=exception, tb=exception.__traceback__
        )
    )

def get_environment_variable(variable):
    output = os.environ.get(variable,None)
    if(output is None):
        raise ValueError(str(variable)+" config parameter missing")
    return output

def get_from_s3(bucket_name, key):
    """
    Given the name of the bucket and the filename(key), this function will return a dataframe.
    File to get MUST be json format.
    :param bucket_name - String: Name of the s3 bucket
    :param key - String: path & name of the file
    :return data_df - DataFrame: Dataframe created from the json file in s3
    """
    s3 = boto3.resource('s3', region_name='eu-west-2')
    content_object = s3.Object(bucket_name, key)
    file_content = content_object.get()['Body'].read().decode('utf-8')
    json_content = json.loads(file_content)
    data_df = pd.DataFrame(json_content)

    return data_df


def send_sqs_message(queue_url, message, sqs_messageid_name):
    """
    Will send an sqs message to a specified queue
    :param queue_url - String: The url of the queue to send to.
    :param message - String: The message to be sent
                    (json string representation of dataframe)
    :param sqs_messageid_name - String: The messageid to attach to the message
    :return Nothing:
    """
    sqs = boto3.client('sqs', region_name='eu-west-2')
    sqs.send_message(QueueUrl=queue_url, MessageBody=message,
                     MessageGroupId=sqs_messageid_name,
                     MessageDeduplicationId=str(random.getrandbits(128)))


def lambda_handler(event, context):
    """
    Lambda function preparing data for enrichment and then calling the enrichment method.
    :param event - String: Json string representing input
    :param context:
    :return Json: success and checkpoint information, andor indication of error message.
    """
    try:
        checkpoint = event['RuntimeVariables']['checkpoint']
        # S3
        bucket_name = get_environment_variable('bucket_name')
        input_data = get_environment_variable('input_data')

        # Set up client
        lambda_client = boto3.client('lambda', region_name='eu-west-2')

        # Sqs
        queue_url = get_environment_variable('queue_url')

        # Sns
        arn = get_environment_variable('arn')

        sqs_messageid_name = get_environment_variable('sqs_messageid_name')
        method_name = get_environment_variable('method_name')

        identifier_column = get_environment_variable('identifier_column')

        data_df = get_from_s3(bucket_name, input_data)
        wrangled_data = wrangle_data(data_df, identifier_column)

        response = lambda_client.invoke(FunctionName=method_name,
                                        Payload=json.dumps(wrangled_data))
        json_response = response.get('Payload').read().decode("utf-8")

        final_output = json.loads(json_response)
        anomalies = final_output['anomalies']
        final_output = final_output['data']

        send_sqs_message(queue_url, final_output, sqs_messageid_name)

        send_sns_message(arn, anomalies, checkpoint)
        checkpoint = checkpoint + 1

    except Exception as exc:

        sqs = boto3.client('sqs', region_name='eu-west-2')
        sqs.purge_queue(
            QueueUrl=queue_url
        )

        return {
            "success": False,
            "checkpoint": checkpoint,
            "error": "Unexpected Wrangler exception {}".format(_get_traceback(exc))
        }

    return {
        "success": True,
        "checkpoint": checkpoint
    }


def send_sns_message(arn, anomalies, checkpoint):
    """
    Sends metadata to an sns queue:
        Success: Whether process succeeded or not
        module: Current module
        checkpoint: Module number of process
        anomalies: Data anomalies picked up during enrichment
                    (non-failing but indicate problems with data)
        message: Summary of metadata.
    :param arn - String: Url of the sns queue
    :param anomalies - String: json string representing data anomalies
    :param checkpoint - int: Current 'module' of process
    :return Nothing:
    """
    sns = boto3.client('sns', region_name='eu-west-2')
    sns_message = {
        "success": True,
        "module": "Enrichment",
        "checkpoint": checkpoint,
        "anomalies": anomalies,
        "message": "Completed Enrichment"

    }

    sns.publish(
        TargetArn=arn,
        Message=json.dumps(sns_message)
    )


def wrangle_data(data_df, identifier_column):
    """
    Prepares data for the enrichment step. May not be necessary going forward.
    Renames a column and returns df as json
    :param data_df - DataFrame: Main input data to process
    :param identifier_column - String: The name of the column
                                representing unique id(usually responder_id)
    :return data_json - String: Json String representation of the input dataframe.
    """
    data_df.rename(columns={"idbr": identifier_column}, inplace=True)
    data_json = data_df.to_json(orient='records')

    return data_json
