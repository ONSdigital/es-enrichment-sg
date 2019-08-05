import json
import traceback
import boto3
import pandas as pd
import os
import random
from marshmallow import Schema, fields
import logging


def _get_traceback(exception):
    """
    Given an exception, returns the traceback as a string.
    :param exception: Exception object
    :return: string
    """

    return "".join(
        traceback.format_exception(
            etype=type(exception), value=exception, tb=exception.__traceback__
        )
    )


def get_from_s3(bucket_name, key):
    """
    Given the name of the bucket and the filename(key), this function will
    return a dataframe.
    File to get MUST be json format.
    :param bucket_name: Name of the s3 bucket - String
    :param key: path & name of the file - String
    :return data_df: Dataframe created from the json file in s3 - DataFrame
    """
    s3 = boto3.resource("s3", region_name="eu-west-2")
    content_object = s3.Object(bucket_name, key)
    file_content = content_object.get()["Body"].read().decode("utf-8")
    json_content = json.loads(file_content)
    data_df = pd.DataFrame(json_content)

    return data_df


def send_sqs_message(queue_url, message, sqs_messageid_name):
    """
    Will send an sqs message to a specified queue
    :param queue_url: The url of the queue to send to. - String
    :param message: The message to be sent
                            (json string representation of dataframe) - String
    :param sqs_messageid_name: The messageid to attach to the message - String
    :return Nothing:
    """
    sqs = boto3.client("sqs", region_name="eu-west-2")
    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=message,
        MessageGroupId=sqs_messageid_name,
        MessageDeduplicationId=str(random.getrandbits(128)),
    )


class EnvironSchema(Schema):
    bucket_name = fields.Str(required=True)
    input_data = fields.Str(required=True)
    queue_url = fields.Str(required=True)
    arn = fields.Str(required=True)
    sqs_messageid_name = fields.Str(required=True)
    method_name = fields.Str(required=True)
    identifier_column = fields.Str(required=True)


def lambda_handler(event, context):
    """
    Lambda function preparing data for enrichment and then
    calling the enrichment method.
    :param event: Json string representing input - String
    :param context:
    :return Json: success and checkpoint information,
                    andor indication of error message.
    """
    try:
        logger = logging.getLogger("Enrichment")
        logger.info("Enrichment Begun")

        checkpoint = event["RuntimeVariables"]["checkpoint"]
        schema = EnvironSchema()
        config, errors = schema.load(os.environ)
        if errors:
            logger.error(f"Error validating environment params: {errors}")
            raise ValueError(f"Error validating environment params: {errors}")

        # S3
        bucket_name = config["bucket_name"]
        input_data = config["input_data"]

        queue_url = config["queue_url"]

        arn = config["arn"]

        sqs_messageid_name = config["sqs_messageid_name"]
        method_name = config["method_name"]

        identifier_column = config["identifier_column"]
        logger.info("Retrieved configuration variables")
        # Set up client
        lambda_client = boto3.client("lambda", region_name="eu-west-2")

        data_df = get_from_s3(bucket_name, input_data)

        logger.info("Retrieved data from s3")
        wrangled_data = wrangle_data(data_df, identifier_column)
        logger.info("Data Wrangled")

        response = lambda_client.invoke(
            FunctionName=method_name, Payload=json.dumps(wrangled_data)
        )
        logger.info("Method Called")
        json_response = response.get("Payload").read().decode("utf-8")
        logger.info("Json extracted from method response.")

        final_output = json.loads(json_response)
        anomalies = final_output["anomalies"]
        final_output = final_output["data"]

        send_sqs_message(queue_url, final_output, sqs_messageid_name)
        send_sns_message(arn, anomalies, checkpoint)
        checkpoint = checkpoint + 1

    except Exception as exc:
        logger.error("An error has occured during wrangling")
        return {
            "success": False,
            "checkpoint": checkpoint,
            "error": "Unexpected Wrangler exception {}".format(_get_traceback(exc)),
        }
    logger.info("Wrangler completed succesfully")
    return {"success": True, "checkpoint": checkpoint}


def send_sns_message(arn, anomalies, checkpoint):
    """
    Sends metadata to an sns queue:
        Success: Whether process succeeded or not
        module: Current module
        checkpoint: Module number of process
        anomalies: Data anomalies picked up during enrichment
        (non-failing but indicate problems with data)
        message: Summary of metadata.
    :param arn: Url of the sns queue - String
    :param anomalies: json string representing data anomalies - String
    :param checkpoint: Current 'module' of process - int
    :return Nothing:
    """
    sns = boto3.client("sns", region_name="eu-west-2")
    sns_message = {
        "success": True,
        "module": "Enrichment",
        "checkpoint": checkpoint,
        "anomalies": anomalies,
        "message": "Completed Enrichment",
    }

    return sns.publish(TargetArn=arn, Message=json.dumps(sns_message))


def wrangle_data(data_df, identifier_column):
    """
    Prepares data for the enrichment step. May not be necessary going forward. # noqa: E501
    Renames a column and returns df as json
    :param data_df: Main input data to process - DataFrame
    :param identifier_column: The name of the column representing unique id
                    (usually responder_id) - String
    :return data_json: Json String representation of the input dataframe. - String
    """
    data_df.rename(columns={"idbr": identifier_column}, inplace=True)
    data_json = data_df.to_json(orient="records")

    return data_json
