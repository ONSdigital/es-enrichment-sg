import json
import logging
import os

import boto3
from botocore.exceptions import ClientError, IncompleteReadError
from esawsfunctions import funk
from marshmallow import Schema, fields


class EnvironSchema(Schema):
    checkpoint = fields.Str(required=True)
    bucket_name = fields.Str(required=True)
    identifier_column = fields.Str(required=True)
    in_file_name = fields.Str(required=True)
    incoming_message_group = fields.Str(required=True)
    method_name = fields.Str(required=True)
    out_file_name = fields.Str(required=True)
    sqs_queue_url = fields.Str(required=True)
    sns_topic_arn = fields.Str(required=True)
    sqs_message_group_id = fields.Str(required=True)


def lambda_handler(event, context):
    """
    Lambda function preparing data for enrichment and then calling the enrichment method.
    :param event: Json string representing input - String
    :param context:
    :return Json: success and checkpoint information, and/or indication of error message.
    """
    # set up logger
    current_module = "Enrichment - Wrangler"
    error_message = ''
    log_message = ''
    logger = logging.getLogger("Enrichment")
    logger.setLevel(10)
    try:
        logger.info("Enrichment Wrangler Begun")
        schema = EnvironSchema()
        config, errors = schema.load(os.environ)
        if errors:
            logger.error(f"Error validating environment params: {errors}")
            raise ValueError(f"Error validating environment params: {errors}")

        logger.info("Validated params.")

        # env vars
        checkpoint = config["checkpoint"]
        bucket_name = config["bucket_name"]
        identifier_column = config["identifier_column"]
        in_file_name = config["in_file_name"]
        incoming_message_group = config["incoming_message_group"]
        method_name = config["method_name"]
        out_file_name = config["out_file_name"]
        sns_topic_arn = config["sns_topic_arn"]
        sqs_message_group_id = config["sqs_message_group_id"]
        sqs_queue_url = config["sqs_queue_url"]

        logger.info("Retrieved configuration variables")

        # set up client
        lambda_client = boto3.client("lambda", region_name="eu-west-2")

        data_df = funk.get_dataframe(sqs_queue_url, bucket_name, in_file_name,
                                     incoming_message_group)

        logger.info("Retrieved data from s3")
        wrangled_data = wrangle_data(data_df, identifier_column)
        logger.info("Data Wrangled")

        response = lambda_client.invoke(
            FunctionName=method_name, Payload=json.dumps(wrangled_data)
        )

        logger.info("Method Called")
        json_response = json.loads(response.get("Payload").read().decode("utf-8"))
        logger.info("Json extracted from method response.")

        anomalies = json_response["anomalies"]
        final_output = str(json_response["data"])

        funk.save_data(bucket_name, out_file_name, final_output, sqs_queue_url,
                       sqs_message_group_id)

        logger.info("Successfully sent data to s3.")

        funk.send_sns_message_with_anomalies(checkpoint, sns_topic_arn, "Enrichment.",
                                             anomalies)

        logger.info("Successfully sent message to sns.")
        checkpoint = checkpoint + 1
    # raise value validation error
    except ValueError as e:
        error_message = "Parameter validation error in " + current_module \
                        + " |- " + str(e.args) + " | Request ID: " \
                        + str(context['aws_request_id'])
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    # raise client based error
    except ClientError as e:
        error_message = "AWS Error (" + str(e.response['Error']['Code']) \
                        + ") " + current_module + " |- " + str(e.args) \
                        + " | Request ID: " + str(context['aws_request_id'])
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    # raise key/index error
    except KeyError as e:
        error_message = "Key Error in " + current_module + " |- " + \
                        str(e.args) + " | Request ID: " \
                        + str(context['aws_request_id'])
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    # raise error in lambda response
    except IncompleteReadError as e:
        error_message = "Incomplete Lambda response encountered in " \
                        + current_module + " |- " + \
                        str(e.args) + " | Request ID: " \
                        + str(context['aws_request_id'])
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    # general exception
    except Exception as e:
        error_message = "General Error in " + current_module +  \
                            " (" + str(type(e)) + ") |- " + str(e.args) + \
                            " | Request ID: " + str(context['aws_request_id'])
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    finally:
        if(len(error_message)) > 0:
            logger.error(log_message)
            return {"success": False, "error": error_message}
        else:
            logger.info("Successfully completed module: " + current_module)
            return {"success": True, "checkpoint": checkpoint}


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
