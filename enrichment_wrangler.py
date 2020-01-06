import json
import logging
import os

import boto3
from botocore.exceptions import ClientError, IncompleteReadError
from es_aws_functions import aws_functions, exception_classes
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
    marine_mismatch_check = fields.Str(required=True)
    period_column = fields.Str(required=True)
    lookup_info = fields.Str(required=True)


def lambda_handler(event, context):
    """
    Lambda function preparing data for enrichment and then calling the enrichment method.
    :param event: Json string representing input - String
    :param context:
    :return Json: success and checkpoint information, and/or indication of error message.
    """

    # Set up logger.
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

        # Env vars.
        checkpoint = int(config["checkpoint"])
        bucket_name = config["bucket_name"]
        in_file_name = config["in_file_name"]
        incoming_message_group = config["incoming_message_group"]
        method_name = config["method_name"]
        out_file_name = config["out_file_name"]
        sns_topic_arn = config["sns_topic_arn"]
        sqs_message_group_id = config["sqs_message_group_id"]
        sqs_queue_url = config["sqs_queue_url"]

        logger.info("Retrieved configuration variables")

        # Set up client.
        lambda_client = boto3.client("lambda", region_name="eu-west-2")
        sqs = boto3.client("sqs", region_name='eu-west-2')
        data_df, receipt_handler = aws_functions.get_dataframe(sqs_queue_url, bucket_name,
                                                               in_file_name,
                                                               incoming_message_group)
        # Parameters.
        marine_mismatch_check = config['marine_mismatch_check']
        survey_column = event['RuntimeVariables']["survey_column"]
        period_column = config['period_column']
        identifier_column = config['identifier_column']

        # Lookup info.
        lookup_info = config['lookup_info']

        # Create parameter json from environment variables.
        parameters = {"marine_mismatch_check": marine_mismatch_check,
                      "survey_column": survey_column,
                      "period_column": period_column,
                      "identifier_column": identifier_column}

        logger.info("Retrieved data from s3")
        data_json = data_df.to_json(orient="records")
        json_payload = {
            "data": json.dumps(data_json),
            "lookups": lookup_info,
            "parameters": json.dumps(parameters),
            "survey_column": survey_column
        }
        response = lambda_client.invoke(
            FunctionName=method_name,
            Payload=json.dumps(json_payload)
        )

        logger.info("Successfully invoked method.")
        json_response = json.loads(response.get("Payload").read().decode("utf-8"))
        logger.info("JSON extracted from method response.")

        if not json_response['success']:
            raise exception_classes.MethodFailure(json_response['error'])

        anomalies = json_response["anomalies"]

        aws_functions.save_data(bucket_name, out_file_name, json_response["data"],
                                sqs_queue_url, sqs_message_group_id)

        logger.info("Successfully sent data to s3.")

        aws_functions.send_sns_message_with_anomalies(checkpoint, anomalies,
                                                      sns_topic_arn, "Enrichment.")
        if receipt_handler:
            sqs.delete_message(QueueUrl=sqs_queue_url, ReceiptHandle=receipt_handler)
        logger.info("Successfully sent message to sns.")
        checkpoint += 1

    # Raise value validation error.
    except ValueError as e:
        error_message = "Parameter validation error in " + current_module \
                        + " |- " + str(e.args) + " | Request ID: " \
                        + str(context.aws_request_id)
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    # Raise client based error.
    except ClientError as e:
        error_message = "AWS Error (" + str(e.response['Error']['Code']) \
                        + ") " + current_module + " |- " + str(e.args) \
                        + " | Request ID: " + str(context.aws_request_id)
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    # Raise key/index error.
    except KeyError as e:
        error_message = "Key Error in " + current_module + " |- " + \
                        str(e.args) + " | Request ID: " \
                        + str(context.aws_request_id)
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    # Raise error in lambda response.
    except IncompleteReadError as e:
        error_message = "Incomplete Lambda response encountered in " \
                        + current_module + " |- " + \
                        str(e.args) + " | Request ID: " \
                        + str(context.aws_request_id)
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    # Raise the Method Failing.
    except exception_classes.MethodFailure as e:
        error_message = e.error_message
        log_message = "Error in " + method_name + "."
    # Raise a general exception.
    except Exception as e:
        error_message = "General Error in " + current_module + \
                        " (" + str(type(e)) + ") |- " + str(e.args) + \
                        " | Request ID: " + str(context.aws_request_id)
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    finally:
        if (len(error_message)) > 0:
            logger.error(log_message)
            return {"success": False, "error": error_message}

    logger.info("Successfully completed module: " + current_module)
    return {"success": True, "checkpoint": checkpoint}
