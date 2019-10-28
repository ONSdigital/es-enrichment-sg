import json
import logging
import os

import boto3
from botocore.exceptions import ClientError, IncompleteReadError
from esawsfunctions import funk
from marshmallow import Schema, fields


class EnvironSchema(Schema):
    bucket_name = fields.Str(required=True)
    input_data = fields.Str(required=True)
    queue_url = fields.Str(required=True)
    arn = fields.Str(required=True)
    sqs_messageid_name = fields.Str(required=True)
    method_name = fields.Str(required=True)
    identifier_column = fields.Str(required=True)
    file_name = fields.Str(required=True)


def lambda_handler(event, context):
    """
    Lambda function preparing data for enrichment and then
    calling the enrichment method.
    :param event: Json string representing input - String
    :param context:
    :return Json: success and checkpoint information,
                  and/or indication of error message.
    """
    # set up logger
    current_module = "Enrichment - Wrangler"
    error_message = ''
    log_message = ''
    logger = logging.getLogger("Enrichment")
    logger.setLevel(10)
    try:
        logger.info("Enrichment Wrangler Begun")

        checkpoint = event["RuntimeVariables"]["checkpoint"]
        schema = EnvironSchema()
        config, errors = schema.load(os.environ)
        if errors:
            logger.error(f"Error validating environment params: {errors}")
            raise ValueError(f"Error validating environment params: {errors}")

        logger.info("Validated params.")

        # env vars
        file_name = config["file_name"]
        bucket_name = config["bucket_name"]
        input_data = config["input_data"]
        queue_url = config["queue_url"]
        arn = config["arn"]
        sqs_messageid_name = config["sqs_messageid_name"]
        method_name = config["method_name"]
        identifier_column = config["identifier_column"]

        logger.info("Retrieved configuration variables")

        # set up client
        lambda_client = boto3.client("lambda", region_name="eu-west-2")

        data_df = funk.read_dataframe_from_s3(bucket_name, input_data)

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

        funk.save_data(bucket_name, file_name, str(final_output), queue_url,
                       sqs_messageid_name)

        logger.info("Successfully sent data to sqs.")

        funk.send_sns_message_with_anomalies(checkpoint, anomalies, arn, "Enrichment")

        logger.info("Successfully sent data to sns.")
        checkpoint = checkpoint + 1
    # raise value validation error
    except ValueError as e:
        error_message = "Parameter validation error" + current_module \
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
