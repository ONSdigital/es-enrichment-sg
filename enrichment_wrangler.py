import json
import logging
import os

import boto3
from es_aws_functions import aws_functions, exception_classes, general_functions
from marshmallow import EXCLUDE, Schema, fields


class EnvironmentSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    def handle_error(self, e, data, **kwargs):
        logging.error(f"Error validating environment params: {e}")
        raise ValueError(f"Error validating environment params: {e}")

    bucket_name = fields.Str(required=True)
    checkpoint = fields.Str(required=True)
    identifier_column = fields.Str(required=True)
    method_name = fields.Str(required=True)


class RuntimeSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    def handle_error(self, e, data, **kwargs):
        logging.error(f"Error validating runtime params: {e}")
        raise ValueError(f"Error validating runtime params: {e}")

    lookups = fields.Dict(required=True)
    in_file_name = fields.Str(required=True)
    incoming_message_group_id = fields.Str(required=True)
    location = fields.Str(required=True)
    out_file_name = fields.Str(required=True)
    outgoing_message_group_id = fields.Str(required=True)
    marine_mismatch_check = fields.Boolean(required=True)
    period_column = fields.Str(required=True)
    sns_topic_arn = fields.Str(required=True)
    queue_url = fields.Str(required=True)
    survey_column = fields.Str(required=True)


def lambda_handler(event, context):
    """
    Lambda function preparing data for enrichment and then calling the enrichment method.
    :param event: Json string representing input - String
    :param context:
    :return Json: success and checkpoint information, and/or indication of error message.
    """

    # Set up logger.
    current_module = "Enrichment - Wrangler"
    error_message = ""
    logger = logging.getLogger("Enrichment")
    logger.setLevel(10)

    # Define run_id outside of try block
    run_id = 0
    try:
        logger.info("Enrichment Wrangler Begun")
        # Retrieve run_id before input validation
        # Because it is used in exception handling
        run_id = event["RuntimeVariables"]["run_id"]

        environment_variables = EnvironmentSchema().load(os.environ)

        runtime_variables = RuntimeSchema().load(event["RuntimeVariables"])

        logger.info("Validated parameters.")

        # Environment Variables.
        bucket_name = environment_variables["bucket_name"]
        checkpoint = environment_variables["checkpoint"]
        identifier_column = environment_variables["identifier_column"]
        method_name = environment_variables["method_name"]

        # Runtime Variables.
        lookups = runtime_variables["lookups"]
        in_file_name = runtime_variables["in_file_name"]
        incoming_message_group_id = runtime_variables["incoming_message_group_id"]
        location = runtime_variables["location"]
        out_file_name = runtime_variables["out_file_name"]
        outgoing_message_group_id = runtime_variables["outgoing_message_group_id"]
        marine_mismatch_check = runtime_variables["marine_mismatch_check"]
        period_column = runtime_variables["period_column"]
        sns_topic_arn = runtime_variables["sns_topic_arn"]
        sqs_queue_url = runtime_variables["queue_url"]
        survey_column = runtime_variables["survey_column"]

        logger.info("Retrieved configuration variables.")

        # Set up client.
        lambda_client = boto3.client("lambda", region_name="eu-west-2")
        sqs = boto3.client("sqs", region_name="eu-west-2")
        data_df, receipt_handler = aws_functions.get_dataframe(sqs_queue_url, bucket_name,
                                                               in_file_name,
                                                               incoming_message_group_id,
                                                               location)

        logger.info("Retrieved data from s3")
        data_json = data_df.to_json(orient="records")
        json_payload = {
            "RuntimeVariables": {
                "data": data_json,
                "lookups": lookups,
                "marine_mismatch_check": marine_mismatch_check,
                "survey_column": survey_column,
                "period_column": period_column,
                "identifier_column": identifier_column,
                "run_id": run_id
            }
        }
        response = lambda_client.invoke(
            FunctionName=method_name,
            Payload=json.dumps(json_payload)
        )

        logger.info("Successfully invoked method.")
        json_response = json.loads(response.get("Payload").read().decode("utf-8"))
        logger.info("JSON extracted from method response.")

        if not json_response["success"]:
            raise exception_classes.MethodFailure(json_response["error"])

        aws_functions.save_data(bucket_name, out_file_name, json_response["data"],
                                sqs_queue_url, outgoing_message_group_id, location)

        logger.info("Successfully sent data to s3.")

        anomalies = json_response["anomalies"]

        if anomalies != "[]":
            aws_functions.save_to_s3(bucket_name, "Anomalies", anomalies, location)
            have_anomalies = True
        else:
            have_anomalies = False

        aws_functions.send_sns_message_with_anomalies(checkpoint, have_anomalies,
                                                      sns_topic_arn, "Enrichment.")
        if receipt_handler:
            sqs.delete_message(QueueUrl=sqs_queue_url, ReceiptHandle=receipt_handler)
        logger.info("Successfully sent message to sns.")

    except Exception as e:
        error_message = general_functions.handle_exception(e, current_module,
                                                           run_id, context)
    finally:
        if (len(error_message)) > 0:
            logger.error(error_message)
            raise exception_classes.LambdaFailure(error_message)

    logger.info("Successfully completed module: " + current_module)
    return {"success": True, "checkpoint": checkpoint}
