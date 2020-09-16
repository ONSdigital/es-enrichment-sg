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
    identifier_column = fields.Str(required=True)
    method_name = fields.Str(required=True)


class RuntimeSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    def handle_error(self, e, data, **kwargs):
        logging.error(f"Error validating runtime params: {e}")
        raise ValueError(f"Error validating runtime params: {e}")

    bpm_queue_url = fields.Str(required=True)
    in_file_name = fields.Str(required=True)
    lookups = fields.Dict(required=True)
    marine_mismatch_check = fields.Boolean(required=True)
    out_file_name = fields.Str(required=True)
    period_column = fields.Str(required=True)
    sns_topic_arn = fields.Str(required=True)
    survey_column = fields.Str(required=True)
    total_steps = fields.Str(required=True)


def lambda_handler(event, context):
    """
    Lambda function preparing data for enrichment and then calling the enrichment method.
    :param event: Json string representing input - String
    :param context:
    :return Json: success and/or indication of error message.
    """

    # Set up logger.
    current_module = "Enrichment - Wrangler"
    error_message = ""
    logger = general_functions.get_logger()

    bpm_queue_url = None
    current_step_num = "2"

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
        identifier_column = environment_variables["identifier_column"]
        method_name = environment_variables["method_name"]

        # Runtime Variables.
        bpm_queue_url = runtime_variables["bpm_queue_url"]
        lookups = runtime_variables["lookups"]
        in_file_name = runtime_variables["in_file_name"]
        out_file_name = runtime_variables["out_file_name"]
        marine_mismatch_check = runtime_variables["marine_mismatch_check"]
        period_column = runtime_variables["period_column"]
        sns_topic_arn = runtime_variables["sns_topic_arn"]
        survey_column = runtime_variables["survey_column"]
        total_steps = runtime_variables["total_steps"]

        logger.info("Retrieved configuration variables.")

        # Send start of method status to BPM.
        status = "IN PROGRESS"
        aws_functions.send_bpm_status(bpm_queue_url, current_module, status, run_id,
                                      current_step_num, total_steps)

        # Set up client.
        lambda_client = boto3.client("lambda", region_name="eu-west-2")

        data_df = aws_functions.read_dataframe_from_s3(bucket_name, in_file_name)

        logger.info("Retrieved data from s3")
        data_json = data_df.to_json(orient="records")
        json_payload = {
            "RuntimeVariables": {
                "bpm_queue_url": bpm_queue_url,
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

        aws_functions.save_to_s3(bucket_name, out_file_name, json_response["data"])

        logger.info("Successfully sent data to s3.")

        anomalies = json_response["anomalies"]

        if anomalies != "[]":
            aws_functions.save_to_s3(bucket_name, "Enrichment_Anomalies", anomalies)
            have_anomalies = True
        else:
            have_anomalies = False

        aws_functions.send_sns_message_with_anomalies(have_anomalies,
                                                      sns_topic_arn, "Enrichment.")

        logger.info("Successfully sent message to sns.")

    except Exception as e:
        error_message = general_functions.handle_exception(e, current_module,
                                                           run_id, context=context,
                                                           bpm_queue_url=bpm_queue_url)

    finally:
        if (len(error_message)) > 0:
            logger.error(error_message)
            raise exception_classes.LambdaFailure(error_message)

    logger.info("Successfully completed module: " + current_module)

    # Send end of method status to BPM.
    status = "DONE"
    aws_functions.send_bpm_status(bpm_queue_url, current_module, status, run_id,
                                  current_step_num, total_steps)

    return {"success": True}
