import logging
import os

import pandas as pd
from botocore.exceptions import ClientError
from es_aws_functions import aws_functions
from marshmallow import Schema, fields


class EnvironSchema(Schema):
    bucket_name = fields.Str(required=True)


def do_merge(input_data, join_data, columns_to_keep, join_column, bucket_name):
    """
    Generic merging function.

    :param input_data: Input data from previous step - Dataframe
    :param join_data: key of lookup file to pick up from s3 - String
    :param columns_to_keep: List of columns from lookup to pick up - List(String)
    :param join_column: Column to join lookup on with - String
    :param bucket_name: Name of bucket to get file - String
    :return outdata: Dataframe with lookup merged on.
    """
    # read and df the joindata
    join_dataframe = aws_functions.read_dataframe_from_s3(bucket_name, join_data)

    #  merge joindata onto main dataset using defined join column
    outdata = pd.merge(input_data,
                       join_dataframe[columns_to_keep],
                       on=join_column, how="left")
    return outdata


def lambda_handler(event, context):
    """
    Performs enrichment process, joining 2 lookups onto data and detecting anomalies.
    :param event: event object.
    :param context: Context object.
    :return final_output: Dict with "success",
            "data" and "anomalies" or "success and "error".
    """
    # set up logger
    current_module = "Enrichment - Method"
    error_message = ''
    log_message = ''
    logger = logging.getLogger("Enrichment")
    logger.setLevel(10)
    try:
        logger.info("Starting Enrichment Method")

        schema = EnvironSchema()
        config, errors = schema.load(os.environ)
        if errors:
            logger.error(f"Error validating environment params: {errors}")
            raise ValueError(f"Error validating environment params: {errors}")

        logger.info("Validated params.")

        bucket_name = config["bucket_name"]

        logger.info("Retrieved configuration variable.")

        # Retrieve data and behaviour information
        data = event['data']
        lookups = event['lookups']
        parameters = event['parameters']
        logger.info("Retrieved data and behaviour from wrangler.")

        identifier_column = parameters["identifier_column"]
        survey_column = parameters["survey_column"]
        period_column = parameters["period_column"]
        marine_mismatch_check = parameters["marine_mismatch_check"]
        logger.info("Retrieved parameters from event.")

        input_data = pd.read_json(data)

        logger.info("JSON converted to Pandas DF(s).")

        enriched_df, anomalies = data_enrichment(input_data,
                                                 marine_mismatch_check,
                                                 survey_column,
                                                 period_column,
                                                 bucket_name,
                                                 lookups,
                                                 identifier_column)

        logger.info("Enrichment function ran successfully.")

        json_out = enriched_df.to_json(orient="records")

        anomaly_out = anomalies.to_json(orient="records")

        logger.info("DF(s) converted back to JSON.")

        final_output = {"data": json_out, "anomalies": anomaly_out}
    # raise value validation error
    except ValueError as e:
        error_message = "Parameter validation error" + current_module \
                        + " |- " + str(e.args) + " | Request ID: " \
                        + str(context.aws_request_id)
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    # raise client based error
    except ClientError as e:
        error_message = "AWS Error (" + str(e.response['Error']['Code']) \
                        + ") " + current_module + " |- " + str(e.args) \
                        + " | Request ID: " + str(context.aws_request_id)
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    # raise key/index error
    except KeyError as e:
        error_message = "Key Error in " + current_module + " |- " + \
                        str(e.args) + " | Request ID: " \
                        + str(context.aws_request_id)
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    # general exception
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
    final_output['success'] = True
    return final_output


def marine_mismatch_detector(data,
                             survey_column,
                             check_column,
                             period_column,
                             identifier_column):
    """
    Detects references that are producing marine but from a county that doesnt produce marine  # noqa: E501
    :param data: Input data after having been merged with responder_county_lookup - DataFrame
    :param survey_column: Survey code value - String
    :param check_column: column to check against(marine) - String
    :param period_column: Column that holds the period - String
    :param identifier_column: Column that holds the unique id of a row(usually responder id) - String
    :return: bad_data_with_marine: Df containing information about any reference that is producing marine when it shouldn't - DataFrame
    """

    bad_data = data[
        (data[survey_column] == "076")
        & (data[check_column] == "n")
        ]
    bad_data["issue"] = "Reference should not produce marine data"
    return bad_data[
        [
            identifier_column,
            "issue",
            survey_column,
            check_column,
            period_column,
        ]
    ]


def missing_column_detector(data, columns_to_check, identifier_column):
    """
    Detects any references that has null values for specified columns # noqa: E501
    :param data: Input data after being combined with lookup(s) - DataFrame
    :param columns_to_check: List of columns to check for - list(String)
    :param identifier_column: Column that holds the unique id of a row(usually responder id) - String
    :return: data_without_columns: DF containing information about any reference without the column. - DataFrame
    """
    # Create empty dataframe to hold output
    data_without_columns = pd.DataFrame()

    # For each of the passed in columns to check(1 or more)
    # Create dataframe holding rows where column was null
    for column_to_check in columns_to_check:
        data_without_column = data[data[column_to_check].isnull()]
        data_without_column["issue"] = str(column_to_check) + " missing in lookup"
        data_without_columns = pd.concat([data_without_columns, data_without_column])

    return data_without_columns[[identifier_column, "issue"]]


def data_enrichment(data_df,
                    marine_mismatch_check,
                    survey_column,
                    period_column,
                    bucket_name,
                    lookups,
                    identifier_column):
    """
    Does the enrichment process by merging together several datasets. Checks for marine
    mismatch, unallocated county, and unallocated region are performed at this point.
    :param data_df: DataFrame of data to be enriched - dataframe
    :param marine_mismatch_check: True/False - Should check be done  - String
    :param survey_column: Survey code value - String
    :param period_column: Column that holds period. (period) - String
    :param bucket_name: Name of the s3 bucket - String
    :param lookups: Information about lookups required. - String(json)
    :param identifier_column: Column representing unique id (responder_id)


    :return: Enriched_data - DataFrame:DataFrame of enriched data.
    :return: Anomalies - DataFrame: DF containing info
                         about data anomalies detected in the process.
    """

    required_columns = []
    for lookup in lookups:
        required_columns.append(lookups[lookup]['required'])
        file_name = lookups[lookup]['file_name']
        columns_to_keep = lookups[lookup]['columns_to_keep']
        join_column = lookups[lookup]['join_column']
        data_df = do_merge(data_df, file_name, columns_to_keep, join_column, bucket_name)

    anomalies = pd.DataFrame()

    # missing column detection
    for column in required_columns:
        anomalies = pd.concat([anomalies,
                               missing_column_detector(data_df,
                                                       column,
                                                       identifier_column)])

    # Do Marine mismatch check here
    if marine_mismatch_check == "true":
        marine_anomalies = marine_mismatch_detector(
            data_df,
            survey_column,
            "marine",
            period_column,
            identifier_column,
        )

        anomalies = pd.concat([marine_anomalies, anomalies])

    return data_df, anomalies
