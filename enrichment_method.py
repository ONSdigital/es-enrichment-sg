import json
import logging
import os

import boto3
import pandas as pd
from botocore.exceptions import ClientError
from marshmallow import Schema, fields


class EnvironSchema(Schema):
    bucket_name = fields.Str(required=True)
    responder_lookup_file = fields.Str(required=True)
    county_lookup_file = fields.Str(required=True)
    identifier_column = fields.Str(required=True)
    county_lookup_column_1 = fields.Str(required=True)
    county_lookup_column_2 = fields.Str(required=True)
    county_lookup_column_3 = fields.Str(required=True)
    county_lookup_column_4 = fields.Str(required=True)
    period_column = fields.Str(required=True)
    marine_mismatch_check = fields.Str(required=True)
    missing_county_check = fields.Str(required=True)
    missing_region_check = fields.Str(required=True)


def lambda_handler(event, context):
    """
    Performs enrichment process, joining 2 lookups onto data and detecting anomalies.
    :param event: event object.
    :param context: Context object.
    :return final_output: Json string representing enriched DataFrame - Type: JSON
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
        responder_lookup_file = config["responder_lookup_file"]
        county_lookup_file = config["county_lookup_file"]
        identifier_column = config["identifier_column"]
        county_lookup_column_1 = config["county_lookup_column_1"]
        county_lookup_column_2 = config["county_lookup_column_2"]
        county_lookup_column_3 = config["county_lookup_column_3"]
        county_lookup_column_4 = config["county_lookup_column_4"]
        period_column = config["period_column"]
        marine_mismatch_check = config["marine_mismatch_check"]
        missing_county_check = config["missing_county_check"]
        missing_region_check = config["missing_region_check"]

        logger.info("Retrieved configuration variables.")

        # Set up clients
        s3 = boto3.resource("s3", region_name="eu-west-2")

        # Reads in responder lookup file
        responder_object = s3.Object(bucket_name, responder_lookup_file)
        responder_content = responder_object.get()["Body"].read()

        logger.info("Retrieved responder lookup file from S3.")

        # Reads in county lookup file
        county_object = s3.Object(bucket_name, county_lookup_file)
        county_content = county_object.get()["Body"].read()

        logger.info("Retrieved county lookup file from S3.")

        input_data = pd.read_json(event)
        responder_lookup = pd.read_json(responder_content)
        county_lookup = pd.read_json(county_content)

        logger.info("JSON converted to Pandas DF(s).")

        enriched_df, anomalies = data_enrichment(
            input_data,
            responder_lookup,
            county_lookup,
            identifier_column,
            county_lookup_column_1,
            county_lookup_column_2,
            county_lookup_column_3,
            county_lookup_column_4,
            marine_mismatch_check,
            missing_county_check,
            missing_region_check,
            period_column,
        )

        logger.info("Enrichment function ran successfully.")

        json_out = enriched_df.to_json(orient="records")

        anomaly_out = anomalies.to_json(orient="records")

        logger.info("DF(s) converted back to JSON.")

        combined_out = {"data": json_out, "anomalies": anomaly_out}

        final_output = json.loads(json.dumps(combined_out))

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
            return final_output


def marine_mismatch_detector(data, county_lookup_df, county_lookup_column_3,
                             county_lookup_column_4, period_column, identifier_column):
    """
    Detects references that are producing marine but from a county that doesnt produce marine  # noqa: E501
    :param data: Input data after having been merged with responder_county_lookup - DataFrame
    :param county_lookup_df: County_marine_lookup dataframe - DataFrame
    :param county_lookup_column_3: Column from county lookup to join on (county) - String
    :param county_lookup_column_4: Column from county lookup representing whether county produces marine or not - String
    :param period_column: Column that holds the period - String
    :param identifier_column: Column that holds the unique id of a row(usually responder id) - String
    :return: bad_data_with_marine: Df containing information about any reference that is producing marine when it shouldn't - DataFrame
    """
    data_with_marine = pd.merge(data, county_lookup_df, on=county_lookup_column_3)
    bad_data_with_marine = data_with_marine[
        (data_with_marine["land_or_marine"] == "M")
        & (data_with_marine[county_lookup_column_4] == "n")
    ]
    bad_data_with_marine["issue"] = "Reference should not produce marine data"
    return bad_data_with_marine[
        [
            identifier_column,
            "issue",
            "land_or_marine",
            county_lookup_column_4,
            period_column,
        ]
    ]


def missing_county_detector(data, county_lookup_column_3, identifier_column):
    """
    Detects any references that didnt gain a county on the join with the county lookup # noqa: E501
    :param data: Input data after being combined with responder_county_lookup - DataFrame
    :param county_lookup_column_3: Column from county lookup to join on (county) - String
    :param identifier_column: Column that holds the unique id of a row(usually responder id) - String
    :return: data_without_county: DF containing information about any reference without a county. - DataFrame
    """
    data_without_county = data[data[county_lookup_column_3].isnull()]
    data_without_county["issue"] = "County missing in lookup"

    return data_without_county[[identifier_column, "issue"]]


def missing_region_detector(data, county_lookup_column_2, identifier_column):
    """
    Detects any references that do not have a region after merge with county marine lookup # noqa: E501
    :param data: Input data after being combined with responder_county_lookup and county_marine_lookup - DataFrame
    :param county_lookup_column_2: Column from county lookup that contains region(region) - String
    :param identifier_column: Column that holds the unique id of a row(usually responder id) - String
    :return: data_without_region: DF containing information about any reference without a region. - DataFrame
    """
    data_without_region = data[data[county_lookup_column_2].isnull()]
    data_without_region["issue"] = "Region missing in lookup"

    return data_without_region[[identifier_column, "issue"]]


def data_enrichment(data_df, responder_lookup_df, county_lookup_df, identifier_column,
                    county_lookup_column_1, county_lookup_column_2,
                    county_lookup_column_3, county_lookup_column_4, marine_mismatch_check,
                    missing_county_check, missing_region_check, period_column):
    """
    Does the enrichment process by merging together several datasets. Checks for marine
    mismatch, unallocated county, and unallocated region are performed at this point.
    :param data_df: DataFrame of data to be enriched - dataframe
    :param responder_lookup_df: Responder lookup DataFrame (map responder code -> county code) - dataframe  # noqa: E501
    :param county_lookup_df: County lookup DataFrame (map county code -> county name) - dataframe
    :param identifier_column: Column representing unique id (reponder_id)
    :param county_lookup_column_1: Column from county lookup file (reference) - String
    :param county_lookup_column_2: Column from county lookup file (region) - String
    :param county_lookup_column_3: Column from county lookup file (county) - String
    :param county_lookup_column_4: Column from county lookup file (marine) - String
    :param marine_mismatch_check: True/False - Should check be done  - String
    :param missing_county_check: True/False - Should check be done  - String
    :param missing_region_check: True/False - Should check be done  - String
    :param period_column: Column that holds period. (period) - String
    :return: (Enriched_data - DataFrame:DataFrame of enriched data, Anomalies - DataFrame: DF containing info about data anomalies detected in the process.)
    """

    # Renaming columns to match what is expected
    responder_lookup_df.rename(
        columns={
            "ref": identifier_column,
            county_lookup_column_3: county_lookup_column_3,
        },
        inplace=True,
    )
    county_lookup_df.rename(columns={"cty_code": county_lookup_column_3}, inplace=True)

    enriched_data = pd.merge(
        data_df, responder_lookup_df, on=identifier_column, how="left"
    )

    anomalies = pd.DataFrame()
    # Do Missing County Check here
    if missing_county_check == "true":
        anomalies = missing_county_detector(
            enriched_data, county_lookup_column_3, identifier_column
        )

    # Do Marine mismatch check here
    if marine_mismatch_check == "true":

        marine_anomalies = marine_mismatch_detector(
            enriched_data,
            county_lookup_df,
            county_lookup_column_3,
            county_lookup_column_4,
            period_column,
            identifier_column,
        )

        anomalies = pd.concat([marine_anomalies, anomalies])

    enriched_data = pd.merge(
        enriched_data,
        county_lookup_df[
            [county_lookup_column_1, county_lookup_column_2, county_lookup_column_3]
        ],
        on=county_lookup_column_3,
        how="left",
    )

    if missing_region_check == "true":

        region_anomalies = missing_region_detector(
            enriched_data, county_lookup_column_2, identifier_column
        )

        anomalies = pd.concat([region_anomalies, anomalies])

    return enriched_data, anomalies
