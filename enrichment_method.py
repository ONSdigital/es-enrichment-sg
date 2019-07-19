import json
import boto3
import pandas as pd
import traceback
import os


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


def lambda_handler(event, context):
    """
    Performs enrichment process, joining 2 lookups onto data and detecting anomalies
    :param event - String: Json string representing input
    :param context:
    :return final_output - String: Json string representing enriched dataframe
    """
    try:

        bucket_name = os.environ['bucket_name']
        responder_lookup_file = os.environ['responder_lookup_file']
        county_lookup_file = os.environ['county_lookup_file']

        # Set up clients
        s3 = boto3.resource('s3', region_name='eu-west-2')
        # Reads in responder lookup file
        responder_object = s3.Object(bucket_name, responder_lookup_file)
        responder_content = responder_object.get()['Body'].read()

        # Reads in county lookup file
        county_object = s3.Object(bucket_name, county_lookup_file)
        county_content = county_object.get()['Body'].read()

        input_data = pd.read_json(event)
        responder_lookup = pd.read_json(responder_content)
        county_lookup = pd.read_json(county_content)

        enriched_df, anomalies = data_enrichment(input_data,
                                                 responder_lookup, county_lookup)

        json_out = enriched_df.to_json(orient='records')

        anomaly_out = anomalies.to_json(orient='records')

        combined_out = {"data": json_out, "anomalies": anomaly_out}

        final_output = json.loads(json.dumps(combined_out))

    except Exception as exc:

        return {
            "success": False,
            "error": "Unexpected method exception {}".format(_get_traceback(exc))
        }

    return final_output


def marine_mismatch_detector(data, county_lookup_df, county_lookup_column_3,
                             county_lookup_column_4, period_column, identifier_column):
    """
    Detects references that are producing marine
    but from a county that doesnt produce marine
    :param data - DataFrame: Input data after having been merged
            with responder_county_lookup
    :param county_lookup_df - DataFrame: County_marine_lookup dataframe
    :param county_lookup_column_3 - String: Column from county lookup
            to join on (county)
    :param county_lookup_column_4 - String: Column from county lookup
            representing whether county produces marine or not
    :param period_column - String: Column that holds the period
    :param identifier_column - String: Column that holds
            the unique id of a row(usually responder id)
    :return: bad_data_with_marine - DataFrame: Df containing information
            about any reference that is producing marine when it shouldnt
    """
    data_with_marine = pd.merge(data,
                                county_lookup_df,
                                on=county_lookup_column_3)
    bad_data_with_marine = data_with_marine[(data_with_marine['land_or_marine'] == 'M') &
                                            (data_with_marine[county_lookup_column_4] == 'n')]
    bad_data_with_marine['issue'] = 'Reference should not produce marine data'
    return bad_data_with_marine[[identifier_column,
                                 'issue',
                                 'land_or_marine',
                                 county_lookup_column_4,
                                 period_column]]


def missing_county_detector(data, county_lookup_column_3, identifier_column):
    """
    Detects any references that didnt gain a county
    on the join with the county lookup
    :param data - DataFrame: Input data after being combined
                    with responder_county_lookup
    :param county_lookup_column_3 - String: Column from
                    county lookup to join on (county)
    :param identifier_column - String: Column that holds the unique id
                    of a row(usually responder id)
    :return: data_without_county - DataFrame: DF containing information
                    about any reference without a county.
    """
    data_without_county = data[data[county_lookup_column_3].isnull()]
    data_without_county['issue'] = 'County missing in lookup'

    return data_without_county[[identifier_column, 'issue']]


def missing_region_detector(data, county_lookup_column_2, identifier_column):
    """
    Detects any references that do not have a region
    after merge with county marine lookup
    :param data - DataFrame: Input data after being combined
            with responder_county_lookup and county_marine_lookup
    :param county_lookup_column_2 - String: Column from county lookup
            that contains region(region)
    :param identifier_column - String: Column that holds
            the unique id of a row(usually responder id)
    :return: data_without_region - DataFrame: DF containing information
                about any reference without a region.
    """
    data_without_region = data[data[county_lookup_column_2].isnull()]
    data_without_region['issue'] = 'Region missing in lookup'

    return data_without_region[[identifier_column, 'issue']]


def data_enrichment(data_df, responder_lookup_df, county_lookup_df):
    """
    :param data_df: Pandas DataFrame of data to be enriched
    :param responder_lookup_df: Responder lookup DataFrame
        (map responder code -> county code)
    :param county_lookup_df: County lookup DataFrame (map county code -> county name)
    :return: (Enriched_data - DataFrame:DataFrame of enriched data,
            Anomalies - DataFrame: DF containing info about data
            anomalies detected in the process.)
    """

    identifier_column = os.environ['identifier_column']

    county_lookup_column_1 = os.environ['county_lookup_column_1']
    county_lookup_column_2 = os.environ['county_lookup_column_2']
    county_lookup_column_3 = os.environ['county_lookup_column_3']
    county_lookup_column_4 = os.environ['county_lookup_column_4']

    period_column = os.environ['period_column']

    marine_mismatch_check = os.environ['marine_mismatch_check']
    missing_county_check = os.environ['missing_county_check']
    missing_region_check = os.environ['missing_region_check']

    # Renaming columns to match what is expected
    responder_lookup_df.rename(columns={"ref": identifier_column,
                                        county_lookup_column_3: county_lookup_column_3},
                               inplace=True)
    county_lookup_df.rename(columns={"cty_code":
                                         county_lookup_column_3}, inplace=True)

    enriched_data = pd.merge(data_df, responder_lookup_df,
                             on=identifier_column, how='left')

    anomalies = pd.DataFrame()
    # Do Missing County Check here
    if missing_county_check == 'true':
        anomalies = missing_county_detector(enriched_data,
                                            county_lookup_column_3,
                                            identifier_column)

    # Do Marine mismatch check here
    if marine_mismatch_check == 'true':

        marine_anomalies = marine_mismatch_detector(enriched_data,
                                                    county_lookup_df,
                                                    county_lookup_column_3,
                                                    county_lookup_column_4,
                                                    period_column,
                                                    identifier_column)

        anomalies = pd.concat([marine_anomalies, anomalies])

    enriched_data = pd.merge(enriched_data, county_lookup_df[[county_lookup_column_1,
                                                              county_lookup_column_2,
                                                              county_lookup_column_3]],
                             on=county_lookup_column_3, how='left')

    if missing_region_check == 'true':

        region_anomalies = missing_region_detector(enriched_data,
                                                   county_lookup_column_2,
                                                   identifier_column)

        anomalies = pd.concat([region_anomalies, anomalies])

    return enriched_data, anomalies
