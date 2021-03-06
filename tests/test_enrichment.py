import json
from unittest import mock

import pandas as pd
import pytest
from es_aws_functions import exception_classes, test_generic_library
from moto import mock_s3
from pandas.testing import assert_frame_equal

import enrichment_method as lambda_method_function
import enrichment_wrangler as lambda_wrangler_function

lookups = {
    "0": {"file_name": "responder_county_lookup",
          "columns_to_keep": ["responder_id", "county"],
          "join_column": "responder_id",
          "required": ["county"]
          },
    "1": {"file_name": "county_marine_lookup",
          "columns_to_keep": ["county_name",
                              "region", "county",
                              "marine"],
          "join_column": "county",
          "required": ["region", "marine"]
          }
}

bricks_blocks_lookups = {
    "0": {"file_name": "region_lookup",
          "columns_to_keep": ["region", "gor_code"],
          "join_column": "gor_code",
          "required": ["region"]
          }
}

method_environment_variables = {
    "bucket_name": "test_bucket"
}

wrangler_environment_variables = {
    "bucket_name": "test_bucket",
    "identifier_column": "responder_id",
    "method_name": "enrichment_method"
}

method_runtime_variables = {
    "RuntimeVariables": {
        "bpm_queue_url": "fake_queue_url",
        "data": None,
        "environment": "sandbox",
        "lookups": lookups,
        "marine_mismatch_check": True,
        "period_column": "period",
        "survey": "BMI_SG",
        "survey_column": "survey",
        "identifier_column": "responder_id",
        "run_id": "bob"
    }
}

wrangler_runtime_variables = {
    "RuntimeVariables":
        {
            "bpm_queue_url": "fake_queue_url",
            "environment": "sandbox",
            "lookups": lookups,
            "survey_column": "survey",
            "run_id": "bob",
            "marine_mismatch_check": True,
            "in_file_name": "test_wrangler_input",
            "out_file_name": "test_wrangler_output.json",
            "sns_topic_arn": "fake_sns_arn",
            "survey": "BMI_SG",
            "period_column": "period",
            "total_steps": 6
        }
}


##########################################################################################
#                                     Generic                                            #
##########################################################################################

@pytest.mark.parametrize(
    "which_lambda,which_runtime_variables,which_environment_variables,"
    "which_data,expected_message,assertion",
    [
        (lambda_method_function, method_runtime_variables,
         method_environment_variables, "tests/fixtures/test_method_input.json",
         "Could not find", test_generic_library.method_assert),
        (lambda_wrangler_function, wrangler_runtime_variables,
         wrangler_environment_variables, None,
         "ClientError", test_generic_library.wrangler_assert)
    ])
@mock.patch('enrichment_wrangler.aws_functions.send_bpm_status')
def test_client_error(mock_send_bpm_status, which_lambda, which_runtime_variables,
                      which_environment_variables, which_data,
                      expected_message, assertion):
    test_generic_library.client_error(which_lambda, which_runtime_variables,
                                      which_environment_variables, which_data,
                                      expected_message, assertion)


@pytest.mark.parametrize(
    "which_lambda,which_runtime_variables,which_environment_variables,mockable_function,"
    "expected_message,assertion",
    [
        (lambda_method_function, method_runtime_variables,
         method_environment_variables, "enrichment_method.EnvironmentSchema",
         "'Exception'", test_generic_library.method_assert),
        (lambda_wrangler_function, wrangler_runtime_variables,
         wrangler_environment_variables, "enrichment_wrangler.EnvironmentSchema",
         "'Exception'", test_generic_library.wrangler_assert)
    ])
def test_general_error(which_lambda, which_runtime_variables,
                       which_environment_variables, mockable_function,
                       expected_message, assertion):
    test_generic_library.general_error(which_lambda, which_runtime_variables,
                                       which_environment_variables, mockable_function,
                                       expected_message, assertion)


@mock_s3
def test_incomplete_read_error():
    file_list = ["test_wrangler_input.json"]

    test_generic_library.incomplete_read_error(lambda_wrangler_function,
                                               wrangler_runtime_variables,
                                               wrangler_environment_variables,
                                               file_list,
                                               "enrichment_wrangler",
                                               "IncompleteReadError")


@pytest.mark.parametrize(
    "which_lambda,which_environment_variables,expected_message,assertion",
    [
        (lambda_method_function, method_environment_variables,
         "KeyError", test_generic_library.method_assert),
        (lambda_wrangler_function, wrangler_environment_variables,
         "KeyError", test_generic_library.wrangler_assert)
    ])
def test_key_error(which_lambda, which_environment_variables,
                   expected_message, assertion):
    test_generic_library.key_error(which_lambda, which_environment_variables,
                                   expected_message, assertion)


@mock_s3
def test_method_error():
    file_list = ["test_wrangler_input.json"]

    test_generic_library.wrangler_method_error(lambda_wrangler_function,
                                               wrangler_runtime_variables,
                                               wrangler_environment_variables,
                                               file_list,
                                               "enrichment_wrangler")


@pytest.mark.parametrize(
    "which_lambda,expected_message,assertion,which_environment_variables",
    [(lambda_method_function,
      "Error validating environment params",
      test_generic_library.method_assert, {}),
     (lambda_method_function,
      "Error validating runtime params",
      test_generic_library.method_assert, method_environment_variables),
     (lambda_wrangler_function,
      "Error validating environment params",
      test_generic_library.wrangler_assert, {}),
     (lambda_wrangler_function,
      "Error validating runtime params",
      test_generic_library.wrangler_assert, wrangler_environment_variables)])
def test_value_error(which_lambda, expected_message, assertion,
                     which_environment_variables):
    test_generic_library.value_error(
        which_lambda, expected_message, assertion,
        environment_variables=which_environment_variables)

##########################################################################################
#                                     Specific                                           #
##########################################################################################


@pytest.mark.parametrize(
    "lookup_data,column_names,file_list,marine_check",
    [
        (lookups, ["county", "county_name"],
         ["responder_county_lookup.json", "county_marine_lookup.json"], True),
        (bricks_blocks_lookups, ["region"],
         ["region_lookup.json"], False)
    ])
@mock_s3
def test_data_enrichment(lookup_data, column_names, file_list, marine_check):
    """
    Runs data_enrichment function.
    NOTE: This function calls do_merge and marine_mismatch_detector and doesn't
    mock them. If this test fails check the tests for these funtions have passed
    as they may be at fault.

    :param lookup_data: Name of bucket to create - Type: dict
    :param column_names: Name of bucket to create - Type: list
    :param file_list: Name of bucket to create - Type: list
    :param marine_check: Name of bucket to create - Type: boolean
    :return Test Pass/Fail
    """
    with mock.patch.dict(lambda_method_function.os.environ,
                         method_environment_variables):
        with open("tests/fixtures/test_method_input.json", "r") as file:
            test_data = file.read()
        test_data = pd.DataFrame(json.loads(test_data))

        bucket_name = method_environment_variables["bucket_name"]
        client = test_generic_library.create_bucket(bucket_name)

        test_generic_library.upload_files(client, bucket_name, file_list)

        output, test_anomalies = lambda_method_function.data_enrichment(
            test_data, marine_check, "survey", "period",
            "test_bucket", lookup_data, "responder_id"
        )
    for column_name in column_names:
        assert column_name in output.columns.values


@pytest.mark.parametrize(
    "file_name,column_names,join_column",
    [
        ("responder_county_lookup", ["responder_id", "county"], "responder_id"),
        ("region_lookup", ["region", "gor_code"], "gor_code")
    ])
@mock_s3
def test_do_merge(file_name, column_names, join_column):
    """
    Runs do_merge function.
    :param file_name: Name of bucket to create - Type: String
    :param column_names: Name of bucket to create - Type: list
    :param join_column: Name of bucket to create - Type: String
    :return Test Pass/Fail
    """
    with open("tests/fixtures/test_method_input.json", "r") as file:
        test_data = file.read()
    test_data = pd.DataFrame(json.loads(test_data))
    bucket_name = "test_bucket"
    client = test_generic_library.create_bucket(bucket_name)

    test_generic_library.upload_files(client, bucket_name, [(file_name + ".json")])

    output = lambda_method_function.do_merge(
        test_data, file_name, column_names, join_column, bucket_name
    )
    for column_name in column_names:
        assert column_name in output.columns.values


def test_marine_mismatch_detector():
    """
    Runs marine_mismatch_detector function.
    :param None
    :return Test Pass/Fail
    """
    with open("tests/fixtures/test_marine_mismatch_detector_input.json", "r") as file:
        test_data = file.read()
    test_data = pd.DataFrame(json.loads(test_data))

    output = lambda_method_function.marine_mismatch_detector(
        test_data, "survey", "marine", "period", "responder_id"
    )

    assert output['issue'][0].__contains__("""should not produce""")


@mock_s3
def test_method_success():
    """
    Runs the method function.
    :param None
    :return Test Pass/Fail
    """
    with mock.patch.dict(lambda_method_function.os.environ,
                         method_environment_variables):
        with open("tests/fixtures/test_method_prepared_output.json", "r") as file_1:
            file_data = file_1.read()
        prepared_data_main = pd.DataFrame(json.loads(file_data))

        with open("tests/fixtures/test_method_anomalies_prepared_output.json", "r")\
                as file_2:
            file_data = file_2.read()
        prepared_data_anomalies = pd.DataFrame(json.loads(file_data))

        with open("tests/fixtures/test_method_input.json", "r") as file_3:
            test_data = file_3.read()
        method_runtime_variables["RuntimeVariables"]["data"] = test_data

        bucket_name = method_environment_variables["bucket_name"]
        client = test_generic_library.create_bucket(bucket_name)

        file_list = ["responder_county_lookup.json",
                     "county_marine_lookup.json"]

        test_generic_library.upload_files(client, bucket_name, file_list)

        output = lambda_method_function.lambda_handler(
            method_runtime_variables, test_generic_library.context_object)

        produced_data_main = pd.DataFrame(json.loads(output["data"])).sort_index(axis=1)
        produced_data_anomalies = pd.DataFrame(
            json.loads(output["anomalies"]))

    assert output["success"]
    assert_frame_equal(produced_data_main, prepared_data_main)
    assert_frame_equal(produced_data_anomalies, prepared_data_anomalies)


def test_missing_column_detector():
    """
    Runs missing_column_detector function.
    :param None
    :return Test Pass/Fail
    """
    data = pd.DataFrame({"county": [1, None, 2], "responder_id": [666, 123, 8008]})

    output = lambda_method_function.missing_column_detector(
        data, ["county"], "responder_id")

    assert output['issue'][1].__contains__("""missing in lookup.""")


@mock_s3
def test_wrangler_success_passed():
    """
    Runs the wrangler function up to the method invoke.
    :param None
    :return Test Pass/Fail
    """
    bucket_name = wrangler_environment_variables["bucket_name"]
    client = test_generic_library.create_bucket(bucket_name)

    file_list = ["test_wrangler_input.json"]

    test_generic_library.upload_files(client, bucket_name, file_list)

    with mock.patch.dict(lambda_wrangler_function.os.environ,
                         wrangler_environment_variables):
        with mock.patch("enrichment_wrangler.boto3.client") as mock_client:
            mock_client_object = mock.Mock()
            mock_client.return_value = mock_client_object

            # Rather than mock the get/decode we tell the code that when the invoke is
            # called pass the variables to this replacement function instead.
            mock_client_object.invoke.side_effect =\
                test_generic_library.replacement_invoke

            # This stops the Error caused by the replacement function from stopping
            # the test.
            with pytest.raises(exception_classes.LambdaFailure):
                lambda_wrangler_function.lambda_handler(
                    wrangler_runtime_variables, test_generic_library.context_object
                )

    with open("tests/fixtures/test_method_input.json", "r") as file_2:
        test_data_prepared = file_2.read()
    prepared_data = pd.DataFrame(json.loads(test_data_prepared))

    with open("tests/fixtures/test_wrangler_to_method_input.json", "r") as file_3:
        test_data_produced = file_3.read()
    produced_data = pd.DataFrame(json.loads(test_data_produced))

    # Compares the data.
    assert_frame_equal(produced_data, prepared_data)

    with open("tests/fixtures/test_wrangler_to_method_runtime.json", "r") as file_4:
        test_dict_prepared = file_4.read()
    produced_dict = json.loads(test_dict_prepared)

    # Ensures data is not in the RuntimeVariables and then compares.
    method_runtime_variables["RuntimeVariables"]["data"] = None
    assert produced_dict == method_runtime_variables["RuntimeVariables"]


@mock_s3
@mock.patch('enrichment_wrangler.aws_functions.save_to_s3',
            side_effect=test_generic_library.replacement_save_to_s3)
def test_wrangler_success_returned(mock_s3_put):
    """
    Runs the wrangler function after the method invoke.
    :param None
    :return Test Pass/Fail
    """
    bucket_name = wrangler_environment_variables["bucket_name"]
    client = test_generic_library.create_bucket(bucket_name)

    file_list = ["test_wrangler_input.json"]

    test_generic_library.upload_files(client, bucket_name, file_list)

    with open("tests/fixtures/test_method_output.json", "r") as file_2:
        test_data_out = file_2.read()

    with mock.patch.dict(lambda_wrangler_function.os.environ,
                         wrangler_environment_variables):
        with mock.patch("enrichment_wrangler.boto3.client") as mock_client:
            mock_client_object = mock.Mock()
            mock_client.return_value = mock_client_object

            mock_client_object.invoke.return_value.get.return_value.read \
                .return_value.decode.return_value = json.dumps({
                 "data": test_data_out,
                 "success": True,
                 "anomalies": "[]"
                })

            output = lambda_wrangler_function.lambda_handler(
                wrangler_runtime_variables, test_generic_library.context_object
            )

    with open("tests/fixtures/test_wrangler_prepared_output.json", "r") as file_3:
        test_data_prepared = file_3.read()
    prepared_data = pd.DataFrame(json.loads(test_data_prepared))

    with open("tests/fixtures/" +
              wrangler_runtime_variables["RuntimeVariables"]["out_file_name"],
              "r") as file_4:
        test_data_produced = file_4.read()
    produced_data = pd.DataFrame(json.loads(test_data_produced))

    assert output
    assert_frame_equal(produced_data, prepared_data)
