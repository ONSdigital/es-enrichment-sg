import json
import unittest
from unittest import mock

import pandas as pd
from moto import mock_s3
from pandas.util.testing import assert_frame_equal
from parameterized import parameterized

import enrichment_method as lambda_method_function
import enrichment_wrangler as lambda_wrangler_function
from es_aws_functions import test_generic_library

lookups = {
    "0": {"file_name": "responder_county_lookup.json",
          "columns_to_keep": ["responder_id", "county"],
          "join_column": "responder_id",
          "required": ["county"]
          },
    "1": {"file_name": "county_marine_lookup.json",
          "columns_to_keep": ["county_name",
                              "region", "county",
                              "marine"],
          "join_column": "county",
          "required": ["region", "marine"]
          }
}

bad_environment_variables = {"checkpoint": "test"}

method_environment_variables = {
    "bucket_name": "test_bucket"
}

wrangler_environment_variables = {
    "sns_topic_arn": "fake_sns_arn",
    "bucket_name": "test_bucket",
    "checkpoint": "999",
    "identifier_column": "responder_id",
    "in_file_name": "test_wrangler_input.json",
    "out_file_name": "test_wrangler_output.json",
    "method_name": "enrichment_method",
    "sqs_queue_url": "test_queue",
    "sqs_message_group_id": "test_id",
    "incoming_message_group": "test_group",
    "period_column": "period",
    "lookups": "insert_fake_here",
    "marine_mismatch_check": "true"
}

bad_runtime_variables = {
    "RuntimeVariables": {}
}

method_runtime_variables = {
    "RuntimeVariables": {
        "data": None,
        "lookups": lookups,
        "marine_mismatch_check": "true",
        "survey_column": "survey",
        "period_column": "period",
        "identifier_column": "responder_id"
    }
}

wrangler_runtime_variables = {
    "RuntimeVariables":
        {
            "checkpoint": "999",
            "survey_column": "survey"
        }
}


class GenericErrors(unittest.TestCase):

    @parameterized.expand([
        (lambda_method_function, method_runtime_variables,
         method_environment_variables, "tests/fixtures/test_method_input.json"),
        (lambda_wrangler_function, wrangler_runtime_variables,
         wrangler_environment_variables, None)
    ])
    def test_client_error(self, which_lambda, which_runtime_variables,
                          which_environment_variables, which_data):
        test_generic_library.client_error(which_lambda, which_runtime_variables,
                                          which_environment_variables, which_data)

    @parameterized.expand([
        (lambda_method_function, method_runtime_variables,
         method_environment_variables, "enrichment_method.EnvironSchema"),
        (lambda_wrangler_function, wrangler_runtime_variables,
         wrangler_environment_variables, "enrichment_wrangler.EnvironSchema")
    ])
    def test_general_error(self, which_lambda, which_runtime_variables,
                           which_environment_variables, chosen_exception):
        test_generic_library.general_error(which_lambda, which_runtime_variables,
                                           which_environment_variables, chosen_exception)

    @mock_s3
    @mock.patch('enrichment_wrangler.aws_functions.get_dataframe',
                side_effect=test_generic_library.replacement_get_dataframe)
    def test_incomplete_read_error(self, mock_s3_get):

        file_list = ["test_wrangler_input.json"]

        test_generic_library.incomplete_read_error(lambda_wrangler_function,
                                                   wrangler_runtime_variables,
                                                   wrangler_environment_variables,
                                                   file_list,
                                                   "enrichment_wrangler")

    @parameterized.expand([
        (lambda_method_function, method_environment_variables),
        (lambda_wrangler_function, wrangler_environment_variables)
    ])
    def test_key_error(self, which_lambda, which_environment_variables):
        test_generic_library.key_error(which_lambda, which_environment_variables, bad_runtime_variables)

    @mock_s3
    @mock.patch('enrichment_wrangler.aws_functions.get_dataframe',
                side_effect=test_generic_library.replacement_get_dataframe)
    def test_method_error(self, mock_s3_get):

        file_list = ["test_wrangler_input.json"]

        test_generic_library.method_error(lambda_wrangler_function,
                                          wrangler_runtime_variables,
                                          wrangler_environment_variables,
                                          file_list,
                                          "enrichment_wrangler")

    @parameterized.expand([(lambda_method_function, ), (lambda_wrangler_function, )])
    def test_value_error(self, which_lambda):
        test_generic_library.value_error(
            which_lambda, bad_runtime_variables, bad_environment_variables
        )


class SpecificFunctions(unittest.TestCase):
    @mock_s3
    def test_data_enrichement(self):
        with mock.patch.dict(lambda_method_function.os.environ,
                             method_environment_variables):
            with open("tests/fixtures/test_method_input.json", "r") as file:
                test_data = file.read()
            test_data = pd.DataFrame(json.loads(test_data))

            bucket_name = method_environment_variables["bucket_name"]
            client = test_generic_library.create_bucket(bucket_name)

            file_list = ["responder_county_lookup.json",
                         "county_marine_lookup.json"]

            test_generic_library.upload_files(client, bucket_name, file_list)

            output, test_anomalies = lambda_method_function.data_enrichment(
                test_data, "true", "survey", "period",
                "test_bucket", lookups, "responder_id"
            )

        assert "county" in output.columns.values
        assert "county_name" in output.columns.values

    def test_marine_mismatch_detector(self):
        with open("tests/fixtures/test_marine_mismatch_detector_input.json", "r") as file:
            test_data = file.read()
        test_data = pd.DataFrame(json.loads(test_data))

        output = lambda_method_function.marine_mismatch_detector(
            test_data, "survey", "marine", "period", "responder_id"
        )

        assert output['issue'][0].__contains__("""should not produce""")

    @mock_s3
    def test_method_success(self):
        with mock.patch.dict(lambda_method_function.os.environ,
                             method_environment_variables):
            with open("tests/fixtures/test_method_prepared_output.json", "r") as file_1:
                file_data = file_1.read()
            prepared_data = pd.DataFrame(json.loads(file_data))

            with open("tests/fixtures/test_method_input.json", "r") as file_2:
                test_data = file_2.read()
            method_runtime_variables["RuntimeVariables"]["data"] = test_data

            bucket_name = method_environment_variables["bucket_name"]
            client = test_generic_library.create_bucket(bucket_name)

            file_list = ["responder_county_lookup.json",
                         "county_marine_lookup.json"]

            test_generic_library.upload_files(client, bucket_name, file_list)

            output = lambda_method_function.lambda_handler(
                method_runtime_variables, test_generic_library.context_object)

            produced_data = pd.DataFrame(json.loads(output["data"]))

        assert output["success"]
        assert_frame_equal(produced_data, prepared_data)

    def test_missing_column_detector(self):
        data = pd.DataFrame({"county": [1, None, 2], "responder_id": [666, 123, 8008]})

        output = lambda_method_function.missing_column_detector(
            data, ["county"], "responder_id")

        assert output['issue'][1].__contains__("""missing in lookup.""")

    @mock_s3
    @mock.patch('enrichment_wrangler.aws_functions.get_dataframe',
                side_effect=test_generic_library.replacement_get_dataframe)
    @mock.patch('enrichment_wrangler.aws_functions.save_data',
                side_effect=test_generic_library.replacement_save_data)
    def test_wrangler_success(self, mock_s3_get, mock_s3_put):

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
                        "anomalies": []
                    })

                output = lambda_wrangler_function.lambda_handler(
                    wrangler_runtime_variables, test_generic_library.context_object
                )

        with open("tests/fixtures/test_wrangler_prepared_output.json", "r") as file_3:
            test_data_prepared = file_3.read()
        prepared_data = pd.DataFrame(json.loads(test_data_prepared))

        with open("tests/fixtures/" + wrangler_environment_variables["out_file_name"],
                  "r") as file_4:
            test_data_produced = file_4.read()
        produced_data = pd.DataFrame(json.loads(test_data_produced))

        assert output
        assert_frame_equal(produced_data, prepared_data)
