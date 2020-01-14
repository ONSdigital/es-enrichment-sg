import json
import unittest
from unittest import mock

import pandas as pd
from moto import mock_s3
from parameterized import parameterized

import enrichment_method as lambda_method_function
import enrichment_wrangler as lambda_wrangler_function
from tests import test_generic_library

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
    "checkpoint": "99",
    "identifier_column": "responder_id",
    "in_file_name": "fake_in.json",
    "out_file_name": "fake_out.json",
    "method_name": "enrichment_method",
    "sqs_queue_url": "test_queue",
    "sqs_message_group_id": "test_id",
    "incoming_message_group": "test_group",
    "period_column": "period",
    "lookup_info": "insert_fake_here",
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
            "checkpoint": "test",
            "survey_column": "survey"
        }
}


class GenericErrorsEnrichment(unittest.TestCase):

    @parameterized.expand([
        (lambda_method_function, method_runtime_variables,
         method_environment_variables, "tests/fixtures/test_data.json"),
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

    def test_incomplete_read_error(self):
        test_generic_library.method_error(lambda_wrangler_function,
                                          wrangler_runtime_variables,
                                          wrangler_environment_variables,
                                          "tests/fixtures/test_data.json",
                                          "enrichment_wrangler")

    @parameterized.expand([
        (lambda_method_function, method_environment_variables),
        (lambda_wrangler_function, wrangler_environment_variables)
    ])
    def test_key_error(self, which_lambda, which_environment_variables):
        test_generic_library.key_error(which_lambda, bad_runtime_variables,
                                       which_environment_variables)

    def test_method_error(self):
        test_generic_library.method_error(lambda_wrangler_function,
                                          wrangler_runtime_variables,
                                          wrangler_environment_variables,
                                          "tests/fixtures/test_data.json",
                                          "enrichment_wrangler")

    @parameterized.expand([(lambda_method_function, ), (lambda_wrangler_function, )])
    def test_value_error(self, which_lambda):
        test_generic_library.value_error(
            which_lambda, bad_runtime_variables, bad_environment_variables
        )


class SpecificFunctionsEnrichment(unittest.TestCase):
    @mock_s3
    def test_data_enrichement(self):
        with mock.patch.dict(lambda_method_function.os.environ,
                             method_environment_variables):
            with open("tests/fixtures/test_data.json", "r") as file:
                test_data = file.read()
            test_data = pd.DataFrame(json.loads(test_data))

            client = test_generic_library.create_bucket()

            file_list = ["responder_county_lookup.json",
                         "county_marine_lookup.json"]

            test_generic_library.upload_file(client, file_list)

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
            test_data,
            "survey",
            "marine",
            "period",
            "responder_id"
        )

        assert output['issue'][0].__contains__("""should not produce""")

    def test_missing_column_detector(self):
        data = pd.DataFrame({"county": [1, None, 2], "responder_id": [666, 123, 8008]})

        output = lambda_method_function.missing_column_detector(
            data, ["county"], "responder_id")

        assert output['issue'][1].__contains__("""missing in lookup.""")
