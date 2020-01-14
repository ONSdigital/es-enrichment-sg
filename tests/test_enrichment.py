import json
import unittest

from parameterized import parameterized

import enrichment_method as lambda_method_function
import enrichment_wrangler as lambda_wrangler_function
from tests import test_generic_library

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
        "lookups": json.dumps({
            "0": {
                "required": "yes",
                "file_name": "test_lookup",
                "columns_to_keep": ["county", "region"],
                "join_column": "county"
            }
        }),
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


class TestEnrichment(unittest.TestCase):

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
