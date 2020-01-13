import copy
import json
import unittest

from parameterized import parameterized

import enrichment_method as lambda_method_function
import enrichment_wrangler as lambda_wrangler_function
from tests import test_librarymarsh

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
    "sqs_queue_url": "not_real_sqs_url",
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
    "data": "to_be_filled_by_test",
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
         method_environment_variables),
        (lambda_wrangler_function, wrangler_runtime_variables,
         wrangler_environment_variables)
    ])
    def test_client_error(self, which_lambda, which_runtime_variables,
                          which_environment_variables, which_data):
        with open(test_data, "r") as file:
            test_data = file.read()

        test_librarymarsh.client_error()

    @parameterized.expand([(lambda_method_function, ""), (lambda_wrangler_function, "")])
    def test_value_error(self, which_lambda):
        test_librarymarsh.value_error(
            which_lambda, bad_runtime_variables, bad_environment_variables
        )
