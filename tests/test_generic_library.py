import json
from unittest import mock

import boto3
import pandas as pd
from moto import mock_sqs, mock_s3, mock_lambda


class MockContext:
    aws_request_id = 999


context_object = MockContext()


@mock_sqs
def mocked_sqs():
    sqs = boto3.resource("sqs", region_name="eu-west-2")
    sqs.create_queue(QueueName="test_queue")
    sqs_queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
    return sqs_queue_url


mock_sqs_queue_url = mocked_sqs()


def client_error(lambda_function, runtime_variables, environment_variables, file_name):
    with mock.patch.dict(lambda_function.os.environ, environment_variables):
        if "data" in runtime_variables["RuntimeVariables"].keys():
            with open(file_name, "r") as file:
                test_data = file.read()
            runtime_variables["RuntimeVariables"]["data"] = test_data

        output = lambda_function.lambda_handler(runtime_variables, context_object)

    assert 'error' in output.keys()
    assert output["error"].__contains__("""AWS Error""")


def general_error(lambda_function, runtime_variables,
                  environment_variables, chosen_exception):
    with mock.patch(chosen_exception) as mock_schema:
        mock_schema.side_effect = Exception("Failed To Log")

        with mock.patch.dict(lambda_function.os.environ, environment_variables):
            output = lambda_function.lambda_handler(runtime_variables, context_object)

    assert 'error' in output.keys()
    assert output["error"].__contains__("""General Error""")


def key_error(lambda_function, runtime_variables,
              environment_variables):
    with mock.patch.dict(lambda_function.os.environ, environment_variables):
        output = lambda_function.lambda_handler(runtime_variables, context_object)

    assert 'error' in output.keys()
    assert output["error"].__contains__("""Key Error""")


def method_error(lambda_function, runtime_variables,
                 environment_variables, file_name, wrangler_name):
    with open(file_name, "r") as file:
        test_data = file.read()
    test_data = pd.DataFrame(json.loads(test_data))

    with mock.patch.dict(lambda_function.os.environ, environment_variables):

        with mock.patch(wrangler_name + ".aws_functions.get_dataframe") as mock_s3_get:
            mock_s3_get.return_value = test_data, 999

            with mock.patch(wrangler_name + ".boto3.client") as mock_client:
                mock_client_object = mock.Mock()
                mock_client.return_value = mock_client_object

                mock_client_object.invoke.return_value.get.return_value \
                    .read.return_value.decode.return_value = \
                    json.dumps({"error": "Test Message",
                                "success": False})

                output = lambda_function.lambda_handler(runtime_variables, context_object)

    assert 'error' in output.keys()
    assert output["error"].__contains__("""Test Message""")


def value_error(lambda_function, runtime_variables, environment_variables):
    with mock.patch.dict(lambda_function.os.environ, environment_variables):
        output = lambda_function.lambda_handler(runtime_variables, context_object)

    assert 'error' in output.keys()
    assert output['error'].__contains__("""Parameter Validation Error""")
