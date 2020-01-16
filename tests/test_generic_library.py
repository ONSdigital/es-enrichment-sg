import json
from unittest import mock

import boto3
import pandas as pd
from botocore.response import StreamingBody


class MockContext:
    aws_request_id = 999


context_object = MockContext()


def create_bucket(bucket_name):
    client = boto3.client(
        "s3",
        region_name="eu-west-2",
        aws_access_key_id="fake_access_key",
        aws_secret_access_key="fake_secret_key",
    )

    client.create_bucket(Bucket=bucket_name)
    return client


def upload_file(client, bucket_name, file_list):
    for x in file_list:
        client.upload_file(
            Filename="tests/fixtures/" + x,
            Bucket=bucket_name,
            Key=x,
        )
    return client


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


def incomplete_read_error(lambda_function, runtime_variables,
                          environment_variables, file_name, wrangler_name):
    with open(file_name, "r") as file:
        test_data_in = file.read()
    test_data_in = pd.DataFrame(json.loads(test_data_in))

    with mock.patch.dict(lambda_function.os.environ, environment_variables):

        with mock.patch(wrangler_name + ".aws_functions.get_dataframe") as mock_s3_get:
            mock_s3_get.return_value = test_data_in, 999

            with mock.patch(wrangler_name + ".boto3.client") as mock_client:
                mock_client_object = mock.Mock()
                mock_client.return_value = mock_client_object

                with open("tests/fixtures/test_incomplete_read_error_input.json", "rb")\
                        as test_data_bad:
                    mock_client_object.invoke.return_value = {
                        "Payload": StreamingBody(test_data_bad, 1)}

                    output = lambda_function.lambda_handler(runtime_variables,
                                                            context_object)

    assert 'error' in output.keys()
    assert output["error"].__contains__("""Incomplete Lambda response""")


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


def replacement_save_data(bucket_name, file_name, data,
                          sqs_queue_url, sqs_message_id):
    with open("tests/fixtures/" + file_name, 'w', encoding='utf-8') as f:
        f.write(data)
        f.close()
