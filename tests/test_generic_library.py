import json
from unittest import mock

import boto3
from botocore.response import StreamingBody
from es_aws_functions import aws_functions


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
    for file in file_list:
        client.upload_file(
            Filename="tests/fixtures/" + file,
            Bucket=bucket_name,
            Key=file,
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
                          environment_variables, file_list, wrangler_name):

    bucket_name = environment_variables["bucket_name"]
    client = create_bucket(bucket_name)
    upload_file(client, bucket_name, file_list)

    with mock.patch.dict(lambda_function.os.environ, environment_variables):

        with mock.patch(wrangler_name + ".boto3.client") as mock_client:
            mock_client_object = mock.Mock()
            mock_client.return_value = mock_client_object

            with open("tests/fixtures/test_incomplete_read_error_input.json", "rb")\
                    as test_data_bad:
                mock_client_object.invoke.return_value = {
                    "Payload": StreamingBody(test_data_bad, 1)}

                output = lambda_function.lambda_handler(runtime_variables, context_object)

    assert 'error' in output.keys()
    assert output["error"].__contains__("""Incomplete Lambda response""")


def key_error(lambda_function, runtime_variables,
              environment_variables):
    with mock.patch.dict(lambda_function.os.environ, environment_variables):
        output = lambda_function.lambda_handler(runtime_variables, context_object)

    assert 'error' in output.keys()
    assert output["error"].__contains__("""Key Error""")


def method_error(lambda_function, runtime_variables,
                 environment_variables, file_list, wrangler_name):

    bucket_name = environment_variables["bucket_name"]
    client = create_bucket(bucket_name)
    upload_file(client, bucket_name, file_list)

    with mock.patch.dict(lambda_function.os.environ, environment_variables):

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


def replacement_get_dataframe(sqs_queue_url, bucket_name,
                              in_file_name, incoming_message_group):
    data = aws_functions.read_dataframe_from_s3(bucket_name, in_file_name)

    return data, 999
