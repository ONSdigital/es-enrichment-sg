import json
from unittest import mock

import boto3
from botocore.response import StreamingBody
from es_aws_functions import aws_functions


class MockContext:
    aws_request_id = 999


bad_runtime_variables = {
    "RuntimeVariables": {}
}


bad_environment_variables = {}


context_object = MockContext()


def create_client(client_type, region="eu-west-2"):
    """
    Create a boto3 client for use with aws tests.
    :param client_type: Type of client(eg. s3, lambda, sqs etc) - Type: String
    :param region: Region to create client in(default is eu-west-2) - Type: String
    :return client: Requested boto3 client
    """
    client = boto3.client(
      client_type,
      region_name=region,
      aws_access_key_id="fake_access_key",
      aws_secret_access_key="fake_secret_key",
      )
    return client

def create_bucket(bucket_name):
    """
    Create an s3 bucket.
    :param bucket_name: Name of bucket to create - Type: String
    :return client: S3 client that created bucket - Type: Boto3 Client
    """
    client = create_client("s3")
    client.create_bucket(Bucket=bucket_name)
    return client


def upload_files(client, bucket_name, file_list):
    """
    Upload a list of files to a given s3 bucket from the test/fixtures folder.
    Key of the uploaded file(s) is their filename.
    :param client: S3 Client - Type: Boto3 Client
    :param bucket_name: Name of bucket to place files in - Type: String
    :param file_list: List of files to upload
    :return client: S3 client that uploaded files
    """
    for file in file_list:
        client.upload_file(
            Filename="tests/fixtures/" + file,
            Bucket=bucket_name,
            Key=file,
        )
    return client


def client_error(lambda_function, runtime_variables, environment_variables, file_name):
    """
    Function to trigger a client error in a lambda.
    By not mocking any of the boto3 functions, once any are used in code they will
    trigger client error due to lack of credentials.

    If used on a method, data is part of the runtime_variables, so the file_name is loaded in
    and the file added to the runtime_variables dictionary.
    :param lambda_function: Lambda function to test - Type: Function
    :param runtime_variables: Runtime variables to send to function - Type: Dict
    :param environment_variables: Environment Vars to send to function - Type: Dict
    :param file_name: Name of file to retrieve data from - Type: String
    :return:
    """
    with mock.patch.dict(lambda_function.os.environ, environment_variables):
        if "data" in runtime_variables["RuntimeVariables"].keys():
            with open(file_name, "r") as file:
                test_data = file.read()
            runtime_variables["RuntimeVariables"]["data"] = test_data

        output = lambda_function.lambda_handler(runtime_variables, context_object)

    assert 'error' in output.keys()
    assert output["error"].__contains__("""AWS Error""")


def general_error(lambda_function, runtime_variables,
                  environment_variables, mockable_function):
    """
    Function to trigger a general error in a given lambda.

    mockable_function defines the function in the lambda that will be mocked out.
    This should be something fairly early in the code(but still within try/except.
    E.G: "enrichment_wrangler.EnvironSchema"
    :param lambda_function: Lambda function to test - Type: Function
    :param runtime_variables: Runtime variables to send to function - Type: Dict
    :param environment_variables: Environment Vars to send to function - Type: Dict
    :param mockable_function: The function in the code to mock out
            (and attach side effect to) - Type: String
    :return:
    """
    with mock.patch(mockable_function) as mock_schema:
        mock_schema.side_effect = Exception("Failed To Log")

        with mock.patch.dict(lambda_function.os.environ, environment_variables):
            output = lambda_function.lambda_handler(runtime_variables, context_object)

    assert 'error' in output.keys()
    assert output["error"].__contains__("""General Error""")


def incomplete_read_error(lambda_function, runtime_variables,
                          environment_variables, file_list, wrangler_name):
    """
    Function to trigger an incomplete read error in a given wrangler.

    Takes in a valid file(s) so that the function performs until after the lambda invoke.

    The file that triggers the incomplete_read is generic, so hardcoded.

    :param lambda_function: Lambda function to test - Type: Function
    :param runtime_variables: Runtime variables to send to function - Type: Dict
    :param environment_variables: Environment Vars to send to function - Type: Dict
    :param file_list: List of input files for the function - Type: List
    :param wrangler_name: Wrangler that is being tested,
            used in mocking boto3. - Type: String
    :return:
    """

    bucket_name = environment_variables["bucket_name"]
    client = create_bucket(bucket_name)
    upload_files(client, bucket_name, file_list)

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


def key_error(lambda_function, environment_variables, runtime_variables=bad_runtime_variables):
    """
    Function to trigger a key error in a given lambda.

    Makes use of an empty dict of runtime variables,
    which triggers a key error once access is attempted.


    :param lambda_function: Lambda function to test - Type: Function
    :param environment_variables: Environment Vars to send to function - Type: Dict
    :param runtime_variables: Runtime variables to send to function
                            (default is empty dict) - Type: Dict
    :return:
    """
    with mock.patch.dict(lambda_function.os.environ, environment_variables):
        output = lambda_function.lambda_handler(runtime_variables, context_object)

    assert 'error' in output.keys()
    assert output["error"].__contains__("""Key Error""")


def method_error(lambda_function, runtime_variables,
                 environment_variables, file_list, wrangler_name):
    """
    Function to trigger a method error in a given function.

    Takes in a valid file(s) so that the function performs until after the lambda invoke.

    :param lambda_function: Lambda function to test - Type: Function
    :param runtime_variables: Runtime variables to send to function - Type: Dict
    :param environment_variables: Environment Vars to send to function - Type: Dict
    :param file_list: List of input files for the function - Type: List
    :param wrangler_name: Wrangler that is being tested,
            used in mocking boto3. - Type: String
    :return:
    """

    bucket_name = environment_variables["bucket_name"]
    client = create_bucket(bucket_name)
    upload_files(client, bucket_name, file_list)

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


def value_error(lambda_function, runtime_variables, environment_variables=bad_environment_variables):
    """
    Function to trigger a value error in a given function.

    Does so by passing an empty list of environment variables
    to trigger an error with marshmallow.

    :param lambda_function: Lambda function to test - Type: Function
    :param runtime_variables: Runtime variables to send to function - Type: Dict
    :param environment_variables: Environment Vars to send to function - Type: Dict
    :return:
    """
    with mock.patch.dict(lambda_function.os.environ, environment_variables):
        output = lambda_function.lambda_handler(runtime_variables, context_object)

    assert 'error' in output.keys()
    assert output['error'].__contains__("""Parameter Validation Error""")


def replacement_save_data(bucket_name, file_name, data,
                          sqs_queue_url, sqs_message_id):
    """
    Function to replace the aws-functions.save_data when performing tests.

    Saves a copy of the file locally.

    Takes the same parameters as save_data, but only uses file_name and data.
    :param bucket_name: Name of bucket. Unused.
    :param file_name: Name of file to save in tests/fixtures - Type: String
    :param data: Data to save - Type: Json String
    :param sqs_queue_url: Name of sqs queue. Unused
    :param sqs_message_id: Name of message group. Unused
    :return:
    """
    with open("tests/fixtures/" + file_name, 'w', encoding='utf-8') as f:
        f.write(data)
        f.close()


def replacement_get_dataframe(sqs_queue_url, bucket_name,
                              in_file_name, incoming_message_group):
    """
    Function to replace the aws-functions.get_dataframe when performing tests.

    Instead of getting an sqs message and then using it to define the file to get,
    Goes straight to s3 for the data.

    Takes the same parameters as get_dataframe, but only uses file_name and data.
    :param sqs_queue_url: Name of sqs queue. Unused
    :param bucket_name: Name of bucket to retrieve data from - Type: String
    :param in_file_name: Name of file to read in - Type: String
    :param incoming_message_group: Name of message group. Unused
    :return: data: Data from file - Type: Dataframe
    :return: receipt: Int to simulate message receipt - Type: Int
    """
    data = aws_functions.read_dataframe_from_s3(bucket_name, in_file_name)

    return data, 999
