from unittest import mock

import boto3
from moto import mock_sqs


class MockContext:
    aws_request_id = 666


context_object = MockContext()


@mock_sqs
def mocked_sqs():
    sqs = boto3.resource("sqs", region_name="eu-west-2")
    sqs.create_queue(QueueName="test_queue")
    sqs_queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
    return sqs_queue_url


mock_sqs_queue_url = mocked_sqs()


def client_error(lambda_function, runtime_variables, environment_variables):
    with mock.patch.dict(lambda_function.os.environ, environment_variables):
        output = lambda_function.lambda_handler(runtime_variables, context_object)

        assert 'error' in output.keys()
        assert output["error"].__contains__("""AWS Error""")


def value_error(lambda_function, runtime_variables, environment_variables):
    environment_variables["sqs_queue_url"] = mock_sqs_queue_url

    with mock.patch.dict(lambda_function.os.environ, environment_variables):
        output = lambda_function.lambda_handler(runtime_variables, context_object)

    assert 'error' in output.keys()
    assert (output['error'].__contains__("""Parameter validation error"""))
