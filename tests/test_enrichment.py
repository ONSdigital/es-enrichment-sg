import json
import unittest
import unittest.mock as mock

import boto3
import pandas as pd
from moto import mock_lambda, mock_s3, mock_sqs

import enrichment_method as lambda_method_function
import enrichment_wrangler as lambda_wrangler_function


class MockContext():
    aws_request_id = 666


context_object = MockContext()


class TestEnrichment(unittest.TestCase):
    @mock_sqs
    @mock_lambda
    def test_catch_wrangler_exception(self):
        # Method
        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        sqs_queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
        with mock.patch.dict(
            lambda_wrangler_function.os.environ,
            {
                "sns_topic_arn": "mike",
                "bucket_name": "mike",
                "checkpoint": "3",
                "identifier_column": "responder_id",
                "in_file_name": "test_data.json",
                "out_file_name": "PhilipeDePhile",
                "method_name": "enrichment_method",
                "sqs_queue_url": sqs_queue_url,
                "sqs_message_group_id": "testytest Mctestytestytesttest",
                "incoming_message_group": "test",
                "period_column": "period",
                "lookup_info": "Look up!!",
                "marine_mismatch_check": "true"
            },
        ):
            # using get_from_s3 to force exception early on.
            with mock.patch("enrichment_wrangler.aws_functions.get_dataframe") as mocked:
                mocked.side_effect = Exception("SQS Failure")
                response = lambda_wrangler_function.lambda_handler(
                    {
                        "RuntimeVariables":
                        {
                            "checkpoint": 666,
                            "survey_column": "survey"
                        }
                    }, context_object
                )
                assert "success" in response
                assert response["success"] is False

    @mock_sqs
    @mock_lambda
    def test_catch_wrangler_keyerror_exception(self):
        # Method
        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        sqs_queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
        with mock.patch.dict(
            lambda_wrangler_function.os.environ,
            {
                "sns_topic_arn": "mike",
                "bucket_name": "mike",
                "checkpoint": "3",
                "identifier_column": "responder_id",
                "in_file_name": "test_data.json",
                "out_file_name": "PhilipeDePhile",
                "method_name": "enrichment_method",
                "sqs_queue_url": sqs_queue_url,
                "sqs_message_group_id": "testytest Mctestytestytesttest",
                "incoming_message_group": "test",
                "period_column": "period",
                "lookup_info": "Look up!!",
                "marine_mismatch_check": "true"
            },
        ):
            # using get_from_s3 to force exception early on.
            with mock.patch("enrichment_wrangler.aws_functions.get_dataframe") as mocked:
                mocked.side_effect = KeyError("SQS Failure")
                response = lambda_wrangler_function.lambda_handler(
                    {
                        "RuntimeVariables":
                        {
                            "checkpoint": 666,
                            "survey_column": "survey"
                        }
                    }, context_object
                )
                assert "success" in response
                assert response["success"] is False

    @mock_sqs
    @mock_lambda
    def test_catch_wrangler_general_exception(self):
        # Method
        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        sqs_queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
        with mock.patch.dict(
            lambda_wrangler_function.os.environ,
            {
                "sns_topic_arn": "mike",
                "bucket_name": "mike",
                "checkpoint": "3",
                "identifier_column": "responder_id",
                "in_file_name": "test_data.json",
                "out_file_name": "PhilipeDePhile",
                "method_name": "enrichment_method",
                "sqs_queue_url": sqs_queue_url,
                "sqs_message_group_id": "testytest Mctestytestytesttest",
                "incoming_message_group": "test",
                "period_column": "period",
                "lookup_info": "Look up!!",
                "marine_mismatch_check": "true"
            },
        ):
            # using get_from_s3 to force exception early on.
            with mock.patch("enrichment_wrangler.aws_functions.get_dataframe") as mocked:
                mocked.side_effect = Exception("SQS Failure")
                response = lambda_wrangler_function.lambda_handler(
                    {
                        "RuntimeVariables":
                        {
                            "checkpoint": 666,
                            "survey_column": "survey"
                        }
                    }, context_object
                )
                assert "success" in response
                assert response["success"] is False

    @mock_sqs
    @mock_s3
    @mock_lambda
    def test_wrangles(self):
        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        sqs_queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
        client = boto3.client(
                 "s3",
                 region_name="eu-west-1",
                 aws_access_key_id="fake_access_key",
                 aws_secret_access_key="fake_secret_key",
             )

        client.create_bucket(Bucket="mike")

        with open("tests/fixtures/test_data.json", "r") as file:
            testdata = file.read()
        testdata = pd.DataFrame(json.loads(testdata))
        with mock.patch.dict(
            lambda_wrangler_function.os.environ,
            {
                "sns_topic_arn": "mike",
                "bucket_name": "mike",
                "checkpoint": "3",
                "identifier_column": "responder_id",
                "in_file_name": "test_data.json",
                "out_file_name": "PhilipeDePhile",
                "method_name": "enrichment_method",
                "sqs_queue_url": sqs_queue_url,
                "sqs_message_group_id": "testytest Mctestytestytesttest",
                "incoming_message_group": "test",
                "period_column": "period",
                "lookup_info": "Look up!!",
                "marine_mismatch_check": "true"
            },
        ):
            with mock.patch("enrichment_wrangler.aws_functions.get_dataframe") as mock_s3:
                mock_s3.return_value = testdata, 666
                with mock.patch(
                    "enrichment_wrangler.boto3.client"
                ) as mock_client:
                    mock_client_object = mock.Mock()
                    mock_client.return_value = mock_client_object
                    with open(
                        "tests/fixtures/test_data_from_method.json", "r"
                    ) as file:
                        mock_client_object.invoke.return_value \
                            .get.return_value.read \
                            .return_value.decode.return_value = json.dumps({
                             "data": file.read(), "success": True, "anomalies": []
                            })
                        response = lambda_wrangler_function.lambda_handler(
                            {
                                "RuntimeVariables":
                                {
                                    "checkpoint": 666,
                                    "survey_column": "survey"
                                }
                            },
                            context_object
                        )

                        assert "success" in response
                        assert response["success"] is True

    @mock_sqs
    @mock_s3
    @mock_lambda
    def test_wrangles_incompletereaderror(self):
        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        sqs_queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
        client = boto3.client(
                 "s3",
                 region_name="eu-west-1",
                 aws_access_key_id="fake_access_key",
                 aws_secret_access_key="fake_secret_key",
             )

        client.create_bucket(Bucket="mike")

        with open("tests/fixtures/test_data.json", "r") as file:
            testdata = file.read()
        testdata = pd.DataFrame(json.loads(testdata))
        with mock.patch.dict(
            lambda_wrangler_function.os.environ,
            {
                "sns_topic_arn": "mike",
                "bucket_name": "mike",
                "checkpoint": "3",
                "identifier_column": "responder_id",
                "in_file_name": "test_data.json",
                "out_file_name": "PhilipeDePhile",
                "method_name": "enrichment_method",
                "sqs_queue_url": sqs_queue_url,
                "sqs_message_group_id": "testytest Mctestytestytesttest",
                "incoming_message_group": "test",
                "period_column": "period",
                "lookup_info": "Look up!!",
                "marine_mismatch_check": "true"
            },
        ):
            from botocore.response import StreamingBody
            with mock.patch("enrichment_wrangler.aws_functions.get_dataframe") as mock_s3:
                mock_s3.return_value = testdata, 666
                with mock.patch(
                    "enrichment_wrangler.boto3.client"
                ) as mock_client:
                    mock_client_object = mock.Mock()
                    mock_client.return_value = mock_client_object
                    with open(
                        "tests/fixtures/test_data_from_method.json", "rb"
                    ) as file:
                        mock_client_object.invoke.return_value = {
                            "Payload": StreamingBody(file, 1)
                        }
                        response = lambda_wrangler_function.lambda_handler(
                            {
                                "RuntimeVariables":
                                {
                                    "checkpoint": 666,
                                    "survey_column": "survey"
                                }
                            },
                            context_object
                        )
                        assert "success" in response
                        assert response["success"] is False
                        assert("Incomplete Lambda response" in response['error'])

    @mock_sqs
    def test_wrangler_client_error(self):
        with mock.patch.dict(
            lambda_wrangler_function.os.environ,
            {
                "sns_topic_arn": "mike",
                "bucket_name": "mike",
                "checkpoint": "3",
                "identifier_column": "responder_id",
                "in_file_name": "test_data.json",
                "out_file_name": "PhilipeDePhile",
                "method_name": "enrichment_method",
                "sqs_queue_url": "aasdasdasd",
                "sqs_message_group_id": "testytest Mctestytestytesttest",
                "incoming_message_group": "test",
                "period_column": "period",
                "lookup_info": "Look up!!",
                "marine_mismatch_check": "true"
            },
        ):
            response = lambda_wrangler_function.lambda_handler(
                    {
                        "RuntimeVariables":
                        {
                            "checkpoint": 666,
                            "survey_column": "survey"
                        }
                    }, context_object
                )
            assert "success" in response
            assert response["success"] is False
            assert response["error"].__contains__("""AWS Error""")

    @mock_sqs
    @mock_lambda
    def test_catch_method_exception(self):
        with mock.patch.dict(
            lambda_wrangler_function.os.environ,
            {
                "bucket_name": "mike"
            },
        ):
            # using get_from_s3 to force exception early on.
            with mock.patch("enrichment_wrangler.boto3.resource") as mocked:
                mocked.side_effect = Exception("SQS Failure")
                response = lambda_method_function.lambda_handler(
                    {
                        "RuntimeVariables":
                        {
                            "checkpoint": 666,
                            "survey_column": "survey"
                        }
                    }, context_object
                )
                assert "success" in response
                assert response["success"] is False

    @mock_sqs
    @mock_lambda
    def test_catch_method_keyerror_exception(self):
        with mock.patch.dict(
            lambda_wrangler_function.os.environ,
            {
                "bucket_name": "mike"
            },
        ):
            # using get_from_s3 to force exception early on.
            with mock.patch("enrichment_wrangler.boto3.resource") as mocked:
                mocked.side_effect = KeyError("SQS Failure")
                response = lambda_method_function.lambda_handler(
                    {
                        "RuntimeVariables":
                        {
                            "checkpoint": 666,
                            "survey_column": "survey"
                        }
                    }, context_object
                )
                assert "success" in response
                assert response["success"] is False

    def test_missing_column_detector(self):
        data = pd.DataFrame(
            {"county": [1, None, 2], "responder_id": [666, 123, 8008]}
        )
        test_output = lambda_method_function.missing_column_detector(
            data, ["county"], "responder_id"
        )
        assert test_output.shape[0] == 1

    def test_marine_mismatch_detector(self):
        # one row in test data has been altered to trigger this.
        with open("tests/fixtures/test_data.json", "r") as file:
            testdata = file.read()
        with open("tests/fixtures/county_marine_lookup.json", "r") as file:
            countylookupdata = file.read()
        with open("tests/fixtures/responder_county_lookup.json", "r") as file:
            responder_lookup = file.read()
        testdata_df = pd.DataFrame(json.loads(testdata))
        countylookupdata_df = pd.DataFrame(json.loads(countylookupdata))
        responder_lookup_df = pd.DataFrame(json.loads(responder_lookup))
        testdata_df = pd.merge(
            testdata_df, responder_lookup_df, on="responder_id", how="left"
        )
        testdata_df = pd.merge(
            testdata_df, countylookupdata_df, on="county", how="left"
        )
        test_output = lambda_method_function.marine_mismatch_detector(
            testdata_df,
            "survey",
            "marine",
            "period",
            "responder_id"
        )
        assert test_output.shape[0] == 1

    @mock_s3
    def test_data_enricher(self):
        with mock.patch.dict(
            lambda_method_function.os.environ,
            {
                "bucket_name": "mike"
            },
        ):
            with open("tests/fixtures/test_data.json", "r") as file:
                testdata = file.read()

            testdata_df = pd.DataFrame(json.loads(testdata))
            client = boto3.client(
                "s3",
                region_name="eu-west-1",
                aws_access_key_id="fake_access_key",
                aws_secret_access_key="fake_secret_key",
            )

            client.create_bucket(Bucket="mike")
            client.upload_file(
                Filename="tests/fixtures/responder_county_lookup.json",
                Bucket="mike",
                Key="responderlookup",
            )
            client.upload_file(
                Filename="tests/fixtures/county_marine_lookup.json",
                Bucket="mike",
                Key="countylookup",
            )

            test_output, test_anomalies = lambda_method_function.data_enrichment(
                testdata_df,
                "true",
                "survey",
                "period",
                "mike",
                {
                 "0": {"file_name": "responderlookup",
                       "columns_to_keep": ["responder_id", "county"],
                       "join_column": "responder_id",
                       "required": ["county"]},
                 "1": {"file_name": "countylookup",
                       "columns_to_keep": ["county_name",
                                           "region", "county",
                                           "marine"],
                       "join_column": "county",
                       "required": ["region", "marine"]}},
                 "responder_id"
            )

            assert "county" in test_output.columns.values
            assert "county_name" in test_output.columns.values

    @mock_s3
    @mock_lambda
    def test_method(self):
        with mock.patch.dict(
            lambda_method_function.os.environ,
            {
                "bucket_name": "MIKE"
            },
        ):
            client = boto3.client(
                "s3",
                region_name="eu-west-1",
                aws_access_key_id="fake_access_key",
                aws_secret_access_key="fake_secret_key",
            )

            client.create_bucket(Bucket="MIKE")
            client.upload_file(
                Filename="tests/fixtures/responder_county_lookup.json",
                Bucket="MIKE",
                Key="responderlookup",
            )
            client.upload_file(
                Filename="tests/fixtures/county_marine_lookup.json",
                Bucket="MIKE",
                Key="countylookup",
            )

            with open("tests/fixtures/test_data.json", "r") as file:
                testdata = file.read()
            parameters = {"marine_mismatch_check": "true",
                          "survey_column": "survey",
                          "period_column": "period",
                          "identifier_column": "responder_id"}

            input = {"data": testdata, "lookups": {
                "0": {"file_name": "responderlookup",
                      "columns_to_keep": ["responder_id", "county"],
                      "join_column": "responder_id",
                      "required": ["county"]},
                "1": {"file_name": "countylookup",
                      "columns_to_keep": ["county_name",
                                          "region", "county",
                                          "marine"],
                      "join_column": "county",
                      "required": ["region", "marine"]}},
                     "parameters": parameters}
            test_output = lambda_method_function.lambda_handler(input, context_object)
            test_output = pd.read_json(test_output["data"])
            assert "county" in test_output.columns.values
            assert "county_name" in test_output.columns.values

    @mock_sqs
    def test_marshmallow_raises_method_exception(self):
        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        sqs_queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
        # Method
        with mock.patch.dict(
            lambda_method_function.os.environ, {"sqs_queue_url": sqs_queue_url}
        ):
            out = lambda_method_function.lambda_handler(
                {
                    "RuntimeVariables":
                    {
                        "checkpoint": 666,
                        "survey_column": "survey"
                    }
                }, context_object
            )
            self.assertRaises(ValueError)
            assert(out['error'].__contains__
                   ("""Parameter validation error"""))

    @mock_sqs
    def test_marshmallow_raises_wrangler_exception(self):
        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        sqs_queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
        # Method
        with mock.patch.dict(
            lambda_wrangler_function.os.environ,
            {"checkpoint": "1", "sqs_queue_url": sqs_queue_url},
        ):
            out = lambda_wrangler_function.lambda_handler(
                {
                    "RuntimeVariables":
                    {
                        "checkpoint": 666,
                        "survey_column": "survey"
                    }
                }, context_object
            )
            self.assertRaises(ValueError)
            assert(out['error'].__contains__
                   ("""Parameter validation error"""))

    def test_for_bad_data(self):
        with mock.patch.dict(
            lambda_wrangler_function.os.environ,
            {"enrichment_column": "enrich", "county": "19"},
        ):
            response = lambda_method_function.lambda_handler(
                "", context_object
            )
            assert response["error"].__contains__("""Parameter validation error""")

    @mock_s3
    def test_method_client_error(self):
        with mock.patch.dict(
            lambda_method_function.os.environ,
            {
                "bucket_name": "MIKE"
            },
        ):
            with open("tests/fixtures/test_data.json", "r") as file:
                testdata = file.read()
            parameters = {"marine_mismatch_check": "true",
                          "survey_column": "survey",
                          "period_column": "period",
                          "identifier_column": "responder_id"}
            input = {"data": testdata, "lookups":
                     {"0": {"required": "yup",
                      "file_name": "mike",
                            "columns_to_keep": "moo",
                            "join_column": "fred"}}, "parameters": parameters}
            response = lambda_method_function.lambda_handler(
                input, context_object
            )

            assert response["error"].__contains__("""AWS Error""")

    def test_method_general_error(self):
        with mock.patch.dict(
            lambda_method_function.os.environ,
            {
                "bucket_name": "MIKE"
            },
        ):
            with mock.patch("enrichment_method.EnvironSchema") as mock_schema:
                mock_schema.side_effect = Exception("uh oh")
                response = lambda_method_function.lambda_handler(
                    None, context_object
                )

                assert "General Error" in response["error"]

    @mock_sqs
    @mock_s3
    @mock_lambda
    def test_method_error(self):
        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        sqs_queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
        client = boto3.client(
                 "s3",
                 region_name="eu-west-1",
                 aws_access_key_id="fake_access_key",
                 aws_secret_access_key="fake_secret_key",
             )

        client.create_bucket(Bucket="mike")

        with open("tests/fixtures/test_data.json", "r") as file:
            testdata = file.read()
        testdata = pd.DataFrame(json.loads(testdata))
        with mock.patch.dict(
            lambda_wrangler_function.os.environ,
            {
                "sns_topic_arn": "mike",
                "bucket_name": "mike",
                "checkpoint": "3",
                "identifier_column": "responder_id",
                "in_file_name": "test_data.json",
                "out_file_name": "PhilipeDePhile",
                "method_name": "enrichment_method",
                "sqs_queue_url": sqs_queue_url,
                "sqs_message_group_id": "testytest Mctestytestytesttest",
                "incoming_message_group": "test",
                "period_column": "period",
                "lookup_info": "Look up!!",
                "marine_mismatch_check": "true"
            },
        ):
            with mock.patch("enrichment_wrangler.aws_functions.get_dataframe") as mock_s3:
                mock_s3.return_value = testdata, 666
                with mock.patch(
                    "enrichment_wrangler.boto3.client"
                ) as mock_client:
                    mock_client_object = mock.Mock()
                    mock_client.return_value = mock_client_object

                    mock_client_object.invoke.return_value.get.return_value \
                        .read.return_value.decode.return_value = \
                        json.dumps({"error": "This is an error message",
                                    "success": False})
                    response = lambda_wrangler_function.lambda_handler(
                        {
                            "RuntimeVariables":
                            {
                                "checkpoint": 666,
                                "survey_column": "survey"
                            }
                        },
                        context_object
                    )

                    assert "success" in response
                    assert response["success"] is False
                    assert response["error"].__contains__(
                        """This is an error message""")
