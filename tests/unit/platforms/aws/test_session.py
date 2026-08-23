from __future__ import annotations

from unittest.mock import MagicMock, patch
from concurrent.futures import ThreadPoolExecutor

from datacoolie.platforms.aws_platform import AWSPlatform

from datacoolie.platforms._aws.session import create_client, create_session


def test_create_session_forwards_region_and_profile() -> None:
    with patch("boto3.Session") as session_cls:
        expected = MagicMock()
        session_cls.return_value = expected

        assert create_session("ap-southeast-1", "dev") is expected
        session_cls.assert_called_once_with(
            region_name="ap-southeast-1",
            profile_name="dev",
        )


def test_create_client_applies_storage_endpoint_only_to_s3() -> None:
    session = MagicMock()

    create_client(session, "s3", storage_endpoint_url="http://localhost:9000")
    create_client(
        session, "secretsmanager", storage_endpoint_url="http://localhost:9000"
    )

    assert (
        session.client.call_args_list[0].kwargs["endpoint_url"]
        == "http://localhost:9000"
    )
    assert "endpoint_url" not in session.client.call_args_list[1].kwargs


def test_explicit_endpoint_overrides_platform_storage_endpoint() -> None:
    session = MagicMock()

    create_client(
        session,
        "s3",
        storage_endpoint_url="http://default:9000",
        endpoint_url="http://custom:9001",
    )

    assert session.client.call_args.kwargs["endpoint_url"] == "http://custom:9001"


def test_internal_clients_are_reused_under_thread_race() -> None:
    with patch("boto3.Session") as session_cls:
        session = MagicMock()
        session_cls.return_value = session
        platform = AWSPlatform(bucket="bucket", region="us-east-1")

        with ThreadPoolExecutor(max_workers=8) as pool:
            clients = list(pool.map(lambda _: platform._cached_client("s3"), range(32)))

        assert len({id(client) for client in clients}) == 1
        session.client.assert_called_once_with("s3")


def test_public_client_factory_remains_fresh() -> None:
    with patch("boto3.Session") as session_cls:
        session = MagicMock()
        session_cls.return_value = session
        session.client.side_effect = [MagicMock(), MagicMock()]
        platform = AWSPlatform(region="us-east-1")

        first = platform.boto3_client("sqs")
        second = platform.boto3_client("sqs")

        assert first is not second
        assert session.client.call_count == 2
