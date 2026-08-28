"""Google Cloud Storage (GCS) implementation for response and batch storage."""

import asyncio
from datetime import timedelta
import json
import logging
from typing import Any, Dict, List, Optional, Tuple

from asyncgw.config import GatewaySettings
from asyncgw.storage.base import BaseBlobStorage

logger = logging.getLogger(__name__)


class GCSBlobStorage(BaseBlobStorage):
    """Stores LLM inference response payloads and batch JSONL files in Google Cloud Storage."""

    def __init__(self, settings: GatewaySettings):
        self.settings = settings
        self.bucket_name = settings.gcs_bucket_name
        self.retention_days = settings.gcs_retention_days
        self._client = None

    def _get_client(self):
        if self._client is None:
            from google.cloud import storage
            self._client = storage.Client(project=self.settings.project_id)
        return self._client

    async def initialize(self) -> None:
        """Ensure GCS bucket exists and configure lifecycle rule for 7-day retention."""
        try:
            from google.cloud import storage
            from google.cloud.exceptions import NotFound

            client = self._get_client()
            try:
                bucket = client.get_bucket(self.bucket_name)
                logger.info(f"GCS bucket {self.bucket_name} exists.")

                rules = list(bucket.lifecycle_rules)
                has_age_rule = any(
                    rule.get("action", {}).get("type") == "Delete" and rule.get("condition", {}).get("age") == self.retention_days
                    for rule in rules
                )
                if not has_age_rule:
                    bucket.add_lifecycle_delete_rule(age=self.retention_days)
                    bucket.patch()
                    logger.info(f"Configured {self.retention_days}-day delete lifecycle rule on {self.bucket_name}")
            except Exception as be:
                logger.info(f"GCS bucket {self.bucket_name} bucket-level management note ({be}); object operations will proceed.")

        except Exception as e:
            logger.warning(f"GCS bucket initialization check note: {e}")

    def _parse_bucket_and_blob(self, path_or_uri: str) -> Tuple[str, str]:
        """Extract (bucket_name, blob_name) from gs://bucket_name/blob_path or relative path."""
        if path_or_uri.startswith("gs://"):
            parts = path_or_uri[5:].split("/", 1)
            if len(parts) == 2:
                return parts[0], parts[1]
            return parts[0], ""
        return self.bucket_name, path_or_uri.lstrip("/")

    async def exists(self, path_or_uri: str) -> bool:
        b_name, blob_path = self._parse_bucket_and_blob(path_or_uri)

        def _sync_exists():
            client = self._get_client()
            bucket = client.bucket(b_name)
            blob = bucket.blob(blob_path)
            return blob.exists()

        return await asyncio.to_thread(_sync_exists)

    async def save_json(
        self, path: str, data: Dict[str, Any], content_type: str = "application/json"
    ) -> str:
        b_name, blob_path = self._parse_bucket_and_blob(path)
        content = json.dumps(data, indent=2)

        def _sync_upload():
            client = self._get_client()
            bucket = client.bucket(b_name)
            blob = bucket.blob(blob_path)
            blob.upload_from_string(content, content_type=content_type)
            return f"gs://{b_name}/{blob_path}"

        return await asyncio.to_thread(_sync_upload)

    async def get_json(self, path_or_uri: str) -> Dict[str, Any]:
        b_name, blob_path = self._parse_bucket_and_blob(path_or_uri)

        def _sync_download():
            client = self._get_client()
            bucket = client.bucket(b_name)
            blob = bucket.blob(blob_path)
            content = blob.download_as_text()
            return json.loads(content)

        return await asyncio.to_thread(_sync_download)

    async def save_jsonl(self, path: str, lines: List[Dict[str, Any]]) -> str:
        b_name, blob_path = self._parse_bucket_and_blob(path)
        content = "\n".join(json.dumps(line) for line in lines) + "\n"

        def _sync_upload():
            client = self._get_client()
            bucket = client.bucket(b_name)
            blob = bucket.blob(blob_path)
            blob.upload_from_string(content, content_type="application/x-ndjson")
            return f"gs://{b_name}/{blob_path}"

        return await asyncio.to_thread(_sync_upload)

    async def get_jsonl(self, path_or_uri: str) -> List[Dict[str, Any]]:
        b_name, blob_path = self._parse_bucket_and_blob(path_or_uri)

        def _sync_download():
            client = self._get_client()
            bucket = client.bucket(b_name)
            blob = bucket.blob(blob_path)
            content = blob.download_as_text()
            return [json.loads(line) for line in content.strip().split("\n") if line.strip()]

        return await asyncio.to_thread(_sync_download)

    async def generate_signed_url(
        self, path_or_uri: str, expiration_minutes: int = 60
    ) -> Optional[str]:
        b_name, blob_path = self._parse_bucket_and_blob(path_or_uri)

        def _sync_sign():
            client = self._get_client()
            bucket = client.bucket(b_name)
            blob = bucket.blob(blob_path)
            return blob.generate_signed_url(
                version="v4",
                expiration=timedelta(minutes=expiration_minutes),
                method="GET",
            )

        try:
            return await asyncio.to_thread(_sync_sign)
        except Exception as e:
            logger.warning(f"Could not generate GCS signed URL: {e}")
            return None
