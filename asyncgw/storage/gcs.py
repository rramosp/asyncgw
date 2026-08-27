"""Google Cloud Storage (GCS) implementation for response and batch storage."""

import asyncio
from datetime import timedelta
import json
import logging
from typing import Any, Dict, List, Optional

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

                # Apply 7-day lifecycle retention rule if not already present
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
                logger.info(f"GCS bucket {self.bucket_name} bucket-level management skipped ({be}); object operations will proceed.")

        except Exception as e:
            logger.warning(f"GCS bucket initialization check note: {e}")

    def _normalize_blob_path(self, path_or_uri: str) -> str:
        """Extract blob name from gs://bucket_name/blob_path or return raw path."""
        prefix = f"gs://{self.bucket_name}/"
        if path_or_uri.startswith(prefix):
            return path_or_uri[len(prefix):]
        if path_or_uri.startswith("gs://"):
            parts = path_or_uri.replace("gs://", "").split("/", 1)
            if len(parts) == 2:
                return parts[1]
        return path_or_uri.lstrip("/")

    async def save_json(
        self, path: str, data: Dict[str, Any], content_type: str = "application/json"
    ) -> str:
        blob_path = self._normalize_blob_path(path)
        content = json.dumps(data, indent=2)

        def _sync_upload():
            client = self._get_client()
            bucket = client.bucket(self.bucket_name)
            blob = bucket.blob(blob_path)
            blob.upload_from_string(content, content_type=content_type)
            return f"gs://{self.bucket_name}/{blob_path}"

        return await asyncio.to_thread(_sync_upload)

    async def get_json(self, path_or_uri: str) -> Dict[str, Any]:
        blob_path = self._normalize_blob_path(path_or_uri)

        def _sync_download():
            client = self._get_client()
            bucket = client.bucket(self.bucket_name)
            blob = bucket.blob(blob_path)
            content = blob.download_as_text()
            return json.loads(content)

        return await asyncio.to_thread(_sync_download)

    async def save_jsonl(self, path: str, lines: List[Dict[str, Any]]) -> str:
        blob_path = self._normalize_blob_path(path)
        content = "\n".join(json.dumps(line) for line in lines) + "\n"

        def _sync_upload():
            client = self._get_client()
            bucket = client.bucket(self.bucket_name)
            blob = bucket.blob(blob_path)
            blob.upload_from_string(content, content_type="application/x-ndjson")
            return f"gs://{self.bucket_name}/{blob_path}"

        return await asyncio.to_thread(_sync_upload)

    async def get_jsonl(self, path_or_uri: str) -> List[Dict[str, Any]]:
        blob_path = self._normalize_blob_path(path_or_uri)

        def _sync_download():
            client = self._get_client()
            bucket = client.bucket(self.bucket_name)
            blob = bucket.blob(blob_path)
            content = blob.download_as_text()
            results = []
            for line in content.strip().split("\n"):
                if line.strip():
                    results.append(json.loads(line.strip()))
            return results

        return await asyncio.to_thread(_sync_download)

    async def exists(self, path_or_uri: str) -> bool:
        blob_path = self._normalize_blob_path(path_or_uri)

        def _sync_exists():
            client = self._get_client()
            bucket = client.bucket(self.bucket_name)
            blob = bucket.blob(blob_path)
            return blob.exists()

        return await asyncio.to_thread(_sync_exists)
