"""BigQuery Request Tracker implementation with date-partitioned table."""

import asyncio
from datetime import datetime, timezone
import json
import logging
from typing import Any, Dict, List, Optional

from asyncgw.config import GatewaySettings
from asyncgw.models.request import AsyncRequestEnvelope, RequestType
from asyncgw.models.response import RequestStatusEnum, RequestStatusResponse
from asyncgw.storage.base import BaseRequestTracker

logger = logging.getLogger(__name__)


class BigQueryRequestTracker(BaseRequestTracker):
    """Tracks request state, metadata, and responses in Google Cloud BigQuery.
    
    The table is partitioned by DATE(created_at) for efficient querying and retention.
    Uses append-only state transition events to ensure real-time consistency without
    triggering BigQuery streaming buffer UPDATE/DELETE limitations.
    """

    def __init__(self, settings: GatewaySettings):
        self.settings = settings
        self.project_id = settings.project_id
        self.dataset_id = settings.bq_dataset
        self.table_id = settings.bq_table
        self.full_table_id = f"{self.project_id}.{self.dataset_id}.{self.table_id}"
        self._client = None

    def _get_client(self):
        if self._client is None:
            from google.cloud import bigquery
            self._client = bigquery.Client(project=self.project_id, location=self.settings.location)
        return self._client

    async def initialize(self) -> None:
        """Create BigQuery dataset and partitioned table if they do not exist."""
        try:
            from google.cloud import bigquery
            from google.cloud.exceptions import NotFound

            client = self._get_client()

            # Ensure dataset exists
            dataset_ref = bigquery.DatasetReference(self.project_id, self.dataset_id)
            try:
                client.get_dataset(dataset_ref)
                logger.info(f"BigQuery dataset {self.dataset_id} exists.")
            except NotFound:
                dataset = bigquery.Dataset(dataset_ref)
                dataset.location = self.settings.location
                dataset.description = "Async LLM Gateway request tracker metrics and statuses"
                client.create_dataset(dataset, exists_ok=True)
                logger.info(f"Created BigQuery dataset {self.dataset_id} in {self.settings.location}")

            # Define schema with all tracking columns specified in requirements
            schema = [
                bigquery.SchemaField("request_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("parent_request_id", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("sequence_number", "INT64", mode="NULLABLE"),
                bigquery.SchemaField("total_items", "INT64", mode="NULLABLE"),
                bigquery.SchemaField("custom_id", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("request_type", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("model", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("max_wait_seconds", "INT64", mode="NULLABLE"),
                bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("expires_at", "TIMESTAMP", mode="NULLABLE"),
                bigquery.SchemaField("started_at", "TIMESTAMP", mode="NULLABLE"),
                bigquery.SchemaField("completed_at", "TIMESTAMP", mode="NULLABLE"),
                bigquery.SchemaField("elapsed_seconds", "FLOAT64", mode="NULLABLE"),
                bigquery.SchemaField("backend_service_id", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("backend_batch_service_mode", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("backend_endpoint", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("response_status_code", "INT64", mode="NULLABLE"),
                bigquery.SchemaField("response_content_length", "INT64", mode="NULLABLE"),
                bigquery.SchemaField("response_gcs_uri", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("error_message", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("retry_count", "INT64", mode="NULLABLE"),
                bigquery.SchemaField("content_tokens", "INT64", mode="NULLABLE"),
                bigquery.SchemaField("metadata_json", "STRING", mode="NULLABLE"),
            ]

            table_ref = dataset_ref.table(self.table_id)
            try:
                client.get_table(table_ref)
                logger.info(f"BigQuery table {self.full_table_id} exists.")
            except NotFound:
                table = bigquery.Table(table_ref, schema=schema)
                # Partition table by date of created_at
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="created_at",
                    expiration_ms=int(self.settings.gcs_retention_days * 86400 * 1000),
                )
                table.clustering_fields = ["status", "request_id", "parent_request_id"]
                table.description = "Partitioned request tracking table for Asynchronous LLM Gateway"
                client.create_table(table)
                logger.info(f"Created BigQuery partitioned table {self.full_table_id}")

        except Exception as e:
            logger.warning(f"BigQuery initialization check note (table managed via Terraform): {e}")

    async def register_request(self, envelope: AsyncRequestEnvelope) -> None:
        """Insert a newly submitted request into BigQuery."""
        row = {
            "request_id": envelope.request_id,
            "parent_request_id": envelope.parent_request_id,
            "sequence_number": envelope.sequence_number,
            "total_items": envelope.total_items,
            "custom_id": envelope.custom_id,
            "request_type": envelope.request_type.value,
            "status": RequestStatusEnum.PENDING.value,
            "model": envelope.model,
            "max_wait_seconds": envelope.max_wait_seconds,
            "created_at": envelope.created_at.isoformat(),
            "expires_at": envelope.expires_at.isoformat() if envelope.expires_at else None,
            "started_at": None,
            "completed_at": None,
            "elapsed_seconds": None,
            "backend_service_id": envelope.target_backend,
            "backend_endpoint": None,
            "response_status_code": None,
            "response_content_length": None,
            "response_gcs_uri": None,
            "error_message": None,
            "retry_count": envelope.retry_count,
            "content_tokens": None,
            "metadata_json": json.dumps(envelope.tags or {}),
        }
        await self._insert_rows([row])

    async def register_batch_sub_requests(
        self, envelopes: List[AsyncRequestEnvelope]
    ) -> None:
        """Insert batch sub-requests."""
        rows = []
        for env in envelopes:
            rows.append({
                "request_id": env.request_id,
                "parent_request_id": env.parent_request_id,
                "sequence_number": env.sequence_number,
                "total_items": env.total_items,
                "custom_id": env.custom_id,
                "request_type": env.request_type.value,
                "status": RequestStatusEnum.PENDING.value,
                "model": env.model,
                "max_wait_seconds": env.max_wait_seconds,
                "created_at": env.created_at.isoformat(),
                "expires_at": env.expires_at.isoformat() if env.expires_at else None,
                "started_at": None,
                "completed_at": None,
                "elapsed_seconds": None,
                "backend_service_id": env.target_backend,
                "backend_endpoint": None,
                "response_status_code": None,
                "response_content_length": None,
                "response_gcs_uri": None,
                "error_message": None,
                "retry_count": env.retry_count,
                "content_tokens": None,
                "metadata_json": json.dumps(env.tags or {}),
            })
        if rows:
            await self._insert_rows(rows)

    async def _insert_rows(self, rows: List[Dict[str, Any]]) -> None:
        def _sync_insert():
            client = self._get_client()
            table_ref = client.dataset(self.dataset_id).table(self.table_id)
            errors = client.insert_rows_json(table_ref, rows)
            if errors:
                raise RuntimeError(f"BigQuery insert error: {errors}")
        await asyncio.to_thread(_sync_insert)

    async def mark_processing(
        self,
        request_id: str,
        backend_service_id: str,
        backend_endpoint: Optional[str] = None,
        sequence_number: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None,
        parent_request_id: Optional[str] = None,
    ) -> None:
        now_iso = datetime.now(timezone.utc).isoformat()
        row = {
            "request_id": request_id,
            "parent_request_id": parent_request_id,
            "sequence_number": sequence_number,
            "request_type": "batch.sub_request" if parent_request_id or sequence_number is not None else "chat.completion",
            "status": RequestStatusEnum.PROCESSING.value,
            "created_at": now_iso,
            "started_at": now_iso,
            "backend_service_id": backend_service_id,
            "backend_endpoint": backend_endpoint or "",
            "metadata_json": json.dumps(metadata) if metadata is not None else None,
        }
        await self._insert_rows([row])

    async def mark_completed(
        self,
        request_id: str,
        response_gcs_uri: str,
        response_status_code: int,
        response_content_length: int,
        elapsed_seconds: float,
        backend_service_id: str,
        content_tokens: Optional[int] = None,
        sequence_number: Optional[int] = None,
        backend_batch_service_mode: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        parent_request_id: Optional[str] = None,
    ) -> None:
        now_iso = datetime.now(timezone.utc).isoformat()
        row = {
            "request_id": request_id,
            "parent_request_id": parent_request_id,
            "sequence_number": sequence_number,
            "request_type": "batch.sub_request" if parent_request_id or sequence_number is not None else "chat.completion",
            "status": RequestStatusEnum.COMPLETED.value,
            "created_at": now_iso,
            "completed_at": now_iso,
            "response_gcs_uri": response_gcs_uri,
            "response_status_code": response_status_code,
            "response_content_length": response_content_length,
            "elapsed_seconds": elapsed_seconds,
            "backend_service_id": backend_service_id,
            "backend_batch_service_mode": backend_batch_service_mode or "",
            "content_tokens": content_tokens or 0,
            "metadata_json": json.dumps(metadata) if metadata is not None else None,
        }
        await self._insert_rows([row])

    async def mark_failed(
        self,
        request_id: str,
        error_message: str,
        response_status_code: Optional[int] = None,
        elapsed_seconds: Optional[float] = None,
        backend_service_id: Optional[str] = None,
        sequence_number: Optional[int] = None,
        backend_batch_service_mode: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        response_gcs_uri: Optional[str] = None,
        parent_request_id: Optional[str] = None,
    ) -> None:
        now_iso = datetime.now(timezone.utc).isoformat()
        row = {
            "request_id": request_id,
            "parent_request_id": parent_request_id,
            "sequence_number": sequence_number,
            "request_type": "batch.sub_request" if parent_request_id or sequence_number is not None else "chat.completion",
            "status": RequestStatusEnum.FAILED.value,
            "created_at": now_iso,
            "completed_at": now_iso,
            "error_message": error_message,
            "response_status_code": response_status_code or 500,
            "response_gcs_uri": response_gcs_uri,
            "elapsed_seconds": elapsed_seconds or 0.0,
            "backend_service_id": backend_service_id or "",
            "backend_batch_service_mode": backend_batch_service_mode or "",
            "metadata_json": json.dumps(metadata) if metadata is not None else None,
        }
        await self._insert_rows([row])

    async def mark_timed_out(
        self,
        request_id: str,
        error_message: str = "Request exceeded user-specified maximum wait time",
        sequence_number: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None,
        parent_request_id: Optional[str] = None,
    ) -> None:
        now_iso = datetime.now(timezone.utc).isoformat()
        row = {
            "request_id": request_id,
            "parent_request_id": parent_request_id,
            "sequence_number": sequence_number,
            "request_type": "batch.sub_request" if parent_request_id or sequence_number is not None else "chat.completion",
            "status": RequestStatusEnum.TIMED_OUT.value,
            "created_at": now_iso,
            "completed_at": now_iso,
            "error_message": error_message,
            "metadata_json": json.dumps(metadata) if metadata is not None else None,
        }
        await self._insert_rows([row])

    async def get_request_status(
        self, request_id: str, sequence_number: Optional[int] = None
    ) -> Optional[RequestStatusResponse]:
        seq_clause = "AND sequence_number = @seq" if sequence_number is not None else "AND sequence_number IS NULL"
        query = f"""
            SELECT
                request_id,
                MAX(parent_request_id) as parent_request_id,
                MAX(sequence_number) as sequence_number,
                MAX(total_items) as total_items,
                MAX(custom_id) as custom_id,
                ARRAY_AGG(request_type IGNORE NULLS ORDER BY CASE WHEN request_type IN ('chat.completion', 'text.completion', 'embeddings', 'batch', 'batch.sub_request') THEN 2 ELSE 1 END DESC, created_at ASC LIMIT 1)[OFFSET(0)] as request_type,
                ARRAY_AGG(status ORDER BY
                    CASE status
                        WHEN 'COMPLETED' THEN 5
                        WHEN 'FAILED' THEN 4
                        WHEN 'TIMED_OUT' THEN 4
                        WHEN 'CANCELLED' THEN 4
                        WHEN 'PROCESSING' THEN 3
                        WHEN 'PENDING' THEN 2
                        ELSE 1
                    END DESC, created_at DESC LIMIT 1)[OFFSET(0)] as status,
                MAX(model) as model,
                MAX(max_wait_seconds) as max_wait_seconds,
                MIN(created_at) as created_at,
                MAX(expires_at) as expires_at,
                MAX(started_at) as started_at,
                MAX(completed_at) as completed_at,
                MAX(elapsed_seconds) as elapsed_seconds,
                MAX(backend_service_id) as backend_service_id,
                MAX(backend_batch_service_mode) as backend_batch_service_mode,
                MAX(backend_endpoint) as backend_endpoint,
                MAX(response_status_code) as response_status_code,
                MAX(response_content_length) as response_content_length,
                MAX(response_gcs_uri) as response_gcs_uri,
                MAX(error_message) as error_message,
                MAX(retry_count) as retry_count,
                MAX(content_tokens) as content_tokens,
                ARRAY_AGG(metadata_json IGNORE NULLS ORDER BY created_at DESC LIMIT 1)[OFFSET(0)] as metadata_json
            FROM `{self.full_table_id}`
            WHERE request_id = @req_id {seq_clause}
            GROUP BY request_id
        """
        params = [("req_id", "STRING", request_id)]
        if sequence_number is not None:
            params.append(("seq", "INT64", sequence_number))

        rows = await self._run_query_fetch(query, params)
        if not rows:
            return None
        return self._row_to_status_response(rows[0])

    async def get_batch_sub_requests(
        self, parent_request_id: str
    ) -> List[RequestStatusResponse]:
        query = f"""
            SELECT
                request_id,
                MAX(parent_request_id) as parent_request_id,
                MAX(sequence_number) as sequence_number,
                MAX(total_items) as total_items,
                MAX(custom_id) as custom_id,
                ARRAY_AGG(request_type IGNORE NULLS ORDER BY CASE WHEN request_type IN ('chat.completion', 'text.completion', 'embeddings', 'batch', 'batch.sub_request') THEN 2 ELSE 1 END DESC, created_at ASC LIMIT 1)[OFFSET(0)] as request_type,
                ARRAY_AGG(status ORDER BY
                    CASE status
                        WHEN 'COMPLETED' THEN 5
                        WHEN 'FAILED' THEN 4
                        WHEN 'TIMED_OUT' THEN 4
                        WHEN 'CANCELLED' THEN 4
                        WHEN 'PROCESSING' THEN 3
                        WHEN 'PENDING' THEN 2
                        ELSE 1
                    END DESC, created_at DESC LIMIT 1)[OFFSET(0)] as status,
                MAX(model) as model,
                MAX(max_wait_seconds) as max_wait_seconds,
                MIN(created_at) as created_at,
                MAX(expires_at) as expires_at,
                MAX(started_at) as started_at,
                MAX(completed_at) as completed_at,
                MAX(elapsed_seconds) as elapsed_seconds,
                MAX(backend_service_id) as backend_service_id,
                MAX(backend_batch_service_mode) as backend_batch_service_mode,
                MAX(backend_endpoint) as backend_endpoint,
                MAX(response_status_code) as response_status_code,
                MAX(response_content_length) as response_content_length,
                MAX(response_gcs_uri) as response_gcs_uri,
                MAX(error_message) as error_message,
                MAX(retry_count) as retry_count,
                MAX(content_tokens) as content_tokens,
                ARRAY_AGG(metadata_json IGNORE NULLS ORDER BY created_at DESC LIMIT 1)[OFFSET(0)] as metadata_json
            FROM `{self.full_table_id}`
            WHERE request_id IN (
                SELECT DISTINCT request_id FROM `{self.full_table_id}` WHERE parent_request_id = @parent_id
            )
            OR parent_request_id = @parent_id
            GROUP BY request_id
            ORDER BY sequence_number ASC
        """
        params = [("parent_id", "STRING", parent_request_id)]
        rows = await self._run_query_fetch(query, params)
        return [self._row_to_status_response(r) for r in rows]

    async def list_recent_requests(
        self, limit: int = 50, status: Optional[RequestStatusEnum] = None
    ) -> List[RequestStatusResponse]:
        status_clause = "WHERE status = @status" if status else ""
        query = f"""
            WITH aggregated_requests AS (
                SELECT
                    request_id,
                    MAX(parent_request_id) as parent_request_id,
                    MAX(sequence_number) as sequence_number,
                    MAX(total_items) as total_items,
                    MAX(custom_id) as custom_id,
                    ARRAY_AGG(request_type IGNORE NULLS ORDER BY CASE WHEN request_type IN ('chat.completion', 'text.completion', 'embeddings', 'batch', 'batch.sub_request') THEN 2 ELSE 1 END DESC, created_at ASC LIMIT 1)[OFFSET(0)] as request_type,
                    ARRAY_AGG(status ORDER BY
                        CASE status
                            WHEN 'COMPLETED' THEN 5
                            WHEN 'FAILED' THEN 4
                            WHEN 'TIMED_OUT' THEN 4
                            WHEN 'CANCELLED' THEN 4
                            WHEN 'PROCESSING' THEN 3
                            WHEN 'PENDING' THEN 2
                            ELSE 1
                        END DESC, created_at DESC LIMIT 1)[OFFSET(0)] as status,
                    MAX(model) as model,
                    MAX(max_wait_seconds) as max_wait_seconds,
                    MIN(created_at) as created_at,
                    MAX(expires_at) as expires_at,
                    MAX(started_at) as started_at,
                    MAX(completed_at) as completed_at,
                    MAX(elapsed_seconds) as elapsed_seconds,
                    MAX(backend_service_id) as backend_service_id,
                    MAX(backend_batch_service_mode) as backend_batch_service_mode,
                    MAX(backend_endpoint) as backend_endpoint,
                    MAX(response_status_code) as response_status_code,
                    MAX(response_content_length) as response_content_length,
                    MAX(response_gcs_uri) as response_gcs_uri,
                    MAX(error_message) as error_message,
                    MAX(retry_count) as retry_count,
                    MAX(content_tokens) as content_tokens,
                    ARRAY_AGG(metadata_json IGNORE NULLS ORDER BY created_at DESC LIMIT 1)[OFFSET(0)] as metadata_json
                FROM `{self.full_table_id}`
                WHERE sequence_number IS NULL
                GROUP BY request_id
            )
            SELECT *
            FROM aggregated_requests
            {status_clause}
            ORDER BY created_at DESC
            LIMIT @limit
        """
        params = [("limit", "INT64", limit)]
        if status:
            params.append(("status", "STRING", status.value))

        rows = await self._run_query_fetch(query, params)
        return [self._row_to_status_response(r) for r in rows]

    def _row_to_status_response(self, row: Dict[str, Any]) -> RequestStatusResponse:
        metadata = {}
        if row.get("metadata_json"):
            try:
                metadata = json.loads(row["metadata_json"])
            except Exception:
                pass

        req_type = row.get("request_type")
        batch_mode = row.get("backend_batch_service_mode")
        if req_type != RequestType.BATCH.value and req_type != "batch":
            batch_mode = None

        return RequestStatusResponse(
            request_id=row["request_id"],
            parent_request_id=row.get("parent_request_id"),
            sequence_number=row.get("sequence_number"),
            total_items=row.get("total_items") or 1,
            status=RequestStatusEnum(row["status"]),
            model=row.get("model"),
            request_type=req_type,
            created_at=row.get("created_at"),
            started_at=row.get("started_at"),
            completed_at=row.get("completed_at"),
            elapsed_seconds=row.get("elapsed_seconds"),
            backend_service_id=row.get("backend_service_id"),
            backend_batch_service_mode=batch_mode,
            response_status_code=row.get("response_status_code"),
            response_content_length=row.get("response_content_length"),
            response_gcs_uri=row.get("response_gcs_uri"),
            error_message=row.get("error_message"),
            retry_count=row.get("retry_count") or 0,
            content_tokens=row.get("content_tokens"),
            metadata=metadata,
        )

    async def _execute_query(self, query: str, params: List[tuple]) -> None:
        def _sync():
            from google.cloud import bigquery
            client = self._get_client()
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(name, type_, val) for name, type_, val in params
                ]
            )
            client.query(query, job_config=job_config, location=self.settings.location).result()
        await asyncio.to_thread(_sync)

    async def _run_query_fetch(self, query: str, params: List[tuple]) -> List[Dict[str, Any]]:
        def _sync():
            from google.cloud import bigquery
            client = self._get_client()
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(name, type_, val) for name, type_, val in params
                ]
            )
            job = client.query(query, job_config=job_config, location=self.settings.location)
            return [dict(row) for row in job.result()]
        return await asyncio.to_thread(_sync)
