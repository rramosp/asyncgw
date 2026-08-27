"""Batch reassembler sorting and aggregating sub-request responses in sequence order."""

import asyncio
from datetime import datetime, timezone
import json
import logging
import time
from typing import Any, Dict, List, Optional

from asyncgw.models.response import (
    BatchAggregatedResponse,
    BatchOutputItem,
    RequestStatusEnum,
    RequestStatusResponse,
)
from asyncgw.storage.base import BaseBlobStorage, BaseRequestTracker

logger = logging.getLogger(__name__)


class BatchReassembler:
    """Reassembles partial batch sub-request results in strict sequence order and uploads the final output to GCS."""

    def __init__(
        self,
        request_tracker: BaseRequestTracker,
        blob_storage: BaseBlobStorage,
    ):
        self.request_tracker = request_tracker
        self.blob_storage = blob_storage

    def get_part_gcs_path(self, parent_request_id: str, sequence_number: int) -> str:
        return f"batches/{parent_request_id}/parts/{sequence_number}.json"

    def get_final_response_gcs_path(self, parent_request_id: str) -> str:
        return f"responses/{parent_request_id}.json"

    async def save_sub_request_part(
        self,
        parent_request_id: str,
        sequence_number: int,
        custom_id: Optional[str],
        result_data: Dict[str, Any],
        is_error: bool = False,
        status_code: int = 200,
        error_message: Optional[str] = None,
    ) -> str:
        """Save individual partial result to GCS for later reassembly."""
        part_path = self.get_part_gcs_path(parent_request_id, sequence_number)
        part_payload = {
            "id": f"batch_req_{sequence_number}",
            "custom_id": custom_id or f"custom_{sequence_number}",
            "sequence_number": sequence_number,
            "response": {"status_code": status_code, "body": result_data} if not is_error else None,
            "error": {"code": status_code, "message": error_message} if is_error else None,
        }
        uri = await self.blob_storage.save_json(part_path, part_payload)
        logger.debug(f"Saved batch sub-request part to {uri}")
        return uri

    async def try_reassemble_batch(
        self, parent_request_id: str
    ) -> Optional[BatchAggregatedResponse]:
        """Check if all sub-requests for a batch have finished, and reassemble them if ready."""
        sub_requests = await self.request_tracker.get_batch_sub_requests(parent_request_id)
        if not sub_requests:
            logger.debug(f"No sub-requests found for {parent_request_id}")
            return None

        total_items = sub_requests[0].total_items or len(sub_requests)
        
        # Check if all items are in terminal states (COMPLETED, FAILED, TIMED_OUT)
        terminal_statuses = {
            RequestStatusEnum.COMPLETED,
            RequestStatusEnum.FAILED,
            RequestStatusEnum.TIMED_OUT,
        }
        
        finished_count = sum(1 for s in sub_requests if s.status in terminal_statuses)
        if finished_count < total_items:
            logger.debug(
                f"Batch {parent_request_id} has {finished_count}/{total_items} items finished. Waiting for rest."
            )
            return None

        logger.info(f"All {total_items} sub-requests finished for batch {parent_request_id}. Reassembling...")

        # Sort sub-requests by sequence number strictly
        sub_requests.sort(key=lambda s: (s.sequence_number if s.sequence_number is not None else 0))

        results: List[BatchOutputItem] = []
        total_tokens = 0
        completed_count = 0
        failed_count = 0
        total_content_length = 0

        for s in sub_requests:
            seq = s.sequence_number if s.sequence_number is not None else 0
            part_path = self.get_part_gcs_path(parent_request_id, seq)

            if s.content_tokens:
                total_tokens += s.content_tokens

            try:
                part_data = await self.blob_storage.get_json(part_path)
                item = BatchOutputItem(
                    id=part_data.get("id", f"batch_req_{seq}"),
                    custom_id=part_data.get("custom_id", f"custom_{seq}"),
                    response=part_data.get("response"),
                    error=part_data.get("error"),
                )
                if item.response:
                    completed_count += 1
                else:
                    failed_count += 1
                results.append(item)
            except Exception as e:
                # If part file is missing due to hard worker failure
                logger.warning(f"Could not load part file {part_path}: {e}")
                failed_count += 1
                results.append(
                    BatchOutputItem(
                        id=f"batch_req_{seq}",
                        custom_id=f"custom_{seq}",
                        response=None,
                        error={"code": 500, "message": f"Worker crashed or part missing: {str(e)}"},
                    )
                )

        now_ts = int(time.time())
        final_status = (
            RequestStatusEnum.COMPLETED
            if failed_count == 0
            else (RequestStatusEnum.COMPLETED if completed_count > 0 else RequestStatusEnum.FAILED)
        )

        # Retrieve the real serving backend ID from the completed sub-requests
        served_backend_id = next(
            (
                s.backend_service_id
                for s in sub_requests
                if s.backend_service_id and s.backend_service_id != "gateway_batch_reassembler"
            ),
            "gemini-flex",
        )

        aggregated_data = {
            "id": parent_request_id,
            "object": "batch",
            "endpoint": "/v1/chat/completions",
            "status": final_status.value,
            "backend_service_id": served_backend_id,
            "backend_batch_service_mode": "decomposed",
            "created_at": int(sub_requests[0].created_at.timestamp()) if sub_requests[0].created_at else now_ts,
            "completed_at": now_ts,
            "request_counts": {
                "total": total_items,
                "completed": completed_count,
                "failed": failed_count,
            },
            "output_file_id": f"responses/{parent_request_id}.json",
            "results": [r.model_dump() for r in results],
        }

        # Upload final aggregated output file to GCS
        final_gcs_path = self.get_final_response_gcs_path(parent_request_id)
        final_gcs_uri = await self.blob_storage.save_json(final_gcs_path, aggregated_data)
        content_len = len(json.dumps(aggregated_data))

        # Calculate total elapsed time
        start_time = sub_requests[0].created_at or datetime.now(timezone.utc)
        elapsed_sec = (datetime.now(timezone.utc) - start_time).total_seconds()

        # Update parent record in BigQuery
        if final_status == RequestStatusEnum.COMPLETED:
            await self.request_tracker.mark_completed(
                request_id=parent_request_id,
                response_gcs_uri=final_gcs_uri,
                response_status_code=200,
                response_content_length=content_len,
                elapsed_seconds=elapsed_sec,
                backend_service_id=served_backend_id,
                backend_batch_service_mode="decomposed",
                content_tokens=total_tokens,
                sequence_number=None,
            )
        else:
            await self.request_tracker.mark_failed(
                request_id=parent_request_id,
                error_message=f"Batch failed: {failed_count}/{total_items} items failed",
                response_status_code=500,
                elapsed_seconds=elapsed_sec,
                backend_service_id=served_backend_id,
                backend_batch_service_mode="decomposed",
                sequence_number=None,
            )

        logger.info(
            f"Successfully reassembled batch {parent_request_id} via {served_backend_id} ({completed_count} ok, {failed_count} failed) -> {final_gcs_uri}"
        )
        return BatchAggregatedResponse(**aggregated_data, output_gcs_uri=final_gcs_uri)
