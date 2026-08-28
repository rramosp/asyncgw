#!/usr/bin/env python3
"""
Script to generate and send randomized individual and batch requests to the LLM Gateway.

Features:
- Pure Python using the `requests` library (no OpenAI or Google client SDKs).
- Configurable parameters for endpoint, model, batch percentages, batch sizes, parallelism, and delays.
- Dynamic generation of realistic, varied questions across multiple domains.
- Randomly mixed sequence of individual and batch requests.
- Parallel execution using ThreadPoolExecutor respecting `MAX_PARALLEL` and `WAIT_TIME`.
- Comprehensive real-time logging and execution summary report.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import json
import logging
import random
import time
from typing import Any, Dict, List
import uuid

import requests

# ==============================================================================
# CONFIGURATION VARIABLES (Modify as needed)
# ==============================================================================
ENDPOINT = "http://localhost:8080"  # The endpoint URI to query (e.g., http://localhost:8000 or https://api.asyncgw.example.com)
MODEL_ID = "gemini-3.6-flash"       # The model to send the requests to (for instance gemini-3.6-flash)
NUM_REQUESTS = 20                   # An integer: total number of requests/queries to generate
PCT_BATCH = 0.4                     # Float between 0.0 and 1.0: percentage of requests sent in batch mode
BATCH_SIZE_MIN = 2                  # Minimum number of items in each batch request
BATCH_SIZE_MAX = 5                  # Maximum number of items in each batch request
MAX_PARALLEL = 5                    # Maximum number of requests that can be sent in parallel
WAIT_TIME = 0.5                     # Time (in seconds) to wait between sending one request and the next
# ==============================================================================

# Configure logging format
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("GatewayClient")


# ==============================================================================
# RANDOM QUESTION GENERATOR
# ==============================================================================
TOPICS = [
    "quantum computing", "distributed databases", "black holes", "cellular biology",
    "cloud architecture", "machine learning optimization", "renewable energy systems",
    "ancient Mesopotamian history", "game theory", "operating system kernels",
    "compiler design", "cryptography", "deep sea ecosystems", "macroeconomics",
    "natural language processing", "CRISPR gene editing", "plate tectonics",
    "cybersecurity zero-trust models", "astrophysics", "cognitive psychology"
]

TEMPLATES = [
    "Explain the core principles of {topic} in simple terms.",
    "What are the main advantages and trade-offs of {topic}?",
    "How has recent research advanced our understanding of {topic}?",
    "Provide a brief historical timeline of key milestones in {topic}.",
    "Compare and contrast {topic} with conventional approaches.",
    "What are the most significant open challenges currently facing {topic}?",
    "Give three real-world examples where {topic} is applied today.",
    "Write a concise executive summary outlining the future outlook of {topic}.",
    "What fundamental equations or theoretical models govern {topic}?",
    "How does {topic} impact modern technological and scientific developments?"
]

STANDALONE_QUESTIONS = [
    "What is the difference between latency and throughput in network systems?",
    "How does backpressure work in distributed event streaming architectures?",
    "Explain the Byzantine Generals Problem and how consensus algorithms solve it.",
    "What are the trade-offs between B-Tree and LSM-Tree storage engines?",
    "How does speculative execution improve CPU performance, and what are its security risks?",
    "What is the difference between synchronous and asynchronous API architectures?",
    "How do transformer neural networks utilize self-attention mechanisms?",
    "Explain the CAP theorem with concrete examples from modern cloud databases.",
    "What are the primary factors contributing to climate change and ocean acidification?",
    "How do zero-knowledge proofs enable privacy in cryptographic protocols?",
    "What is the role of mitochondria in cellular energy production?",
    "Explain the difference between optimistic and pessimistic concurrency control.",
    "What are the mathematical foundations behind RSA encryption?",
    "How does the garbage collector in modern runtimes track and reclaim unreachable memory?",
    "What are the differences between vector embeddings and traditional keyword search?"
]


def generate_random_question() -> str:
    """Generate a diverse, realistic question for the LLM."""
    if random.random() < 0.5:
        topic = random.choice(TOPICS)
        template = random.choice(TEMPLATES)
        return template.format(topic=topic)
    return random.choice(STANDALONE_QUESTIONS)


# ==============================================================================
# URL RESOLUTION HELPER
# ==============================================================================
def get_base_url(endpoint_str: str) -> str:
    """Normalize user-provided endpoint to a base URL without endpoint paths."""
    base = endpoint_str.strip().rstrip("/")
    # If the user passed a full sub-path, strip it to extract the base URL
    for suffix in ["/v1/chat/completions", "/v1/completions", "/v1/batches", "/v1/embeddings"]:
        if base.endswith(suffix):
            base = base[:-len(suffix)]
            break
    return base


# ==============================================================================
# REQUEST BUILDERS & DISPATCHERS
# ==============================================================================
def build_individual_payload(model_id: str, question: str) -> Dict[str, Any]:
    """Construct standard OpenAI-compatible chat completion payload."""
    return {
        "model": model_id,
        "messages": [
            {"role": "user", "content": question}
        ],
        "temperature": 0.7,
        "max_tokens": 512,
    }


def build_batch_payload(model_id: str, questions: List[str]) -> Dict[str, Any]:
    """Construct standard OpenAI-compatible batch payload with sub-requests."""
    batch_items = []
    batch_uuid = uuid.uuid4().hex[:6]
    for idx, q in enumerate(questions, start=1):
        batch_items.append({
            "custom_id": f"item-{batch_uuid}-{idx:03d}",
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": {
                "model": model_id,
                "messages": [
                    {"role": "user", "content": q}
                ],
                "temperature": 0.7,
                "max_tokens": 512,
            }
        })
    return {
        "endpoint": "/v1/chat/completions",
        "requests": batch_items,
    }


def send_single_request(
    session: requests.Session,
    request_plan: Dict[str, Any],
    base_url: str,
) -> Dict[str, Any]:
    """
    Send an individual or batch request to the gateway via HTTP POST using `requests`.
    Returns execution metadata and response summary.
    """
    req_index = request_plan["index"]
    req_type = request_plan["type"]
    payload = request_plan["payload"]
    num_items = request_plan["num_items"]
    sample_question = request_plan["sample_question"]

    if req_type == "batch":
        target_url = f"{base_url}/v1/batches"
    else:
        target_url = f"{base_url}/v1/chat/completions"

    start_time = time.perf_counter()
    headers = {"Content-Type": "application/json"}

    result = {
        "index": req_index,
        "type": req_type,
        "num_items": num_items,
        "url": target_url,
        "sample_question": sample_question,
        "success": False,
        "status_code": None,
        "response_id": None,
        "status_url": None,
        "elapsed_seconds": 0.0,
        "error": None,
    }

    try:
        response = session.post(
            target_url,
            json=payload,
            headers=headers,
            timeout=30,
        )
        elapsed = time.perf_counter() - start_time
        result["elapsed_seconds"] = elapsed
        result["status_code"] = response.status_code

        if response.status_code in [200, 201, 202]:
            result["success"] = True
            try:
                resp_json = response.json()
                result["response_id"] = (
                    resp_json.get("request_id")
                    or resp_json.get("batch_id")
                    or resp_json.get("id")
                )
                result["status_url"] = resp_json.get("status_url")
            except Exception:
                result["response_id"] = "non-json-response"

            logger.info(
                f"[Req #{req_index:03d}] SUCCESS ({response.status_code}) | Type: {req_type.upper():<10} | "
                f"Items: {num_items:2d} | Time: {elapsed * 1000:6.1f}ms | "
                f"ID: {result['response_id']} | Q: \"{sample_question[:45]}...\""
            )
        else:
            result["error"] = f"HTTP {response.status_code}: {response.text[:200]}"
            logger.warning(
                f"[Req #{req_index:03d}] FAILED  ({response.status_code}) | Type: {req_type.upper():<10} | "
                f"Error: {response.text[:120]}"
            )

    except requests.RequestException as exc:
        elapsed = time.perf_counter() - start_time
        result["elapsed_seconds"] = elapsed
        result["error"] = str(exc)
        logger.error(
            f"[Req #{req_index:03d}] ERROR   | Type: {req_type.upper():<10} | "
            f"Exception: {exc}"
        )

    return result


# ==============================================================================
# MAIN EXECUTION WORKFLOW
# ==============================================================================
def main():
    """Main workflow to prepare, schedule, and execute requests."""
    print("=" * 80)
    print("           ASYNC GATEWAY RANDOM REQUEST GENERATOR & RUNNER")
    print("=" * 80)

    # 1. Validation and parameter sanitation
    if NUM_REQUESTS <= 0:
        logger.error(f"NUM_REQUESTS must be greater than 0. Got: {NUM_REQUESTS}")
        return

    pct_batch = max(0.0, min(1.0, float(PCT_BATCH)))
    min_batch = max(1, int(BATCH_SIZE_MIN))
    max_batch = max(min_batch, int(BATCH_SIZE_MAX))
    max_parallel = max(1, int(MAX_PARALLEL))
    wait_time = max(0.0, float(WAIT_TIME))
    base_url = get_base_url(ENDPOINT)

    # 2. Compute counts
    num_batch_requests = int(round(NUM_REQUESTS * pct_batch))
    num_individual_requests = NUM_REQUESTS - num_batch_requests

    print(f"Target Gateway Endpoint : {base_url}")
    print(f"Target Model ID         : {MODEL_ID}")
    print(f"Total Requests to Send  : {NUM_REQUESTS}")
    print(f"Batch Request Ratio     : {pct_batch * 100:.1f}% ({num_batch_requests} batch, {num_individual_requests} individual)")
    print(f"Batch Size Range        : [{min_batch} .. {max_batch}] items per batch")
    print(f"Parallel Worker Limit   : {max_parallel} concurrent threads")
    print(f"Inter-Request Wait Time : {wait_time}s")
    print("=" * 80)

    # 3. Generate Request Plans
    request_plans: List[Dict[str, Any]] = []

    # Create batch request payloads
    for _ in range(num_batch_requests):
        batch_size = random.randint(min_batch, max_batch)
        questions = [generate_random_question() for _ in range(batch_size)]
        payload = build_batch_payload(MODEL_ID, questions)
        request_plans.append({
            "type": "batch",
            "num_items": batch_size,
            "sample_question": questions[0],
            "payload": payload,
        })

    # Create individual request payloads
    for _ in range(num_individual_requests):
        q = generate_random_question()
        payload = build_individual_payload(MODEL_ID, q)
        request_plans.append({
            "type": "individual",
            "num_items": 1,
            "sample_question": q,
            "payload": payload,
        })

    # 4. Randomly mix batch and individual requests
    random.shuffle(request_plans)

    # Assign sequential 1-based index to the mixed plan
    for idx, plan in enumerate(request_plans, start=1):
        plan["index"] = idx

    total_questions_generated = sum(p["num_items"] for p in request_plans)
    logger.info(
        f"Generated {len(request_plans)} requests containing a total of "
        f"{total_questions_generated} queries across mixed sequence."
    )
    logger.info("Starting parallel dispatch...")

    # 5. Dispatch in parallel using ThreadPoolExecutor
    results: List[Dict[str, Any]] = []
    overall_start_time = time.perf_counter()

    # Use a persistent requests.Session for connection reuse and efficiency
    with requests.Session() as session:
        with ThreadPoolExecutor(max_workers=max_parallel) as executor:
            futures = []

            for idx, plan in enumerate(request_plans):
                # Submit task to thread pool
                future = executor.submit(send_single_request, session, plan, base_url)
                futures.append(future)

                # Wait between launching requests if configured and not last item
                if wait_time > 0 and idx < len(request_plans) - 1:
                    time.sleep(wait_time)

            # Collect completed results
            for future in as_completed(futures):
                try:
                    res = future.result()
                    results.append(res)
                except Exception as e:
                    logger.error(f"Worker task raised unexpected exception: {e}")

    overall_elapsed = time.perf_counter() - overall_start_time

    # Sort results by original request index for clean reporting
    results.sort(key=lambda r: r["index"])

    # 6. Final Summary Report
    successful = [r for r in results if r["success"]]
    failed = [r for r in results if not r["success"]]
    batch_results = [r for r in results if r["type"] == "batch"]
    indiv_results = [r for r in results if r["type"] == "individual"]

    print("\n" + "=" * 80)
    print("                           EXECUTION SUMMARY")
    print("=" * 80)
    print(f"Total Requests Dispatched  : {len(results)}")
    print(f"  - Individual Requests    : {len(indiv_results)}")
    print(f"  - Batch Requests         : {len(batch_results)} (Total Batch Items: {sum(r['num_items'] for r in batch_results)})")
    print(f"Total Queries Generated    : {total_questions_generated}")
    print(f"Successful HTTP Submissions: {len(successful)} / {len(results)} ({len(successful)/len(results)*100:.1f}%)")
    print(f"Failed Submissions         : {len(failed)}")
    print(f"Total Wall-Clock Time      : {overall_elapsed:.2f} seconds")
    if successful:
        avg_latency = sum(r["elapsed_seconds"] for r in successful) / len(successful) * 1000
        print(f"Average Request Latency    : {avg_latency:.1f} ms")
    print("=" * 80)

    if successful:
        print("\nSample Generated Request / Batch IDs:")
        for r in successful[:10]:
            print(f"  - [Req #{r['index']:03d}] {r['type'].upper():<10} | ID: {r['response_id']}")
        if len(successful) > 10:
            print(f"  ... and {len(successful) - 10} more.")

    print("\nDone.")


if __name__ == "__main__":
    main()
