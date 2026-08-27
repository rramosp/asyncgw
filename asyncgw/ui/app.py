"""Web UI Dashboard Application for Asynchronous LLM Gateway."""

import json
import logging
from typing import Any, Dict, Optional
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
import uvicorn

from asyncgw.config import GatewaySettings, load_backends_config, load_policies_config
from asyncgw.gateway.api import create_app

logger = logging.getLogger(__name__)

DASHBOARD_HTML = r"""
<!DOCTYPE html>
<html lang="en" class="dark">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Async Gateway for LLM Inference</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script>
        tailwind.config = {
            darkMode: 'class',
            theme: {
                extend: {
                    colors: {
                        brand: {
                            50: '#eef2ff',
                            100: '#e0e7ff',
                            500: '#6366f1',
                            600: '#4f46e5',
                            700: '#4338ca',
                        },
                        gcp: {
                            blue: '#4285F4',
                            green: '#34A853',
                            yellow: '#FBBC05',
                            red: '#EA4335',
                            dark: '#1e293b',
                            card: '#0f172a'
                        }
                    }
                }
            }
        }
    </script>
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css" rel="stylesheet">
    <style>
        .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; }
        ::-webkit-scrollbar { width: 6px; height: 6px; }
        ::-webkit-scrollbar-track { background: #0f172a; }
        ::-webkit-scrollbar-thumb { background: #334155; border-radius: 3px; }
    </style>
</head>
<body class="bg-slate-950 text-slate-100 min-h-screen flex flex-col font-sans antialiased">
    <!-- Header -->
    <header class="border-b border-slate-800 bg-slate-900/80 backdrop-blur sticky top-0 z-50">
        <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 h-16 flex items-center justify-between">
            <div class="flex items-center space-x-3">
                <div class="w-10 h-10 rounded-xl bg-gradient-to-tr from-blue-600 to-indigo-600 flex items-center justify-center shadow-lg shadow-indigo-500/20">
                    <i class="fa-solid fa-microchip text-white text-lg"></i>
                </div>
                <div>
                    <h1 class="font-bold text-lg leading-tight tracking-tight">Async LLM Gateway</h1>
                    <p class="text-xs text-slate-400">GCP Provisioned Throughput & Queued Inference</p>
                </div>
            </div>
            <div class="flex items-center space-x-4">
                <span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-emerald-950 text-emerald-400 border border-emerald-800">
                    <span class="w-1.5 h-1.5 rounded-full bg-emerald-400 mr-1.5 animate-pulse"></span> System Active
                </span>
                <a href="/docs" target="_blank" class="text-xs font-semibold px-3 py-1.5 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-300 transition flex items-center gap-1.5">
                    <i class="fa-solid fa-book"></i> API Docs
                </a>
            </div>
        </div>
    </header>

    <!-- Main Navigation Tabs -->
    <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 pt-6 w-full">
        <div class="flex border-b border-slate-800 space-x-8 text-sm font-medium">
            <button onclick="switchTab('explorer')" id="tab-btn-explorer" class="pb-3 border-b-2 border-indigo-500 text-indigo-400 flex items-center gap-2">
                <i class="fa-solid fa-list-check"></i> Request Explorer
            </button>
            <button onclick="switchTab('submit')" id="tab-btn-submit" class="pb-3 border-b-2 border-transparent text-slate-400 hover:text-slate-200 flex items-center gap-2">
                <i class="fa-solid fa-paper-plane"></i> Submit Request
            </button>
            <button onclick="switchTab('backends')" id="tab-btn-backends" class="pb-3 border-b-2 border-transparent text-slate-400 hover:text-slate-200 flex items-center gap-2">
                <i class="fa-solid fa-server"></i> Backend Services
            </button>
            <button onclick="switchTab('policies')" id="tab-btn-policies" class="pb-3 border-b-2 border-transparent text-slate-400 hover:text-slate-200 flex items-center gap-2">
                <i class="fa-solid fa-route"></i> Routing Policies
            </button>
            <button onclick="switchTab('infra')" id="tab-btn-infra" class="pb-3 border-b-2 border-transparent text-slate-400 hover:text-slate-200 flex items-center gap-2">
                <i class="fa-solid fa-cloud"></i> GCP Infrastructure
            </button>
        </div>
    </div>

    <!-- Main Body Container -->
    <main class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6 flex-1 w-full">
        <!-- Metrics Banner -->
        <div class="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-4 mb-6">
            <div class="bg-slate-900 border border-slate-800/80 rounded-xl p-4">
                <div class="text-xs font-semibold text-slate-400 uppercase tracking-wider">Total Tracked</div>
                <div class="text-2xl font-bold text-slate-100 mt-1" id="stat-total">0</div>
            </div>
            <div class="bg-slate-900 border border-slate-800/80 rounded-xl p-4">
                <div class="text-xs font-semibold text-amber-400 uppercase tracking-wider">Pending (Queued)</div>
                <div class="text-2xl font-bold text-amber-400 mt-1" id="stat-pending">0</div>
            </div>
            <div class="bg-slate-900 border border-slate-800/80 rounded-xl p-4">
                <div class="text-xs font-semibold text-blue-400 uppercase tracking-wider">Processing</div>
                <div class="text-2xl font-bold text-blue-400 mt-1" id="stat-processing">0</div>
            </div>
            <div class="bg-slate-900 border border-slate-800/80 rounded-xl p-4">
                <div class="text-xs font-semibold text-emerald-400 uppercase tracking-wider">Completed</div>
                <div class="text-2xl font-bold text-emerald-400 mt-1" id="stat-completed">0</div>
            </div>
            <div class="bg-slate-900 border border-slate-800/80 rounded-xl p-4">
                <div class="text-xs font-semibold text-rose-400 uppercase tracking-wider">Failed</div>
                <div class="text-2xl font-bold text-rose-400 mt-1" id="stat-failed">0</div>
            </div>
            <div class="bg-slate-900 border border-slate-800/80 rounded-xl p-4">
                <div class="text-xs font-semibold text-orange-400 uppercase tracking-wider">Timed Out</div>
                <div class="text-2xl font-bold text-orange-400 mt-1" id="stat-timedout">0</div>
            </div>
        </div>

        <!-- TAB: REQUEST EXPLORER -->
        <section id="view-explorer" class="space-y-4">
            <div class="flex justify-between items-center bg-slate-900 p-4 rounded-xl border border-slate-800">
                <div class="flex items-center space-x-3">
                    <input type="text" id="search-input" placeholder="Search by Request ID or Model..." class="bg-slate-950 border border-slate-700 rounded-lg px-3 py-1.5 text-sm w-72 focus:outline-none focus:border-indigo-500">
                    <select id="filter-status" class="bg-slate-950 border border-slate-700 rounded-lg px-3 py-1.5 text-sm focus:outline-none focus:border-indigo-500" onchange="loadRequests()">
                        <option value="">All Statuses</option>
                        <option value="PENDING">PENDING</option>
                        <option value="PROCESSING">PROCESSING</option>
                        <option value="COMPLETED">COMPLETED</option>
                        <option value="FAILED">FAILED</option>
                        <option value="TIMED_OUT">TIMED_OUT</option>
                    </select>
                </div>
                <button onclick="loadRequests()" class="px-3 py-1.5 rounded-lg bg-indigo-600 hover:bg-indigo-500 text-white text-sm font-semibold flex items-center gap-2">
                    <i class="fa-solid fa-rotate"></i> Refresh
                </button>
            </div>

            <div class="bg-slate-900 rounded-xl border border-slate-800 overflow-hidden shadow">
                <table class="w-full text-left text-sm">
                    <thead class="bg-slate-950 text-slate-400 font-semibold border-b border-slate-800">
                        <tr>
                            <th class="px-4 py-3">Request ID</th>
                            <th class="px-4 py-3">Type</th>
                            <th class="px-4 py-3">Model</th>
                            <th class="px-4 py-3">Status</th>
                            <th class="px-4 py-3">Backend Served</th>
                            <th class="px-4 py-3">Latency</th>
                            <th class="px-4 py-3">Submitted At</th>
                            <th class="px-4 py-3 text-right">Actions</th>
                        </tr>
                    </thead>
                    <tbody id="requests-tbody" class="divide-y divide-slate-800/60">
                        <tr>
                            <td colspan="8" class="text-center py-8 text-slate-500">Loading requests...</td>
                        </tr>
                    </tbody>
                </table>
            </div>
        </section>

        <!-- TAB: SUBMIT REQUEST -->
        <section id="view-submit" class="hidden space-y-6">
            <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
                <!-- Request Form -->
                <div class="bg-slate-900 border border-slate-800 rounded-xl p-6 space-y-4">
                    <h2 class="text-lg font-bold flex items-center gap-2">
                        <i class="fa-solid fa-terminal text-indigo-400"></i> New Inference Request
                    </h2>
                    
                    <div>
                        <label class="block text-xs font-semibold text-slate-400 mb-1">Request Mode</label>
                        <div class="flex gap-4">
                            <label class="flex items-center gap-2 text-sm">
                                <input type="radio" name="req-mode" value="single" checked onchange="toggleBatchMode()"> Single Online / Queued
                            </label>
                            <label class="flex items-center gap-2 text-sm">
                                <input type="radio" name="req-mode" value="batch" onchange="toggleBatchMode()"> Batch (Bulk decomposing)
                            </label>
                        </div>
                    </div>

                    <div class="grid grid-cols-2 gap-4">
                        <div>
                            <label class="block text-xs font-semibold text-slate-400 mb-1">Model Name / Backend Route</label>
                            <input type="text" id="submit-model" list="model-suggestions" value="gemini-flex" class="w-full bg-slate-950 border border-slate-700 rounded-lg px-3 py-2 text-sm focus:outline-none focus:border-indigo-500">
                            <datalist id="model-suggestions">
                                <option value="gemini-flex">gemini-flex (Auto-Decomposed into parallel sub-requests)</option>
                                <option value="mock-model-v1">mock-model-v1 (Internal Mock - Auto-Decomposed)</option>
                                <option value="gemini-2.0-flash">gemini-2.0-flash (GCP Provisioned - Native bulk batch)</option>
                                <option value="gpt-4o">gpt-4o (OpenAI Direct)</option>
                            </datalist>
                            <p class="text-[11px] text-slate-500 mt-1">Tip: Use <b>gemini-flex</b> or <b>mock-model-v1</b> to test parallel sub-request decomposition.</p>
                        </div>
                        <div>
                            <label class="block text-xs font-semibold text-slate-400 mb-1">Max Wait Time (Seconds)</label>
                            <input type="number" id="submit-maxwait" value="120" class="w-full bg-slate-950 border border-slate-700 rounded-lg px-3 py-2 text-sm focus:outline-none focus:border-indigo-500">
                        </div>
                    </div>

                    <div id="single-prompt-container">
                        <label class="block text-xs font-semibold text-slate-400 mb-1">Prompt / User Message</label>
                        <textarea id="submit-prompt" rows="5" class="w-full bg-slate-950 border border-slate-700 rounded-lg p-3 text-sm font-mono focus:outline-none focus:border-indigo-500" placeholder="Type user prompt here...">Explain how GCP Provisioned Throughput enables cost-effective asynchronous LLM inference.</textarea>
                    </div>

                    <div id="batch-prompt-container" class="hidden">
                        <label class="block text-xs font-semibold text-slate-400 mb-1">Batch Items (JSON array)</label>
                        <textarea id="submit-batch-json" rows="6" class="w-full bg-slate-950 border border-slate-700 rounded-lg p-3 text-xs font-mono focus:outline-none focus:border-indigo-500">[
  {"custom_id": "batch-1", "method": "POST", "url": "/v1/chat/completions", "body": {"model": "gemini-2.0-flash", "messages": [{"role": "user", "content": "What is GCP Cloud Run?"}]}},
  {"custom_id": "batch-2", "method": "POST", "url": "/v1/chat/completions", "body": {"model": "gemini-2.0-flash", "messages": [{"role": "user", "content": "What is GCP BigQuery partitioning?"}]}},
  {"custom_id": "batch-3", "method": "POST", "url": "/v1/chat/completions", "body": {"model": "gemini-2.0-flash", "messages": [{"role": "user", "content": "What is Google Cloud Storage lifecycle rule?"}]}}
]</textarea>
                    </div>

                    <button onclick="submitInferenceRequest()" class="w-full py-2.5 rounded-lg bg-gradient-to-r from-blue-600 to-indigo-600 hover:from-blue-500 hover:to-indigo-500 text-white font-bold text-sm shadow-md transition flex items-center justify-center gap-2">
                        <i class="fa-solid fa-bolt"></i> Enqueue Request
                    </button>
                </div>

                <!-- Real-time Poller & Inspector -->
                <div class="bg-slate-900 border border-slate-800 rounded-xl p-6 space-y-4">
                    <div class="flex items-center justify-between">
                        <h2 class="text-lg font-bold flex items-center gap-2">
                            <i class="fa-solid fa-clock-rotate-left text-blue-400"></i> Asynchronous Execution Monitor
                        </h2>
                        <span id="active-poll-badge" class="hidden px-2.5 py-0.5 rounded-full text-xs font-semibold bg-indigo-950 text-indigo-400 border border-indigo-800 animate-pulse">
                            Polling Active
                        </span>
                    </div>

                    <div id="monitor-details" class="bg-slate-950 rounded-xl p-4 border border-slate-800 text-sm space-y-2">
                        <p class="text-slate-500 italic text-center py-6">No active submission. Submit a request to watch async lifecycle.</p>
                    </div>

                    <div>
                        <div class="flex justify-between items-center mb-1">
                            <label class="text-xs font-semibold text-slate-400">Response Payload / GCS Output</label>
                            <span id="response-time-tag" class="text-xs text-slate-500"></span>
                        </div>
                        <pre id="response-viewer" class="bg-slate-950 border border-slate-800 rounded-xl p-4 text-xs font-mono text-emerald-400 max-h-72 overflow-y-auto">// Response JSON will render here...</pre>
                    </div>
                </div>
            </div>
        </section>

        <!-- TAB: BACKENDS -->
        <section id="view-backends" class="hidden space-y-4">
            <div class="flex justify-between items-center">
                <div>
                    <h2 class="text-lg font-bold">Registered LLM Backend Services</h2>
                    <p class="text-xs text-slate-400">Configured in backends.yaml. Health probes and availability criteria dictate queue dispatching.</p>
                </div>
                <button onclick="probeAllBackends()" class="px-3 py-1.5 rounded-lg bg-indigo-600 hover:bg-indigo-500 text-white text-sm font-semibold flex items-center gap-2">
                    <i class="fa-solid fa-stethoscope"></i> Probe All Endpoints
                </button>
            </div>
            <div id="backends-grid" class="grid grid-cols-1 md:grid-cols-2 gap-4">
                <!-- Dynamically filled -->
            </div>
        </section>

        <!-- TAB: POLICIES -->
        <section id="view-policies" class="hidden space-y-6">
            <div>
                <h2 class="text-lg font-bold">Routing & Failover Strategies</h2>
                <p class="text-xs text-slate-400">Rules controlling backend preference orders, failover retry sequences, and token thresholds.</p>
            </div>
            <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
                <div class="bg-slate-900 border border-slate-800 rounded-xl p-6 space-y-4">
                    <h3 class="font-bold text-indigo-400 flex items-center gap-2">
                        <i class="fa-solid fa-list-ol"></i> Configured Strategies
                    </h3>
                    <div id="strategies-list" class="space-y-3">
                        <!-- Filled by JS -->
                    </div>
                </div>
                <div class="bg-slate-900 border border-slate-800 rounded-xl p-6 space-y-4">
                    <h3 class="font-bold text-amber-400 flex items-center gap-2">
                        <i class="fa-solid fa-filter"></i> Content & Size Rules
                    </h3>
                    <div id="content-rules-list" class="space-y-3">
                        <!-- Filled by JS -->
                    </div>
                </div>
            </div>
        </section>

        <!-- TAB: INFRASTRUCTURE -->
        <section id="view-infra" class="hidden space-y-8">
            <!-- Top Infrastructure Header & Context Banner -->
            <div class="bg-gradient-to-r from-slate-900 via-slate-900 to-indigo-950/40 border border-slate-800 rounded-2xl p-6 shadow-xl">
                <div class="flex flex-col lg:flex-row lg:items-center justify-between gap-4">
                    <div>
                        <div class="flex items-center gap-2.5">
                            <div class="w-9 h-9 rounded-lg bg-indigo-600/20 border border-indigo-500/30 flex items-center justify-center text-indigo-400">
                                <i class="fa-solid fa-cloud text-lg"></i>
                            </div>
                            <div>
                                <h2 class="text-lg font-bold text-slate-100">GCP Infrastructure & Architecture Topology</h2>
                                <p class="text-xs text-slate-400">Live cloud footprint across Artifact Registry container images, Cloud Run workers & triggers, Pub/Sub queuing, and storage.</p>
                            </div>
                        </div>
                    </div>
                    <div class="flex flex-wrap items-center gap-3 text-xs">
                        <div class="bg-slate-950/90 border border-slate-800 px-3 py-1.5 rounded-lg flex items-center gap-2">
                            <span class="text-slate-400">Project:</span>
                            <span id="infra-project-id" class="mono font-semibold text-indigo-300">asyncgw-demo-project</span>
                        </div>
                        <div class="bg-slate-950/90 border border-slate-800 px-3 py-1.5 rounded-lg flex items-center gap-2">
                            <span class="text-slate-400">Region:</span>
                            <span id="infra-region" class="mono font-semibold text-indigo-300">us-central1</span>
                        </div>
                        <div id="infra-env-badge" class="px-2.5 py-1.5 rounded-lg font-semibold bg-indigo-950 text-indigo-400 border border-indigo-800">
                            LOCAL / MOCK MODE
                        </div>
                    </div>
                </div>
            </div>

            <!-- SECTION 1: ARTIFACT REGISTRY & CONTAINERS -->
            <div class="space-y-4">
                <div class="flex flex-col sm:flex-row sm:items-center justify-between gap-2 border-b border-slate-800 pb-3">
                    <div>
                        <h3 class="text-base font-bold text-slate-100 flex items-center gap-2">
                            <i class="fa-solid fa-boxes-stacked text-indigo-400"></i> Artifact Registry Container Images
                        </h3>
                        <p class="text-xs text-slate-400">OCI Docker container images built and published to Google Cloud Artifact Registry for gateway and worker execution.</p>
                    </div>
                    <div class="flex items-center gap-2">
                        <span class="text-xs px-2.5 py-1 rounded bg-slate-900 border border-slate-800 mono text-slate-300 flex items-center gap-1.5">
                            <i class="fa-brands fa-docker text-blue-400"></i> Repo: <span id="infra-ar-repo-name" class="text-indigo-400 font-semibold">asyncgw-docker</span>
                        </span>
                        <a id="infra-ar-console-link" href="#" target="_blank" rel="noopener noreferrer" class="hidden text-xs font-semibold px-3 py-1 rounded bg-indigo-600 hover:bg-indigo-500 text-white transition flex items-center gap-1.5">
                            Console <i class="fa-solid fa-arrow-up-right-from-square text-[10px]"></i>
                        </a>
                    </div>
                </div>

                <div class="grid grid-cols-1 lg:grid-cols-2 gap-5">
                    <!-- Container 1: Gateway Image -->
                    <div class="bg-slate-900 border border-slate-800 rounded-xl p-5 space-y-4 hover:border-slate-700 transition shadow-lg">
                        <div class="flex justify-between items-start">
                            <div class="flex items-center gap-3">
                                <div class="w-10 h-10 rounded-xl bg-gradient-to-tr from-cyan-600 to-blue-600 flex items-center justify-center text-white shadow-md">
                                    <i class="fa-solid fa-server text-base"></i>
                                </div>
                                <div>
                                    <h4 class="font-bold text-sm text-slate-100">asyncgw-gateway</h4>
                                    <span class="text-[11px] text-slate-400">FastAPI Gateway & UI Dashboard Image</span>
                                </div>
                            </div>
                            <span class="px-2 py-0.5 rounded text-[11px] font-bold bg-cyan-950 text-cyan-400 border border-cyan-800 mono">:latest</span>
                        </div>

                        <div>
                            <div class="text-[11px] font-semibold text-slate-400 mb-1 flex items-center justify-between">
                                <span>Artifact Registry URI</span>
                                <span class="text-[10px] text-slate-500">Docker / OCI Format</span>
                            </div>
                            <div id="infra-gw-img-uri" class="mono text-[11px] text-cyan-300 bg-slate-950 p-2.5 rounded-lg border border-slate-800/80 break-all select-all">
                                us-central1-docker.pkg.dev/asyncgw-demo-project/asyncgw-docker/asyncgw-gateway:latest
                            </div>
                        </div>

                        <div class="grid grid-cols-2 gap-3 text-xs bg-slate-950/60 p-3 rounded-lg border border-slate-800/60">
                            <div>
                                <span class="text-slate-500 block text-[11px]">Build Dockerfile:</span>
                                <span class="mono font-semibold text-slate-300">Dockerfile.gateway</span>
                            </div>
                            <div>
                                <span class="text-slate-500 block text-[11px]">Base Image:</span>
                                <span class="mono font-semibold text-slate-300">python:3.11-slim</span>
                            </div>
                            <div>
                                <span class="text-slate-500 block text-[11px]">Exposed Ports:</span>
                                <span class="mono font-semibold text-emerald-400">8080 (HTTP), 8000</span>
                            </div>
                            <div>
                                <span class="text-slate-500 block text-[11px]">Entrypoint / CMD:</span>
                                <span class="mono font-semibold text-indigo-400">gateway</span>
                            </div>
                        </div>

                        <div class="text-xs text-slate-400 border-t border-slate-800/80 pt-3 flex items-start gap-2">
                            <i class="fa-solid fa-circle-info text-cyan-400 mt-0.5 text-[11px]"></i>
                            <span>Deploys the public Cloud Run service providing OpenAI-compatible REST API endpoints, OpenAPI Swagger documentation, Web UI dashboard, and Pub/Sub producer.</span>
                        </div>
                    </div>

                    <!-- Container 2: Worker Image -->
                    <div class="bg-slate-900 border border-slate-800 rounded-xl p-5 space-y-4 hover:border-slate-700 transition shadow-lg">
                        <div class="flex justify-between items-start">
                            <div class="flex items-center gap-3">
                                <div class="w-10 h-10 rounded-xl bg-gradient-to-tr from-amber-600 to-orange-600 flex items-center justify-center text-white shadow-md">
                                    <i class="fa-solid fa-gears text-base"></i>
                                </div>
                                <div>
                                    <h4 class="font-bold text-sm text-slate-100">asyncgw-worker</h4>
                                    <span class="text-[11px] text-slate-400">Inference Engine & Batch Worker Image</span>
                                </div>
                            </div>
                            <span class="px-2 py-0.5 rounded text-[11px] font-bold bg-amber-950 text-amber-400 border border-amber-800 mono">:latest</span>
                        </div>

                        <div>
                            <div class="text-[11px] font-semibold text-slate-400 mb-1 flex items-center justify-between">
                                <span>Artifact Registry URI</span>
                                <span class="text-[10px] text-slate-500">Docker / OCI Format</span>
                            </div>
                            <div id="infra-wk-img-uri" class="mono text-[11px] text-amber-300 bg-slate-950 p-2.5 rounded-lg border border-slate-800/80 break-all select-all">
                                us-central1-docker.pkg.dev/asyncgw-demo-project/asyncgw-docker/asyncgw-worker:latest
                            </div>
                        </div>

                        <div class="grid grid-cols-2 gap-3 text-xs bg-slate-950/60 p-3 rounded-lg border border-slate-800/60">
                            <div>
                                <span class="text-slate-500 block text-[11px]">Build Dockerfile:</span>
                                <span class="mono font-semibold text-slate-300">Dockerfile.worker</span>
                            </div>
                            <div>
                                <span class="text-slate-500 block text-[11px]">Base Image:</span>
                                <span class="mono font-semibold text-slate-300">python:3.11-slim</span>
                            </div>
                            <div>
                                <span class="text-slate-500 block text-[11px]">Health Probe Port:</span>
                                <span class="mono font-semibold text-emerald-400">8080 (/healthz)</span>
                            </div>
                            <div>
                                <span class="text-slate-500 block text-[11px]">Supported Modes:</span>
                                <span class="mono font-semibold text-indigo-400">worker-all | primary | batch</span>
                            </div>
                        </div>

                        <div class="text-xs text-slate-400 border-t border-slate-800/80 pt-3 flex items-start gap-2">
                            <i class="fa-solid fa-circle-info text-amber-400 mt-0.5 text-[11px]"></i>
                            <span>Deploys continuous worker fleets and Cloud Run jobs for Pub/Sub stream consumption, Gemini Provisioned Throughput/Flex inference, batch splitting, and GCS reassembly.</span>
                        </div>
                    </div>
                </div>
            </div>

            <!-- SECTION 2: CLOUD RUN WORKERS & TRIGGER CONFIGURATION -->
            <div class="space-y-4">
                <div class="border-b border-slate-800 pb-3">
                    <h3 class="text-base font-bold text-slate-100 flex items-center gap-2">
                        <i class="fa-solid fa-bolt text-amber-400"></i> Cloud Run Workers & Trigger Architecture
                    </h3>
                    <p class="text-xs text-slate-400">Detailed trigger mechanisms, event sources, scaling parameters, and runtime configuration for worker fleet services and jobs.</p>
                </div>

                <div class="grid grid-cols-1 lg:grid-cols-2 gap-5">
                    <!-- Worker 1: Continuous Fleet Service -->
                    <div class="bg-slate-900 border border-slate-800 rounded-xl p-5 space-y-4 shadow-lg">
                        <div class="flex justify-between items-start">
                            <div>
                                <div class="flex items-center gap-2">
                                    <h4 class="font-bold text-sm text-slate-100">asyncgw-worker-fleet</h4>
                                    <span class="px-2 py-0.5 rounded text-[10px] font-bold bg-emerald-950 text-emerald-400 border border-emerald-800">Service</span>
                                </div>
                                <p class="text-xs text-slate-400 mt-0.5">Continuous auto-scaling worker fleet for online requests and decomposed batch tasks</p>
                            </div>
                            <span class="px-2 py-0.5 rounded text-[11px] font-bold bg-indigo-950 text-indigo-300 border border-indigo-800 mono">1 &rarr; 50 instances</span>
                        </div>

                        <!-- Trigger Model Box -->
                        <div class="bg-slate-950 p-3.5 rounded-xl border border-emerald-900/40 space-y-2.5">
                            <div class="flex items-center justify-between">
                                <span class="text-xs font-bold text-emerald-300 flex items-center gap-1.5">
                                    <i class="fa-solid fa-satellite-dish text-emerald-400"></i> Trigger: Continuous Pub/Sub Streaming Pull
                                </span>
                                <span class="text-[10px] font-semibold px-2 py-0.5 rounded bg-emerald-950/80 border border-emerald-800 text-emerald-400">Always Active</span>
                            </div>
                            <p class="text-xs text-slate-300 leading-relaxed">
                                Continuously streams and pulls messages directly from Pub/Sub subscriptions <span class="mono text-indigo-300 font-semibold">asyncgw-requests-sub</span> and <span class="mono text-indigo-300 font-semibold">asyncgw-batch-items-sub</span> with zero polling idle delay.
                            </p>
                            <div class="text-[11px] text-slate-400 bg-slate-900/80 p-2 rounded border border-slate-800/80 space-y-1">
                                <div class="flex justify-between"><span class="text-slate-500">Trigger Flow:</span> <span class="text-slate-300 font-medium">Pub/Sub publish &rarr; Streaming pull dispatch &rarr; LLM backend call &rarr; GCS & BQ update</span></div>
                                <div class="flex justify-between"><span class="text-slate-500">CPU Allocation:</span> <span class="mono text-emerald-400 font-semibold">cpu_idle = false (Always Allocated)</span></div>
                            </div>
                        </div>

                        <!-- Specs Table -->
                        <div class="grid grid-cols-2 gap-2.5 text-xs">
                            <div class="bg-slate-950/70 p-2.5 rounded-lg border border-slate-800/80">
                                <span class="text-slate-500 text-[11px] block">Compute Sizing</span>
                                <span class="font-semibold text-slate-200 mono">4 vCPU, 4 GiB RAM</span>
                            </div>
                            <div class="bg-slate-950/70 p-2.5 rounded-lg border border-slate-800/80">
                                <span class="text-slate-500 text-[11px] block">Ingress Security</span>
                                <span class="font-semibold text-slate-200 mono">Internal Only (VPC)</span>
                            </div>
                            <div class="bg-slate-950/70 p-2.5 rounded-lg border border-slate-800/80">
                                <span class="text-slate-500 text-[11px] block">Execution Command</span>
                                <span class="font-semibold text-indigo-400 mono">python -m asyncgw.main worker-all</span>
                            </div>
                            <div class="bg-slate-950/70 p-2.5 rounded-lg border border-slate-800/80">
                                <span class="text-slate-500 text-[11px] block">Service Account</span>
                                <span id="infra-wk-fleet-sa" class="font-semibold text-slate-300 mono text-[11px] truncate block">asyncgw-worker-sa@...</span>
                            </div>
                        </div>
                    </div>

                    <!-- Worker 2: Scheduled / Burst Primary Job -->
                    <div class="bg-slate-900 border border-slate-800 rounded-xl p-5 space-y-4 shadow-lg">
                        <div class="flex justify-between items-start">
                            <div>
                                <div class="flex items-center gap-2">
                                    <h4 class="font-bold text-sm text-slate-100">asyncgw-job-primary</h4>
                                    <span class="px-2 py-0.5 rounded text-[10px] font-bold bg-blue-950 text-blue-400 border border-blue-800">Job</span>
                                </div>
                                <p class="text-xs text-slate-400 mt-0.5">Scheduled or event-driven burst worker job for primary queue backlog draining</p>
                            </div>
                            <span class="px-2 py-0.5 rounded text-[11px] font-bold bg-blue-950 text-blue-300 border border-blue-800 mono">5 Tasks Parallel</span>
                        </div>

                        <!-- Trigger Model Box -->
                        <div class="bg-slate-950 p-3.5 rounded-xl border border-blue-900/40 space-y-2.5">
                            <div class="flex items-center justify-between">
                                <span class="text-xs font-bold text-blue-300 flex items-center gap-1.5">
                                    <i class="fa-solid fa-clock text-blue-400"></i> Trigger: Cloud Scheduler / Eventarc / Manual CLI
                                </span>
                                <span class="text-[10px] font-semibold px-2 py-0.5 rounded bg-blue-950/80 border border-blue-800 text-blue-400">On-Demand</span>
                            </div>
                            <p class="text-xs text-slate-300 leading-relaxed">
                                Triggered on cron schedule (e.g. <span class="mono text-indigo-300">*/5 * * * *</span>) via Cloud Scheduler, Eventarc queue threshold alert, or via <span class="mono text-indigo-300">gcloud run jobs execute asyncgw-job-primary</span>.
                            </p>
                            <div class="text-[11px] text-slate-400 bg-slate-900/80 p-2 rounded border border-slate-800/80 space-y-1">
                                <div class="flex justify-between"><span class="text-slate-500">Trigger Flow:</span> <span class="text-slate-300 font-medium">Trigger signal &rarr; Spawns 5 parallel container tasks &rarr; Drains primary topic &rarr; Exits</span></div>
                                <div class="flex justify-between"><span class="text-slate-500">Target Topic:</span> <span class="mono text-blue-300 font-semibold">asyncgw-requests-topic</span></div>
                            </div>
                        </div>

                        <!-- Specs Table -->
                        <div class="grid grid-cols-2 gap-2.5 text-xs">
                            <div class="bg-slate-950/70 p-2.5 rounded-lg border border-slate-800/80">
                                <span class="text-slate-500 text-[11px] block">Task Sizing</span>
                                <span class="font-semibold text-slate-200 mono">2 vCPU, 2 GiB RAM / task</span>
                            </div>
                            <div class="bg-slate-950/70 p-2.5 rounded-lg border border-slate-800/80">
                                <span class="text-slate-500 text-[11px] block">Task Concurrency</span>
                                <span class="font-semibold text-slate-200 mono">task_count = 5</span>
                            </div>
                            <div class="bg-slate-950/70 p-2.5 rounded-lg border border-slate-800/80">
                                <span class="text-slate-500 text-[11px] block">Execution Command</span>
                                <span class="font-semibold text-indigo-400 mono">python -m asyncgw.main worker-primary</span>
                            </div>
                            <div class="bg-slate-950/70 p-2.5 rounded-lg border border-slate-800/80">
                                <span class="text-slate-500 text-[11px] block">Service Account</span>
                                <span id="infra-wk-pri-sa" class="font-semibold text-slate-300 mono text-[11px] truncate block">asyncgw-worker-sa@...</span>
                            </div>
                        </div>
                    </div>

                    <!-- Worker 3: Batch Decomposed Parallel Job -->
                    <div class="bg-slate-900 border border-slate-800 rounded-xl p-5 space-y-4 shadow-lg">
                        <div class="flex justify-between items-start">
                            <div>
                                <div class="flex items-center gap-2">
                                    <h4 class="font-bold text-sm text-slate-100">asyncgw-job-batch</h4>
                                    <span class="px-2 py-0.5 rounded text-[10px] font-bold bg-indigo-950 text-indigo-400 border border-indigo-800">Job</span>
                                </div>
                                <p class="text-xs text-slate-400 mt-0.5">Parallel decomposed batch item worker job with automated reassembly trigger</p>
                            </div>
                            <span class="px-2 py-0.5 rounded text-[11px] font-bold bg-indigo-950 text-indigo-300 border border-indigo-800 mono">10 Tasks Parallel</span>
                        </div>

                        <!-- Trigger Model Box -->
                        <div class="bg-slate-950 p-3.5 rounded-xl border border-indigo-900/40 space-y-2.5">
                            <div class="flex items-center justify-between">
                                <span class="text-xs font-bold text-indigo-300 flex items-center gap-1.5">
                                    <i class="fa-solid fa-layer-group text-indigo-400"></i> Trigger: Batch Enqueue Event / Cloud Scheduler / Manual
                                </span>
                                <span class="text-[10px] font-semibold px-2 py-0.5 rounded bg-indigo-950/80 border border-indigo-800 text-indigo-400">Batch Trigger</span>
                            </div>
                            <p class="text-xs text-slate-300 leading-relaxed">
                                Triggered upon batch submission events via Eventarc notification, scheduled cron intervals, or <span class="mono text-indigo-300">gcloud run jobs execute asyncgw-job-batch</span>.
                            </p>
                            <div class="text-[11px] text-slate-400 bg-slate-900/80 p-2 rounded border border-slate-800/80 space-y-1">
                                <div class="flex justify-between"><span class="text-slate-500">Trigger Flow:</span> <span class="text-slate-300 font-medium">Batch decomposed &rarr; 10 parallel tasks process items &rarr; Reassembler triggers on completion</span></div>
                                <div class="flex justify-between"><span class="text-slate-500">Target Topic:</span> <span class="mono text-indigo-300 font-semibold">asyncgw-batch-items-topic</span></div>
                            </div>
                        </div>

                        <!-- Specs Table -->
                        <div class="grid grid-cols-2 gap-2.5 text-xs">
                            <div class="bg-slate-950/70 p-2.5 rounded-lg border border-slate-800/80">
                                <span class="text-slate-500 text-[11px] block">Task Sizing</span>
                                <span class="font-semibold text-slate-200 mono">2 vCPU, 2 GiB RAM / task</span>
                            </div>
                            <div class="bg-slate-950/70 p-2.5 rounded-lg border border-slate-800/80">
                                <span class="text-slate-500 text-[11px] block">Task Concurrency</span>
                                <span class="font-semibold text-slate-200 mono">task_count = 10</span>
                            </div>
                            <div class="bg-slate-950/70 p-2.5 rounded-lg border border-slate-800/80">
                                <span class="text-slate-500 text-[11px] block">Execution Command</span>
                                <span class="font-semibold text-indigo-400 mono">python -m asyncgw.main worker-batch</span>
                            </div>
                            <div class="bg-slate-950/70 p-2.5 rounded-lg border border-slate-800/80">
                                <span class="text-slate-500 text-[11px] block">Service Account</span>
                                <span id="infra-wk-batch-sa" class="font-semibold text-slate-300 mono text-[11px] truncate block">asyncgw-worker-sa@...</span>
                            </div>
                        </div>
                    </div>

                    <!-- Worker 4: API Gateway & UI Service -->
                    <div class="bg-slate-900 border border-slate-800 rounded-xl p-5 space-y-4 shadow-lg">
                        <div class="flex justify-between items-start">
                            <div>
                                <div class="flex items-center gap-2">
                                    <h4 class="font-bold text-sm text-slate-100">asyncgw-gateway</h4>
                                    <span class="px-2 py-0.5 rounded text-[10px] font-bold bg-cyan-950 text-cyan-400 border border-cyan-800">Service</span>
                                </div>
                                <p class="text-xs text-slate-400 mt-0.5">Public synchronous HTTP REST API Gateway, Swagger docs, and Web UI Dashboard</p>
                            </div>
                            <span class="px-2 py-0.5 rounded text-[11px] font-bold bg-cyan-950 text-cyan-300 border border-cyan-800 mono">1 &rarr; 20 instances</span>
                        </div>

                        <!-- Trigger Model Box -->
                        <div class="bg-slate-950 p-3.5 rounded-xl border border-cyan-900/40 space-y-2.5">
                            <div class="flex items-center justify-between">
                                <span class="text-xs font-bold text-cyan-300 flex items-center gap-1.5">
                                    <i class="fa-solid fa-globe text-cyan-400"></i> Trigger: Synchronous HTTP / HTTPS REST & UI
                                </span>
                                <span class="text-[10px] font-semibold px-2 py-0.5 rounded bg-cyan-950/80 border border-cyan-800 text-cyan-400">HTTP Concurrency</span>
                            </div>
                            <p class="text-xs text-slate-300 leading-relaxed">
                                Triggered on incoming client REST calls (<span class="mono text-cyan-300">POST /v1/chat/completions</span>, <span class="mono text-cyan-300">POST /v1/batches</span>) and browser Web UI Dashboard visits.
                            </p>
                            <div class="text-[11px] text-slate-400 bg-slate-900/80 p-2 rounded border border-slate-800/80 space-y-1">
                                <div class="flex justify-between"><span class="text-slate-500">Trigger Flow:</span> <span class="text-slate-300 font-medium">Client HTTP POST &rarr; Writes BQ PENDING state &rarr; Publishes to Pub/Sub &rarr; 202 Accepted</span></div>
                                <div class="flex justify-between"><span class="text-slate-500">Ingress Traffic:</span> <span class="mono text-cyan-400 font-semibold">INGRESS_TRAFFIC_ALL (Public)</span></div>
                            </div>
                        </div>

                        <!-- Specs Table -->
                        <div class="grid grid-cols-2 gap-2.5 text-xs">
                            <div class="bg-slate-950/70 p-2.5 rounded-lg border border-slate-800/80">
                                <span class="text-slate-500 text-[11px] block">Compute Sizing</span>
                                <span class="font-semibold text-slate-200 mono">2 vCPU, 2 GiB RAM</span>
                            </div>
                            <div class="bg-slate-950/70 p-2.5 rounded-lg border border-slate-800/80">
                                <span class="text-slate-500 text-[11px] block">Listening Port</span>
                                <span class="font-semibold text-slate-200 mono">Port 8080</span>
                            </div>
                            <div class="bg-slate-950/70 p-2.5 rounded-lg border border-slate-800/80">
                                <span class="text-slate-500 text-[11px] block">Execution Command</span>
                                <span class="font-semibold text-indigo-400 mono">python -m asyncgw.main gateway</span>
                            </div>
                            <div class="bg-slate-950/70 p-2.5 rounded-lg border border-slate-800/80">
                                <span class="text-slate-500 text-[11px] block">Service Account</span>
                                <span id="infra-gw-service-sa" class="font-semibold text-slate-300 mono text-[11px] truncate block">asyncgw-gateway-sa@...</span>
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            <!-- SECTION 3: STORAGE, QUEUING & PERSISTENCE RESOURCES -->
            <div class="space-y-4">
                <div class="border-b border-slate-800 pb-3">
                    <h3 class="text-base font-bold text-slate-100 flex items-center gap-2">
                        <i class="fa-solid fa-database text-emerald-400"></i> Data Persistence, Queuing & Tracking Resources
                    </h3>
                    <p class="text-xs text-slate-400">Google Cloud Pub/Sub message queues, BigQuery time-partitioned metrics, and Cloud Storage response payloads.</p>
                </div>

                <div class="grid grid-cols-1 md:grid-cols-3 gap-6">
                    <!-- Pub/Sub Card -->
                    <div class="bg-slate-900 border border-slate-800 rounded-xl p-6 space-y-4 shadow-lg">
                        <div class="text-blue-400 text-base font-bold flex items-center justify-between border-b border-slate-800 pb-2">
                            <div class="flex items-center gap-2">
                                <i class="fa-solid fa-envelope"></i> Google Cloud Pub/Sub
                            </div>
                            <span class="text-[10px] font-semibold px-2 py-0.5 rounded bg-blue-950 border border-blue-800 text-blue-300">Queue Layer</span>
                        </div>
                        <ul class="text-xs space-y-2.5 text-slate-300">
                            <li class="flex justify-between border-b border-slate-800/70 pb-1.5">
                                <span class="text-slate-400">Primary Topic:</span>
                                <span id="infra-pubsub-req" class="mono font-semibold text-indigo-300">asyncgw-requests-topic</span>
                            </li>
                            <li class="flex justify-between border-b border-slate-800/70 pb-1.5">
                                <span class="text-slate-400">Primary Subscription:</span>
                                <span id="infra-pubsub-req-sub" class="mono font-semibold text-emerald-400">asyncgw-requests-sub</span>
                            </li>
                            <li class="flex justify-between border-b border-slate-800/70 pb-1.5">
                                <span class="text-slate-400">Batch Items Topic:</span>
                                <span id="infra-pubsub-batch" class="mono font-semibold text-indigo-300">asyncgw-batch-items-topic</span>
                            </li>
                            <li class="flex justify-between border-b border-slate-800/70 pb-1.5">
                                <span class="text-slate-400">Batch Subscription:</span>
                                <span id="infra-pubsub-batch-sub" class="mono font-semibold text-emerald-400">asyncgw-batch-items-sub</span>
                            </li>
                            <li class="flex justify-between border-b border-slate-800/70 pb-1.5">
                                <span class="text-slate-400">Dead Letter Queue:</span>
                                <span id="infra-pubsub-dlq" class="mono font-semibold text-rose-400">asyncgw-dlq-topic</span>
                            </li>
                            <li class="flex justify-between">
                                <span class="text-slate-400">Ack Deadline / Max Retries:</span>
                                <span class="mono font-semibold text-slate-200">60s / 5 attempts</span>
                            </li>
                        </ul>
                    </div>

                    <!-- BigQuery Card -->
                    <div class="bg-slate-900 border border-slate-800 rounded-xl p-6 space-y-4 shadow-lg">
                        <div class="text-emerald-400 text-base font-bold flex items-center justify-between border-b border-slate-800 pb-2">
                            <div class="flex items-center gap-2">
                                <i class="fa-solid fa-table"></i> Google Cloud BigQuery
                            </div>
                            <span class="text-[10px] font-semibold px-2 py-0.5 rounded bg-emerald-950 border border-emerald-800 text-emerald-300">State & Analytics</span>
                        </div>
                        <ul class="text-xs space-y-2.5 text-slate-300">
                            <li class="flex justify-between border-b border-slate-800/70 pb-1.5">
                                <span class="text-slate-400">Dataset:</span>
                                <span id="infra-bq-dataset" class="mono font-semibold text-indigo-300">asyncgw_metrics</span>
                            </li>
                            <li class="flex justify-between border-b border-slate-800/70 pb-1.5">
                                <span class="text-slate-400">Table:</span>
                                <span id="infra-bq-table" class="mono font-semibold text-slate-200">request_tracker</span>
                            </li>
                            <li class="flex justify-between border-b border-slate-800/70 pb-1.5">
                                <span class="text-slate-400">Partitioning:</span>
                                <span class="mono font-semibold text-emerald-400">DATE(created_at)</span>
                            </li>
                            <li class="flex justify-between border-b border-slate-800/70 pb-1.5">
                                <span class="text-slate-400">Clustering Keys:</span>
                                <span class="mono font-semibold text-slate-300">status, request_id</span>
                            </li>
                            <li class="flex justify-between">
                                <span class="text-slate-400">Write Model:</span>
                                <span class="mono font-semibold text-slate-200">Streaming & Merge Upsert</span>
                            </li>
                        </ul>
                    </div>

                    <!-- GCS Card -->
                    <div class="bg-slate-900 border border-slate-800 rounded-xl p-6 space-y-4 shadow-lg">
                        <div class="text-amber-400 text-base font-bold flex items-center justify-between border-b border-slate-800 pb-2">
                            <div class="flex items-center gap-2">
                                <i class="fa-solid fa-bucket"></i> Google Cloud Storage
                            </div>
                            <span class="text-[10px] font-semibold px-2 py-0.5 rounded bg-amber-950 border border-amber-800 text-amber-300">Payload Storage</span>
                        </div>
                        <ul class="text-xs space-y-2.5 text-slate-300">
                            <li class="flex justify-between border-b border-slate-800/70 pb-1.5">
                                <span class="text-slate-400">Bucket:</span>
                                <span id="infra-gcs-bucket" class="mono font-semibold text-amber-300">asyncgw-responses-*</span>
                            </li>
                            <li class="flex justify-between border-b border-slate-800/70 pb-1.5">
                                <span class="text-slate-400">Lifecycle TTL:</span>
                                <span class="mono font-semibold text-emerald-400">7 Days Auto-Delete</span>
                            </li>
                            <li class="flex justify-between border-b border-slate-800/70 pb-1.5">
                                <span class="text-slate-400">Responses Path:</span>
                                <span class="mono font-semibold text-slate-300">responses/{id}.json</span>
                            </li>
                            <li class="flex justify-between border-b border-slate-800/70 pb-1.5">
                                <span class="text-slate-400">Batch Parts Path:</span>
                                <span class="mono font-semibold text-slate-300">batches/{id}/parts/</span>
                            </li>
                            <li class="flex justify-between">
                                <span class="text-slate-400">Content Type:</span>
                                <span class="mono font-semibold text-slate-200">application/json</span>
                            </li>
                        </ul>
                    </div>
                </div>
            </div>

            <!-- SECTION 4: IAM SERVICE ACCOUNTS -->
            <div class="bg-slate-900/60 border border-slate-800 rounded-xl p-5 space-y-3 text-xs">
                <div class="flex items-center justify-between border-b border-slate-800/80 pb-2">
                    <h4 class="font-bold text-slate-200 flex items-center gap-2">
                        <i class="fa-solid fa-shield-halved text-indigo-400"></i> IAM Security & Service Accounts
                    </h4>
                    <span class="text-[11px] text-slate-500 font-mono">Least-Privilege Role Model</span>
                </div>
                <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div class="bg-slate-950 p-3 rounded-lg border border-slate-800/80">
                        <span class="font-bold text-slate-200 block mb-1">Gateway Service Account (<span id="infra-gw-sa-card" class="mono text-indigo-300">asyncgw-gateway-sa</span>)</span>
                        <div class="text-slate-400 text-[11px] space-y-0.5">
                            <div>&bull; <span class="mono text-slate-300">roles/pubsub.publisher</span> &mdash; Publish incoming request envelopes</div>
                            <div>&bull; <span class="mono text-slate-300">roles/bigquery.dataEditor</span> &mdash; Register PENDING status rows</div>
                            <div>&bull; <span class="mono text-slate-300">roles/storage.objectAdmin</span> &mdash; Read completed responses from GCS</div>
                            <div>&bull; <span class="mono text-slate-300">roles/artifactregistry.reader</span> &mdash; Pull container images</div>
                        </div>
                    </div>
                    <div class="bg-slate-950 p-3 rounded-lg border border-slate-800/80">
                        <span class="font-bold text-slate-200 block mb-1">Worker Service Account (<span id="infra-wk-sa-card" class="mono text-indigo-300">asyncgw-worker-sa</span>)</span>
                        <div class="text-slate-400 text-[11px] space-y-0.5">
                            <div>&bull; <span class="mono text-slate-300">roles/pubsub.subscriber</span> &mdash; Pull and acknowledge queue envelopes</div>
                            <div>&bull; <span class="mono text-slate-300">roles/bigquery.dataEditor</span> &mdash; Update request execution states</div>
                            <div>&bull; <span class="mono text-slate-300">roles/storage.objectAdmin</span> &mdash; Write completed response JSON blobs</div>
                            <div>&bull; <span class="mono text-slate-300">roles/aiplatform.user</span> &mdash; Vertex AI Provisioned Throughput & Flex inference</div>
                        </div>
                    </div>
                </div>
            </div>
        </section>
    </main>

    <!-- Modal for inspecting request details -->
    <div id="json-modal" class="fixed inset-0 bg-black/75 backdrop-blur-sm hidden items-center justify-center z-50 p-4">
        <div class="bg-slate-900 border border-slate-800 rounded-2xl max-w-4xl w-full p-6 space-y-4 max-h-[90vh] flex flex-col shadow-2xl">
            <div class="flex justify-between items-center border-b border-slate-800 pb-3">
                <div class="flex items-center gap-3">
                    <h3 class="font-bold text-base text-slate-100 flex items-center gap-2" id="modal-title">
                        <i class="fa-solid fa-file-code text-indigo-400"></i> Request Inspection
                    </h3>
                    <span id="modal-status-badge"></span>
                </div>
                <button onclick="closeModal()" class="text-slate-400 hover:text-slate-200 text-xl font-bold">&times;</button>
            </div>
            
            <div class="overflow-y-auto flex-1 space-y-4 pr-1">
                <!-- Batch Decomposed Sub-Requests Breakdown Card (Shown if request is a batch) -->
                <div id="modal-batch-breakdown" class="hidden bg-slate-950/90 border border-indigo-900/50 rounded-xl p-4 space-y-3">
                    <div class="flex justify-between items-center">
                        <h4 class="text-xs font-bold text-indigo-300 flex items-center gap-2">
                            <i class="fa-solid fa-sitemap text-indigo-400"></i> Decomposed Sub-Requests Breakdown
                        </h4>
                        <span id="modal-batch-progress" class="text-xs font-semibold px-2.5 py-0.5 rounded-full bg-indigo-950 border border-indigo-700 text-indigo-300"></span>
                    </div>
                    <div id="modal-batch-items-container" class="overflow-x-auto"></div>
                </div>

                <!-- Raw JSON / Output -->
                <div>
                    <h4 class="text-xs font-semibold text-slate-400 mb-1" id="modal-json-heading">Payload & Execution Metadata</h4>
                    <pre id="modal-json-content" class="bg-slate-950 p-4 rounded-xl text-xs font-mono text-slate-300 overflow-x-auto border border-slate-800/80"></pre>
                </div>
            </div>
            
            <div class="flex justify-end pt-2 border-t border-slate-800">
                <button onclick="closeModal()" class="px-4 py-2 bg-slate-800 hover:bg-slate-700 text-sm font-semibold rounded-lg text-slate-200">Close</button>
            </div>
        </div>
    </div>

    <script>
        let currentTab = 'explorer';
        let pollInterval = null;
        let allLoadedRequests = [];
        let expandedBatchIds = new Set();
        let systemInfo = { dev_mode: true, project_id: '', gcs_bucket_name: '' };

        async function loadSystemInfo() {
            try {
                const res = await fetch('/v1/admin/info');
                if (res.ok) {
                    systemInfo = await res.json();
                }
            } catch (e) {
                console.warn("Could not load system info", e);
            }
        }

        async function downloadBatchResponse(requestId) {
            try {
                let respData = null;
                const res = await fetch(`/v1/batches/${requestId}/output`);
                if (res.ok) {
                    respData = await res.json();
                } else {
                    const res2 = await fetch(`/v1/requests/${requestId}/response`);
                    if (res2.ok) {
                        respData = await res2.json();
                    } else {
                        throw new Error("Response is not ready yet or request failed.");
                    }
                }

                const blob = new Blob([JSON.stringify(respData, null, 2)], { type: 'application/json' });
                const url = URL.createObjectURL(blob);
                const a = document.createElement('a');
                a.href = url;
                a.download = `${requestId}_response.json`;
                document.body.appendChild(a);
                a.click();
                document.body.removeChild(a);
                URL.revokeObjectURL(url);
            } catch (e) {
                alert("Could not download batch response: " + e.message);
            }
        }

        function getBatchResponseLink(r) {
            const isLocal = (systemInfo && systemInfo.dev_mode) || !r.response_gcs_uri || r.response_gcs_uri.includes('mock-') || r.response_gcs_uri.startsWith('gs://mock-');
            if (isLocal) {
                if (r.status === 'COMPLETED') {
                    return `<button onclick="event.stopPropagation(); downloadBatchResponse('${r.request_id}')" class="text-xs text-indigo-400 hover:text-indigo-300 font-mono font-semibold underline hover:no-underline transition" title="Download all responses as single JSON">[download]</button>`;
                } else {
                    return `<span class="text-xs text-slate-500 font-mono" title="Batch still processing">[download]</span>`;
                }
            } else {
                // GCP Deployed Gateway -> Link directly to Google Cloud Storage object
                let gcpUrl = 'https://console.cloud.google.com/storage/browser';
                if (r.response_gcs_uri) {
                    const m = r.response_gcs_uri.match(/^gs:\/\/([^\/]+)\/(.+)$/);
                    if (m) {
                        const bucket = m[1];
                        const path = m[2];
                        const projParam = systemInfo?.project_id ? `?project=${encodeURIComponent(systemInfo.project_id)}` : '';
                        gcpUrl = `https://console.cloud.google.com/storage/browser/_details/${encodeURIComponent(bucket)}/${encodeURIComponent(path)}${projParam}`;
                    }
                }
                if (r.status === 'COMPLETED' && r.response_gcs_uri) {
                    return `<a href="${gcpUrl}" target="_blank" rel="noopener noreferrer" onclick="event.stopPropagation();" class="text-xs text-emerald-400 hover:text-emerald-300 font-mono font-semibold underline hover:no-underline transition inline-flex items-center gap-1" title="Open response object in Google Cloud Storage Console">[response in gcp storage] <i class="fa-solid fa-arrow-up-right-from-square text-[9px]"></i></a>`;
                } else {
                    return `<span class="text-xs text-slate-500 font-mono" title="Available in GCS once completed">[response in gcp storage]</span>`;
                }
            }
        }

        function switchTab(tabId) {
            currentTab = tabId;
            ['explorer', 'submit', 'backends', 'policies', 'infra'].forEach(t => {
                document.getElementById(`view-${t}`).classList.add('hidden');
                document.getElementById(`tab-btn-${t}`).className = 'pb-3 border-b-2 border-transparent text-slate-400 hover:text-slate-200 flex items-center gap-2';
            });
            document.getElementById(`view-${tabId}`).classList.remove('hidden');
            document.getElementById(`tab-btn-${tabId}`).className = 'pb-3 border-b-2 border-indigo-500 text-indigo-400 flex items-center gap-2';

            if (tabId === 'explorer') loadRequests();
            if (tabId === 'backends') loadBackends();
            if (tabId === 'policies') loadPolicies();
            if (tabId === 'infra') loadInfra();
        }

        function toggleBatchMode() {
            const isBatch = document.querySelector('input[name="req-mode"]:checked').value === 'batch';
            document.getElementById('single-prompt-container').classList.toggle('hidden', isBatch);
            document.getElementById('batch-prompt-container').classList.toggle('hidden', !isBatch);
        }

        async function updateStats() {
            try {
                const res = await fetch('/v1/admin/stats');
                if (res.ok) {
                    const data = await res.json();
                    document.getElementById('stat-total').innerText = data.total_requests_tracked || 0;
                    document.getElementById('stat-pending').innerText = data.status_breakdown.PENDING || 0;
                    document.getElementById('stat-processing').innerText = data.status_breakdown.PROCESSING || 0;
                    document.getElementById('stat-completed').innerText = data.status_breakdown.COMPLETED || 0;
                    document.getElementById('stat-failed').innerText = data.status_breakdown.FAILED || 0;
                    document.getElementById('stat-timedout').innerText = data.status_breakdown.TIMED_OUT || 0;
                }
            } catch (e) {
                console.error("Stats update failed", e);
            }
        }

        function getStatusBadge(status) {
            if (status === 'COMPLETED') return '<span class="px-2 py-0.5 rounded text-xs font-semibold bg-emerald-950 text-emerald-400 border border-emerald-800">COMPLETED</span>';
            if (status === 'PROCESSING') return '<span class="px-2 py-0.5 rounded text-xs font-semibold bg-blue-950 text-blue-400 border border-blue-800 animate-pulse">PROCESSING</span>';
            if (status === 'PENDING') return '<span class="px-2 py-0.5 rounded text-xs font-semibold bg-amber-950 text-amber-400 border border-amber-800">PENDING</span>';
            if (status === 'TIMED_OUT') return '<span class="px-2 py-0.5 rounded text-xs font-semibold bg-orange-950 text-orange-400 border border-orange-800">TIMED_OUT</span>';
            return '<span class="px-2 py-0.5 rounded text-xs font-semibold bg-rose-950 text-rose-400 border border-rose-800">FAILED</span>';
        }

        async function loadRequests() {
            updateStats();
            const status = document.getElementById('filter-status').value;
            const query = status ? `?status=${status}&limit=100` : '?limit=100';
            try {
                const res = await fetch(`/v1/admin/requests${query}`);
                const data = await res.json();
                allLoadedRequests = data.requests || [];
                filterAndRenderRequests();
            } catch (e) {
                console.error("Requests load failed", e);
            }
        }

        function filterAndRenderRequests() {
            const searchTerm = (document.getElementById('search-input')?.value || '').toLowerCase().trim();
            const filtered = allLoadedRequests.filter(r => {
                if (!searchTerm) return true;
                const matchId = (r.request_id || '').toLowerCase().includes(searchTerm);
                const matchModel = (r.model || '').toLowerCase().includes(searchTerm);
                const matchType = (r.request_type || '').toLowerCase().includes(searchTerm);
                const matchParent = (r.parent_request_id || '').toLowerCase().includes(searchTerm);
                const matchCustom = (r.custom_id || '').toLowerCase().includes(searchTerm);
                return matchId || matchModel || matchType || matchParent || matchCustom;
            });

            const tbody = document.getElementById('requests-tbody');
            if (filtered.length === 0) {
                tbody.innerHTML = '<tr><td colspan="8" class="text-center py-8 text-slate-500">No matching requests found.</td></tr>';
                return;
            }

            // Only show top-level requests in the primary table (sub-requests are grouped under batch)
            const parentRequests = filtered.filter(r => !r.parent_request_id || r.request_type === 'batch');
            const listToRender = parentRequests.length > 0 ? parentRequests : filtered;

            tbody.innerHTML = listToRender.map(r => {
                const isBatch = r.request_type === 'batch' || (r.total_items && r.total_items > 1) || (r.request_id && r.request_id.startsWith('batch_'));
                const isExpanded = expandedBatchIds.has(r.request_id);
                const statusBadge = getStatusBadge(r.status);
                const latency = r.elapsed_seconds ? `${r.elapsed_seconds.toFixed(2)}s` : '-';
                const dateStr = r.created_at ? new Date(r.created_at).toLocaleTimeString() : '-';
                const backend = r.backend_service_id || '-';
                const batchLink = isBatch ? getBatchResponseLink(r) : '';

                let typeCol = `<span class="text-xs text-slate-400">${r.request_type}</span>`;
                if (isBatch) {
                    typeCol = `
                        <div class="flex items-center gap-1.5">
                            <span class="px-2 py-0.5 rounded text-[11px] font-bold bg-indigo-950 text-indigo-300 border border-indigo-800 flex items-center gap-1">
                                <i class="fa-solid fa-layer-group text-[10px]"></i> Batch
                            </span>
                        </div>
                    `;
                }

                let batchBtn = '';
                if (isBatch) {
                    batchBtn = `
                        <button onclick="toggleBatchItems('${r.request_id}', this)" class="inline-flex items-center gap-1.5 px-2 py-1 rounded bg-indigo-950/80 hover:bg-indigo-900 border border-indigo-700/60 text-indigo-300 text-xs font-semibold transition shadow-sm">
                            <i class="fa-solid fa-chevron-right text-[10px] transition-transform duration-200 ${isExpanded ? 'rotate-90' : ''}" id="chevron-${r.request_id}"></i>
                            <span>Breakdown (${r.total_items || '?'})</span>
                        </button>
                    `;
                }

                return `
                    <tr class="hover:bg-slate-800/40 transition">
                        <td class="px-4 py-3 font-mono text-xs text-indigo-400 font-semibold">
                            <div class="flex items-center gap-2">
                                <span>${r.request_id}</span>
                            </div>
                            ${isBatch ? `<div class="mt-1">${batchLink}</div>` : ''}
                        </td>
                        <td class="px-4 py-3">${typeCol}</td>
                        <td class="px-4 py-3 text-xs font-semibold">${r.model || '-'}</td>
                        <td class="px-4 py-3">${statusBadge}</td>
                        <td class="px-4 py-3 text-xs text-slate-300 font-mono text-[11px]">${backend}</td>
                        <td class="px-4 py-3 text-xs font-mono">${latency}</td>
                        <td class="px-4 py-3 text-xs text-slate-400">${dateStr}</td>
                        <td class="px-4 py-3 text-right space-x-2">
                            ${isBatch ? `<span class="inline-block mr-1">${batchLink}</span>` : ''}
                            ${batchBtn}
                            <button onclick="inspectRequest('${r.request_id}')" class="px-2.5 py-1 bg-slate-800 hover:bg-slate-700 rounded text-xs text-slate-300 font-medium">
                                <i class="fa-solid fa-eye"></i> View
                            </button>
                        </td>
                    </tr>
                    ${isBatch ? `
                    <tr id="sub-row-${r.request_id}" class="${isExpanded ? '' : 'hidden'} bg-slate-950/80 border-y border-indigo-900/40">
                        <td colspan="8" class="p-3 pl-8">
                            <div class="bg-slate-900/90 border border-indigo-800/40 rounded-xl p-3 space-y-2.5 shadow-inner">
                                <div class="flex items-center justify-between">
                                    <div class="flex items-center gap-2 text-xs font-bold text-indigo-300">
                                        <i class="fa-solid fa-sitemap text-indigo-400"></i> Decomposed Sub-Requests Breakdown for <span class="font-mono text-slate-200">${r.request_id}</span>
                                    </div>
                                    <div class="flex items-center gap-3">
                                        ${batchLink}
                                        <span class="text-[11px] text-slate-400">&bull; Reassembled in sequential order</span>
                                    </div>
                                </div>
                                <div id="sub-content-${r.request_id}">
                                    <div class="text-xs text-slate-400 text-center py-2">
                                        <i class="fa-solid fa-spinner fa-spin text-indigo-400"></i> Loading decomposed sub-requests...
                                    </div>
                                </div>
                            </div>
                        </td>
                    </tr>
                    ` : ''}
                `;
            }).join('');

            // If any batch rows were expanded, load their items
            expandedBatchIds.forEach(id => loadBatchItemsContent(id));
        }

        async function toggleBatchItems(batchId, btn) {
            const row = document.getElementById(`sub-row-${batchId}`);
            const chevron = document.getElementById(`chevron-${batchId}`);
            if (!row) return;

            if (row.classList.contains('hidden')) {
                row.classList.remove('hidden');
                expandedBatchIds.add(batchId);
                if (chevron) chevron.classList.add('rotate-90');
                await loadBatchItemsContent(batchId);
            } else {
                row.classList.add('hidden');
                expandedBatchIds.delete(batchId);
                if (chevron) chevron.classList.remove('rotate-90');
            }
        }

        async function fetchBatchItemsOrNativeResults(batchId) {
            // 1. Try fetching decomposed sub-requests from tracker
            try {
                const res = await fetch(`/v1/requests/${batchId}/items`);
                if (res.ok) {
                    const data = await res.json();
                    if (data.items && data.items.length > 0) {
                        return { mode: 'decomposed', items: data.items, raw_output: null };
                    }
                }
            } catch (e) {
                console.warn("Could not fetch decomposed items", e);
            }

            // 2. If no sub-requests, check if batch was executed natively via GCS response output
            try {
                const res = await fetch(`/v1/requests/${batchId}/response`);
                if (res.ok) {
                    const data = await res.json();
                    if (data && data.results && Array.isArray(data.results)) {
                        const batchMode = data.backend_batch_service_mode || 'native';
                        const servedBackendId = data.backend_service_id || 'gcp-provisioned-gemini';
                        const nativeItems = data.results.map((r, idx) => {
                            const isSuccess = r.response && (r.response.status_code === 200 || r.response.status_code === undefined);
                            const body = r.response?.body || r.response || {};
                            const model = body.model || 'batch-model';
                            return {
                                sequence_number: idx,
                                custom_id: r.custom_id || `item-${idx}`,
                                request_type: 'batch.native_item',
                                model: model,
                                status: isSuccess ? 'COMPLETED' : 'FAILED',
                                backend_service_id: servedBackendId,
                                backend_batch_service_mode: batchMode,
                                elapsed_seconds: null,
                                response_payload: body,
                                error_message: r.error?.message,
                                request_id: `${batchId}_${idx}`,
                            };
                        });
                        return { mode: batchMode, items: nativeItems, raw_output: data };
                    }
                }
            } catch (e) {
                console.warn("Could not fetch native batch response", e);
            }

            return { mode: 'pending', items: [], raw_output: null };
        }

        async function loadBatchItemsContent(batchId) {
            const container = document.getElementById(`sub-content-${batchId}`);
            if (!container) return;
            try {
                const { mode, items } = await fetchBatchItemsOrNativeResults(batchId);

                if (items.length === 0) {
                    container.innerHTML = `<div class="text-xs text-slate-400 py-3 text-center flex items-center justify-center gap-2"><i class="fa-solid fa-spinner fa-spin text-indigo-400"></i> Batch request is processing or awaiting response...</div>`;
                    return;
                }

                const modeLabel = mode === 'decomposed' ? 'decomposed' : 'native';
                const modeBadge = `<span class="px-2 py-0.5 rounded text-[10px] font-bold bg-indigo-950 text-indigo-300 border border-indigo-700/80 mono">mode: ${modeLabel}</span>`;

                container.innerHTML = `
                    <div class="space-y-2">
                        <div class="flex justify-between items-center px-1">
                            <span class="text-[11px] text-slate-400">${items.length} items &bull; Batch Mode: ${modeBadge}</span>
                        </div>
                        <div class="overflow-x-auto rounded-lg border border-slate-800">
                            <table class="w-full text-left text-xs">
                                <thead class="bg-slate-950 text-slate-400 border-b border-slate-800">
                                    <tr>
                                        <th class="px-3 py-2">Seq # & Custom ID</th>
                                        <th class="px-3 py-2">Type</th>
                                        <th class="px-3 py-2">Model</th>
                                        <th class="px-3 py-2">Status</th>
                                        <th class="px-3 py-2">Backend Served</th>
                                        <th class="px-3 py-2">Latency</th>
                                        <th class="px-3 py-2 text-right">Action</th>
                                    </tr>
                                </thead>
                                <tbody class="divide-y divide-slate-800/60 font-sans">
                                    ${items.map(item => `
                                        <tr class="hover:bg-slate-800/50">
                                            <td class="px-3 py-2 font-mono text-slate-200">
                                                <span class="text-indigo-400 font-bold">#${item.sequence_number !== null ? item.sequence_number : 0}</span> 
                                                ${item.custom_id ? `<span class="text-slate-400 text-[11px]">(${item.custom_id})</span>` : ''}
                                            </td>
                                            <td class="px-3 py-2 mono text-slate-400 text-[11px]">${item.request_type}</td>
                                            <td class="px-3 py-2 font-semibold text-slate-300">${item.model || '-'}</td>
                                            <td class="px-3 py-2">${getStatusBadge(item.status)}</td>
                                            <td class="px-3 py-2 text-slate-300 font-mono text-[11px]">${item.backend_service_id || 'Serving...'}</td>
                                            <td class="px-3 py-2 font-mono">${item.elapsed_seconds ? item.elapsed_seconds.toFixed(2) + 's' : '-'}</td>
                                            <td class="px-3 py-2 text-right">
                                                <button onclick="inspectBatchItem('${batchId}', ${item.sequence_number !== null ? item.sequence_number : 0})" class="px-2 py-0.5 bg-slate-800 hover:bg-slate-700 rounded text-xs text-slate-300">
                                                    <i class="fa-solid fa-file-lines text-indigo-400"></i> View Item
                                                </button>
                                            </td>
                                        </tr>
                                    `).join('')}
                                </tbody>
                            </table>
                        </div>
                    </div>
                `;
            } catch (e) {
                container.innerHTML = `<div class="text-xs text-rose-400 py-2">Error loading batch items: ${e}</div>`;
            }
        }

        async function inspectRequest(reqId) {
            try {
                const res = await fetch(`/v1/requests/${reqId}`);
                const statusData = await res.json();
                let fullData = { status: statusData };

                if (statusData.status === 'COMPLETED') {
                    const resp = await fetch(`/v1/requests/${reqId}/response`);
                    if (resp.ok) fullData.response_payload = await resp.json();
                }

                const isBatch = statusData.request_type === 'batch' || (statusData.total_items && statusData.total_items > 1) || reqId.startsWith('batch_');
                const breakdownSection = document.getElementById('modal-batch-breakdown');
                const itemsContainer = document.getElementById('modal-batch-items-container');
                const progressBadge = document.getElementById('modal-batch-progress');

                if (isBatch) {
                    const { mode, items } = await fetchBatchItemsOrNativeResults(reqId);
                    fullData.batch_items = items;

                    if (items.length > 0) {
                        const completedCount = items.filter(i => i.status === 'COMPLETED').length;
                        const failedCount = items.filter(i => i.status === 'FAILED' || i.status === 'TIMED_OUT').length;
                        const modeLabel = mode === 'decomposed' ? 'decomposed' : 'native';
                        const batchLink = getBatchResponseLink(statusData);

                        progressBadge.innerHTML = `<span class="text-indigo-400 font-bold">${completedCount}/${items.length} Completed</span> &bull; <span class="text-indigo-300 font-mono text-[11px]">mode: ${modeLabel}</span> &bull; ${batchLink}`;
                        breakdownSection.classList.remove('hidden');

                        itemsContainer.innerHTML = `
                            <table class="w-full text-left text-xs mt-2 border border-slate-800 rounded-lg overflow-hidden">
                                <thead class="bg-slate-950 text-slate-400 border-b border-slate-800">
                                    <tr>
                                        <th class="px-2.5 py-1.5">Seq # / ID</th>
                                        <th class="px-2.5 py-1.5">Model</th>
                                        <th class="px-2.5 py-1.5">Status</th>
                                        <th class="px-2.5 py-1.5">Backend</th>
                                        <th class="px-2.5 py-1.5">Latency</th>
                                        <th class="px-2.5 py-1.5 text-right">Action</th>
                                    </tr>
                                </thead>
                                <tbody class="divide-y divide-slate-800/60 font-sans">
                                    ${items.map(item => `
                                        <tr class="hover:bg-slate-800/40">
                                            <td class="px-2.5 py-1.5 font-mono text-indigo-400 font-bold">
                                                #${item.sequence_number} 
                                                ${item.custom_id ? `<span class="text-slate-400 font-normal text-[11px]">(${item.custom_id})</span>` : ''}
                                            </td>
                                            <td class="px-2.5 py-1.5 font-semibold text-slate-300">${item.model || '-'}</td>
                                            <td class="px-2.5 py-1.5">${getStatusBadge(item.status)}</td>
                                            <td class="px-2.5 py-1.5 text-slate-300 font-mono text-[11px]">${item.backend_service_id || '-'}</td>
                                            <td class="px-2.5 py-1.5 font-mono">${item.elapsed_seconds ? item.elapsed_seconds.toFixed(2) + 's' : '-'}</td>
                                            <td class="px-2.5 py-1.5 text-right">
                                                <button onclick="inspectBatchItem('${reqId}', ${item.sequence_number})" class="px-2 py-0.5 bg-slate-800 hover:bg-slate-700 rounded text-[11px] text-slate-300">
                                                    <i class="fa-solid fa-eye text-indigo-400"></i> View
                                                </button>
                                            </td>
                                        </tr>
                                    `).join('')}
                                </tbody>
                            </table>
                        `;
                    } else {
                        progressBadge.innerText = statusData.status === 'COMPLETED' ? 'Processing Finished' : 'Processing...';
                        breakdownSection.classList.remove('hidden');
                        itemsContainer.innerHTML = `<p class="text-xs text-slate-400 italic py-2">Batch envelope queued; decomposing or awaiting backend output...</p>`;
                    }
                } else {
                    breakdownSection.classList.add('hidden');
                }

                document.getElementById('modal-title').innerHTML = `<i class="fa-solid fa-file-code text-indigo-400"></i> Request Inspection: <span class="font-mono text-indigo-300 ml-1">${reqId}</span>`;
                document.getElementById('modal-status-badge').innerHTML = getStatusBadge(statusData.status);
                document.getElementById('modal-json-content').innerText = JSON.stringify(fullData, null, 2);
                document.getElementById('json-modal').classList.remove('hidden');
                document.getElementById('json-modal').classList.add('flex');
            } catch (e) {
                alert("Inspection error: " + e);
            }
        }

        async function inspectBatchItem(batchId, seqNum) {
            try {
                const { mode, items } = await fetchBatchItemsOrNativeResults(batchId);
                const item = (items || []).find(i => i.sequence_number === seqNum);

                if (!item) {
                    alert(`Item #${seqNum} not found for batch ${batchId}`);
                    return;
                }

                document.getElementById('modal-batch-breakdown').classList.add('hidden');
                document.getElementById('modal-title').innerHTML = `<i class="fa-solid fa-sitemap text-indigo-400"></i> Batch Item #${seqNum} <span class="text-xs text-slate-400 font-normal">(Parent: ${batchId}, ID: ${item.custom_id || '-'})</span>`;
                document.getElementById('modal-status-badge').innerHTML = getStatusBadge(item.status);
                document.getElementById('modal-json-content').innerText = JSON.stringify(item, null, 2);
                document.getElementById('json-modal').classList.remove('hidden');
                document.getElementById('json-modal').classList.add('flex');
            } catch (e) {
                alert("Batch item inspection error: " + e);
            }
        }

        function closeModal() {
            document.getElementById('json-modal').classList.add('hidden');
            document.getElementById('json-modal').classList.remove('flex');
        }

        async function submitInferenceRequest() {
            const isBatch = document.querySelector('input[name="req-mode"]:checked').value === 'batch';
            const model = document.getElementById('submit-model').value;
            const maxWait = parseInt(document.getElementById('submit-maxwait').value) || 120;

            let endpoint = '/v1/chat/completions';
            let body = {};

            if (isBatch) {
                endpoint = '/v1/batches';
                try {
                    const items = JSON.parse(document.getElementById('submit-batch-json').value);
                    body = {
                        endpoint: "/v1/chat/completions",
                        completion_window: "24h",
                        max_wait_seconds: maxWait,
                        requests: items
                    };
                } catch(e) {
                    alert("Invalid JSON format in batch items!");
                    return;
                }
            } else {
                const prompt = document.getElementById('submit-prompt').value;
                body = {
                    model: model,
                    messages: [{"role": "user", "content": prompt}],
                    max_wait_seconds: maxWait
                };
            }

            try {
                const res = await fetch(endpoint, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(body)
                });
                const data = await res.json();
                if (res.ok) {
                    startMonitoring(data.request_id);
                    updateStats();
                } else {
                    alert("Submission failed: " + JSON.stringify(data));
                }
            } catch(e) {
                alert("Error submitting request: " + e);
            }
        }

        function startMonitoring(requestId) {
            if (pollInterval) clearInterval(pollInterval);
            document.getElementById('active-poll-badge').classList.remove('hidden');
            document.getElementById('response-viewer').innerText = "// Polling gateway for asynchronous execution...";

            const pollFn = async () => {
                try {
                    const res = await fetch(`/v1/requests/${requestId}`);
                    if (!res.ok) return;
                    const statusData = await res.json();

                    document.getElementById('monitor-details').innerHTML = `
                        <div class="flex justify-between"><span class="text-slate-400">Request ID:</span> <span class="mono font-semibold text-indigo-400">${statusData.request_id}</span></div>
                        <div class="flex justify-between"><span class="text-slate-400">Status:</span> <span class="font-bold">${statusData.status}</span></div>
                        <div class="flex justify-between"><span class="text-slate-400">Backend:</span> <span>${statusData.backend_service_id || 'Queued in Pub/Sub'}</span></div>
                        <div class="flex justify-between"><span class="text-slate-400">Elapsed:</span> <span class="mono">${statusData.elapsed_seconds ? statusData.elapsed_seconds.toFixed(2) + 's' : '0s'}</span></div>
                    `;

                    if (statusData.status === 'COMPLETED') {
                        clearInterval(pollInterval);
                        document.getElementById('active-poll-badge').classList.add('hidden');
                        const resp = await fetch(`/v1/requests/${requestId}/response`);
                        if (resp.ok) {
                            const result = await resp.json();
                            document.getElementById('response-viewer').innerText = JSON.stringify(result, null, 2);
                            document.getElementById('response-time-tag').innerText = `Delivered in ${statusData.elapsed_seconds?.toFixed(2)}s`;
                        }
                        updateStats();
                    } else if (statusData.status === 'FAILED' || statusData.status === 'TIMED_OUT') {
                        clearInterval(pollInterval);
                        document.getElementById('active-poll-badge').classList.add('hidden');
                        document.getElementById('response-viewer').innerText = JSON.stringify(statusData, null, 2);
                        updateStats();
                    }
                } catch(e) {
                    console.error("Poll error", e);
                }
            };

            pollFn();
            pollInterval = setInterval(pollFn, 1000);
        }

        async function loadBackends() {
            try {
                const res = await fetch('/v1/admin/backends');
                const data = await res.json();
                const container = document.getElementById('backends-grid');
                container.innerHTML = data.backends.map(item => {
                    const b = item.config;
                    const h = item.health;
                    const isHealthy = h.is_healthy;
                    const healthBadge = isHealthy 
                        ? '<span class="text-emerald-400 text-xs font-semibold bg-emerald-950 border border-emerald-800 px-2 py-0.5 rounded">Healthy</span>'
                        : '<span class="text-rose-400 text-xs font-semibold bg-rose-950 border border-rose-800 px-2 py-0.5 rounded">Unhealthy</span>';

                    return `
                        <div class="bg-slate-900 border border-slate-800 rounded-xl p-5 space-y-3">
                            <div class="flex justify-between items-start">
                                <div>
                                    <h3 class="font-bold text-slate-100">${b.name}</h3>
                                    <span class="mono text-xs text-indigo-400">${b.id}</span>
                                </div>
                                ${healthBadge}
                            </div>
                            <p class="text-xs text-slate-400">${b.description || ''}</p>
                            <div class="text-xs space-y-1 text-slate-300 border-t border-slate-800/80 pt-2">
                                <div><span class="text-slate-500">Endpoint:</span> <span class="mono text-[11px]">${b.endpoint_url}</span></div>
                                <div><span class="text-slate-500">Capabilities:</span> Online: ${b.capabilities.supports_online ? 'Yes' : 'No'} | Batch: ${b.capabilities.supports_batch ? 'Yes' : 'No'}</div>
                                <div><span class="text-slate-500">Priority Weight:</span> ${b.priority_weight} | Cost Tier: <span class="uppercase font-semibold">${b.cost_tier}</span></div>
                                <div><span class="text-slate-500">Supported Models:</span> ${(b.supported_models || []).join(', ')}</div>
                            </div>
                            <div class="pt-2 flex justify-end">
                                <button onclick="probeBackend('${b.id}')" class="px-2.5 py-1 bg-slate-800 hover:bg-slate-700 rounded text-xs text-slate-300 flex items-center gap-1.5">
                                    <i class="fa-solid fa-heart-pulse"></i> Probe Health
                                </button>
                            </div>
                        </div>
                    `;
                }).join('');
            } catch(e) {
                console.error("Backends load error", e);
            }
        }

        async function probeBackend(id) {
            try {
                const res = await fetch(`/v1/admin/backends/${id}/probe`, { method: 'POST' });
                const data = await res.json();
                alert(`Probe result for ${id}: ${data.status.is_healthy ? 'HEALTHY' : 'UNHEALTHY'} (latency: ${data.status.last_latency_ms?.toFixed(1)}ms)`);
                loadBackends();
            } catch(e) {
                alert("Probe failed: " + e);
            }
        }

        async function probeAllBackends() {
            await fetch('/v1/admin/backends');
            loadBackends();
        }

        async function loadPolicies() {
            try {
                const res = await fetch('/v1/admin/policies');
                const data = await res.json();
                
                document.getElementById('strategies-list').innerHTML = data.routing_strategies.map(s => `
                    <div class="bg-slate-950 p-4 rounded-xl border border-slate-800 space-y-2">
                        <div class="flex justify-between items-center">
                            <span class="font-bold text-sm text-slate-200">${s.name}</span>
                            <span class="mono text-xs text-indigo-400">${s.id}</span>
                        </div>
                        <p class="text-xs text-slate-400">${s.description || ''}</p>
                        <div class="text-xs">
                            <span class="text-slate-500 font-semibold">Preference Order:</span>
                            <div class="flex flex-wrap gap-1.5 mt-1">
                                ${s.preference_order.map(p => `<span class="px-2 py-0.5 bg-slate-900 border border-slate-700 rounded text-[11px] mono text-slate-300">${p}</span>`).join(' &rarr; ')}
                            </div>
                        </div>
                    </div>
                `).join('');

                document.getElementById('content-rules-list').innerHTML = data.content_rules.map(r => `
                    <div class="bg-slate-950 p-4 rounded-xl border border-slate-800 space-y-2 text-xs">
                        <span class="font-bold text-slate-200 text-sm">${r.name}</span>
                        <pre class="bg-slate-900 p-2 rounded text-[11px] mono text-slate-400">${JSON.stringify(r, null, 2)}</pre>
                    </div>
                `).join('');
            } catch(e) {
                console.error("Policies load error", e);
            }
        }

        async function loadInfra() {
            try {
                await loadSystemInfo();
                if (!systemInfo) return;

                const proj = systemInfo.project_id || 'asyncgw-demo-project';
                const region = systemInfo.location || systemInfo.region || 'us-central1';
                const isLocal = (systemInfo.dev_mode !== undefined) ? systemInfo.dev_mode : (systemInfo.environment_mode === 'mock');

                // Project & Region Context
                const projEl = document.getElementById('infra-project-id');
                if (projEl) projEl.innerText = proj;
                const regionEl = document.getElementById('infra-region');
                if (regionEl) regionEl.innerText = region;
                const envEl = document.getElementById('infra-env-badge');
                if (envEl) {
                    envEl.innerText = isLocal ? 'LOCAL / MOCK MODE' : 'GCP PRODUCTION CLOUD RUN';
                    envEl.className = isLocal 
                        ? 'px-2.5 py-1.5 rounded-lg font-semibold bg-amber-950 text-amber-400 border border-amber-800'
                        : 'px-2.5 py-1.5 rounded-lg font-semibold bg-emerald-950 text-emerald-400 border border-emerald-800';
                }

                // Artifact Registry
                const arRepo = systemInfo.artifact_registry?.repository || 'asyncgw-docker';
                const arRepoEl = document.getElementById('infra-ar-repo-name');
                if (arRepoEl) arRepoEl.innerText = arRepo;

                const arRepoUrl = systemInfo.artifact_registry?.repository_url || `${region}-docker.pkg.dev/${proj}/${arRepo}`;
                const gwImgUri = systemInfo.artifact_registry?.images?.[0]?.full_image_uri || `${arRepoUrl}/asyncgw-gateway:latest`;
                const wkImgUri = systemInfo.artifact_registry?.images?.[1]?.full_image_uri || `${arRepoUrl}/asyncgw-worker:latest`;

                const gwImgEl = document.getElementById('infra-gw-img-uri');
                if (gwImgEl) gwImgEl.innerText = gwImgUri;
                const wkImgEl = document.getElementById('infra-wk-img-uri');
                if (wkImgEl) wkImgEl.innerText = wkImgUri;

                const arLinkEl = document.getElementById('infra-ar-console-link');
                if (arLinkEl) {
                    if (isLocal || proj === 'asyncgw-demo-project') {
                        arLinkEl.classList.add('hidden');
                    } else {
                        arLinkEl.classList.remove('hidden');
                        arLinkEl.href = `https://console.cloud.google.com/artifacts/docker/${encodeURIComponent(proj)}/${encodeURIComponent(region)}/${encodeURIComponent(arRepo)}?project=${encodeURIComponent(proj)}`;
                    }
                }

                // Service Accounts
                const gwSa = `asyncgw-gateway-sa@${proj}.iam.gserviceaccount.com`;
                const wkSa = `asyncgw-worker-sa@${proj}.iam.gserviceaccount.com`;
                const gwFleetSaEl = document.getElementById('infra-gw-service-sa');
                if (gwFleetSaEl) gwFleetSaEl.innerText = gwSa;
                const wkFleetSaEl = document.getElementById('infra-wk-fleet-sa');
                if (wkFleetSaEl) wkFleetSaEl.innerText = wkSa;
                const wkPriSaEl = document.getElementById('infra-wk-pri-sa');
                if (wkPriSaEl) wkPriSaEl.innerText = wkSa;
                const wkBatchSaEl = document.getElementById('infra-wk-batch-sa');
                if (wkBatchSaEl) wkBatchSaEl.innerText = wkSa;
                const gwSaCardEl = document.getElementById('infra-gw-sa-card');
                if (gwSaCardEl) gwSaCardEl.innerText = gwSa;
                const wkSaCardEl = document.getElementById('infra-wk-sa-card');
                if (wkSaCardEl) wkSaCardEl.innerText = wkSa;

                // Data Persistence & Queues
                const reqTopic = systemInfo.pubsub_topic_requests || 'asyncgw-requests-topic';
                const batchTopic = systemInfo.pubsub_topic_batch_items || 'asyncgw-batch-items-topic';
                const dlqTopic = systemInfo.pubsub_dlq_topic || 'asyncgw-dlq-topic';
                const reqSub = systemInfo.pubsub_subscription_requests || `${reqTopic}-sub`;
                const batchSub = systemInfo.pubsub_subscription_batch_items || `${batchTopic}-sub`;
                const dlqSub = `${dlqTopic}-sub`;

                const pubsubReqEl = document.getElementById('infra-pubsub-req');
                if (pubsubReqEl) pubsubReqEl.innerText = reqTopic;
                const pubsubReqSubEl = document.getElementById('infra-pubsub-req-sub');
                if (pubsubReqSubEl) pubsubReqSubEl.innerText = reqSub;
                const pubsubBatchEl = document.getElementById('infra-pubsub-batch');
                if (pubsubBatchEl) pubsubBatchEl.innerText = batchTopic;
                const pubsubBatchSubEl = document.getElementById('infra-pubsub-batch-sub');
                if (pubsubBatchSubEl) pubsubBatchSubEl.innerText = batchSub;
                const pubsubDlqEl = document.getElementById('infra-pubsub-dlq');
                if (pubsubDlqEl) pubsubDlqEl.innerText = dlqTopic;
                const pubsubDlqSubEl = document.getElementById('infra-pubsub-dlq-sub');
                if (pubsubDlqSubEl) pubsubDlqSubEl.innerText = dlqSub;

                const bqDataset = systemInfo.bq_dataset || 'asyncgw_metrics';
                const bqTable = systemInfo.bq_table || 'request_tracker';
                const bqDsEl = document.getElementById('infra-bq-dataset');
                if (bqDsEl) bqDsEl.innerText = bqDataset;
                const bqTableEl = document.getElementById('infra-bq-table');
                if (bqTableEl) bqTableEl.innerText = `${proj}.${bqDataset}.${bqTable}`;

                const gcsBucket = systemInfo.gcs_bucket_name || 'asyncgw-responses-storage';
                const gcsEl = document.getElementById('infra-gcs-bucket');
                if (gcsEl) gcsEl.innerText = `gs://${gcsBucket}`;
            } catch (e) {
                console.error('Error loading infrastructure view', e);
            }
        }

        // Initialize on load
        window.addEventListener('DOMContentLoaded', () => {
            loadSystemInfo().then(() => loadInfra());
            loadRequests();
            const searchInput = document.getElementById('search-input');
            if (searchInput) {
                searchInput.addEventListener('input', () => filterAndRenderRequests());
            }
            setInterval(updateStats, 5000);
        });
    </script>
</body>
</html>
"""


def create_ui_app(gateway_app: Optional[FastAPI] = None) -> FastAPI:
    """Create UI FastAPI app or attach UI route to existing Gateway application."""
    app = gateway_app or create_app()

    @app.get("/", response_class=HTMLResponse, tags=["UI Dashboard"])
    async def dashboard_view():
        return HTMLResponse(content=DASHBOARD_HTML, status_code=200)

    return app


def run_ui():
    settings = GatewaySettings()
    logger.info(f"Starting UI Dashboard on {settings.api_host}:{settings.ui_port}")
    app = create_ui_app()
    uvicorn.run(app, host=settings.api_host, port=settings.ui_port)
