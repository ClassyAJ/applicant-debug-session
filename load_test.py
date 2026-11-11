"""
uv run python load_test.py -n 500 -c 25 -u http://127.0.0.1:1338/api/v1/exec-test
Starting load test: 500 requests to http://127.0.0.1:1338/api/v1/exec-test (concurrency: 25, test_type: 'REST')

=======================================================
               LOAD TEST SUMMARY
=======================================================
Total Requests:     500
Successful:         500
Failed:             0
Success Rate:       100.00%
Total Wall Time:    0.1770 s
Throughput:         2824.41 req/s
-------------------------------------------------------
Latency (per request):
  Average:          6.20 ms
  Median (P50):     5.78 ms
  P95:              7.30 ms
  Min:              3.85 ms
  Max:              19.13 ms
=======================================================
"""
import argparse
import asyncio
import http.client
import json
import statistics
import time
import urllib.parse
from asyncio.events import AbstractEventLoop
from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class RequestResult:
    status_code: int
    duration_ms: float
    success: bool
    error: str | None = None


def send_single_request(
    host: str,
    port: int,
    path: str,
    payload_bytes: bytes,
    timeout_s: float,
) -> RequestResult:
    start_time = time.perf_counter()
    conn = http.client.HTTPConnection(host=host, port=port, timeout=timeout_s)
    try:
        headers: dict[str, str] = {
            "Content-Type": "application/json",
            "Content-Length": str(len(payload_bytes)),
            "Connection": "keep-alive",
        }
        conn.request(method="POST", url=path, body=payload_bytes, headers=headers)
        response: http.client.HTTPResponse = conn.getresponse()
        _ = response.read()
        duration_ms: float = (time.perf_counter() - start_time) * 1000.0
        success: bool = 200 <= response.status < 300
        return RequestResult(
            status_code=response.status,
            duration_ms=duration_ms,
            success=success,
        )
    except Exception as exc:  # noqa: BLE001
        duration_ms = (time.perf_counter() - start_time) * 1000.0
        return RequestResult(
            status_code=0,
            duration_ms=duration_ms,
            success=False,
            error=str(exc),
        )
    finally:
        conn.close()


async def run_worker(
    queue: asyncio.Queue[int],
    host: str,
    port: int,
    path: str,
    payload_bytes: bytes,
    timeout_s: float,
    results: list[RequestResult],
) -> None:
    loop: AbstractEventLoop = asyncio.get_running_loop()
    while not queue.empty():
        try:
            queue.get_nowait()
        except asyncio.QueueEmpty:
            break

        result: RequestResult = await loop.run_in_executor(
            None,
            send_single_request,
            host,
            port,
            path,
            payload_bytes,
            timeout_s,
        )
        results.append(result)
        queue.task_done()


async def run_load_test(
    url: str,
    total_requests: int,
    concurrency: int,
    payload: dict,
    timeout_s: float,
) -> tuple[list[RequestResult], float]:
    parsed: urllib.parse.ParseResult = urllib.parse.urlparse(url)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    path = parsed.path or "/"
    if parsed.query:
        path = f"{path}?{parsed.query}"

    payload_bytes: bytes = json.dumps(payload).encode(encoding="utf-8")

    queue: asyncio.Queue[int] = asyncio.Queue()
    for i in range(total_requests):
        queue.put_nowait(item=i)

    results: list[RequestResult] = []
    worker_count: int = min(concurrency, total_requests)

    start_total: float = time.perf_counter()
    workers = [
        asyncio.create_task(
            coro=run_worker(queue, host, port, path, payload_bytes, timeout_s, results)
        )
        for _ in range(worker_count)
    ]
    await asyncio.gather(*workers)
    total_duration_s: float = time.perf_counter() - start_total

    return results, total_duration_s


def print_summary(results: list[RequestResult], total_duration_s: float) -> None:
    total_sent: int = len(results)
    if total_sent == 0:
        print("No requests were sent.")
        return

    durations: list[float] = [r.duration_ms for r in results]
    successes: list[RequestResult] = [r for r in results if r.success]
    failures: list[RequestResult] = [r for r in results if not r.success]

    success_rate: float = (len(successes) / total_sent) * 100.0
    avg_duration: float = statistics.mean(durations)
    median_duration: float = statistics.median(durations)
    min_duration: float = min(durations)
    max_duration: float = max(durations)
    p95_duration: float = (
        statistics.quantiles(durations, n=100)[94] if total_sent >= 100 else max_duration
    )
    rps = total_sent / total_duration_s if total_duration_s > 0 else 0.0

    print("\n" + "=" * 55)
    print("               LOAD TEST SUMMARY")
    print("=" * 55)
    print(f"Total Requests:     {total_sent}")
    print(f"Successful:         {len(successes)}")
    print(f"Failed:             {len(failures)}")
    print(f"Success Rate:       {success_rate:.2f}%")
    print(f"Total Wall Time:    {total_duration_s:.4f} s")
    print(f"Throughput:         {rps:.2f} req/s")
    print("-" * 55)
    print("Latency (per request):")
    print(f"  Average:          {avg_duration:.2f} ms")
    print(f"  Median (P50):     {median_duration:.2f} ms")
    print(f"  P95:              {p95_duration:.2f} ms")
    print(f"  Min:              {min_duration:.2f} ms")
    print(f"  Max:              {max_duration:.2f} ms")
    print("=" * 55)

    if failures:
        status_counts: dict[str, int] = {}
        for f in failures:
            key: str = f"HTTP {f.status_code}" if f.status_code > 0 else f"Error ({f.error})"
            status_counts[key] = status_counts.get(key, 0) + 1
        print("Failure breakdown:")
        for reason, count in status_counts.items():
            print(f"  - {reason}: {count}")
        print("=" * 55)


def main() -> None:
    parser = argparse.ArgumentParser(description="Bare-minimum HTTP Load Tester")
    parser.add_argument(
        "-n",
        "--requests",
        type=int,
        default=100,
        help="Total number of requests to send (default: 100)",
    )
    parser.add_argument(
        "-c",
        "--concurrency",
        type=int,
        default=10,
        help="Number of concurrent worker connections (default: 10)",
    )
    parser.add_argument(
        "-u",
        "--url",
        type=str,
        default="http://127.0.0.1:8000/api/v1/exec-test",
        help="Target URL (default: http://127.0.0.1:8000/api/v1/exec-test)",
    )
    parser.add_argument(
        "--test-type",
        type=str,
        default="REST",
        help="Test type in payload (default: REST)",
    )
    parser.add_argument(
        "--target",
        type=str,
        default="https://api.example.com",
        help="Target field in payload (default: https://api.example.com)",
    )
    parser.add_argument(
        "-t",
        "--timeout",
        type=float,
        default=10.0,
        help="Request timeout in seconds (default: 10.0)",
    )

    args: argparse.Namespace = parser.parse_args()
    payload: dict[str, Any] = {"type": args.test_type, "target": args.target}

    print(
        f"Starting load test: {args.requests} requests to {args.url} "
        f"(concurrency: {args.concurrency}, test_type: '{args.test_type}')"
    )

    results, total_duration = asyncio.run(
        main=run_load_test(
            url=args.url,
            total_requests=args.requests,
            concurrency=args.concurrency,
            payload=payload,
            timeout_s=args.timeout,
        )
    )

    print_summary(results, total_duration_s=total_duration)


if __name__ == "__main__":
    main()
