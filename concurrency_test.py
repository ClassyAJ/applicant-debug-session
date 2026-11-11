"""
uv run python concurrency_test.py -n 100 -c 20 -u http://127.0.0.1:1338/api/v1/exec-test
"""
import argparse
import asyncio
import http.client
import json
import sys
import time
import urllib.parse
from asyncio.events import AbstractEventLoop
from dataclasses import dataclass


@dataclass(slots=True)
class IsolationResult:
    request_id: int
    sent_target: str
    status_code: int
    duration_ms: float
    success: bool
    leaked: bool
    response_message: str | None = None
    error: str | None = None


def send_isolated_request(
    request_id: int,
    host: str,
    port: int,
    path: str,
    base_target: str,
    test_type: str,
    timeout_s: float,
) -> IsolationResult:
    target = f"{base_target}/{request_id}"
    payload: dict[str, str] = {"type": test_type, "target": target}
    payload_bytes: bytes = json.dumps(payload).encode(encoding="utf-8")

    start_time: float = time.perf_counter()
    conn = http.client.HTTPConnection(host=host, port=port, timeout=timeout_s)
    try:
        headers: dict[str, str] = {
            "Content-Type": "application/json",
            "Content-Length": str(len(payload_bytes)),
            "Connection": "keep-alive",
        }
        conn.request(method="POST", url=path, body=payload_bytes, headers=headers)
        response: http.client.HTTPResponse = conn.getresponse()
        raw_body: bytes = response.read()
        duration_ms: float = (time.perf_counter() - start_time) * 1000.0

        if not (200 <= response.status < 300):
            return IsolationResult(
                request_id=request_id,
                sent_target=target,
                status_code=response.status,
                duration_ms=duration_ms,
                success=False,
                leaked=False,
                error=f"HTTP {response.status}: {raw_body.decode(encoding='utf-8', errors='replace')}",
            )

        data = json.loads(raw_body)
        message = data.get("message", "")
        is_leaked: bool = target not in message

        return IsolationResult(
            request_id=request_id,
            sent_target=target,
            status_code=response.status,
            duration_ms=duration_ms,
            success=not is_leaked,
            leaked=is_leaked,
            response_message=message,
        )
    except Exception as exc:  # noqa: BLE001
        duration_ms = (time.perf_counter() - start_time) * 1000.0
        return IsolationResult(
            request_id=request_id,
            sent_target=target,
            status_code=0,
            duration_ms=duration_ms,
            success=False,
            leaked=False,
            error=str(exc),
        )
    finally:
        conn.close()


async def run_worker(
    queue: asyncio.Queue[int],
    host: str,
    port: int,
    path: str,
    base_target: str,
    test_type: str,
    timeout_s: float,
    results: list[IsolationResult],
) -> None:
    loop: AbstractEventLoop = asyncio.get_running_loop()
    while not queue.empty():
        try:
            req_id: int = queue.get_nowait()
        except asyncio.QueueEmpty:
            break

        result: IsolationResult = await loop.run_in_executor(
            None,
            send_isolated_request,
            req_id,
            host,
            port,
            path,
            base_target,
            test_type,
            timeout_s,
        )
        results.append(result)
        queue.task_done()


async def run_isolation_test(
    url: str,
    total_requests: int,
    concurrency: int,
    base_target: str,
    test_type: str,
    timeout_s: float,
) -> tuple[list[IsolationResult], float]:
    parsed: urllib.parse.ParseResult = urllib.parse.urlparse(url)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    path = parsed.path or "/"
    if parsed.query:
        path = f"{path}?{parsed.query}"

    queue: asyncio.Queue[int] = asyncio.Queue()
    for i in range(total_requests):
        queue.put_nowait(item=i)

    results: list[IsolationResult] = []
    worker_count: int = min(concurrency, total_requests)

    start_total: float = time.perf_counter()
    workers = [
        asyncio.create_task(
            coro=run_worker(
                queue, host, port, path, base_target, test_type, timeout_s, results
            )
        )
        for _ in range(worker_count)
    ]
    await asyncio.gather(*workers)
    total_duration_s: float = time.perf_counter() - start_total

    return results, total_duration_s


def print_summary(results: list[IsolationResult], total_duration_s: float) -> bool:
    total_sent = len(results)
    if total_sent == 0:
        print("No requests were sent.")
        return False

    clean_results: list[IsolationResult] = [r for r in results if r.success and not r.leaked]
    leaked_results: list[IsolationResult] = [r for r in results if r.leaked]
    error_results: list[IsolationResult] = [r for r in results if not r.success and not r.leaked]

    isolation_rate: float = (len(clean_results) / total_sent) * 100.0

    print("\n" + "=" * 60)
    print("           CONCURRENCY & ISOLATION TEST SUMMARY")
    print("=" * 60)
    print(f"Total Requests Sent:        {total_sent}")
    print(f"Clean (Isolated) Responses: {len(clean_results)}")
    print(f"Leaked (Polluted) Responses: {len(leaked_results)}")
    print(f"Network / HTTP Errors:      {len(error_results)}")
    print(f"Total Wall Time:            {total_duration_s:.4f} s")
    print("-" * 60)
    print(f"Isolation Success Rate:     {isolation_rate:.2f}%")
    print("=" * 60)

    if leaked_results:
        print("\nSample Data Leakage Incidents (Cross-talk):")
        for sample in leaked_results[:5]:
            print(f"  - Request #{sample.request_id}:")
            print(f"      Sent target:   {sample.sent_target}")
            print(f"      Got response:  {sample.response_message}")
        print("=" * 60)
        print("RESULT: FAILED -> State pollution detected under concurrency.")
        print("=" * 60)
        return False

    if error_results:
        print("\nErrors encountered:")
        for err in error_results[:5]:
            print(f"  - Request #{err.request_id}: {err.error}")
        print("=" * 60)
        print("RESULT: FAILED -> Request errors occurred.")
        print("=" * 60)
        return False

    print("RESULT: PASSED -> All requests strictly isolated under concurrency.")
    print("=" * 60)
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Concurrent Request Isolation Tester")
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
        default=20,
        help="Number of concurrent worker connections (default: 20)",
    )
    parser.add_argument(
        "-u",
        "--url",
        type=str,
        default="http://127.0.0.1:1338/api/v1/exec-test",
        help="Target URL (default: http://127.0.0.1:1338/api/v1/exec-test)",
    )
    parser.add_argument(
        "--test-type",
        type=str,
        default="REST",
        help="Test type in payload (default: REST)",
    )
    parser.add_argument(
        "--base-target",
        type=str,
        default="https://api.example.com/user",
        help="Base target URL to append request IDs to (default: https://api.example.com/user)",
    )
    parser.add_argument(
        "-t",
        "--timeout",
        type=float,
        default=10.0,
        help="Request timeout in seconds (default: 10.0)",
    )

    args: argparse.Namespace = parser.parse_args()

    print(
        f"Starting concurrency test: {args.requests} requests to {args.url} "
        f"(concurrency: {args.concurrency}, test_type: '{args.test_type}')"
    )

    results, total_duration = asyncio.run(
        run_isolation_test(
            url=args.url,
            total_requests=args.requests,
            concurrency=args.concurrency,
            base_target=args.base_target,
            test_type=args.test_type,
            timeout_s=args.timeout,
        )
    )

    passed: bool = print_summary(results, total_duration_s=total_duration)
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
