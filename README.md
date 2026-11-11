# Applicants - Coding Task

## Task Description

You are free to modify the code in any way you want. You just need to guarantee that the logical behavior of responses does not change.

1. The service currently crashes immediately upon startup. Please locate and fix the syntax issue so that the application can start.
2. We observed a high 95 percentile on the request response during load testing. Please identify the root cause (you can use a profilers like `cProfile`, `tachyon (on py > py3.15)` or `sentry`) and patch the program. Use `uv run python load_test.py -n 500 -c 25 -u http://127.0.0.1:1338/api/v1/exec-test` to see the results for yourself and to test your changes.
3. In the application logs we observed a `RuntimeWarning` and test run records are not being stored in the repository. Please identify and fix the issue.
4. Under concurrent traffic with differing targets we observed data leakage where a response intermittently returns target details from another request. Please identify the shared state issue and ensure request isolation. Use `uv run python concurrency_test.py -n 100 -c 20 -u http://127.0.0.1:1338/api/v1/exec-test` to reproduce the issue and verify your fix.
5. After running concurrent load tests the execution count returned by `/api/v1/stats` is significantly lower than the total number of requests dispatched. What causes this? Mitigate the issue.
    - hint: curl the stats endpoint after load testing and compare the tracked amount of requests and the amount of requests sent

## Running the example

```bash
uv run uvicorn src.example:app --host 127.0.0.1 --port 1338 --http httptools
```

### Load Testing

```bash
uv run python load_test.py -n 500 -c 25 -u http://127.0.0.1:1338/api/v1/exec-test
```

### Concurrency / Isolation Testing

```bash
uv run python concurrency_test.py -n 100 -c 20 -u http://127.0.0.1:1338/api/v1/exec-test
```

### Manual Testing

REST endpoint (success):

```bash
curl -X POST "http://127.0.0.1:1338/api/v1/exec-test" -H "Content-Type: application/json" -d '{"type": "REST", "target": "https://api.example.com/users"}'
```

GraphQL endpoint (success):

```bash
curl -X POST "http://127.0.0.1:1338/api/v1/exec-test" -H "Content-Type: application/json" -d '{"type": "GRAPHQL", "target": "https://api.example.com/graphql"}'
```

Unknown type (failure):

```bash
curl -X POST "http://127.0.0.1:1338/api/v1/exec-test" -H "Content-Type: application/json" -d '{"type": "SOAP", "target": "https://api.example.com/soap"}'
```

Stats endpoint:

```bash
curl "http://127.0.0.1:1338/api/v1/stats"
```
