"""Smoke test for the gateway against a real Redis in Docker.

Needs: Redis container named ember-redis on port 6379, gateway on port 8000.
Run:   ..\\.venv\\Scripts\\python smoke_test.py
"""
import json
import subprocess
import time
import urllib.error
import urllib.request

BASE = "http://localhost:8000"
REDIS = "ember-redis"
failures = 0


def call(method, path, body=None):
    data = body.encode() if isinstance(body, str) else json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, json.loads(resp.read() or b"null")
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, json.loads(raw)
        except json.JSONDecodeError:
            return e.code, raw.decode(errors="replace")


def check(name, got_status, want_status, extra_ok=True):
    global failures
    ok = got_status == want_status and extra_ok
    failures += not ok
    print(f"{'PASS' if ok else 'FAIL'}  {name:<42} got {got_status}, want {want_status}")


def docker(*args):
    subprocess.run(["docker", *args], check=True, capture_output=True)


def redis_cli(*args):
    out = subprocess.run(["docker", "exec", REDIS, "redis-cli", *args],
                         check=True, capture_output=True, text=True)
    return out.stdout.strip()


def gen(body):
    return call("POST", "/v1/generate", body)


print("== happy path")
s, b = call("GET", "/health")
check("health ok", s, 200)
before = call("GET", "/v1/queue")[1]["length"]
s, b = gen({"model": "mock", "prompt": "hello"})
job = b.get("job_id", "") if isinstance(b, dict) else ""
check("generate -> 202 + 12-char id", s, 202, len(job) == 12)
check("queue length +1", 200, 200, call("GET", "/v1/queue")[1]["length"] == before + 1)
s, b = call("GET", f"/v1/result/{job}")
check("result -> queued", s, 200, b.get("status") == "queued")
check("status key has TTL", 200, 200, 0 < int(redis_cli("TTL", f"status:{job}")) <= 3600)
check("max_tokens=1024 accepted", gen({"model": "mock", "prompt": "x", "max_tokens": 1024})[0], 202)

print("== validation")
check("empty prompt", gen({"model": "mock", "prompt": ""})[0], 422)
check("whitespace prompt", gen({"model": "mock", "prompt": "   "})[0], 422)
check("prompt 8001 chars", gen({"model": "mock", "prompt": "a" * 8001})[0], 422)
check("missing model", gen({"prompt": "hi"})[0], 422)
check("empty model", gen({"model": "", "prompt": "hi"})[0], 422)
check("max_tokens=0", gen({"model": "mock", "prompt": "hi", "max_tokens": 0})[0], 422)
check("max_tokens=1025", gen({"model": "mock", "prompt": "hi", "max_tokens": 1025})[0], 422)
check("body is not JSON", gen("hello")[0], 422)

print("== results")
check("unknown job -> 404", call("GET", "/v1/result/doesnotexist")[0], 404)
redis_cli("SET", "result:smoke_ok", '{"text":"hi"}', "EX", "60")
s, b = call("GET", "/v1/result/smoke_ok")
check("done result parsed", s, 200, b.get("result") == {"text": "hi"})
redis_cli("SET", "result:smoke_bad", "not json", "EX", "60")
s, b = call("GET", "/v1/result/smoke_bad")
check("corrupt result -> clean 500", s, 500, isinstance(b, dict) and "detail" in b)

print("== redis stopped")
docker("stop", REDIS)
try:
    for name, (m, p, body) in {
        "health": ("GET", "/health", None),
        "generate": ("POST", "/v1/generate", {"model": "mock", "prompt": "hi"}),
        "result": ("GET", "/v1/result/abc", None),
        "queue": ("GET", "/v1/queue", None),
    }.items():
        check(f"{name} -> 503", call(m, p, body)[0], 503)
finally:
    docker("run", "-d", "--rm", "--name", REDIS, "-p", "6379:6379", "redis:7")
    time.sleep(2)

print("== redis hung (paused)")
docker("pause", REDIS)
try:
    t0 = time.time()
    check("health -> 503", call("GET", "/health")[0], 503)
    check("generate -> 503", gen({"model": "mock", "prompt": "hi"})[0], 503)
    print(f"      (hung-Redis requests took {time.time() - t0:.1f}s)")
finally:
    docker("unpause", REDIS)

print("== recovered")
check("health ok again", call("GET", "/health")[0], 200)

print(f"\n{'ALL PASSED' if failures == 0 else f'{failures} FAILED'}")
raise SystemExit(1 if failures else 0)
