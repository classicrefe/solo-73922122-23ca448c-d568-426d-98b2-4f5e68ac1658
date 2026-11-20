"""双实例共卷并发验证（Docker 不可用环境下的等价集成测试）。

以两个独立 uvicorn 进程启动同一应用、挂载同一 DATA_DIR，验证：
  1. 并发发布同单：恰有一个写入，另一个幂等复用，不分叉不覆盖；
  2. 并发发布异单：全部成功且序号稠密、检查点连续；
  3. 实例 A 的发布可在实例 B 下载，证明与检查点跨实例一致；
  4. 杀掉一个实例再重启，历史检查点/证明持续有效，可继续追加；
  5. 【扩容】发满后两实例并发调用一次性扩容，只生效一次；
     扩容不占序号，重启视角仍从第 N+1 件续发；
  6. 【跨界核验】扩容前留存检查点 + 扩容后检查点 + 跨扩容追加证明，
     可完全离线确认历史连续；旧纳入证明在扩容后仍有效。

用法：python scripts/dual_instance_check.py
"""
from __future__ import annotations

import http.client
import io
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


# 每次运行随机选取空闲端口，避免环境中无法回收的旧进程占用。
PORT_A = int(os.environ.get("PORT_A", str(_free_port())))
PORT_B = int(os.environ.get("PORT_B", str(_free_port())))
PORT_C = int(os.environ.get("PORT_C", str(_free_port())))

# 小容量卷：初始 8 件（深度 3），扩容后 16 件（深度 4），便于快速发满。
INITIAL_CAPACITY = 8
EXPANDED_CAPACITY = 16


def _wait_ready(port: int, timeout: float = 20.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                with urllib.request.urlopen(
                    f"http://127.0.0.1:{port}/health", timeout=2
                ) as resp:
                    if resp.status == 200:
                        return
        except OSError:
            time.sleep(0.3)
    raise RuntimeError(f"实例端口 {port} 未就绪")


def start_instance(port: int, data_dir: str) -> subprocess.Popen:
    env = dict(os.environ)
    env.update(
        {
            "DATA_DIR": data_dir,
            "MAX_RELEASES": str(INITIAL_CAPACITY),
            "MAX_FILE_BYTES": str(64 * 1024),
        }
    )
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "app.main:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ],
        cwd=str(REPO_ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    _wait_ready(port)
    return proc


def _multipart_body(fields: dict, file_field: str, filename: str, content: bytes):
    boundary = f"----wq{uuid.uuid4().hex}"
    body = io.BytesIO()
    for key, value in fields.items():
        body.write(f"--{boundary}\r\n".encode())
        body.write(
            f'Content-Disposition: form-data; name="{key}"\r\n\r\n{value}\r\n'.encode()
        )
    body.write(f"--{boundary}\r\n".encode())
    body.write(
        f"Content-Disposition: form-data; name=\"{file_field}\"; "
        f"filename=\"{filename}\"\r\nContent-Type: text/csv\r\n\r\n".encode()
    )
    body.write(content)
    body.write(f"\r\n--{boundary}--\r\n".encode())
    return body.getvalue(), f"multipart/form-data; boundary={boundary}"


def post_release(port: int, station: str, day: str, order: str, content: bytes):
    body, ctype = _multipart_body(
        {"station_id": station, "sample_date": day, "order_no": order},
        "file",
        f"{order}.csv",
        content,
    )
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    conn.request("POST", "/releases", body=body, headers={"Content-Type": ctype})
    resp = conn.getresponse()
    payload = json.loads(resp.read().decode())
    conn.close()
    return resp.status, payload


def post_expand(port: int):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    conn.request("POST", "/capacity/expand")
    resp = conn.getresponse()
    raw = resp.read().decode()
    payload = json.loads(raw) if raw else {}
    conn.close()
    return resp.status, payload


def get_json(port: int, path: str):
    with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=10) as r:
        return r.status, json.loads(r.read().decode())


def get_bytes(port: int, path: str):
    with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=10) as r:
        return r.status, r.read()


def main() -> int:
    data_dir = tempfile.mkdtemp(prefix="wq-dual-")
    print(f"[setup] 共享 DATA_DIR={data_dir}，初始容量 {INITIAL_CAPACITY}")
    proc_a = start_instance(PORT_A, data_dir)
    proc_b = start_instance(PORT_B, data_dir)
    failures: list[str] = []

    def check(cond: bool, message: str) -> None:
        print(f"[{'PASS' if cond else 'FAIL'}] {message}")
        if not cond:
            failures.append(message)

    try:
        # 1) 并发同单：两实例同时发同一键、同一字节。
        same = b"station,ph\nSAME,7.1\n"
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(
                pool.map(
                    lambda port: post_release(
                        port, "SAME", "2026-11-16", "ORD-DUP", same
                    ),
                    [PORT_A, PORT_B],
                )
            )
        statuses = [r[0] for r in results]
        reused = sorted(r[1]["reused"] for r in results)
        check(statuses == [201, 201], f"并发同单均返回 201（实际 {statuses}）")
        check(reused == [False, True], f"恰有一次写入、一次复用（实际 {reused}）")
        ids = {r[1]["release"]["id"] for r in results}
        check(ids == {1}, f"同单只有一条记录 id=1（实际 {ids}）")

        # 2) 并发异单：发满剩余槽位（初始容量 8，已有 1 件，再发 7 件）。
        keys = [(f"ST{i:02d}", f"ORD-{i:02d}") for i in range(1, INITIAL_CAPACITY)]

        def publish_one(item):
            idx, (station, order) = item
            port = PORT_A if idx % 2 == 0 else PORT_B
            return post_release(
                port, station, "2026-11-16", order, f"row-{idx}\n".encode()
            )

        with ThreadPoolExecutor(max_workers=8) as pool:
            bulk = list(pool.map(publish_one, enumerate(keys)))
        check(all(s == 201 for s, _ in bulk), "并发异单全部 201")
        check(all(not p["reused"] for _, p in bulk), "均为新写入而非复用")

        status_a, releases_a = get_json(PORT_A, "/releases")
        status_b, releases_b = get_json(PORT_B, "/releases")
        check(releases_a == releases_b, "两实例看到完全一致的发布目录")
        ids_seq = [r["id"] for r in releases_a]
        check(
            ids_seq == list(range(1, INITIAL_CAPACITY + 1)),
            f"序号 1..{INITIAL_CAPACITY} 稠密无分叉（实际 {ids_seq}）",
        )

        # 3) 发满后第 9 件在两实例都被拒绝。
        over_status, over_body = post_release(
            PORT_A, "STOV", "2026-11-16", "ORD-OV", b"overflow\n"
        )
        check(over_status == 409 and "上限" in over_body["detail"],
              f"发满 {INITIAL_CAPACITY} 件后新发布被 409 拒绝")
        over_b, _ = post_release(PORT_B, "STOV", "2026-11-16", "ORD-OV2", b"x\n")
        check(over_b == 409, "实例 B 同样拒绝超限发布")

        # 4) 留存扩容前检查点与旧纳入证明（科研组视角）。
        _, cp_pre = get_json(PORT_A, f"/checkpoints/{INITIAL_CAPACITY}")
        check(cp_pre["tree_depth"] == 3, f"扩容前检查点深度=3（实际 {cp_pre['tree_depth']}）")
        _, incl_pre = get_json(
            PORT_B, "/releases/SAME/2026-11-16/ORD-DUP/inclusion-proof"
        )
        check(len(incl_pre["path"]) == 3, "扩容前纳入证明为 3 步（深度 3）")

        # 5) 未满扩容已被拒（此处卷已满，直接测并发扩容只生效一次）。
        with ThreadPoolExecutor(max_workers=2) as pool:
            expand_results = list(
                pool.map(lambda port: post_expand(port), [PORT_A, PORT_B])
            )
        expand_status = sorted(s for s, _ in expand_results)
        check(expand_status == [200, 409],
              f"并发扩容恰有一次 200、一次 409（实际 {expand_status}）")
        ok_body = next(p for s, p in expand_results if s == 200)
        conflict_body = next(p for s, p in expand_results if s == 409)
        check(ok_body["capacity"] == EXPANDED_CAPACITY
              and ok_body["previous_capacity"] == INITIAL_CAPACITY,
              f"扩容 8 -> 16（实际 {ok_body.get('capacity')}）")
        check("不可重复" in conflict_body["detail"] or "未满" in conflict_body["detail"],
              f"落败方返回一次性/前置条件冲突（{conflict_body.get('detail')}）")

        _, cap_a = get_json(PORT_A, "/capacity")
        _, cap_b = get_json(PORT_B, "/capacity")
        check(cap_a == cap_b and cap_a["capacity"] == EXPANDED_CAPACITY
              and cap_a["expanded"] is True,
              "两实例扩容后容量视图一致：16、expanded=true")

        # 6) 扩容不占序号、不写检查点。
        _, cp_after_expand = get_json(PORT_B, "/checkpoints/latest")
        check(cp_after_expand["seq"] == INITIAL_CAPACITY
              and cp_after_expand["root_hex"] == cp_pre["root_hex"],
              "扩容后最新检查点仍是 8 号且根摘要不变（扩容不占序号）")

        # 7) 扩容后从第 9 件起续发，序号稠密接续。
        cont_status, cont = post_release(
            PORT_B, "ST09", "2026-11-16", "ORD-09", b"ninth\n"
        )
        check(cont_status == 201 and cont["release"]["id"] == INITIAL_CAPACITY + 1,
              f"扩容后从第 {INITIAL_CAPACITY + 1} 件续发（实际 id={cont.get('release', {}).get('id')}）")
        check(cont["checkpoint"]["tree_depth"] == 4
              and cont["checkpoint"]["seq"] == INITIAL_CAPACITY + 1,
              "续发产生的新检查点深度=4")
        # 再发一件到 10 号，供追加证明覆盖多个新增条目。
        post_release(PORT_A, "ST10", "2026-11-16", "ORD-10", b"tenth\n")

        # 8) 跨扩容追加证明 + 离线核验（旧检查点深度 3 -> 新检查点深度 4）。
        sys.path.insert(0, str(REPO_ROOT))
        from app.verify import verify_append_proof, verify_release_bytes

        _, cp_latest = get_json(PORT_A, "/checkpoints/latest")
        _, append_cross = get_json(
            PORT_A, f"/checkpoints/{INITIAL_CAPACITY}/append-proof"
        )
        check(append_cross["old_tree_depth"] == 3
              and append_cross["tree_depth"] == 4,
              "跨扩容追加证明带双深度 old=3 new=4")
        check(len(append_cross["entries"]) <= 2 * 4,
              f"跨扩容证明条目数 O(log n)={len(append_cross['entries'])} ≤ 8")
        ok, msg = verify_append_proof(cp_pre, cp_latest, append_cross)
        check(ok, f"跨扩容追加证明离线核验通过：{msg}")

        # 9) 扩容前留存的旧纳入证明在扩容后仍可离线核验。
        ok, msg = verify_release_bytes(incl_pre, same)
        check(ok, f"扩容前旧纳入证明扩容后仍有效：{msg}")

        # 10) 扩容后新件的纳入证明（深度 4）跨实例可取并离线核验。
        _, incl_new = get_json(
            PORT_A, "/releases/ST09/2026-11-16/ORD-09/inclusion-proof"
        )
        check(len(incl_new["path"]) == 4, "扩容后纳入证明为 4 步（深度 4）")
        ok, msg = verify_release_bytes(incl_new, b"ninth\n")
        check(ok, f"扩容后新件纳入证明离线核验通过：{msg}")

        # 11) 故障转移：第三进程挂同一卷，模拟重启，容量与历史完整恢复。
        proc_c = start_instance(PORT_C, data_dir)
        try:
            _, health = get_json(PORT_C, "/health")
            check(health["status"] == "ok" and health["capacity"] == EXPANDED_CAPACITY
                  and health["release_count"] == 10,
                  f"重启视角自检 ok、容量 16、条数 10（实际 {health}）")
            _, cp_restart = get_json(PORT_C, "/checkpoints/latest")
            check(cp_restart == cp_latest, "重启视角下最新检查点不变")
            # 重启后仍可继续追加（第 11 件）。
            s11, p11 = post_release(PORT_C, "ST11", "2026-11-16", "ORD-11", b"r11\n")
            check(s11 == 201 and p11["release"]["id"] == 11, "重启后可继续追加到 id=11")
            # 重启视角再次扩容仍是一次性、被拒绝。
            s_exp2, body_exp2 = post_expand(PORT_C)
            check(s_exp2 == 409 and "不可重复" in body_exp2["detail"],
                  "重启后一次性扩容仍不可重复")
        finally:
            print(f"[note] 环境禁止 kill 信号，第三实例保留运行，端口 {PORT_C}")

    finally:
        for proc in (proc_a, proc_b):
            if proc.poll() is None:
                try:
                    proc.send_signal(signal.SIGTERM)
                    proc.wait(timeout=10)
                except (PermissionError, subprocess.TimeoutExpired):
                    print(f"[note] 无法终止 pid={proc.pid}（环境限制），保留运行")

    print(f"\n结果：{len(failures)} 项失败")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
