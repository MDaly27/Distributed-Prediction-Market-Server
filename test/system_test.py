#!/usr/bin/env python3
import argparse
import asyncio
import json
import os
import signal
import subprocess
import sys
import time
import traceback
import uuid
from dataclasses import dataclass
from pathlib import Path

import asyncpg


@dataclass
class ManagedProc:
    name: str
    proc: subprocess.Popen
    log_path: Path
    log_file: object


def _uuid() -> str:
    return str(uuid.uuid4())


def _uuid_csv(values: list[str]) -> str:
    return ", ".join(f"'{v}'::uuid" for v in values)


def _is_retryable_occ(exc: Exception) -> bool:
    if isinstance(exc, asyncpg.PostgresError):
        if exc.sqlstate in {"40001", "40P01"}:
            return True
    return "OC000" in str(exc)


def _build_submit_message(
    auth_token: str,
    request_id: str,
    account_id: str,
    market_id: str,
    side: str,
    qty: int,
    price_cents: int,
    tif: str = "GTC",
) -> bytes:
    msg = {
        "action": "submit_order",
        "auth_token": auth_token,
        "request": {
            "request_id": request_id,
            "account_id": account_id,
            "market_id": market_id,
            "side": side,
            "qty": qty,
            "price_cents": price_cents,
            "time_in_force": tif,
            "ingress_ts_ns": time.time_ns(),
        },
    }
    return (json.dumps(msg, separators=(",", ":")) + "\n").encode("utf-8")


def _build_cancel_message(
    auth_token: str,
    cancel_id: str,
    order_id: str,
    account_id: str,
    reason: str,
) -> bytes:
    msg = {
        "action": "cancel_order",
        "auth_token": auth_token,
        "request": {
            "cancel_id": cancel_id,
            "order_id": order_id,
            "account_id": account_id,
            "reason": reason,
        },
    }
    return (json.dumps(msg, separators=(",", ":")) + "\n").encode("utf-8")


async def _send_tcp_json(host: str, port: int, payload: bytes) -> dict:
    reader, writer = await asyncio.open_connection(host, port)
    try:
        writer.write(payload)
        await writer.drain()
        line = await reader.readline()
        return json.loads(line.decode("utf-8"))
    finally:
        writer.close()
        await writer.wait_closed()


async def _wait_for_listener(host: str, port: int, timeout_s: float = 20.0) -> None:
    start = time.monotonic()
    payload = b'{"action":"ping"}\n'
    while time.monotonic() - start < timeout_s:
        try:
            resp = await _send_tcp_json(host, port, payload)
            if resp.get("ok") and resp.get("pong"):
                return
        except Exception:
            pass
        await asyncio.sleep(0.25)
    raise RuntimeError(f"listener on {host}:{port} did not become ready")


def _start_proc(name: str, cmd: list[str], cwd: Path, env: dict[str, str], log_dir: Path) -> ManagedProc:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{name}.log"
    log_file = log_path.open("w", encoding="utf-8")
    proc = subprocess.Popen(cmd, cwd=str(cwd), env=env, stdout=log_file, stderr=log_file)
    return ManagedProc(name=name, proc=proc, log_path=log_path, log_file=log_file)


def _tail(path: Path, n: int = 80) -> str:
    if not path.exists():
        return "<no log file>"
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return "\n".join(lines[-n:])


def _stop_procs(procs: list[ManagedProc]) -> None:
    for p in procs:
        if p.proc.poll() is None:
            p.proc.send_signal(signal.SIGTERM)
    deadline = time.monotonic() + 8
    for p in procs:
        if p.proc.poll() is None:
            remain = max(0.0, deadline - time.monotonic())
            try:
                p.proc.wait(timeout=remain)
            except subprocess.TimeoutExpired:
                p.proc.kill()
    for p in procs:
        try:
            p.log_file.close()
        except Exception:
            pass


async def _create_pool(dsn: str) -> asyncpg.Pool:
    async def _dsql_safe_reset(_conn: asyncpg.Connection) -> None:
        return None

    return await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=5, reset=_dsql_safe_reset)


async def _seed_data(conn: asyncpg.Connection, run_id: str) -> dict:
    accounts = {"a_yes": _uuid(), "a_no": _uuid(), "a_third": _uuid()}
    markets = {"m1": _uuid(), "m2": _uuid(), "m3": _uuid()}

    await conn.execute(
        """
        INSERT INTO accounts (account_id, external_user_id, username, status, available_cash_cents, locked_cash_cents)
        VALUES
          ($1, $2, $3, 'ACTIVE', 200000, 0),
          ($4, $5, $6, 'ACTIVE', 200000, 0),
          ($7, $8, $9, 'ACTIVE', 200000, 0)
        """,
        accounts["a_yes"],
        f"{run_id}-user-yes",
        f"{run_id}-yes",
        accounts["a_no"],
        f"{run_id}-user-no",
        f"{run_id}-no",
        accounts["a_third"],
        f"{run_id}-user-third",
        f"{run_id}-third",
    )

    for key, desc, resolve_interval in [
        ("m1", "cross + partial", "30 minutes"),
        ("m2", "full cross", "30 minutes"),
        ("m3", "open unmatched", "2 days"),
    ]:
        await conn.execute(
            """
            INSERT INTO markets (
                market_id, slug, title, description, status,
                tick_size_cents, min_price_cents, max_price_cents,
                close_time, resolve_time, created_by
            )
            VALUES (
                $1, $2, $3, $4, 'ACTIVE',
                1, 1, 99,
                now() + interval '1 day',
                now() + ($5::text || ' minutes')::interval,
                $6
            )
            """,
            markets[key],
            f"{run_id}-{key}",
            f"{run_id} {key}",
            desc,
            "30" if resolve_interval == "30 minutes" else "2880",
            accounts["a_yes"],
        )

    return {"accounts": accounts, "markets": markets}


async def _cleanup(conn: asyncpg.Connection, market_ids: list[str], account_ids: list[str]) -> None:
    m_ids = _uuid_csv(market_ids)
    a_ids = _uuid_csv(account_ids)
    await conn.execute(f"DELETE FROM market_settlements WHERE market_id IN ({m_ids})")
    await conn.execute(f"DELETE FROM market_settlement_runs WHERE market_id IN ({m_ids})")
    await conn.execute(f"DELETE FROM market_resolutions WHERE market_id IN ({m_ids})")

    trade_rows = await conn.fetch(f"SELECT trade_id FROM trades WHERE market_id IN ({m_ids})")
    trade_ids = [str(r["trade_id"]) for r in trade_rows]
    if trade_ids:
        t_ids = _uuid_csv(trade_ids)
        await conn.execute(f"DELETE FROM trade_parties WHERE trade_id IN ({t_ids})")

    await conn.execute(
        f"DELETE FROM ledger_entries WHERE market_id IN ({m_ids}) OR account_id IN ({a_ids})"
    )
    await conn.execute(
        f"DELETE FROM order_cancels WHERE order_id IN (SELECT request_id FROM orders WHERE market_id IN ({m_ids}))"
    )
    await conn.execute(f"DELETE FROM trades WHERE market_id IN ({m_ids})")
    await conn.execute(f"DELETE FROM positions WHERE market_id IN ({m_ids})")
    await conn.execute(f"DELETE FROM orders WHERE market_id IN ({m_ids})")
    await conn.execute(f"DELETE FROM execution_market_leases WHERE market_id IN ({m_ids})")
    await conn.execute(f"DELETE FROM matchmaker_market_leases WHERE market_id IN ({m_ids})")
    await conn.execute(f"DELETE FROM markets WHERE market_id IN ({m_ids})")
    await conn.execute(f"DELETE FROM accounts WHERE account_id IN ({a_ids})")


async def _cleanup_with_retries(
    conn: asyncpg.Connection, market_ids: list[str], account_ids: list[str]
) -> None:
    attempts = 6
    for i in range(attempts):
        try:
            await _cleanup(conn, market_ids, account_ids)
            return
        except Exception as exc:
            if not _is_retryable_occ(exc) or i == attempts - 1:
                raise
            await asyncio.sleep(0.15 * (i + 1))


async def _run_test(args: argparse.Namespace) -> None:
    repo = Path(__file__).resolve().parents[1]
    listener_dir = repo / "client" / "client-listener"
    matchmaker_dir = repo / "matchmaker"
    executor_dir = repo / "executor"
    log_dir = repo / "test" / ".logs"

    procs: list[ManagedProc] = []
    pool = await _create_pool(args.db_dsn)
    run_id = f"testrun-{uuid.uuid4().hex[:10]}"
    test_data: dict | None = None

    try:
        base_env = os.environ.copy()

        for idx, port in enumerate([args.listener_port_a, args.listener_port_b], start=1):
            env = base_env.copy()
            env.update(
                {
                    "CLIENT_LISTENER_HOST": args.listener_host,
                    "CLIENT_LISTENER_PORT": str(port),
                    "CLIENT_LISTENER_DB_DSN": args.db_dsn,
                    "CLIENT_LISTENER_AUTH_MODE": "static-token",
                    "CLIENT_LISTENER_AUTH_TOKEN": args.auth_token,
                }
            )
            procs.append(
                _start_proc(f"listener-{idx}", [sys.executable, "server.py"], listener_dir, env, log_dir)
            )

        for idx in range(1, 3):
            env = base_env.copy()
            env.update(
                {
                    "MATCHMAKER_DB_DSN": args.db_dsn,
                    "MATCHMAKER_INSTANCE_ID": f"{run_id}-mm-{idx}",
                    "MATCHMAKER_POLL_INTERVAL_MS": "200",
                }
            )
            procs.append(
                _start_proc(f"matchmaker-{idx}", [sys.executable, "matchmaker.py"], matchmaker_dir, env, log_dir)
            )

        for idx in range(1, 3):
            env = base_env.copy()
            env.update(
                {
                    "EXECUTOR_DB_DSN": args.db_dsn,
                    "EXECUTOR_INSTANCE_ID": f"{run_id}-ex-{idx}",
                    "EXECUTOR_POLL_INTERVAL_MS": "300",
                }
            )
            procs.append(
                _start_proc(f"executor-{idx}", [sys.executable, "executor.py"], executor_dir, env, log_dir)
            )

        await _wait_for_listener(args.listener_host, args.listener_port_a)
        await _wait_for_listener(args.listener_host, args.listener_port_b)

        async with pool.acquire() as conn:
            test_data = await _seed_data(conn, run_id)

        a_yes = test_data["accounts"]["a_yes"]
        a_no = test_data["accounts"]["a_no"]
        a_third = test_data["accounts"]["a_third"]
        m1 = test_data["markets"]["m1"]
        m2 = test_data["markets"]["m2"]
        m3 = test_data["markets"]["m3"]

        order_ids = {
            "m1_yes": _uuid(),
            "m1_no_cross": _uuid(),
            "m1_no_open": _uuid(),
            "m2_yes_cross": _uuid(),
            "m2_no_cross": _uuid(),
            "m3_yes_open": _uuid(),
            "m3_no_open": _uuid(),
        }

        submissions = [
            (args.listener_port_a, order_ids["m1_yes"], a_yes, m1, "YES", 2, 60),
            (args.listener_port_b, order_ids["m1_no_cross"], a_no, m1, "NO", 1, 40),
            (args.listener_port_a, order_ids["m1_no_open"], a_third, m1, "NO", 1, 35),
            (args.listener_port_b, order_ids["m2_yes_cross"], a_yes, m2, "YES", 1, 45),
            (args.listener_port_a, order_ids["m2_no_cross"], a_no, m2, "NO", 1, 60),
            (args.listener_port_b, order_ids["m3_yes_open"], a_yes, m3, "YES", 1, 30),
            (args.listener_port_a, order_ids["m3_no_open"], a_no, m3, "NO", 1, 20),
        ]

        for port, req_id, acct, mkt, side, qty, price in submissions:
            resp = await _send_tcp_json(
                args.listener_host,
                port,
                _build_submit_message(args.auth_token, req_id, acct, mkt, side, qty, price),
            )
            if not resp.get("ok"):
                raise AssertionError(f"order submission failed for {req_id}: {resp}")

        await asyncio.sleep(args.match_wait_seconds)

        cancel_id = _uuid()
        cancel_resp = await _send_tcp_json(
            args.listener_host,
            args.listener_port_b,
            _build_cancel_message(
                args.auth_token,
                cancel_id=cancel_id,
                order_id=order_ids["m3_no_open"],
                account_id=a_no,
                reason="system-test-cancel-open-order",
            ),
        )
        if not cancel_resp.get("ok"):
            raise AssertionError(f"cancel request failed: {cancel_resp}")
        cancel_info = cancel_resp.get("cancel", {})
        if cancel_info.get("status") != "CANCELLED":
            raise AssertionError(f"unexpected cancel status: {cancel_resp}")
        if int(cancel_info.get("unlocked_cash_cents", -1)) != 20:
            raise AssertionError(f"unexpected cancel unlock amount: {cancel_resp}")

        cancel_retry_resp = await _send_tcp_json(
            args.listener_host,
            args.listener_port_b,
            _build_cancel_message(
                args.auth_token,
                cancel_id=cancel_id,
                order_id=order_ids["m3_no_open"],
                account_id=a_no,
                reason="retry-same-cancel-id",
            ),
        )
        if not cancel_retry_resp.get("ok"):
            raise AssertionError(f"idempotent cancel retry failed: {cancel_retry_resp}")
        if not cancel_retry_resp.get("cancel", {}).get("idempotent"):
            raise AssertionError(f"cancel retry should be idempotent: {cancel_retry_resp}")

        async with pool.acquire() as conn:
            all_order_ids = list(order_ids.values())
            all_oids = _uuid_csv(all_order_ids)
            m123 = _uuid_csv([m1, m2, m3])
            rows = await conn.fetch(
                f"SELECT request_id, remaining_qty, status FROM orders WHERE request_id IN ({all_oids})",
            )
            state = {str(r["request_id"]): (int(r["remaining_qty"]), str(r["status"])) for r in rows}

            expected = {
                order_ids["m1_yes"]: (1, "PARTIALLY_FILLED"),
                order_ids["m1_no_cross"]: (0, "FILLED"),
                order_ids["m1_no_open"]: (1, "ACCEPTED"),
                order_ids["m2_yes_cross"]: (0, "FILLED"),
                order_ids["m2_no_cross"]: (0, "FILLED"),
                order_ids["m3_yes_open"]: (1, "ACCEPTED"),
                order_ids["m3_no_open"]: (0, "CANCELLED"),
            }
            for req_id, exp in expected.items():
                got = state.get(req_id)
                if got != exp:
                    raise AssertionError(f"unexpected order state for {req_id}: got={got} expected={exp}")

            trade_count = await conn.fetchval(
                f"SELECT count(*) FROM trades WHERE market_id IN ({m123})",
            )
            if int(trade_count) != 2:
                raise AssertionError(f"expected 2 trades after matching, got {trade_count}")

            open_count = await conn.fetchval(
                f"""
                SELECT count(*) FROM orders
                WHERE request_id IN ({_uuid_csv([order_ids["m1_yes"], order_ids["m1_no_open"], order_ids["m3_yes_open"], order_ids["m3_no_open"]])})
                  AND status IN ('ACCEPTED', 'OPEN', 'PARTIALLY_FILLED')
                  AND remaining_qty > 0
                """,
            )
            if int(open_count) != 3:
                raise AssertionError(f"expected 3 open unmatched/partial orders after cancel, got {open_count}")

            cancel_rows = await conn.fetchval(
                """
                SELECT count(*)
                FROM order_cancels
                WHERE cancel_id = $1
                """,
                cancel_id,
            )
            if int(cancel_rows) != 1:
                raise AssertionError(f"expected one cancel event row, got {cancel_rows}")

            unlock_rows = await conn.fetchval(
                """
                SELECT count(*)
                FROM ledger_entries
                WHERE order_id = $1
                  AND reason = 'ORDER_UNLOCK'
                """,
                order_ids["m3_no_open"],
            )
            if int(unlock_rows) != 1:
                raise AssertionError(f"expected one ORDER_UNLOCK ledger entry, got {unlock_rows}")

            await conn.execute(f"UPDATE markets SET status='RESOLVED', resolve_time=now() - interval '1 minute' WHERE market_id IN ({_uuid_csv([m1, m2])})")
            await conn.execute(
                """
                INSERT INTO market_resolutions (resolution_id, market_id, outcome, resolved_by, notes)
                VALUES
                  ($1, $2, 'YES', $3, 'test resolution m1 yes'),
                  ($4, $5, 'NO', $6, 'test resolution m2 no')
                """,
                _uuid(),
                m1,
                a_yes,
                _uuid(),
                m2,
                a_no,
            )

        await asyncio.sleep(args.execute_wait_seconds)

        async with pool.acquire() as conn:
            runs = await conn.fetch(
                f"SELECT market_id, status FROM market_settlement_runs WHERE market_id IN ({_uuid_csv([m1, m2])})",
            )
            run_map = {str(r["market_id"]): str(r["status"]) for r in runs}
            for mid in [m1, m2]:
                if run_map.get(mid) != "COMPLETED":
                    raise AssertionError(f"settlement run for market {mid} not completed: {run_map.get(mid)}")

            settlements = await conn.fetch(
                f"SELECT market_id, account_id, payout_cents FROM market_settlements WHERE market_id IN ({_uuid_csv([m1, m2])})",
            )
            payout = {(str(r["market_id"]), str(r["account_id"])): int(r["payout_cents"]) for r in settlements}
            expected_payouts = {
                (m1, a_yes): 100,
                (m1, a_no): 0,
                (m2, a_yes): 0,
                (m2, a_no): 100,
            }
            for k, exp in expected_payouts.items():
                got = payout.get(k)
                if got != exp:
                    raise AssertionError(f"unexpected payout {k}: got={got} expected={exp}")

            m3_open = await conn.fetchval(
                """
                SELECT count(*) FROM orders
                WHERE market_id = $1
                  AND status IN ('ACCEPTED', 'OPEN', 'PARTIALLY_FILLED')
                  AND remaining_qty > 0
                """,
                m3,
            )
            if int(m3_open) != 1:
                raise AssertionError(f"expected 1 open order in unresolved market m3 after cancel, got {m3_open}")

        print("SYSTEM TEST PASSED")

    except Exception:
        print("\n--- component logs (tail) ---")
        for p in procs:
            print(f"\n[{p.name}] {p.log_path}")
            print(_tail(p.log_path))
        raise
    finally:
        try:
            if test_data is not None:
                async with pool.acquire() as conn:
                    await _cleanup_with_retries(
                        conn,
                        list(test_data["markets"].values()),
                        list(test_data["accounts"].values()),
                    )
        finally:
            await pool.close()
            _stop_procs(procs)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="End-to-end system test with concurrent listeners/matchmakers/executors")
    p.add_argument("--db-dsn", required=True, help="Full PostgreSQL/DSQL DSN with credentials")
    p.add_argument("--auth-token", default="dev-shared-token")
    p.add_argument("--listener-host", default="127.0.0.1")
    p.add_argument("--listener-port-a", type=int, default=9101)
    p.add_argument("--listener-port-b", type=int, default=9102)
    p.add_argument("--match-wait-seconds", type=float, default=4.0)
    p.add_argument("--execute-wait-seconds", type=float, default=5.0)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    try:
        asyncio.run(_run_test(args))
        return 0
    except Exception as exc:
        print(f"SYSTEM TEST FAILED: {exc}")
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
