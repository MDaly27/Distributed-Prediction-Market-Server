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
    if isinstance(exc, asyncpg.PostgresError) and exc.sqlstate in {"40001", "40P01"}:
        return True
    return "OC000" in str(exc)


def _build_action(action: str, auth_token: str | None = None, request: dict | None = None) -> bytes:
    msg: dict[str, object] = {"action": action}
    if auth_token is not None:
        msg["auth_token"] = auth_token
    if request is not None:
        msg["request"] = request
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
    payload = _build_action("ping")
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
    accounts = {"a1": _uuid(), "a2": _uuid(), "a3": _uuid()}
    markets = {"m_active": _uuid(), "m_draft": _uuid()}

    await conn.execute(
        """
        INSERT INTO accounts (account_id, external_user_id, username, status, available_cash_cents, locked_cash_cents)
        VALUES
          ($1, $2, $3, 'ACTIVE', 300000, 0),
          ($4, $5, $6, 'ACTIVE', 300000, 0),
          ($7, $8, $9, 'ACTIVE', 300000, 0)
        """,
        accounts["a1"], f"{run_id}-user-a1", f"{run_id}-a1",
        accounts["a2"], f"{run_id}-user-a2", f"{run_id}-a2",
        accounts["a3"], f"{run_id}-user-a3", f"{run_id}-a3",
    )

    await conn.execute(
        """
        INSERT INTO markets (
            market_id, slug, title, description, status,
            tick_size_cents, min_price_cents, max_price_cents,
            close_time, resolve_time, created_by
        ) VALUES
        ($1, $2, $3, $4, 'ACTIVE', 1, 1, 99, now() + interval '1 day', now() + interval '2 days', $5),
        ($6, $7, $8, $9, 'DRAFT', 1, 1, 99, now() + interval '1 day', now() + interval '2 days', $5)
        """,
        markets["m_active"], f"{run_id}-active", f"{run_id} active", "rpc active", accounts["a1"],
        markets["m_draft"], f"{run_id}-draft", f"{run_id} draft", "rpc draft",
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
        await conn.execute(f"DELETE FROM trade_parties WHERE trade_id IN ({_uuid_csv(trade_ids)})")

    await conn.execute(f"DELETE FROM ledger_entries WHERE market_id IN ({m_ids}) OR account_id IN ({a_ids})")
    await conn.execute(f"DELETE FROM order_cancels WHERE order_id IN (SELECT request_id FROM orders WHERE market_id IN ({m_ids}))")
    await conn.execute(f"DELETE FROM trades WHERE market_id IN ({m_ids})")
    await conn.execute(f"DELETE FROM positions WHERE market_id IN ({m_ids})")
    await conn.execute(f"DELETE FROM orders WHERE market_id IN ({m_ids})")
    await conn.execute(f"DELETE FROM execution_market_leases WHERE market_id IN ({m_ids})")
    await conn.execute(f"DELETE FROM matchmaker_market_leases WHERE market_id IN ({m_ids})")
    await conn.execute(f"DELETE FROM cash_transactions WHERE account_id IN ({a_ids})")
    await conn.execute(f"DELETE FROM markets WHERE market_id IN ({m_ids})")
    await conn.execute(f"DELETE FROM accounts WHERE account_id IN ({a_ids})")


async def _cleanup_with_retries(conn: asyncpg.Connection, market_ids: list[str], account_ids: list[str]) -> None:
    for i in range(6):
        try:
            await _cleanup(conn, market_ids, account_ids)
            return
        except Exception as exc:
            if not _is_retryable_occ(exc) or i == 5:
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
    run_id = f"rpc-{uuid.uuid4().hex[:10]}"
    test_data: dict | None = None
    extra_market_ids: list[str] = []

    try:
        base_env = os.environ.copy()

        listener_env = base_env.copy()
        listener_env.update(
            {
                "CLIENT_LISTENER_HOST": args.listener_host,
                "CLIENT_LISTENER_PORT": str(args.listener_port),
                "CLIENT_LISTENER_DB_DSN": args.db_dsn,
                "CLIENT_LISTENER_AUTH_MODE": "static-token",
                "CLIENT_LISTENER_AUTH_TOKEN": args.auth_token,
            }
        )
        procs.append(_start_proc("rpc-listener", [sys.executable, "server.py"], listener_dir, listener_env, log_dir))

        mm_env = base_env.copy()
        mm_env.update(
            {
                "MATCHMAKER_DB_DSN": args.db_dsn,
                "MATCHMAKER_INSTANCE_ID": f"{run_id}-mm",
                "MATCHMAKER_POLL_INTERVAL_MS": "200",
            }
        )
        procs.append(_start_proc("rpc-matchmaker", [sys.executable, "matchmaker.py"], matchmaker_dir, mm_env, log_dir))

        ex_env = base_env.copy()
        ex_env.update(
            {
                "EXECUTOR_DB_DSN": args.db_dsn,
                "EXECUTOR_INSTANCE_ID": f"{run_id}-ex",
                "EXECUTOR_POLL_INTERVAL_MS": "300",
            }
        )
        procs.append(_start_proc("rpc-executor", [sys.executable, "executor.py"], executor_dir, ex_env, log_dir))

        await _wait_for_listener(args.listener_host, args.listener_port)

        async with pool.acquire() as conn:
            test_data = await _seed_data(conn, run_id)

        a1 = test_data["accounts"]["a1"]
        a2 = test_data["accounts"]["a2"]
        a3 = test_data["accounts"]["a3"]
        m_active = test_data["markets"]["m_active"]

        for action in ("health", "ready"):
            resp = await _send_tcp_json(args.listener_host, args.listener_port, _build_action(action))
            if not resp.get("ok"):
                raise AssertionError(f"{action} failed: {resp}")

        m_new = _uuid()
        extra_market_ids.append(m_new)
        create_resp = await _send_tcp_json(
            args.listener_host,
            args.listener_port,
            _build_action(
                "create_market",
                args.auth_token,
                {
                    "market_id": m_new,
                    "slug": f"{run_id}-rpc-market",
                    "title": f"{run_id} rpc market",
                    "description": "rpc test market",
                    "status": "DRAFT",
                    "created_by": a1,
                },
            ),
        )
        if not create_resp.get("ok"):
            raise AssertionError(f"create_market failed: {create_resp}")

        status_resp = await _send_tcp_json(
            args.listener_host,
            args.listener_port,
            _build_action("update_market_status", args.auth_token, {"market_id": m_new, "new_status": "ACTIVE"}),
        )
        if not status_resp.get("ok"):
            raise AssertionError(f"update_market_status failed: {status_resp}")

        order1 = _uuid()
        order2 = _uuid()
        for req in [
            {
                "request_id": order1,
                "account_id": a1,
                "market_id": m_new,
                "side": "YES",
                "qty": 2,
                "price_cents": 55,
                "time_in_force": "GTC",
                "ingress_ts_ns": time.time_ns(),
            },
            {
                "request_id": order2,
                "account_id": a2,
                "market_id": m_new,
                "side": "NO",
                "qty": 1,
                "price_cents": 30,
                "time_in_force": "GTC",
                "ingress_ts_ns": time.time_ns(),
            },
        ]:
            resp = await _send_tcp_json(args.listener_host, args.listener_port, _build_action("submit_order", args.auth_token, req))
            if not resp.get("ok"):
                raise AssertionError(f"submit_order failed: {resp}")

        get_order_resp = await _send_tcp_json(
            args.listener_host,
            args.listener_port,
            _build_action("get_order", args.auth_token, {"order_id": order1}),
        )
        if not get_order_resp.get("ok") or get_order_resp["order"]["request_id"] != order1:
            raise AssertionError(f"get_order failed: {get_order_resp}")

        list_orders_resp = await _send_tcp_json(
            args.listener_host,
            args.listener_port,
            _build_action("list_orders", args.auth_token, {"market_id": m_new, "limit": 20}),
        )
        if not list_orders_resp.get("ok") or len(list_orders_resp["result"]["orders"]) < 2:
            raise AssertionError(f"list_orders failed: {list_orders_resp}")

        open_orders_resp = await _send_tcp_json(
            args.listener_host,
            args.listener_port,
            _build_action("list_open_orders", args.auth_token, {"account_id": a1, "market_id": m_new}),
        )
        if not open_orders_resp.get("ok") or not open_orders_resp["result"]["orders"]:
            raise AssertionError(f"list_open_orders failed: {open_orders_resp}")

        replaced_order = _uuid()
        replace_resp = await _send_tcp_json(
            args.listener_host,
            args.listener_port,
            _build_action(
                "replace_order",
                args.auth_token,
                {
                    "cancel_id": _uuid(),
                    "old_order_id": order1,
                    "account_id": a1,
                    "new_order": {
                        "request_id": replaced_order,
                        "account_id": a1,
                        "market_id": m_new,
                        "side": "YES",
                        "qty": 1,
                        "price_cents": 20,
                        "time_in_force": "GTC",
                        "ingress_ts_ns": time.time_ns(),
                    },
                },
            ),
        )
        if not replace_resp.get("ok"):
            raise AssertionError(f"replace_order failed: {replace_resp}")

        cancel_all_resp = await _send_tcp_json(
            args.listener_host,
            args.listener_port,
            _build_action("cancel_all_orders", args.auth_token, {"account_id": a2, "market_id": m_new, "reason": "rpc-test"}),
        )
        if not cancel_all_resp.get("ok") or int(cancel_all_resp["result"]["cancelled_count"]) < 1:
            raise AssertionError(f"cancel_all_orders failed: {cancel_all_resp}")

        book_resp = await _send_tcp_json(
            args.listener_host,
            args.listener_port,
            _build_action("get_order_book", args.auth_token, {"market_id": m_new, "depth": 5}),
        )
        if not book_resp.get("ok"):
            raise AssertionError(f"get_order_book failed: {book_resp}")

        dep_id = _uuid()
        dep_resp = await _send_tcp_json(
            args.listener_host,
            args.listener_port,
            _build_action(
                "deposit_cash",
                args.auth_token,
                {"cash_txn_id": dep_id, "account_id": a3, "amount_cents": 5000, "notes": "rpc deposit"},
            ),
        )
        if not dep_resp.get("ok"):
            raise AssertionError(f"deposit_cash failed: {dep_resp}")

        wd_id = _uuid()
        wd_resp = await _send_tcp_json(
            args.listener_host,
            args.listener_port,
            _build_action(
                "withdraw_cash",
                args.auth_token,
                {"cash_txn_id": wd_id, "account_id": a3, "amount_cents": 1200, "notes": "rpc withdraw"},
            ),
        )
        if not wd_resp.get("ok"):
            raise AssertionError(f"withdraw_cash failed: {wd_resp}")

        bal_resp = await _send_tcp_json(
            args.listener_host,
            args.listener_port,
            _build_action("get_account_balances", args.auth_token, {"account_id": a3}),
        )
        if not bal_resp.get("ok"):
            raise AssertionError(f"get_account_balances failed: {bal_resp}")
        bal = bal_resp["result"]
        if int(bal["total_deposits_cents"]) < 5000 or int(bal["total_withdrawals_cents"]) < 1200:
            raise AssertionError(f"unexpected balances summary: {bal_resp}")

        cross_yes = _uuid()
        cross_no = _uuid()
        for req in [
            {
                "request_id": cross_yes,
                "account_id": a1,
                "market_id": m_active,
                "side": "YES",
                "qty": 1,
                "price_cents": 60,
                "time_in_force": "GTC",
                "ingress_ts_ns": time.time_ns(),
            },
            {
                "request_id": cross_no,
                "account_id": a2,
                "market_id": m_active,
                "side": "NO",
                "qty": 1,
                "price_cents": 45,
                "time_in_force": "GTC",
                "ingress_ts_ns": time.time_ns(),
            },
        ]:
            resp = await _send_tcp_json(args.listener_host, args.listener_port, _build_action("submit_order", args.auth_token, req))
            if not resp.get("ok"):
                raise AssertionError(f"cross submit failed: {resp}")

        await asyncio.sleep(args.match_wait_seconds)

        trades_resp = await _send_tcp_json(
            args.listener_host,
            args.listener_port,
            _build_action("get_trades", args.auth_token, {"market_id": m_active, "limit": 20}),
        )
        if not trades_resp.get("ok") or len(trades_resp["result"]["trades"]) < 1:
            raise AssertionError(f"get_trades failed: {trades_resp}")

        pos_resp = await _send_tcp_json(
            args.listener_host,
            args.listener_port,
            _build_action("get_positions", args.auth_token, {"account_id": a1, "market_id": m_active}),
        )
        if not pos_resp.get("ok") or not pos_resp["result"]["positions"]:
            raise AssertionError(f"get_positions failed: {pos_resp}")

        resolve_resp = await _send_tcp_json(
            args.listener_host,
            args.listener_port,
            _build_action(
                "resolve_market",
                args.auth_token,
                {
                    "resolution_id": _uuid(),
                    "market_id": m_new,
                    "outcome": "CANCELLED",
                    "resolved_by": a1,
                    "notes": "rpc resolve",
                },
            ),
        )
        if not resolve_resp.get("ok"):
            raise AssertionError(f"resolve_market failed: {resolve_resp}")

        print("RPC FEATURES TEST PASSED")

    except Exception:
        print("\n--- rpc component logs (tail) ---")
        for p in procs:
            print(f"\n[{p.name}] {p.log_path}")
            print(_tail(p.log_path))
        raise
    finally:
        try:
            if test_data is not None:
                async with pool.acquire() as conn:
                    all_markets = list(test_data["markets"].values()) + extra_market_ids
                    await _cleanup_with_retries(conn, all_markets, list(test_data["accounts"].values()))
        finally:
            await pool.close()
            _stop_procs(procs)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="RPC feature coverage test")
    p.add_argument("--db-dsn", required=True, help="Full PostgreSQL/DSQL DSN with credentials")
    p.add_argument("--auth-token", default="dev-shared-token")
    p.add_argument("--listener-host", default="127.0.0.1")
    p.add_argument("--listener-port", type=int, default=9201)
    p.add_argument("--match-wait-seconds", type=float, default=4.0)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    try:
        asyncio.run(_run_test(args))
        return 0
    except Exception as exc:
        print(f"RPC FEATURES TEST FAILED: {exc}")
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
