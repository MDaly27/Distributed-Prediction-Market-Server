import asyncio
import json
import traceback
from datetime import datetime, timezone
from typing import Any

from auth import AuthError, build_authenticator
from config import load_settings
from db import OrderRepository, SubmissionError, create_pool
from models import CancelOrderRequest, SubmitOrderRequest, ValidationError


def _json_response(payload: dict[str, Any]) -> bytes:
    return (json.dumps(payload, separators=(",", ":")) + "\n").encode("utf-8")


async def _handle_submit_order(
    message: dict[str, Any],
    auth_token: str,
    auth,
    repo: OrderRepository,
) -> dict[str, Any]:
    if not await auth.validate(auth_token):
        return {"ok": False, "error": "unauthorized"}

    request_payload = message.get("request")
    if not isinstance(request_payload, dict):
        return {"ok": False, "error": "request payload must be an object"}

    req = SubmitOrderRequest.from_dict(request_payload)
    order = await repo.submit_order(req)
    return {"ok": True, "order": order}


async def _handle_cancel_order(
    message: dict[str, Any],
    auth_token: str,
    auth,
    repo: OrderRepository,
) -> dict[str, Any]:
    if not await auth.validate(auth_token):
        return {"ok": False, "error": "unauthorized"}

    request_payload = message.get("request")
    if not isinstance(request_payload, dict):
        return {"ok": False, "error": "request payload must be an object"}

    req = CancelOrderRequest.from_dict(request_payload)
    cancel = await repo.cancel_order(req)
    return {"ok": True, "cancel": cancel}


async def handle_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    auth,
    repo: OrderRepository,
) -> None:
    peer = writer.get_extra_info("peername")
    print(f"[{datetime.now(timezone.utc).isoformat()}] connected: {peer}")

    try:
        while True:
            line = await reader.readline()
            if not line:
                break

            try:
                message = json.loads(line.decode("utf-8"))
                action = str(message.get("action", "")).lower()
                auth_token = str(message.get("auth_token", ""))

                if action == "ping":
                    response = {"ok": True, "pong": True}
                elif action == "submit_order":
                    response = await _handle_submit_order(message, auth_token, auth, repo)
                elif action == "cancel_order":
                    response = await _handle_cancel_order(message, auth_token, auth, repo)
                else:
                    response = {"ok": False, "error": f"unsupported action: {action}"}
            except (json.JSONDecodeError, UnicodeDecodeError):
                response = {"ok": False, "error": "invalid JSON line"}
            except ValidationError as exc:
                response = {"ok": False, "error": f"validation error: {exc}"}
            except SubmissionError as exc:
                response = {"ok": False, "error": f"submission failed: {exc}"}
            except Exception as exc:
                print(f"unexpected handler error: {exc}")
                traceback.print_exc()
                response = {"ok": False, "error": "internal server error"}

            writer.write(_json_response(response))
            await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()
        print(f"[{datetime.now(timezone.utc).isoformat()}] disconnected: {peer}")


async def main() -> None:
    settings = load_settings()
    try:
        auth = build_authenticator(settings)
    except AuthError as exc:
        raise SystemExit(f"auth configuration error: {exc}") from exc

    pool = await create_pool(
        dsn=settings.db_dsn,
        min_size=settings.db_min_pool_size,
        max_size=settings.db_max_pool_size,
    )
    repo = OrderRepository(pool)

    server = await asyncio.start_server(
        lambda r, w: handle_client(r, w, auth=auth, repo=repo),
        settings.host,
        settings.port,
    )
    sockets = ", ".join(str(sock.getsockname()) for sock in (server.sockets or []))
    print(f"client-listener running on {sockets}")

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
