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
    return (json.dumps(payload, separators=(',', ':')) + '\n').encode('utf-8')


async def _ensure_auth(auth, auth_token: str) -> None:
    if not await auth.validate(auth_token):
        raise SubmissionError('unauthorized')


async def _handle_submit_order(message: dict[str, Any], auth_token: str, auth, repo: OrderRepository) -> dict[str, Any]:
    await _ensure_auth(auth, auth_token)
    request_payload = message.get('request')
    if not isinstance(request_payload, dict):
        raise SubmissionError('request payload must be an object')
    req = SubmitOrderRequest.from_dict(request_payload)
    order = await repo.submit_order(req)
    return {'ok': True, 'order': order}


async def _handle_cancel_order(message: dict[str, Any], auth_token: str, auth, repo: OrderRepository) -> dict[str, Any]:
    await _ensure_auth(auth, auth_token)
    request_payload = message.get('request')
    if not isinstance(request_payload, dict):
        raise SubmissionError('request payload must be an object')
    req = CancelOrderRequest.from_dict(request_payload)
    cancel = await repo.cancel_order(req)
    return {'ok': True, 'cancel': cancel}


async def _handle_repo_call(
    *,
    key: str,
    fn,
    message: dict[str, Any],
    auth_token: str,
    auth,
) -> dict[str, Any]:
    await _ensure_auth(auth, auth_token)
    request_payload = message.get('request')
    if request_payload is None:
        request_payload = {}
    if not isinstance(request_payload, dict):
        raise SubmissionError('request payload must be an object')
    result = await fn(request_payload)
    return {'ok': True, key: result}


async def handle_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    auth,
    repo: OrderRepository,
) -> None:
    peer = writer.get_extra_info('peername')
    print(f'[{datetime.now(timezone.utc).isoformat()}] connected: {peer}')

    try:
        while True:
            line = await reader.readline()
            if not line:
                break

            try:
                message = json.loads(line.decode('utf-8'))
                action = str(message.get('action', '')).lower()
                auth_token = str(message.get('auth_token', ''))

                if action == 'ping':
                    response = {'ok': True, 'pong': True}
                elif action == 'health':
                    response = {'ok': True, 'status': 'healthy'}
                elif action == 'ready':
                    response = {'ok': True, 'ready': await repo.ready()}
                elif action == 'create_account':
                    response = await _handle_repo_call(key='result', fn=repo.create_account, message=message, auth_token=auth_token, auth=auth)
                elif action == 'authenticate_account':
                    response = await _handle_repo_call(key='result', fn=repo.authenticate_account, message=message, auth_token=auth_token, auth=auth)
                elif action == 'submit_order':
                    response = await _handle_submit_order(message, auth_token, auth, repo)
                elif action == 'cancel_order':
                    response = await _handle_cancel_order(message, auth_token, auth, repo)
                elif action == 'get_order':
                    response = await _handle_repo_call(key='order', fn=repo.get_order, message=message, auth_token=auth_token, auth=auth)
                elif action == 'list_orders':
                    response = await _handle_repo_call(key='result', fn=repo.list_orders, message=message, auth_token=auth_token, auth=auth)
                elif action == 'list_open_orders':
                    response = await _handle_repo_call(key='result', fn=repo.list_open_orders, message=message, auth_token=auth_token, auth=auth)
                elif action == 'cancel_all_orders':
                    response = await _handle_repo_call(key='result', fn=repo.cancel_all_orders, message=message, auth_token=auth_token, auth=auth)
                elif action == 'replace_order':
                    response = await _handle_repo_call(key='result', fn=repo.replace_order, message=message, auth_token=auth_token, auth=auth)
                elif action == 'get_trades':
                    response = await _handle_repo_call(key='result', fn=repo.get_trades, message=message, auth_token=auth_token, auth=auth)
                elif action == 'get_positions':
                    response = await _handle_repo_call(key='result', fn=repo.get_positions, message=message, auth_token=auth_token, auth=auth)
                elif action == 'get_account_balances':
                    response = await _handle_repo_call(key='result', fn=repo.get_account_balances, message=message, auth_token=auth_token, auth=auth)
                elif action == 'get_wallet_history':
                    response = await _handle_repo_call(key='result', fn=repo.get_wallet_history, message=message, auth_token=auth_token, auth=auth)
                elif action == 'deposit_cash':
                    response = await _handle_repo_call(key='result', fn=repo.deposit_cash, message=message, auth_token=auth_token, auth=auth)
                elif action == 'withdraw_cash':
                    response = await _handle_repo_call(key='result', fn=repo.withdraw_cash, message=message, auth_token=auth_token, auth=auth)
                elif action == 'list_markets':
                    response = await _handle_repo_call(key='result', fn=repo.list_markets, message=message, auth_token=auth_token, auth=auth)
                elif action == 'get_market_history':
                    response = await _handle_repo_call(key='result', fn=repo.get_market_history, message=message, auth_token=auth_token, auth=auth)
                elif action == 'create_market':
                    response = await _handle_repo_call(key='result', fn=repo.create_market, message=message, auth_token=auth_token, auth=auth)
                elif action == 'update_market_status':
                    response = await _handle_repo_call(key='result', fn=repo.update_market_status, message=message, auth_token=auth_token, auth=auth)
                elif action == 'resolve_market':
                    response = await _handle_repo_call(key='result', fn=repo.resolve_market, message=message, auth_token=auth_token, auth=auth)
                elif action == 'get_order_book':
                    response = await _handle_repo_call(key='result', fn=repo.get_order_book, message=message, auth_token=auth_token, auth=auth)
                else:
                    response = {'ok': False, 'error': f'unsupported action: {action}'}
            except (json.JSONDecodeError, UnicodeDecodeError):
                response = {'ok': False, 'error': 'invalid JSON line'}
            except ValidationError as exc:
                response = {'ok': False, 'error': f'validation error: {exc}'}
            except SubmissionError as exc:
                response = {'ok': False, 'error': f'submission failed: {exc}'}
            except Exception as exc:
                print(f'unexpected handler error: {exc}')
                traceback.print_exc()
                response = {'ok': False, 'error': 'internal server error'}

            writer.write(_json_response(response))
            await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()
        print(f'[{datetime.now(timezone.utc).isoformat()}] disconnected: {peer}')


async def main() -> None:
    settings = load_settings()
    try:
        auth = build_authenticator(settings)
    except AuthError as exc:
        raise SystemExit(f'auth configuration error: {exc}') from exc

    pool = await create_pool(
        dsn=settings.db_dsn,
        min_size=settings.db_min_pool_size,
        max_size=settings.db_max_pool_size,
    )
    repo = OrderRepository(
        pool,
        account_session_ttl_seconds=settings.account_session_ttl_seconds,
    )

    server = await asyncio.start_server(
        lambda r, w: handle_client(r, w, auth=auth, repo=repo),
        settings.host,
        settings.port,
    )
    sockets = ', '.join(str(sock.getsockname()) for sock in (server.sockets or []))
    print(f'client-listener running on {sockets}')

    async with server:
        await server.serve_forever()


if __name__ == '__main__':
    asyncio.run(main())
