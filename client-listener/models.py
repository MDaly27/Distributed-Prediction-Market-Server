from dataclasses import dataclass
from typing import Any


class ValidationError(Exception):
    pass


@dataclass(frozen=True)
class SubmitOrderRequest:
    request_id: str
    account_id: str
    market_id: str
    side: str
    qty: int
    price_cents: int
    time_in_force: str
    ingress_ts_ns: int

    @staticmethod
    def from_dict(payload: dict[str, Any]) -> "SubmitOrderRequest":
        try:
            model = SubmitOrderRequest(
                request_id=str(payload["request_id"]),
                account_id=str(payload["account_id"]),
                market_id=str(payload["market_id"]),
                side=str(payload["side"]).upper(),
                qty=int(payload["qty"]),
                price_cents=int(payload["price_cents"]),
                time_in_force=str(payload["time_in_force"]).upper(),
                ingress_ts_ns=int(payload["ingress_ts_ns"]),
            )
        except KeyError as exc:
            raise ValidationError(f"missing required field: {exc.args[0]}") from exc
        except (TypeError, ValueError) as exc:
            raise ValidationError(f"invalid field type: {exc}") from exc

        if model.side not in {"YES", "NO"}:
            raise ValidationError("side must be YES or NO")
        if model.qty <= 0:
            raise ValidationError("qty must be > 0")
        if not 0 <= model.price_cents <= 100:
            raise ValidationError("price_cents must be between 0 and 100")
        if model.time_in_force not in {"GTC", "IOC", "FOK"}:
            raise ValidationError("time_in_force must be GTC, IOC, or FOK")
        if model.ingress_ts_ns <= 0:
            raise ValidationError("ingress_ts_ns must be > 0")
        return model
