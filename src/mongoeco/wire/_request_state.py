"""Request-local phases; only scalar projections reach the diagnostic buffer."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

from mongoeco.wire._diagnostics import classify_result


if TYPE_CHECKING:
    from mongoeco.wire._diagnostics import WireDiagnostics
    from mongoeco.wire.protocol import MessageHeader
    from mongoeco.wire.requests import WireRequestContext


@dataclass(slots=True)
class WireRequestState:
    phase: str = "decode"
    disposition: Literal["reply", "suppress", "close"] = "reply"
    expects_reply: bool = True
    legacy_reply_flags: int = 0
    response: str = "not_produced"
    outcome: str = "error"
    result: dict[str, Any] = field(default_factory=dict)
    context: WireRequestContext | None = None
    recorded: bool = False
    command_name: str = "<unknown>"
    mirrored: bool = False

    def normalized(self, name: str, body: dict[str, Any]) -> None:
        self.command_name = name
        self.mirrored = body.get("mirrored") is True

    def received(
        self, capture: WireDiagnostics, header: MessageHeader, connection_id: int
    ) -> None:
        if not capture.capacity or self.recorded:
            return
        capture.record(
            "request_received",
            connection_id=connection_id,
            request_id=header.request_id,
            opcode=header.op_code,
            command=self.command_name,
            mirrored=self.mirrored,
            expects_reply=self.expects_reply,
        )
        self.recorded = True

    def completed(
        self,
        capture: WireDiagnostics,
        header: MessageHeader,
        connection_id: int,
        duration_ms: float,
    ) -> None:
        if not capture.capacity:
            return
        self.received(capture, header, connection_id)
        result_outcome, code = classify_result(self.result)
        capture.record(
            "request_completed",
            connection_id=connection_id,
            request_id=header.request_id,
            outcome=self.outcome if self.outcome != "completed" else result_outcome,
            code=code,
            response=self.response,
            phase=self.phase,
            duration_ms=duration_ms,
        )
