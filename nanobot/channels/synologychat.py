"""Synology Chat channel implementation using REST API and webhooks."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import urllib.parse
from typing import Any

import aiohttp
from aiohttp import web
from nanobot.utils.logging_bridge import redirect_lib_logging
from pydantic import Field

from nanobot.bus.events import InboundMessage, OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel
from nanobot.config.schema import Base


class SynologyChatConfig(Base):
    """Synology Chat channel configuration."""

    enabled: bool = False
    webhook_url: str = ""  # Incoming webhook URL for sending messages
    webhook_secret: str = ""  # Secret for webhook verification
    verify_signature: bool = True  # Whether to verify webhook signatures
    webhook_path: str = "/synology/webhook"
    webhook_host: str = "127.0.0.1"
    webhook_port: int = 8766
    allow_from: list[str] = Field(default_factory=list)  # List of allowed user IDs
    timeout: int = 30  # Request timeout in seconds


class SynologyChatChannel(BaseChannel):
    """Synology Chat channel using REST API for sending and webhooks for receiving."""

    name = "synologychat"
    display_name = "Synology Chat"

    @classmethod
    def default_config(cls) -> dict[str, Any]:
        return SynologyChatConfig().model_dump(by_alias=True)

    def __init__(self, config: Any, bus: MessageBus):
        if isinstance(config, dict):
            config = SynologyChatConfig.model_validate(config)
        super().__init__(config, bus)
        self.config: SynologyChatConfig = config
        self.session: aiohttp.ClientSession | None = None
        self.webhook_app: web.Application | None = None
        self.webhook_runner: web.AppRunner | None = None
        self.webhook_site: web.TCPSite | None = None

    async def _verify_webhook_signature(self, request: web.Request, body: bytes) -> bool:
        """Verify webhook signature using HMAC-SHA256."""
        if not self.config.verify_signature or not self.config.webhook_secret:
            return True  # No verification if disabled or no secret configured

        signature = request.headers.get('X-Synology-Signature')
        if not signature:
            self.logger.warning("Synology webhook: missing signature header")
            return False

        expected_signature = hmac.new(
            self.config.webhook_secret.encode(),
            body,
            hashlib.sha256
        ).hexdigest()

        return hmac.compare_digest(signature, expected_signature)


    async def _handle_webhook(self, request: web.Request) -> web.Response:
        """Handle incoming webhook from Synology Chat."""
        try:
            body = await request.read()
            body_str = body.decode(errors="replace")

            if not await self._verify_webhook_signature(request, body):
                self.logger.warning("Synology webhook: invalid signature")
                return web.Response(status=401, text="Invalid signature")

            data = None
            if body_str.strip().startswith("{"):
                try:
                    data = json.loads(body_str)
                except json.JSONDecodeError as e:
                    self.logger.warning(f"Synology webhook: JSON decode failed, trying form parse - {e}")

            if data is None:
                parsed = urllib.parse.parse_qs(body_str, keep_blank_values=True)
                if parsed:
                    data = {k: v[0] if len(v) == 1 else v for k, v in parsed.items()}
                else:
                    data = {k: v for k, v in request.query.items()}

            if not isinstance(data, dict) or not data:
                self.logger.error(f"Synology webhook: unable to parse payload, body={body_str[:500]}")
                return web.Response(status=400, text="Invalid payload")

            # Synology Chat webhook payload structure may be either JSON with nested data or URL-encoded form values.
            payload = data.get('data') if isinstance(data.get('data'), dict) else data
            event = data.get('event')
            if event and event != 'message':
                return web.Response(status=200, text="OK")  # Ignore non-message events

            user_id = str(payload.get('user_id', '') or '')
            chat_id = str(payload.get('channel_id', payload.get('user_id', '')) or '')
            text = str(payload.get('text', '') or '').strip()
            username = payload.get('username', payload.get('user_name', 'Unknown')) or 'Unknown'

            if not text:
                return web.Response(status=200, text="OK")

            # Check allow_from list
            if self.config.allow_from and username not in self.config.allow_from:
                self.logger.info(f"Synology: ignoring message from unauthorized user {username}")
                return web.Response(status=200, text="OK")

            # Create inbound message
            inbound_msg = InboundMessage(
                channel=self.name,
                chat_id=chat_id,
                sender_id=user_id,
                content=text,
            )

            # Publish inbound message to the agent
            await self.bus.publish_inbound(inbound_msg)

            return web.Response(status=200, text="OK")

        except Exception as e:
            self.logger.error(f"Synology webhook error: {e}")
            return web.Response(status=500, text="Internal error")

    async def _send_message_api(self, chat_id: str, text: str) -> bool:
        """Send message via Synology Chat incoming webhook."""
        if not self.session or not self.config.webhook_url:
            return False

        try:
            payload = {
                'text': text,
                'channel_id': chat_id
            }
            data = {'payload': json.dumps(payload)}

            async with self.session.post(self.config.webhook_url, data=data, timeout=self.config.timeout) as resp:
                if resp.status == 200:
                    return True
                else:
                    self.logger.error(f"Synology send HTTP error: {resp.status}")
                    return False

        except Exception as e:
            self.logger.error(f"Synology send error: {e}")
            return False

    async def start(self) -> None:
        """Start the Synology Chat channel."""
        self.logger.info("Starting Synology Chat channel")

        # Create HTTP session for API calls
        self.session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(verify_ssl=self.config.verify_signature))

        # Setup webhook server
        self.webhook_app = web.Application()
        self.webhook_app.router.add_post(self.config.webhook_path, self._handle_webhook)

        self.webhook_runner = web.AppRunner(self.webhook_app)
        await self.webhook_runner.setup()

        self.webhook_site = web.TCPSite(
            self.webhook_runner,
            self.config.webhook_host,
            self.config.webhook_port
        )
        await self.webhook_site.start()

        self.logger.info(f"Synology webhook listening on {self.config.webhook_host}:{self.config.webhook_port}{self.config.webhook_path}")

        self._running = True

        # Keep running
        while self._running:
            await asyncio.sleep(1)

    async def stop(self) -> None:
        """Stop the Synology Chat channel."""
        self.logger.info("Stopping Synology Chat channel")
        self._running = False

        if self.webhook_site:
            await self.webhook_site.stop()
            self.webhook_site = None

        if self.webhook_runner:
            await self.webhook_runner.cleanup()
            self.webhook_runner = None

        if self.session:
            await self.session.close()
            self.session = None

    async def send(self, msg: OutboundMessage) -> None:
        """Send a message through Synology Chat."""
        if not msg.chat_id:
            self.logger.error("Synology send: no chat_id provided")
            raise ValueError("chat_id is required for Synology Chat")

        success = await self._send_message_api(msg.chat_id, msg.content)
        if not success:
            raise Exception("Failed to send message via Synology Chat API")

    async def send_delta(self, chat_id: str, delta: str, metadata: dict[str, Any] | None = None) -> None:
        """Send a streaming text delta. For Synology Chat, we accumulate and send complete messages."""
        # Synology Chat doesn't support streaming deltas, so we ignore them
        pass