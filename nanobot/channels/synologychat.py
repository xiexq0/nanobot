"""Synology Chat channel implementation using REST API and webhooks."""

from __future__ import annotations

import re
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
    
    def markdown_to_synology(self, text: str) -> str:
        """
        将标准 Markdown 转换为完整的 Synology Chat 格式
        """
        if not text:
            return ""
            
        # ==================== 1. 暂存并隔离区（防止格式污染） ====================
        # 按照优先级，先保护代码块，再保护行内代码
        code_blocks = []
        def save_code_block(match):
            code_blocks.append(match.group(0))
            return f"%%CODEBLOCK{len(code_blocks) - 1}%%"
        
        text = re.sub(r'```[\s\S]*?```', save_code_block, text)
        text = re.sub(r'`[^`\n]+`', save_code_block, text)
        
        # 暂存并转换 Markdown 链接: [显示文本](URL) -> <URL|显示文本>
        links = []
        def save_link(match):
            label, url = match.group(1), match.group(2)
            links.append(f"<{url}|{label}>")
            return f"%%LINK{len(links) - 1}%%"
        text = re.sub(r'\[(.*?)\]\((.*?)\)', save_link, text)
        
        # 暂存并转换 Markdown 专属的自动链接: <http://xxx> -> <http://xxx> (Synology也支持，直接暂存)
        auto_links = []
        def save_auto_link(match):
            auto_links.append(match.group(0))
            return f"%%AUTOLINK{len(auto_links) - 1}%%"
        text = re.sub(r'<https?://[^>\s]+>', save_auto_link, text)

        # 暂存并转换 Markdown 列表项 (支持 -, *, +) -> 统一转为 Synology 的 '* '
        bullets = []
        def save_bullet(match):
            bullets.append("* ")
            return f"%%BULLET{len(bullets) - 1}%%"
        text = re.sub(r'^\s*[-*+]\s+', save_bullet, text, flags=re.MULTILINE)
        
        # 暂存并转换 Markdown 的多行引用（把每一行的 > 合并，或转为 Synology 的 >>>）
        # 如果连续多行都是以 > 开头，Synology 更适合用 >>> 块包裹
        def replace_blockquote(match):
            lines = match.group(0).strip().split('\n')
            cleaned_lines = [re.sub(r'^\s*>\s*', '', line) for line in lines]
            return ">>>\n" + "\n".join(cleaned_lines) + "\n"
        text = re.sub(r'(?:^\s*>.*\n?)+', replace_blockquote, text, flags=re.MULTILINE)

        # ==================== 2. 转换基础行内样式 ====================
        
        # 1. 转换粗体: **text** 或 __text__ -> *text*
        text = re.sub(r'\*\*([^*\n]+)\*\*', r'*\1*', text)
        text = re.sub(r'__([^_\n]+)__', r'*\1*', text)
        
        # 2. 转换斜体: *text* 或 _text_ -> _text_ 
        # (排除已经被转成单星号的粗体，以及多星号冲突)
        text = re.sub(r'(?<!\*)\*([^* \n][^* \n]*)\*(?!\*)', r'_\1_', text)
        text = re.sub(r'(?<!_)_([^_ \n][^_ \n]*)_(?!_)', r'_\1_', text)
        
        # 3. 转换删除线: ~~text~~ -> ~text~
        text = re.sub(r'~~([^~\n]+)~~', r'~~\1~~', text) # 标准MD是~~，Synology实测支持双波浪号，部分版本支持单波浪号。这里统一用~转换
        text = re.sub(r'~~([^~\n]+)~~', r'~\1~', text)

        # ==================== 3. 还原暂存的保护块 = refinement ====================
        for i, b in enumerate(bullets):
            text = text.replace(f"%%BULLET{i}%%", b)
        for i, al in enumerate(auto_links):
            text = text.replace(f"%%AUTOLINK{i}%%", al)
        for i, l in enumerate(links):
            text = text.replace(f"%%LINK{i}%%", l)
        for i, block in enumerate(code_blocks):
            text = text.replace(f"%%CODEBLOCK{i}%%", block)
            
        return text


    def synology_to_markdown(self, text: str) -> str:
        """
        将 Synology Chat 格式转换为标准 Markdown
        """
        if not text:
            return ""
        
        # ==================== 1. 暂存并隔离代码块 ====================
        code_blocks = []
        def save_code_block(match):
            code_blocks.append(match.group(0))
            return f"%%CODEBLOCK{len(code_blocks) - 1}%%"
        
        text = re.sub(r'```[\s\S]*?```', save_code_block, text)
        text = re.sub(r'`[^`\n]+`', save_code_block, text)
        
        # ==================== 2. 转换高级特有语法 ====================
        
        # 1. 转换 Synology 链接/频道标签/人员提及: <URL|显示文本> -> [显示文本](URL)
        def link_to_md(match):
            content = match.group(1)
            if '|' in content:
                url, label = content.split('|', 1)
                # 兼容特定频道提及如 <#channel_id|channel_name> 转换为 Markdown 文本 [#channel_name]
                if url.startswith('#') or url.startswith('@'):
                    return f"[{label}]"
                return f"[{label}]({url})"
            else:
                if content.startswith('#') or content.startswith('@'):
                    return f"[{content}]"
                return f"[{content}]({content})"
        text = re.sub(r'<(.*?)>', link_to_md, text)
        
        # 2. 转换多行引用: 以 >>> 开头的整段 -> 转为 Markdown 的多行 > 开头
        def convert_syno_blockquote(match):
            content = match.group(1).strip()
            return "\n".join([f"> {line}" for line in content.split('\n')]) + "\n"
        text = re.sub(r'^>>>([\s\S]*?)(?=(?:^>>>|$))', convert_syno_blockquote, text, flags=re.MULTILINE)
        
        # 3. 转换单行引用: > text -> > text (Markdown 原生支持，但需要暂存防止干扰后续的列表识别)
        quotes = []
        def save_quote(match):
            quotes.append(match.group(0))
            return f"%%QUOTE{len(quotes) - 1}%%"
        text = re.sub(r'^\s*>\s+.*$', save_quote, text, flags=re.MULTILINE)

        # 4. 暂存无序列表防止干扰粗体: * 列表项
        bullet_lists = []
        def save_bullet(match):
            bullet_lists.append(match.group(0))
            return f"%%BULLET{len(bullet_lists) - 1}%%"
        text = re.sub(r'^\s*\*\s+', save_bullet, text, flags=re.MULTILINE)
        
        # ==================== 3. 转换行内样式 ====================
        
        # 1. 转换粗体: *text* -> **text**
        text = re.sub(r'\*([^* \n][^* \n]*)\*', r'**\1**', text)
        
        # 2. 还原无序列表（Markdown 原生支持 * 列表）
        for i, b in enumerate(bullet_lists):
            text = text.replace(f"%%BULLET{i}%%", b)
            
        # 3. 转换斜体: _text_ -> *text*
        text = re.sub(r'_([^_ \n][^_ \n]*)_', r'*\1*', text)
        
        # 4. 转换删除线: ~text~ -> ~~text~~
        text = re.sub(r'~([^~ \n][^~ \n]*)~', r'~~\1~~', text)
        
        # ==================== 4. 终期还原 ====================
        for i, q in enumerate(quotes):
            text = text.replace(f"%%QUOTE{i}%%", q)
        for i, block in enumerate(code_blocks):
            text = text.replace(f"%%CODEBLOCK{i}%%", block)
            
        return text

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
            # 将 Synology 格式转为 Markdown
            text = self.synology_to_markdown(text)
            # Check allow_from list
            if self.config.allow_from and username not in self.config.allow_from:
                self.warning.info(f"Synology: ignoring message from unauthorized user {username}")
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
        syn_text = self.markdown_to_synology(text)
        try:
            payload = {
                'text': syn_text,
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
        self.logger.warning("Starting Synology Chat channel")

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

        self.logger.warning(f"Synology webhook listening on {self.config.webhook_host}:{self.config.webhook_port}{self.config.webhook_path}")

        self._running = True

        # Keep running
        while self._running:
            await asyncio.sleep(1)

    async def stop(self) -> None:
        """Stop the Synology Chat channel."""
        self.logger.warning("Stopping Synology Chat channel")
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