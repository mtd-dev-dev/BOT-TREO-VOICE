import asyncio
import json
import os
import time
import aiohttp
from aiohttp import web
import discord
from discord.ext import commands
import websockets

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
PORT = int(os.getenv("PORT", "10000"))
GATEWAY_URL = "wss://gateway.discord.gg/?v=10&encoding=json"

BADGE_MILESTONES = [
    ("Người mới", 1),
    ("Chim non", 5),
    ("Đột phá", 20),
    ("Nổi bật", 75),
    ("Người tạo xu hướng", 150),
    ("Tiêu điểm", 300),
    ("Ngôi sao", 500),
    ("Cảm giác", 1000),
    ("Nhà tiên tri", 2000),
    ("Xuất thần", 5000),
]

active_tasks: dict[int, asyncio.Task] = {}


async def send_webhook(webhook_url: str, content: str):
    if not webhook_url:
        return
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(webhook_url, json={"content": content}) as resp:
                if resp.status not in (200, 204):
                    print(f"[Webhook] Lỗi HTTP {resp.status}")
        except Exception as e:
            print(f"[Webhook] Lỗi gửi: {e}")


async def heartbeat_loop(ws, interval_sec: float, seq_holder: dict):
    while True:
        await asyncio.sleep(interval_sec)
        try:
            await ws.send(json.dumps({"op": 1, "d": seq_holder["seq"]}))
        except Exception:
            break


async def milestone_watcher(
    start_time: float,
    webhook_url: str,
    user_label: str,
    guild_id: str,
    channel_id: str,
):
    notified = set()
    while True:
        await asyncio.sleep(10)
        elapsed_hours = (time.monotonic() - start_time) / 3600.0

        for badge_name, required_hours in BADGE_MILESTONES:
            if badge_name not in notified and elapsed_hours >= required_hours:
                notified.add(badge_name)
                msg = (
                    f"🎉 **Đạt mốc huy hiệu Voice!**\n"
                    f"• Tài khoản: `{user_label}`\n"
                    f"• Huy hiệu: **{badge_name}** ({required_hours} giờ)\n"
                    f"• Server / Kênh: `{guild_id}` / `{channel_id}`\n"
                    f"• Thời gian treo thực tế: `{elapsed_hours:.2f}` giờ"
                )
                await send_webhook(webhook_url, msg)


async def run_voice_afk(
    owner_id: int,
    token: str,
    guild_id: str,
    channel_id: str,
    webhook_url: str,
    self_mute: bool = True,
    self_deaf: bool = True,
):
    seq_holder = {"seq": None}
    start_time = time.monotonic()
    watcher_task = None

    try:
        while True:
            hb_task = None
            try:
                async with websockets.connect(
                    GATEWAY_URL,
                    max_size=None,
                    ping_interval=None,
                ) as ws:
                    hello_data = json.loads(await ws.recv())
                    heartbeat_interval = hello_data["d"]["heartbeat_interval"] / 1000.0

                    hb_task = asyncio.create_task(
                        heartbeat_loop(ws, heartbeat_interval, seq_holder)
                    )

                    identify_payload = {
                        "op": 2,
                        "d": {
                            "token": token.strip(),
                            "capabilities": 16381,
                            "properties": {
                                "os": "Windows",
                                "browser": "Chrome",
                                "device": "",
                            },
                            "presence": {
                                "status": "online",
                                "since": 0,
                                "activities": [],
                                "afk": True,
                            },
                        },
                    }
                    await ws.send(json.dumps(identify_payload))

                    voice_payload = {
                        "op": 4,
                        "d": {
                            "guild_id": str(guild_id),
                            "channel_id": str(channel_id),
                            "self_mute": self_mute,
                            "self_deaf": self_deaf,
                        },
                    }
                    await ws.send(json.dumps(voice_payload))

                    if watcher_task is None or watcher_task.done():
                        watcher_task = asyncio.create_task(
                            milestone_watcher(
                                start_time=start_time,
                                webhook_url=webhook_url,
                                user_label=token[:12] + "...",
                                guild_id=guild_id,
                                channel_id=channel_id,
                            )
                        )

                    async for raw_msg in ws:
                        event = json.loads(raw_msg)
                        if event.get("s") is not None:
                            seq_holder["seq"] = event["s"]

                        op = event.get("op")
                        if op == 1:
                            await ws.send(json.dumps({"op": 1, "d": seq_holder["seq"]}))
                        elif op == 7:
                            break
                        elif op == 9:
                            await asyncio.sleep(5)
                            break

            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(f"[Gateway][User {owner_id}] Mất kết nối: {e}. Thử lại sau 5s...")
                await asyncio.sleep(5)
            finally:
                if hb_task and not hb_task.done():
                    hb_task.cancel()

    except asyncio.CancelledError:
        if watcher_task and not watcher_task.done():
            watcher_task.cancel()
        print(f"[Task][User {owner_id}] Đã dừng phiên treo voice.")


class TreoVoiceModal(discord.ui.Modal, title="Cấu Hình Treo Voice AFK"):
    token_user = discord.ui.TextInput(
        label="Token User",
        placeholder="Nhập Token tài khoản cần treo...",
        style=discord.TextStyle.short,
        required=True,
    )
    guild_id = discord.ui.TextInput(
        label="ID Guild (ID Server)",
        placeholder="Ví dụ: 1569569166246154240",
        style=discord.TextStyle.short,
        required=True,
    )
    channel_id = discord.ui.TextInput(
        label="ID Voice (ID Kênh Thoại)",
        placeholder="Ví dụ: 1573358338236883005",
        style=discord.TextStyle.short,
        required=True,
    )
    webhook_url = discord.ui.TextInput(
        label="Webhook URL (Thông báo đạt mốc)",
        placeholder="https://discord.com/api/webhooks/... (có thể để trống)",
        style=discord.TextStyle.short,
        required=False,
    )

    async def on_submit(self, interaction: discord.Interaction):
        user_id = interaction.user.id
        token = self.token_user.value.strip()
        g_id = self.guild_id.value.strip()
        c_id = self.channel_id.value.strip()
        wh_url = self.webhook_url.value.strip()

        if not g_id.isdigit() or not c_id.isdigit():
            await interaction.response.send_message(
                "❌ **ID Guild** và **ID Voice** phải là dãy số hợp lệ.",
                ephemeral=True,
            )
            return

        old_task = active_tasks.get(user_id)
        if old_task and not old_task.done():
            old_task.cancel()

        task = asyncio.create_task(
            run_voice_afk(
                owner_id=user_id,
                token=token,
                guild_id=g_id,
                channel_id=c_id,
                webhook_url=wh_url,
            )
        )
        active_tasks[user_id] = task

        embed = discord.Embed(
            title="✅ Đã Bắt Đầu Treo Voice",
            color=discord.Color.from_rgb(0, 255, 240),
        )
        embed.add_field(name="Token", value=f"`{token[:12]}...`", inline=True)
        embed.add_field(name="Guild ID", value=f"`{g_id}`", inline=True)
        embed.add_field(name="Voice ID", value=f"`{c_id}`", inline=True)
        embed.add_field(
            name="Webhook",
            value="Đã cấu hình" if wh_url else "Không sử dụng",
            inline=False,
        )
        embed.set_footer(text="Bot sẽ tự gửi Webhook khi đạt từng mốc huy hiệu.")
        await interaction.response.send_message(embed=embed, ephemeral=True)


class TreoVoicePanelView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="Nhập Thông Tin & Treo",
        style=discord.ButtonStyle.success,
        emoji="🎧",
        custom_id="btn_open_treo_voice_modal",
    )
    async def open_modal_button(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
        await interaction.response.send_modal(TreoVoiceModal())

    @discord.ui.button(
        label="Dừng Treo",
        style=discord.ButtonStyle.danger,
        emoji="🛑",
        custom_id="btn_stop_treo_voice",
    )
    async def stop_button(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
        task = active_tasks.pop(interaction.user.id, None)
        if task and not task.done():
            task.cancel()
            await interaction.response.send_message(
                "🛑 Đã dừng phiên treo Voice của bạn.", ephemeral=True
            )
        else:
            await interaction.response.send_message(
                "ℹ️ Bạn hiện không có phiên treo Voice nào đang chạy.", ephemeral=True
            )


intents = discord.Intents.default()
intents.message_content = True


class TreoVoiceBot(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix=".", intents=intents, case_insensitive=True)

    async def setup_hook(self):
        self.add_view(TreoVoicePanelView())


bot = TreoVoiceBot()


@bot.event
async def on_ready():
    print(f"Bot đã online: {bot.user} (ID: {bot.user.id})")


@bot.command(name="TreoVoice")
async def treo_voice_cmd(ctx: commands.Context):
    embed = discord.Embed(
        title="🎙️ PANEL TREO VOICE CÀY HUY HIỆU",
        description=(
            "Nhấn nút **Nhập Thông Tin & Treo** bên dưới để mở biểu mẫu cấu hình.\n\n"
            "**Các mốc huy hiệu tự động báo Webhook:**\n"
            "• `1h` Người mới • `5h` Chim non • `20h` Đột phá\n"
            "• `75h` Nổi bật • `150h` Người tạo xu hướng • `300h` Tiêu điểm\n"
            "• `500h` Ngôi sao • `1.000h` Cảm giác • `2.000h` Nhà tiên tri • `5.000h` Xuất thần"
        ),
        color=discord.Color.from_rgb(0, 255, 240),
    )
    embed.set_footer(text="Thông tin nhập vào Modal được ẩn riêng tư (Ephemeral).")
    await ctx.send(embed=embed, view=TreoVoicePanelView())


async def handle_health(request: web.Request) -> web.Response:
    return web.Response(text="OK - Bot is running", status=200)


async def start_web_server():
    app = web.Application()
    app.router.add_get("/", handle_health)
    app.router.add_get("/health", handle_health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    print(f"[HTTP] Keepalive server đang chạy trên cổng {PORT}")


async def main():
    if not BOT_TOKEN:
        raise RuntimeError("Thiếu biến môi trường BOT_TOKEN")
    await start_web_server()
    async with bot:
        await bot.start(BOT_TOKEN)


if __name__ == "__main__":
    asyncio.run(main())
