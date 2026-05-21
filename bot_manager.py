"""
╔══════════════════════════════════════════════╗
║        Bot Manager Pro - بوت إدارة البوتات     ║
║         Developed with Pyrogram + Redis        ║
╚══════════════════════════════════════════════╝

الأوامر المتاحة:
  /start         - رسالة الترحيب
  /help          - قائمة الأوامر
  /status        - حالة جميع البوتات
  /list          - قائمة البوتات المسجلة
  /start_bot     - تشغيل بوت
  /stop_bot      - إيقاف بوت
  /restart_bot   - إعادة تشغيل بوت
  /logs          - عرض آخر سطور الـ log
  /delete_bot    - حذف بوت
  /install       - تثبيت requirements.txt
  /server        - معلومات السيرفر
  /disk          - استخدام القرص
  /memory        - استخدام الذاكرة
  /cpu           - استخدام المعالج
  /broadcast     - إرسال رسالة لجميع المشرفين
  /addadmin      - إضافة مشرف
  /removeadmin   - إزالة مشرف
  /admins        - قائمة المشرفين
  /backup        - ضغط وتنزيل ملفات بوت
  /rename        - إعادة تسمية بوت
  /setenv        - ضبط متغير بيئة للبوت
  /getenv        - عرض متغيرات بيئة بوت
  /schedule      - جدولة إعادة تشغيل تلقائية
  /uptime        - وقت تشغيل السيرفر

إرسال ملف .py أو .zip مباشرة لرفعه.
"""

import asyncio
import json
import logging
import os
import platform
import re
import shutil
import signal
import subprocess
import sys
import tarfile
import time
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import psutil
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pyrogram import Client, filters
from pyrogram.types import (
    BotCommand,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

# ───────────────────────────── إعدادات ─────────────────────────────

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))          # يوزر ID المالك
BOTS_DIR = Path(os.getenv("BOTS_DIR", "/home/ubuntu/managed_bots"))
DATA_FILE = Path(os.getenv("DATA_FILE", "/home/ubuntu/bot_manager_data.json"))
LOG_LINES = int(os.getenv("LOG_LINES", "30"))        # عدد سطور الـ log

BOTS_DIR.mkdir(parents=True, exist_ok=True)

# ───────────────────────────── Logging ──────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("bot_manager.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("BotManager")

# ───────────────────────────── Data Store ───────────────────────────

def load_data() -> dict:
    if DATA_FILE.exists():
        try:
            return json.loads(DATA_FILE.read_text())
        except Exception:
            pass
    return {"admins": [OWNER_ID], "bots": {}, "schedules": {}, "envs": {}}

def save_data(data: dict):
    DATA_FILE.write_text(json.dumps(data, indent=2, ensure_ascii=False))

data = load_data()

def get_admins() -> list[int]:
    return data.get("admins", [OWNER_ID])

def is_admin(user_id: int) -> bool:
    return user_id in get_admins()

def get_bots() -> dict:
    return data.get("bots", {})

def save_bot(name: str, info: dict):
    data.setdefault("bots", {})[name] = info
    save_data(data)

def remove_bot_data(name: str):
    data.get("bots", {}).pop(name, None)
    data.get("envs", {}).pop(name, None)
    data.get("schedules", {}).pop(name, None)
    save_data(data)

# ───────────────────────────── Process Manager ──────────────────────

running_processes: dict[str, subprocess.Popen] = {}

def get_bot_env(name: str) -> dict:
    envs = data.get("envs", {}).get(name, {})
    env = os.environ.copy()
    env.update(envs)
    return env

def start_process(name: str, script_path: Path) -> subprocess.Popen:
    log_path = BOTS_DIR / name / f"{name}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_file = open(log_path, "a")
    env = get_bot_env(name)
    proc = subprocess.Popen(
        [sys.executable, str(script_path)],
        stdout=log_file,
        stderr=log_file,
        cwd=str(script_path.parent),
        env=env,
        preexec_fn=os.setsid,
    )
    running_processes[name] = proc
    info = get_bots().get(name, {})
    info.update({"pid": proc.pid, "started_at": datetime.now().isoformat(), "script": str(script_path)})
    save_bot(name, info)
    log.info(f"Started bot '{name}' PID={proc.pid}")
    return proc

def stop_process(name: str) -> bool:
    proc = running_processes.get(name)
    if proc and proc.poll() is None:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            proc.wait(timeout=5)
        except Exception:
            proc.kill()
        del running_processes[name]
        info = get_bots().get(name, {})
        info["pid"] = None
        save_bot(name, info)
        log.info(f"Stopped bot '{name}'")
        return True
    return False

def is_running(name: str) -> bool:
    proc = running_processes.get(name)
    if proc and proc.poll() is None:
        return True
    # تحقق من PID المحفوظ
    pid = get_bots().get(name, {}).get("pid")
    if pid:
        try:
            os.kill(pid, 0)
            return True
        except Exception:
            pass
    return False

# ───────────────────────────── Helpers ──────────────────────────────

def human_size(size: float) -> str:
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} PB"

def uptime_str(seconds: float) -> str:
    d, r = divmod(int(seconds), 86400)
    h, r = divmod(r, 3600)
    m, s = divmod(r, 60)
    parts = []
    if d: parts.append(f"{d}d")
    if h: parts.append(f"{h}h")
    if m: parts.append(f"{m}m")
    parts.append(f"{s}s")
    return " ".join(parts)

def bot_status_emoji(name: str) -> str:
    return "🟢" if is_running(name) else "🔴"

def safe_name(name: str) -> str:
    return re.sub(r"[^\w\-]", "_", name)

admin_filter = filters.create(lambda _, __, m: is_admin(m.from_user.id) if m.from_user else False)

# ───────────────────────────── Bot Client ───────────────────────────

app = Client("bot_manager", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
scheduler = AsyncIOScheduler()

# ═══════════════════════════ أوامر عامة ════════════════════════════

@app.on_message(filters.command("start") & admin_filter)
async def cmd_start(_, m: Message):
    bots_count = len(get_bots())
    running_count = sum(1 for n in get_bots() if is_running(n))
    await m.reply(
        "🤖 **Bot Manager Pro**\n\n"
        f"👤 مرحباً `{m.from_user.first_name}`\n"
        f"📦 البوتات المسجلة: `{bots_count}`\n"
        f"✅ تعمل الآن: `{running_count}`\n\n"
        "📌 أرسل `/help` لعرض الأوامر\n"
        "📁 أرسل ملف `.py` أو `.zip` لرفع بوت جديد",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📊 الحالة", callback_data="status"),
             InlineKeyboardButton("📋 القائمة", callback_data="list")],
            [InlineKeyboardButton("🖥 السيرفر", callback_data="server"),
             InlineKeyboardButton("❓ المساعدة", callback_data="help")],
        ]),
    )

@app.on_message(filters.command("help") & admin_filter)
async def cmd_help(_, m: Message):
    await m.reply(
        "📖 **قائمة الأوامر**\n\n"
        "**📦 إدارة البوتات:**\n"
        "`/list` — قائمة البوتات\n"
        "`/status` — حالة جميع البوتات\n"
        "`/start_bot <name>` — تشغيل بوت\n"
        "`/stop_bot <name>` — إيقاف بوت\n"
        "`/restart_bot <name>` — إعادة تشغيل\n"
        "`/logs <name> [lines]` — عرض الـ logs\n"
        "`/delete_bot <name>` — حذف بوت\n"
        "`/rename <old> <new>` — إعادة تسمية\n"
        "`/backup <name>` — تنزيل ملفات البوت\n"
        "`/install <name>` — تثبيت requirements\n\n"
        "**🔧 متغيرات البيئة:**\n"
        "`/setenv <name> KEY=VALUE` — ضبط متغير\n"
        "`/getenv <name>` — عرض المتغيرات\n\n"
        "**⏰ الجدولة:**\n"
        "`/schedule <name> <interval_min>` — إعادة تشغيل دورية\n"
        "`/unschedule <name>` — إلغاء الجدولة\n\n"
        "**🖥 السيرفر:**\n"
        "`/server` — معلومات عامة\n"
        "`/cpu` — استخدام المعالج\n"
        "`/memory` — استخدام الذاكرة\n"
        "`/disk` — استخدام القرص\n"
        "`/uptime` — وقت التشغيل\n\n"
        "**👥 المشرفون:**\n"
        "`/admins` — قائمة المشرفين\n"
        "`/addadmin <id>` — إضافة مشرف\n"
        "`/removeadmin <id>` — إزالة مشرف\n"
        "`/broadcast <msg>` — إرسال جماعي\n\n"
        "📁 **أرسل ملف `.py` أو `.zip` لرفع بوت جديد**"
    )

# ═══════════════════════════ رفع البوتات ═══════════════════════════

@app.on_message(filters.document & admin_filter)
async def handle_upload(_, m: Message):
    doc = m.document
    fname = doc.file_name or "unknown"
    ext = Path(fname).suffix.lower()

    if ext not in (".py", ".zip"):
        return await m.reply("❌ فقط ملفات `.py` أو `.zip` مسموحة")

    msg = await m.reply("⏳ جاري الرفع...")
    tmp = BOTS_DIR / "_tmp" / fname
    tmp.parent.mkdir(parents=True, exist_ok=True)
    await m.download(str(tmp))

    if ext == ".py":
        name = safe_name(Path(fname).stem)
        bot_dir = BOTS_DIR / name
        bot_dir.mkdir(parents=True, exist_ok=True)
        dest = bot_dir / fname
        shutil.move(str(tmp), str(dest))
        save_bot(name, {"script": str(dest), "pid": None, "uploaded_at": datetime.now().isoformat()})
        await msg.edit(
            f"✅ **تم رفع البوت**\n\n"
            f"📛 الاسم: `{name}`\n"
            f"📄 الملف: `{fname}`\n\n"
            f"▶️ شغّله بـ `/start_bot {name}`",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(f"▶️ تشغيل {name}", callback_data=f"start:{name}")]
            ]),
        )

    elif ext == ".zip":
        name = safe_name(Path(fname).stem)
        bot_dir = BOTS_DIR / name
        bot_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(tmp, "r") as z:
            z.extractall(bot_dir)
        tmp.unlink()

        # ابحث عن main.py أو bot.py
        scripts = list(bot_dir.rglob("main.py")) + list(bot_dir.rglob("bot.py")) + list(bot_dir.glob("*.py"))
        if not scripts:
            return await msg.edit("❌ لم أجد ملف `.py` داخل الـ ZIP")

        script = scripts[0]
        save_bot(name, {"script": str(script), "pid": None, "uploaded_at": datetime.now().isoformat()})

        # تثبيت requirements تلقائياً إن وجد
        req = bot_dir / "requirements.txt"
        extra = ""
        if req.exists():
            await msg.edit("📦 جاري تثبيت المكتبات...")
            result = subprocess.run(
                [sys.executable, "-m", "pip", "install", "-r", str(req), "-q"],
                capture_output=True, text=True, timeout=120
            )
            extra = "\n✅ تم تثبيت المكتبات" if result.returncode == 0 else f"\n⚠️ خطأ في التثبيت:\n`{result.stderr[-300:]}`"

        await msg.edit(
            f"✅ **تم رفع البوت**\n\n"
            f"📛 الاسم: `{name}`\n"
            f"📄 السكريبت: `{script.name}`{extra}\n\n"
            f"▶️ شغّله بـ `/start_bot {name}`",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(f"▶️ تشغيل {name}", callback_data=f"start:{name}")]
            ]),
        )

# ═══════════════════════════ إدارة البوتات ═════════════════════════

@app.on_message(filters.command("list") & admin_filter)
async def cmd_list(_, m: Message):
    bots = get_bots()
    if not bots:
        return await m.reply("📭 لا يوجد بوتات مسجلة بعد\n\nأرسل ملف `.py` أو `.zip` لإضافة بوت")

    lines = ["📋 **قائمة البوتات**\n"]
    for name, info in bots.items():
        status = bot_status_emoji(name)
        uploaded = info.get("uploaded_at", "—")[:10]
        lines.append(f"{status} `{name}` — رُفع: {uploaded}")

    await m.reply("\n".join(lines))

@app.on_message(filters.command("status") & admin_filter)
async def cmd_status(_, m: Message):
    bots = get_bots()
    if not bots:
        return await m.reply("📭 لا يوجد بوتات")

    lines = ["📊 **حالة البوتات**\n"]
    for name, info in bots.items():
        running = is_running(name)
        status = "🟢 يعمل" if running else "🔴 متوقف"
        pid = info.get("pid", "—")
        started = info.get("started_at", "—")
        started = started[:16].replace("T", " ") if started and started != "—" else "—"
        lines.append(f"**{name}**\n  ↳ {status} | PID: `{pid}` | بدأ: `{started}`")

    await m.reply("\n\n".join(lines))

@app.on_message(filters.command("start_bot") & admin_filter)
async def cmd_start_bot(_, m: Message):
    if len(m.command) < 2:
        return await m.reply("❌ الاستخدام: `/start_bot <name>`")
    name = m.command[1]
    info = get_bots().get(name)
    if not info:
        return await m.reply(f"❌ البوت `{name}` غير موجود")
    if is_running(name):
        return await m.reply(f"⚠️ البوت `{name}` يعمل بالفعل")

    script = Path(info["script"])
    if not script.exists():
        return await m.reply(f"❌ الملف غير موجود: `{script}`")

    msg = await m.reply(f"⏳ جاري تشغيل `{name}`...")
    proc = start_process(name, script)
    await asyncio.sleep(2)
    if proc.poll() is not None:
        log_path = BOTS_DIR / name / f"{name}.log"
        err = log_path.read_text()[-500:] if log_path.exists() else "لا يوجد log"
        await msg.edit(f"❌ فشل تشغيل `{name}`\n\n```{err}```")
    else:
        await msg.edit(
            f"✅ **تم تشغيل البوت**\n\n"
            f"📛 الاسم: `{name}`\n"
            f"🔢 PID: `{proc.pid}`",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🛑 إيقاف", callback_data=f"stop:{name}"),
                 InlineKeyboardButton("📜 Logs", callback_data=f"logs:{name}")]
            ]),
        )

@app.on_message(filters.command("stop_bot") & admin_filter)
async def cmd_stop_bot(_, m: Message):
    if len(m.command) < 2:
        return await m.reply("❌ الاستخدام: `/stop_bot <name>`")
    name = m.command[1]
    if not get_bots().get(name):
        return await m.reply(f"❌ البوت `{name}` غير موجود")
    stopped = stop_process(name)
    if stopped:
        await m.reply(f"🛑 تم إيقاف `{name}`")
    else:
        await m.reply(f"⚠️ `{name}` لم يكن يعمل")

@app.on_message(filters.command("restart_bot") & admin_filter)
async def cmd_restart_bot(_, m: Message):
    if len(m.command) < 2:
        return await m.reply("❌ الاستخدام: `/restart_bot <name>`")
    name = m.command[1]
    info = get_bots().get(name)
    if not info:
        return await m.reply(f"❌ البوت `{name}` غير موجود")
    msg = await m.reply(f"🔄 جاري إعادة تشغيل `{name}`...")
    stop_process(name)
    await asyncio.sleep(1)
    proc = start_process(name, Path(info["script"]))
    await asyncio.sleep(2)
    if proc.poll() is not None:
        await msg.edit(f"❌ فشل إعادة التشغيل لـ `{name}`")
    else:
        await msg.edit(f"✅ تم إعادة تشغيل `{name}` — PID: `{proc.pid}`")

@app.on_message(filters.command("logs") & admin_filter)
async def cmd_logs(_, m: Message):
    if len(m.command) < 2:
        return await m.reply("❌ الاستخدام: `/logs <name> [lines]`")
    name = m.command[1]
    lines = int(m.command[2]) if len(m.command) > 2 else LOG_LINES

    if not get_bots().get(name):
        return await m.reply(f"❌ البوت `{name}` غير موجود")

    log_path = BOTS_DIR / name / f"{name}.log"
    if not log_path.exists():
        return await m.reply(f"📭 لا يوجد log لـ `{name}` بعد")

    content = subprocess.getoutput(f"tail -{lines} {log_path}")
    if not content.strip():
        return await m.reply(f"📭 الـ log فارغ لـ `{name}`")

    # إذا الـ log طويل، أرسله كملف
    if len(content) > 3500:
        tmp = Path(f"/tmp/{name}_logs.txt")
        tmp.write_text(content)
        await m.reply_document(str(tmp), caption=f"📜 آخر {lines} سطر من `{name}`")
        tmp.unlink()
    else:
        await m.reply(f"📜 **Logs — {name}** (آخر {lines} سطر)\n\n```\n{content}\n```")

@app.on_message(filters.command("delete_bot") & admin_filter)
async def cmd_delete_bot(_, m: Message):
    if len(m.command) < 2:
        return await m.reply("❌ الاستخدام: `/delete_bot <name>`")
    name = m.command[1]
    if not get_bots().get(name):
        return await m.reply(f"❌ البوت `{name}` غير موجود")

    await m.reply(
        f"⚠️ **تأكيد الحذف**\n\nهل أنت متأكد من حذف `{name}`؟\nسيتم حذف الملفات نهائياً!",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ نعم، احذف", callback_data=f"delete_confirm:{name}"),
             InlineKeyboardButton("❌ إلغاء", callback_data="cancel")]
        ]),
    )

@app.on_message(filters.command("rename") & admin_filter)
async def cmd_rename(_, m: Message):
    if len(m.command) < 3:
        return await m.reply("❌ الاستخدام: `/rename <old_name> <new_name>`")
    old, new = m.command[1], safe_name(m.command[2])
    info = get_bots().get(old)
    if not info:
        return await m.reply(f"❌ البوت `{old}` غير موجود")
    if get_bots().get(new):
        return await m.reply(f"❌ الاسم `{new}` مستخدم بالفعل")

    stop_process(old)
    old_dir = BOTS_DIR / old
    new_dir = BOTS_DIR / new
    shutil.move(str(old_dir), str(new_dir))

    new_script = str(info["script"]).replace(str(old_dir), str(new_dir))
    info["script"] = new_script
    remove_bot_data(old)
    save_bot(new, info)
    await m.reply(f"✅ تمت إعادة التسمية: `{old}` ← `{new}`")

@app.on_message(filters.command("backup") & admin_filter)
async def cmd_backup(_, m: Message):
    if len(m.command) < 2:
        return await m.reply("❌ الاستخدام: `/backup <name>`")
    name = m.command[1]
    if not get_bots().get(name):
        return await m.reply(f"❌ البوت `{name}` غير موجود")

    bot_dir = BOTS_DIR / name
    archive = Path(f"/tmp/{name}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip")
    msg = await m.reply(f"📦 جاري ضغط ملفات `{name}`...")

    with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as z:
        for f in bot_dir.rglob("*"):
            if f.is_file() and ".log" not in f.name:
                z.write(f, f.relative_to(bot_dir))

    await m.reply_document(str(archive), caption=f"📦 نسخة احتياطية من `{name}`")
    archive.unlink()
    await msg.delete()

@app.on_message(filters.command("install") & admin_filter)
async def cmd_install(_, m: Message):
    if len(m.command) < 2:
        return await m.reply("❌ الاستخدام: `/install <name>`")
    name = m.command[1]
    if not get_bots().get(name):
        return await m.reply(f"❌ البوت `{name}` غير موجود")

    req = BOTS_DIR / name / "requirements.txt"
    if not req.exists():
        return await m.reply(f"❌ لا يوجد `requirements.txt` في `{name}`")

    msg = await m.reply(f"📦 جاري تثبيت مكتبات `{name}`...")
    result = subprocess.run(
        [sys.executable, "-m", "pip", "install", "-r", str(req)],
        capture_output=True, text=True, timeout=180
    )
    output = (result.stdout + result.stderr)[-2000:]
    status = "✅ تم التثبيت بنجاح" if result.returncode == 0 else "❌ فشل التثبيت"
    await msg.edit(f"{status}\n\n```\n{output}\n```")

# ═══════════════════════════ متغيرات البيئة ════════════════════════

@app.on_message(filters.command("setenv") & admin_filter)
async def cmd_setenv(_, m: Message):
    if len(m.command) < 3:
        return await m.reply("❌ الاستخدام: `/setenv <name> KEY=VALUE`")
    name = m.command[1]
    kv = m.command[2]
    if "=" not in kv:
        return await m.reply("❌ الصيغة: `KEY=VALUE`")
    if not get_bots().get(name):
        return await m.reply(f"❌ البوت `{name}` غير موجود")

    key, value = kv.split("=", 1)
    data.setdefault("envs", {}).setdefault(name, {})[key] = value
    save_data(data)
    await m.reply(f"✅ تم ضبط `{key}` لـ `{name}`")

@app.on_message(filters.command("getenv") & admin_filter)
async def cmd_getenv(_, m: Message):
    if len(m.command) < 2:
        return await m.reply("❌ الاستخدام: `/getenv <name>`")
    name = m.command[1]
    envs = data.get("envs", {}).get(name, {})
    if not envs:
        return await m.reply(f"📭 لا توجد متغيرات بيئة لـ `{name}`")
    lines = [f"🔧 **متغيرات البيئة — {name}**\n"]
    for k, v in envs.items():
        lines.append(f"`{k}` = `{v}`")
    await m.reply("\n".join(lines))

# ═══════════════════════════ الجدولة ═══════════════════════════════

@app.on_message(filters.command("schedule") & admin_filter)
async def cmd_schedule(_, m: Message):
    if len(m.command) < 3:
        return await m.reply("❌ الاستخدام: `/schedule <name> <interval_minutes>`")
    name, interval = m.command[1], int(m.command[2])
    if not get_bots().get(name):
        return await m.reply(f"❌ البوت `{name}` غير موجود")

    job_id = f"restart_{name}"
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)

    async def restart_job():
        info = get_bots().get(name)
        if info:
            stop_process(name)
            await asyncio.sleep(1)
            start_process(name, Path(info["script"]))
            log.info(f"Scheduled restart: {name}")

    scheduler.add_job(restart_job, "interval", minutes=interval, id=job_id)
    data.setdefault("schedules", {})[name] = interval
    save_data(data)
    await m.reply(f"⏰ تم جدولة إعادة تشغيل `{name}` كل `{interval}` دقيقة")

@app.on_message(filters.command("unschedule") & admin_filter)
async def cmd_unschedule(_, m: Message):
    if len(m.command) < 2:
        return await m.reply("❌ الاستخدام: `/unschedule <name>`")
    name = m.command[1]
    job_id = f"restart_{name}"
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)
        data.get("schedules", {}).pop(name, None)
        save_data(data)
        await m.reply(f"✅ تم إلغاء جدولة `{name}`")
    else:
        await m.reply(f"⚠️ لا توجد جدولة لـ `{name}`")

# ═══════════════════════════ معلومات السيرفر ═══════════════════════

@app.on_message(filters.command("server") & admin_filter)
async def cmd_server(_, m: Message):
    cpu = psutil.cpu_percent(interval=1)
    mem = psutil.virtual_memory()
    disk = psutil.disk_usage("/")
    boot = datetime.fromtimestamp(psutil.boot_time())
    up = uptime_str((datetime.now() - boot).total_seconds())

    await m.reply(
        "🖥 **معلومات السيرفر**\n\n"
        f"🐧 النظام: `{platform.system()} {platform.release()}`\n"
        f"🏠 الاسم: `{platform.node()}`\n"
        f"⚙️ المعالج: `{platform.processor() or 'N/A'}`\n"
        f"🔢 الأنوية: `{psutil.cpu_count()} cores`\n"
        f"📊 CPU: `{cpu}%`\n"
        f"💾 RAM: `{human_size(mem.used)} / {human_size(mem.total)}` ({mem.percent}%)\n"
        f"💿 Disk: `{human_size(disk.used)} / {human_size(disk.total)}` ({disk.percent}%)\n"
        f"⏱ Uptime: `{up}`\n"
        f"🐍 Python: `{platform.python_version()}`"
    )

@app.on_message(filters.command("cpu") & admin_filter)
async def cmd_cpu(_, m: Message):
    cpu = psutil.cpu_percent(interval=1, percpu=True)
    avg = sum(cpu) / len(cpu)
    lines = [f"🔲 Core {i}: `{p}%`" for i, p in enumerate(cpu)]
    await m.reply(f"⚙️ **استخدام المعالج**\n\nالمتوسط: `{avg:.1f}%`\n\n" + "\n".join(lines))

@app.on_message(filters.command("memory") & admin_filter)
async def cmd_memory(_, m: Message):
    mem = psutil.virtual_memory()
    swap = psutil.swap_memory()
    await m.reply(
        "💾 **استخدام الذاكرة**\n\n"
        f"الكلي: `{human_size(mem.total)}`\n"
        f"المستخدم: `{human_size(mem.used)}` ({mem.percent}%)\n"
        f"المتاح: `{human_size(mem.available)}`\n\n"
        f"**SWAP:**\n"
        f"الكلي: `{human_size(swap.total)}`\n"
        f"المستخدم: `{human_size(swap.used)}` ({swap.percent}%)"
    )

@app.on_message(filters.command("disk") & admin_filter)
async def cmd_disk(_, m: Message):
    partitions = psutil.disk_partitions()
    lines = ["💿 **استخدام القرص**\n"]
    for p in partitions:
        try:
            usage = psutil.disk_usage(p.mountpoint)
            lines.append(
                f"📁 `{p.mountpoint}`\n"
                f"   {human_size(usage.used)} / {human_size(usage.total)} ({usage.percent}%)"
            )
        except Exception:
            pass
    await m.reply("\n".join(lines))

@app.on_message(filters.command("uptime") & admin_filter)
async def cmd_uptime(_, m: Message):
    boot = datetime.fromtimestamp(psutil.boot_time())
    up = uptime_str((datetime.now() - boot).total_seconds())
    await m.reply(f"⏱ **وقت التشغيل:** `{up}`\n🕐 بدأ من: `{boot.strftime('%Y-%m-%d %H:%M:%S')}`")

# ═══════════════════════════ إدارة المشرفين ════════════════════════

@app.on_message(filters.command("admins") & admin_filter)
async def cmd_admins(_, m: Message):
    admins = get_admins()
    lines = [f"👤 `{uid}`" for uid in admins]
    await m.reply("👥 **المشرفون**\n\n" + "\n".join(lines))

@app.on_message(filters.command("addadmin") & filters.user(OWNER_ID))
async def cmd_add_admin(_, m: Message):
    if len(m.command) < 2:
        return await m.reply("❌ الاستخدام: `/addadmin <user_id>`")
    uid = int(m.command[1])
    if uid in get_admins():
        return await m.reply("⚠️ هذا المستخدم مشرف بالفعل")
    data.setdefault("admins", [OWNER_ID]).append(uid)
    save_data(data)
    await m.reply(f"✅ تم إضافة `{uid}` كمشرف")

@app.on_message(filters.command("removeadmin") & filters.user(OWNER_ID))
async def cmd_remove_admin(_, m: Message):
    if len(m.command) < 2:
        return await m.reply("❌ الاستخدام: `/removeadmin <user_id>`")
    uid = int(m.command[1])
    if uid == OWNER_ID:
        return await m.reply("❌ لا يمكن إزالة المالك")
    if uid not in get_admins():
        return await m.reply("⚠️ هذا المستخدم ليس مشرفاً")
    data["admins"].remove(uid)
    save_data(data)
    await m.reply(f"✅ تم إزالة `{uid}` من المشرفين")

@app.on_message(filters.command("broadcast") & filters.user(OWNER_ID))
async def cmd_broadcast(_, m: Message):
    if len(m.command) < 2:
        return await m.reply("❌ الاستخدام: `/broadcast <رسالة>`")
    text = m.text.split(None, 1)[1]
    sent, failed = 0, 0
    for uid in get_admins():
        try:
            await app.send_message(uid, f"📢 **إعلان:**\n\n{text}")
            sent += 1
        except Exception:
            failed += 1
    await m.reply(f"📢 تم الإرسال\n✅ نجح: {sent}\n❌ فشل: {failed}")

# ═══════════════════════════ Callbacks ═════════════════════════════

@app.on_callback_query()
async def handle_callbacks(_, q):
    if not is_admin(q.from_user.id):
        return await q.answer("❌ ليس لديك صلاحية", show_alert=True)

    data_cb = q.data

    if data_cb == "status":
        bots = get_bots()
        if not bots:
            return await q.answer("لا يوجد بوتات", show_alert=True)
        lines = ["📊 حالة البوتات\n"]
        for name in bots:
            lines.append(f"{bot_status_emoji(name)} {name}")
        await q.message.reply("\n".join(lines))

    elif data_cb == "list":
        bots = get_bots()
        if not bots:
            return await q.answer("لا يوجد بوتات", show_alert=True)
        lines = ["📋 البوتات\n"] + [f"• `{n}`" for n in bots]
        await q.message.reply("\n".join(lines))

    elif data_cb == "server":
        cpu = psutil.cpu_percent(interval=0.5)
        mem = psutil.virtual_memory()
        await q.message.reply(
            f"🖥 CPU: `{cpu}%` | RAM: `{mem.percent}%` ({human_size(mem.used)}/{human_size(mem.total)})"
        )

    elif data_cb == "help":
        await q.message.reply("📖 أرسل `/help` لقائمة الأوامر الكاملة")

    elif data_cb.startswith("start:"):
        name = data_cb.split(":", 1)[1]
        info = get_bots().get(name)
        if not info:
            return await q.answer("البوت غير موجود", show_alert=True)
        if is_running(name):
            return await q.answer("البوت يعمل بالفعل ✅", show_alert=True)
        proc = start_process(name, Path(info["script"]))
        await asyncio.sleep(1)
        if proc.poll() is None:
            await q.answer(f"✅ تم تشغيل {name}", show_alert=True)
        else:
            await q.answer(f"❌ فشل تشغيل {name}", show_alert=True)

    elif data_cb.startswith("stop:"):
        name = data_cb.split(":", 1)[1]
        stopped = stop_process(name)
        await q.answer(f"🛑 تم إيقاف {name}" if stopped else "⚠️ لم يكن يعمل", show_alert=True)

    elif data_cb.startswith("logs:"):
        name = data_cb.split(":", 1)[1]
        log_path = BOTS_DIR / name / f"{name}.log"
        if not log_path.exists():
            return await q.answer("لا يوجد log", show_alert=True)
        content = subprocess.getoutput(f"tail -10 {log_path}")
        await q.message.reply(f"📜 `{name}`\n\n```\n{content}\n```")

    elif data_cb.startswith("delete_confirm:"):
        name = data_cb.split(":", 1)[1]
        stop_process(name)
        bot_dir = BOTS_DIR / name
        if bot_dir.exists():
            shutil.rmtree(bot_dir)
        remove_bot_data(name)
        await q.message.edit(f"🗑 تم حذف `{name}` نهائياً")

    elif data_cb == "cancel":
        await q.message.edit("❌ تم الإلغاء")

    await q.answer()

# ═══════════════════════════ Watchdog ══════════════════════════════

async def watchdog():
    """يراقب البوتات ويعيد تشغيلها إذا توقفت"""
    while True:
        for name, info in list(get_bots().items()):
            if info.get("pid") and not is_running(name):
                script = Path(info.get("script", ""))
                if script.exists():
                    log.warning(f"Watchdog: restarting '{name}'")
                    start_process(name, script)
                    # إشعار المالك
                    try:
                        await app.send_message(OWNER_ID, f"⚠️ **Watchdog**: أعدت تشغيل `{name}` تلقائياً")
                    except Exception:
                        pass
        await asyncio.sleep(30)

# ═══════════════════════════ التشغيل ═══════════════════════════════

async def set_commands():
    await app.set_bot_commands([
        BotCommand("start", "الصفحة الرئيسية"),
        BotCommand("help", "قائمة الأوامر"),
        BotCommand("list", "قائمة البوتات"),
        BotCommand("status", "حالة البوتات"),
        BotCommand("start_bot", "تشغيل بوت"),
        BotCommand("stop_bot", "إيقاف بوت"),
        BotCommand("restart_bot", "إعادة تشغيل"),
        BotCommand("logs", "عرض السجلات"),
        BotCommand("backup", "نسخة احتياطية"),
        BotCommand("server", "معلومات السيرفر"),
        BotCommand("cpu", "استخدام المعالج"),
        BotCommand("memory", "استخدام الذاكرة"),
        BotCommand("disk", "استخدام القرص"),
        BotCommand("uptime", "وقت التشغيل"),
        BotCommand("admins", "قائمة المشرفين"),
        BotCommand("broadcast", "إرسال جماعي"),
    ])

async def main():
    await app.start()
    await set_commands()
    log.info("Bot Manager Pro started ✅")

    # استعادة الجدولة المحفوظة
    for name, interval in data.get("schedules", {}).items():
        if get_bots().get(name):
            job_id = f"restart_{name}"
            async def _job(n=name):
                info = get_bots().get(n)
                if info:
                    stop_process(n)
                    await asyncio.sleep(1)
                    start_process(n, Path(info["script"]))
            scheduler.add_job(_job, "interval", minutes=interval, id=job_id)
            log.info(f"Restored schedule for '{name}' every {interval}m")

    scheduler.start()
    asyncio.create_task(watchdog())

    await app.send_message(OWNER_ID, "🚀 **Bot Manager Pro** تم التشغيل بنجاح!")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
