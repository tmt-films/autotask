import asyncio
import logging
import sqlite3
import json
import re
from datetime import datetime, timedelta, timezone # Import timezone
from typing import Dict, List, Optional, Tuple, Union
from pyrogram import Client, filters, enums
from pyrogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, 
    InlineKeyboardButton, User, Chat
)
from pyrogram.errors import (
    FloodWait, ChatAdminRequired, UserNotParticipant,
    MessageNotModified, ButtonDataInvalid, RPCError
)
import aiofiles
import aiohttp
import os
from pathlib import Path

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Configuration with environment variable support
def load_config():
    """Load configuration from environment variables or config file"""
    config = {}
    
    # Try to load from environment variables first
    config['API_ID'] = os.getenv('API_ID')
    config['API_HASH'] = os.getenv('API_HASH') 
    config['BOT_TOKEN'] = os.getenv('BOT_TOKEN')
    config['FORCE_SUB_CHANNEL_ID'] = os.getenv('FORCE_SUB_CHANNEL_ID')
    admin_ids_str = os.getenv('ADMIN_IDS')
    config['ADMIN_IDS'] = [int(x.strip()) for x in admin_ids_str.split(',') if x.strip()] if admin_ids_str else []
    
    # If not found in env vars, try to load from config.py
    if not all([config['API_ID'], config['API_HASH'], config['BOT_TOKEN']]):
        try:
            from config import API_ID, API_HASH, BOT_TOKEN, FORCE_SUB_CHANNEL_ID, ADMIN_IDS
            config['API_ID'] = config['API_ID'] or API_ID
            config['API_HASH'] = config['API_HASH'] or API_HASH
            config['BOT_TOKEN'] = config['BOT_TOKEN'] or BOT_TOKEN
            config['FORCE_SUB_CHANNEL_ID'] = config['FORCE_SUB_CHANNEL_ID'] or FORCE_SUB_CHANNEL_ID
            config['ADMIN_IDS'] = config['ADMIN_IDS'] or ADMIN_IDS
        except ImportError:
            pass
    
    # Validate configuration
    missing = []
    if not config['API_ID']:
        missing.append('API_ID')
    if not config['API_HASH']:
        missing.append('API_HASH')
    if not config['BOT_TOKEN']:
        missing.append('BOT_TOKEN')
    
    if missing:
        print("❌ Missing required configuration:")
        for item in missing:
            print(f"   - {item}")
        print("\n📋 Setup Instructions:")
        print("1. Get API_ID and API_HASH from https://my.telegram.org")
        print("2. Get BOT_TOKEN from @BotFather")
        print("3. Either:")
        print("   a) Set environment variables: API_ID, API_HASH, BOT_TOKEN, (optional) FORCE_SUB_CHANNEL_ID, (optional) ADMIN_IDS (comma-separated)")
        print("   b) Create config.py with your credentials")
        print("\nExample config.py:")
        print("API_ID = 12345678")
        print("API_HASH = 'your_api_hash_here'")
        print("BOT_TOKEN = 'your_bot_token_here'")
        print("FORCE_SUB_CHANNEL_ID = '@YourPublicChannel'")
        print("ADMIN_IDS = [123456789, 987654321]")
        exit(1)
    
    # Convert API_ID to integer
    try:
        config['API_ID'] = int(config['API_ID'])
    except (ValueError, TypeError):
        print("❌ API_ID must be a valid integer")
        exit(1)
    
    return config

# Load configuration
CONFIG = load_config()
API_ID = CONFIG['API_ID']
API_HASH = CONFIG['API_HASH']
BOT_TOKEN = CONFIG['BOT_TOKEN']
FORCE_SUB_CHANNEL_ID = CONFIG['FORCE_SUB_CHANNEL_ID']
ADMIN_IDS = CONFIG['ADMIN_IDS']

# Rate limiting delays
ADMIN_DELAY = 1.5
FORWARD_DELAY = 2.0
BATCH_DELAY = 0.5
DELETE_DELAY = 1.0
FORCE_SUB_CHECK_DELAY = 0.5 # Small delay for force sub check
from pymongo import MongoClient
from bson.objectid import ObjectId

class Database:
    def __init__(self):
        self.client = MongoClient(os.getenv('MONGO_DB_URL'))
        self.db = self.client.autoposter
        self.jobs = self.db.jobs
        self.forwarded_messages = self.db.forwarded_messages
        self.user_states = self.db.user_states
        self.users = self.db.users
    
    def create_job(self, user_id: int, job_data: dict) -> str:
        """Create a new forwarding job"""
        job_document = {
            'user_id': user_id,
            'job_name': job_data['name'],
            'source_channel_id': job_data['source'],
            'target_channel_id': job_data['target'],
            'start_post_id': job_data['start_id'],
            'end_post_id': job_data['end_id'],
            'batch_size': job_data['batch_size'],
            'recurring_time': job_data['recurring_time'],
            'delete_time': job_data['delete_time'],
            'filter_type': job_data['filter_type'],
            'custom_caption': job_data.get('caption', ''),
            'button_text': job_data.get('button_text', ''),
            'button_url': job_data.get('button_url', ''),
            'is_active': False,
            'last_forwarded_id': 0,
            'created_at': datetime.utcnow(),
            'updated_at': datetime.utcnow()
        }
        result = self.jobs.insert_one(job_document)
        return str(result.inserted_id)
    
    def get_user_jobs(self, user_id: int) -> List[dict]:
        """Get all jobs for a user"""
        return list(self.jobs.find({'user_id': user_id}).sort('created_at', -1))
    
    def get_job(self, job_id: str) -> Optional[dict]:
        """Get a specific job by ID"""
        return self.jobs.find_one({'_id': ObjectId(job_id)})
    
    def update_job_status(self, job_id: str, is_active: bool):
        """Update job active status"""
        self.jobs.update_one({'_id': ObjectId(job_id)}, {'$set': {'is_active': is_active, 'updated_at': datetime.utcnow()}})
    
    def update_last_forwarded(self, job_id: str, message_id: int):
        """Update the last forwarded message ID"""
        self.jobs.update_one({'_id': ObjectId(job_id)}, {'$set': {'last_forwarded_id': message_id, 'updated_at': datetime.utcnow()}})
    
    def add_forwarded_message(self, job_id: str, original_id: int, forwarded_id: int):
        """Track a forwarded message"""
        self.forwarded_messages.insert_one({
            'job_id': ObjectId(job_id),
            'original_message_id': original_id,
            'forwarded_message_id': forwarded_id,
            'forwarded_at': datetime.utcnow()
        })
    
    def get_old_forwarded_messages(self, job_id: str, minutes_ago: int) -> List[int]:
        """Get forwarded messages older than specified minutes"""
        if minutes_ago <= 0:
            return []
            
        cutoff_time = datetime.utcnow() - timedelta(minutes=minutes_ago)
        
        messages = list(self.forwarded_messages.find({
            'job_id': ObjectId(job_id),
            'forwarded_at': {'$lt': cutoff_time}
        }))
        
        message_ids = [msg['forwarded_message_id'] for msg in messages]
        
        if message_ids:
            self.forwarded_messages.delete_many({'_id': {'$in': [msg['_id'] for msg in messages]}})

        return message_ids
    
    def save_user_state(self, user_id: int, state_data: dict):
        """Save user's current state"""
        self.user_states.update_one(
            {'user_id': user_id},
            {'$set': {'state_data': json.dumps(state_data), 'updated_at': datetime.utcnow()}},
            upsert=True
        )
    
    def get_user_state(self, user_id: int) -> Optional[dict]:
        """Get user's current state"""
        state = self.user_states.find_one({'user_id': user_id})
        if state:
            return json.loads(state['state_data'])
        return None
    
    def clear_user_state(self, user_id: int):
        """Clear user's state"""
        self.user_states.delete_one({'user_id': user_id})

    def reset_job_progress(self, job_id: str, start_post_id: int):
        """Reset the last forwarded message ID for a job"""
        self.jobs.update_one(
            {'_id': ObjectId(job_id)},
            {'$set': {'last_forwarded_id': start_post_id - 1, 'updated_at': datetime.utcnow()}}
        )
        self.forwarded_messages.delete_many({'job_id': ObjectId(job_id)})

    def delete_job(self, job_id: str):
        """Delete a job from the database"""
        self.jobs.delete_one({'_id': ObjectId(job_id)})
        self.forwarded_messages.delete_many({'job_id': ObjectId(job_id)})

    def update_job(self, job_id: str, job_data: dict):
        """Update a job in the database"""
        update_doc = {
            'job_name': job_data['name'],
            'source_channel_id': job_data['source'],
            'target_channel_id': job_data['target'],
            'start_post_id': job_data['start_id'],
            'end_post_id': job_data['end_id'],
            'batch_size': job_data['batch_size'],
            'recurring_time': job_data['recurring_time'],
            'delete_time': job_data['delete_time'],
            'filter_type': job_data['filter_type'],
            'custom_caption': job_data.get('caption', ''),
            'button_text': job_data.get('button_text', ''),
            'button_url': job_data.get('button_url', ''),
            'updated_at': datetime.utcnow()
        }
        self.jobs.update_one({'_id': ObjectId(job_id)}, {'$set': update_doc})

    def add_user_if_not_exists(self, user_id: int):
        """Add a user to the users table if they don't already exist."""
        self.users.update_one(
            {'user_id': user_id},
            {'$setOnInsert': {'first_interaction_at': datetime.utcnow()}},
            upsert=True
        )

    def get_total_users(self) -> int:
        """Get the total count of unique users who have interacted with the bot."""
        return self.users.count_documents({})

    def get_total_jobs(self) -> int:
        """Get the total count of all jobs created."""
        return self.jobs.count_documents({})

    def get_total_forwarded_messages(self) -> int:
        """Get the total count of all messages ever forwarded."""
        return self.forwarded_messages.count_documents({})

    def get_jobs_created_today(self) -> int:
        """Get the count of jobs created today (UTC)."""
        today_start_utc = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        return self.jobs.count_documents({'created_at': {'$gte': today_start_utc}})

    def get_forwarded_messages_today(self) -> int:
        """Get the count of messages forwarded today (UTC)."""
        today_start_utc = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        return self.forwarded_messages.count_documents({'forwarded_at': {'$gte': today_start_utc}})


class AutoposterBot:
    def __init__(self):
        try:
            self.app = Client(
                "autoposter_bot",
                api_id=API_ID,
                api_hash=API_HASH,
                bot_token=BOT_TOKEN
            )
            
            self.db = Database()
            self.active_jobs = {}
            self.job_locks = {}
            self.force_sub_channel_id = FORCE_SUB_CHANNEL_ID
            self.admin_ids = ADMIN_IDS # Store admin IDs
            
            # Register handlers
            self.register_handlers()
            
            logger.info("✅ Bot initialized successfully!")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize bot: {e}")
            print(f"❌ Bot initialization failed: {e}")
            print("\n🔧 Please check your configuration and try again.")
            exit(1)
    
    def register_handlers(self):
        """Register all bot handlers"""
        
        @self.app.on_message(filters.command("start") & filters.private)
        async def start_command(client: Client, message: Message):
            await self.handle_start(client, message)
        
        @self.app.on_message(filters.command("stats") & filters.private)
        async def stats_command(client: Client, message: Message):
            await self.handle_stats(client, message)

        @self.app.on_callback_query()
        async def callback_handler(client: Client, callback_query: CallbackQuery):
            await self.handle_callback(client, callback_query)
        
        @self.app.on_message(filters.text & filters.private & ~filters.command("start") & ~filters.command("stats"))
        async def text_handler(client: Client, message: Message):
            await self.handle_text_message(client, message)
    
    def is_user_admin(self, user_id: int) -> bool:
        """Check if the given user ID is in the admin list."""
        return user_id in self.admin_ids

    async def check_user_subscription(self, user_id: int, message_obj: Union[Message, CallbackQuery]) -> bool:
        """
        Checks if the user is subscribed to the force subscribe channel.
        If not, sends a message prompting them to join.
        Returns True if subscribed, False otherwise.
        """
        if not self.force_sub_channel_id:
            return True # Force subscribe is not configured, allow access

        async with aiohttp.ClientSession() as session:
            await asyncio.sleep(FORCE_SUB_CHECK_DELAY) # Small delay to prevent hammering API
            
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/getChatMember"
            params = {
                'chat_id': self.force_sub_channel_id,
                'user_id': user_id
            }
            
            try:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get('ok'):
                            status = data['result']['status']
                            if status in ['member', 'administrator', 'creator']:
                                return True # User is subscribed
                            else:
                                # User is not subscribed, send prompt
                                channel_info = await self.get_channel_info(session, self.app, self.force_sub_channel_id)
                                channel_name = channel_info['title'] if channel_info else "the required channel"
                                channel_link = f"https://t.me/{channel_info['username']}" if channel_info and channel_info.get('username') else "https://t.me/" # Fallback link

                                keyboard = InlineKeyboardMarkup([
                                    [InlineKeyboardButton(f"🚀 Join {channel_name}", url=channel_link)]
                                ])
                                
                                text = f"""👋 Hello! To use this bot, you must join our channel: <b>{channel_name}</b>.

Please join the channel and then send /start again.
"""
                                if isinstance(message_obj, Message):
                                    await message_obj.reply_text(text, reply_markup=keyboard, parse_mode=enums.ParseMode.HTML)
                                elif isinstance(message_obj, CallbackQuery):
                                    await message_obj.message.edit_text(text, reply_markup=keyboard, parse_mode=enums.ParseMode.HTML)
                                return False
                        else:
                            logger.error(f"Telegram API error checking subscription: {data.get('description', 'Unknown error')}")
                            if isinstance(message_obj, Message):
                                await message_obj.reply_text("❌ Error checking subscription. Please try again later.")
                            elif isinstance(message_obj, CallbackQuery):
                                await message_obj.message.edit_text("❌ Error checking subscription. Please try again later.")
                            return False
                    else:
                        error_text = await response.text()
                        logger.error(f"HTTP error checking subscription: {response.status} - {error_text}")
                        if isinstance(message_obj, Message):
                            await message_obj.reply_text("❌ Error checking subscription. Please try again later.")
                        elif isinstance(message_obj, CallbackQuery):
                            await message_obj.message.edit_text("❌ Error checking subscription. Please try again later.")
                        return False
            except Exception as e:
                logger.error(f"Exception checking subscription: {e}")
                if isinstance(message_obj, Message):
                    await message_obj.reply_text("❌ An unexpected error occurred while checking subscription. Please try again.")
                elif isinstance(message_obj, CallbackQuery):
                    await message_obj.message.edit_text("❌ An unexpected error occurred while checking subscription. Please try again.")
                return False

    async def handle_start(self, client: Client, message: Message, is_edit: bool = False):
        """Handle /start command or return to main menu"""
        user_id = message.from_user.id
        self.db.add_user_if_not_exists(user_id) # Record user interaction
        
        if not await self.check_user_subscription(user_id, message):
            return # User not subscribed, message already sent

        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🆕 Create New Job", callback_data="create_job")],
            [InlineKeyboardButton("📋 My Jobs", callback_data="my_jobs")],
            [InlineKeyboardButton("ℹ️ Help", callback_data="help")]
        ])
        
        welcome_text = """🤖 <b>Autoposter Bot</b>
Forward posts between channels automatically."""
        
        if is_edit:
            await message.edit_text(
                welcome_text,
                reply_markup=keyboard,
                parse_mode=enums.ParseMode.HTML
            )
        else:
            await message.reply_text(
                welcome_text,
                reply_markup=keyboard,
                parse_mode=enums.ParseMode.HTML
            )
 
    async def handle_stats(self, client: Client, message: Message):
        """Handle /stats command for admin users."""
        user_id = message.from_user.id

        if not self.is_user_admin(user_id):
            await message.reply_text("🚫 You are not authorized to use this command.")
            return

        # Fetch stats from the database
        total_users = self.db.get_total_users()
        total_jobs = self.db.get_total_jobs()
        total_forwarded_messages = self.db.get_total_forwarded_messages()
        
        today_jobs = self.db.get_jobs_created_today()
        today_forwarded_messages = self.db.get_forwarded_messages_today()

        stats_text = f"""📊 <b>Bot Statistics</b>

<b>Today's Stats:</b>
• New Jobs Created: <b>{today_jobs}</b>
• Messages Forwarded: <b>{today_forwarded_messages}</b>

<b>Overall Stats:</b>
• Total Unique Users: <b>{total_users}</b>
• Total Jobs Created: <b>{total_jobs}</b>
• Total Messages Forwarded: <b>{total_forwarded_messages}</b>
"""
        await message.reply_text(stats_text, parse_mode=enums.ParseMode.HTML)

    async def handle_callback(self, client: Client, callback_query: CallbackQuery):
        """Handle callback queries"""
        try:
            await callback_query.answer()
            
            user_id = callback_query.from_user.id
            self.db.add_user_if_not_exists(user_id) # Record user interaction
            
            if not await self.check_user_subscription(user_id, callback_query):
                return # User not subscribed, message already sent

            data = callback_query.data
            
            if data == "create_job":
                await self.start_job_creation(client, callback_query)
            elif data == "my_jobs":
                await self.show_user_jobs(client, callback_query)
            elif data == "help":
                await self.show_help(client, callback_query)
            elif data.startswith("job_"):
                await self.handle_job_action(client, callback_query, data)
            elif data.startswith("filter_"):
                await self.handle_filter_selection(client, callback_query, data)
            elif data == "back_to_main":
                await self.handle_start(client, callback_query.message, is_edit=True) # Edit existing message
            
        except Exception as e:
            logger.error(f"Error in callback handler: {e}")
            await callback_query.answer("❌ An error occurred. Please try again.", show_alert=True)
    
    async def start_job_creation(self, client: Client, callback_query: CallbackQuery):
        """Start job creation process"""
        user_id = callback_query.from_user.id
        
        # Initialize user state
        state = {"step": "job_name"}
        self.db.save_user_state(user_id, state)
        
        text = """🆕 <b>Create New Autoposter Job</b>

Let's set up your forwarding job step by step.

<b>Step 1:</b> Enter a name for your job
Example: <code>News Channel Forward</code>
        """
        
        await callback_query.edit_message_text(
            text,
            parse_mode=enums.ParseMode.HTML
        )
    
    async def show_user_jobs(self, client: Client, callback_query: CallbackQuery):
        """Show user's jobs"""
        user_id = callback_query.from_user.id
        jobs = self.db.get_user_jobs(user_id)
        
        if not jobs:
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("🆕 Create First Job", callback_data="create_job")],
                [InlineKeyboardButton("🔙 Back", callback_data="back_to_main")]
            ])
            
            await callback_query.edit_message_text(
                "📋 <b>Your Jobs</b>\n\nYou don't have any jobs yet. Create your first job!",
                reply_markup=keyboard,
                parse_mode=enums.ParseMode.HTML
            )
            return
        
        text = "📋 <b>Your Jobs</b>\n\n"
        keyboard = []
        
        for job in jobs:
            status = "🟢 Active" if job['is_active'] else "🔴 Inactive"
            text += f"• <b>{job['job_name']}</b> - {status}\n"
            
            keyboard.append([
                InlineKeyboardButton(
                    f"⚙️ {job['job_name']}", 
                    callback_data=f"job_manage_{str(job['_id'])}"
                )
            ])
        
        keyboard.extend([
            [InlineKeyboardButton("🆕 Create New Job", callback_data="create_job")],
            [InlineKeyboardButton("🔙 Back", callback_data="back_to_main")]
        ])
        
        await callback_query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=enums.ParseMode.HTML
        )
    
    async def show_help(self, client: Client, callback_query: CallbackQuery):
        """Show help information"""
        help_text = """ℹ️ <b>Help</b>
Bot must be admin in both channels. Use message links for start/end posts."""
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Back", callback_data="back_to_main")]
        ])
        
        await callback_query.edit_message_text(
            help_text,
            reply_markup=keyboard,
            parse_mode=enums.ParseMode.HTML
        )
    
    async def handle_text_message(self, client: Client, message: Message):
        """Handle text messages based on user state"""
        user_id = message.from_user.id
        self.db.add_user_if_not_exists(user_id) # Record user interaction
        
        if not await self.check_user_subscription(user_id, message):
            return # User not subscribed, message already sent

        state = self.db.get_user_state(user_id)
        
        if not state:
            return # No active state, ignore message
        
        step = state.get("step")
        
        try:
            if step == "job_name":
                await self.handle_job_name(client, message, state)
            elif step == "source_channel":
                await self.handle_source_channel(client, message, state)
            elif step == "target_channel":
                await self.handle_target_channel(client, message, state)
            elif step == "start_post":
                await self.handle_start_post(client, message, state)
            elif step == "end_post":
                await self.handle_end_post(client, message, state)
            elif step == "batch_size":
                await self.handle_batch_size(client, message, state)
            elif step == "recurring_time":
                await self.handle_recurring_time(client, message, state)
            elif step == "delete_time":
                await self.handle_delete_time(client, message, state)
            elif step == "custom_caption":
                await self.handle_custom_caption(client, message, state)
            elif step == "button_text":
                await self.handle_button_text(client, message, state)
            elif step == "button_url":
                await self.handle_button_url(client, message, state)
            
        except Exception as e:
            logger.error(f"Error handling text message: {e}")
            await message.reply_text("❌ An error occurred. Please try again or use /start to restart.")
    
    async def handle_job_name(self, client: Client, message: Message, state: dict):
        """Handle job name input"""
        job_name = message.text.strip()
        
        if job_name.lower() != 'skip':
            if len(job_name) < 3:
                await message.reply_text("❌ Job name must be at least 3 characters long.")
                return
            state["job_name"] = job_name
        
        state["step"] = "source_channel"
        self.db.save_user_state(message.from_user.id, state)
        
        text = """✅ Job name saved!

<b>Step 2:</b> Enter the source channel ID or username
You can send:
• Channel ID: <code>-1001234567890</code>
• Username: <code>@channelname</code>
• Channel link: <code>https://t.me/channelname</code>

⚠️ <b>Important:</b> Make sure the bot is admin in this channel!
        """
        
        if 'editing_job_id' in state:
            text += f"\nOr send `skip` to keep the current source channel: <code>{state['source_channel']}</code>"

        await message.reply_text(text, parse_mode=enums.ParseMode.HTML)
    
    async def handle_source_channel(self, client: Client, message: Message, state: dict):
        """Handle source channel input"""
        channel_input = message.text.strip()

        if channel_input.lower() == 'skip' and 'editing_job_id' in state:
            state["step"] = "target_channel"
            self.db.save_user_state(message.from_user.id, state)
            text = f"""✅ Source channel kept as <code>{state['source_channel']}</code>.

<b>Step 3:</b> Enter the target channel ID or username.
Or send `skip` to keep the current target channel: <code>{state['target_channel']}</code>
            """
            await message.reply_text(text, parse_mode=enums.ParseMode.HTML)
            return

        channel_id = self.extract_channel_id(channel_input)
        
        if not channel_id:
            await message.reply_text("❌ Invalid channel format. Please try again.")
            return
        
        progress_msg = await message.reply_text("🔍 Checking channel access...")
        
        try:
            # Create a session for this specific check, it will be closed automatically
            async with aiohttp.ClientSession() as session: 
                # First test basic access
                can_access = await self.test_channel_access(session, channel_id)
                if not can_access:
                    await progress_msg.edit_text(
                        "❌ Cannot access this channel. Please check:\n"
                        "• Channel ID/username is correct\n"
                        "• Channel exists and is accessible\n"
                        "• Bot has been added to the channel"
                    )
                    return
            
                # Get channel info
                channel_info = await self.get_channel_info(session, client, channel_id)
                if not channel_info:
                    await progress_msg.edit_text("❌ Cannot get channel information. Please try again.")
                    return
            
                # Check admin status
                await progress_msg.edit_text("🔍 Checking admin permissions...")
                is_admin = await self.check_admin_status(session, client, channel_id)
                if not is_admin:
                    await progress_msg.edit_text(
                        f"❌ Bot is not admin in <b>{channel_info['title']}</b>\n\n"
                        "Please:\n"
                        "1. Add the bot to the channel as admin\n"
                        "2. Give permissions: Post Messages, Delete Messages\n"
                        "3. Try again\n\n"
                        f"Channel: <code>{channel_id}</code>",
                        parse_mode=enums.ParseMode.HTML
                    )
                    return
            
                state["source_channel"] = channel_id
                state["source_info"] = channel_info
                state["step"] = "target_channel"
                self.db.save_user_state(message.from_user.id, state)
            
                text = f"""✅ Source channel verified: <b>{channel_info['title']}</b>

<b>Step 3:</b> Enter the target channel ID or username
This is where the posts will be forwarded to.

⚠️ <b>Important:</b> Make sure the bot is admin in this channel too!
            """
                if 'editing_job_id' in state:
                    text += f"\nOr send `skip` to keep the current target channel: <code>{state['target_channel']}</code>"
            
                await progress_msg.edit_text(text, parse_mode=enums.ParseMode.HTML)
        
        except Exception as e:
            logger.error(f"Error checking source channel: {e}")
            await progress_msg.edit_text("❌ Error checking channel. Please try again.")
    
    async def handle_target_channel(self, client: Client, message: Message, state: dict):
        """Handle target channel input"""
        channel_input = message.text.strip()

        if channel_input.lower() == 'skip' and 'editing_job_id' in state:
            # Show filter selection
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("📷 Media Only", callback_data="filter_media")],
                [InlineKeyboardButton("📝 Text Only", callback_data="filter_text")],
                [InlineKeyboardButton("📋 All Posts", callback_data="filter_all")]
            ])
            text = f"""✅ Target channel kept as <code>{state['target_channel']}</code>.

<b>Step 4:</b> Choose what type of posts to forward.
Or send `skip` to keep the current filter: <code>{state['filter_type']}</code>
            """
            await message.reply_text(text, reply_markup=keyboard, parse_mode=enums.ParseMode.HTML)
            return

        channel_id = self.extract_channel_id(channel_input)
        
        if not channel_id:
            await message.reply_text("❌ Invalid channel format. Please try again.")
            return
        
        progress_msg = await message.reply_text("🔍 Checking channel access...")
        
        try:
            # Create a session for this specific check, it will be closed automatically
            async with aiohttp.ClientSession() as session: 
                # First test basic access
                can_access = await self.test_channel_access(session, channel_id)
                if not can_access:
                    await progress_msg.edit_text(
                        "❌ Cannot access this channel. Please check:\n"
                        "• Channel ID/username is correct\n"
                        "• Channel exists and is accessible\n"
                        "• Bot has been added to the channel"
                    )
                    return
            
                # Get channel info
                channel_info = await self.get_channel_info(session, client, channel_id)
                if not channel_info:
                    await progress_msg.edit_text("❌ Cannot get channel information. Please try again.")
                    return
            
                # Check admin status
                await progress_msg.edit_text("🔍 Checking admin permissions...")
                is_admin = await self.check_admin_status(session, client, channel_id)
                if not is_admin:
                    await progress_msg.edit_text(
                        f"❌ Bot is not admin in <b>{channel_info['title']}</b>\n\n"
                        "Please:\n"
                        "1. Add the bot to the channel as admin\n"
                        "2. Give permissions: Post Messages, Delete Messages\n"
                        "3. Try again\n\n"
                        f"Channel: <code>{channel_id}</code>",
                        parse_mode=enums.ParseMode.HTML
                    )
                    return
            
                state["target_channel"] = channel_id
                state["target_info"] = channel_info
                self.db.save_user_state(message.from_user.id, state)
            
                # Show filter selection
                keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("📷 Media Only", callback_data="filter_media")],
                    [InlineKeyboardButton("📝 Text Only", callback_data="filter_text")],
                    [InlineKeyboardButton("📋 All Posts", callback_data="filter_all")]
                ])
            
                text = f"""✅ Target channel verified: <b>{channel_info['title']}</b>

<b>Step 4:</b> Choose what type of posts to forward:
            """
                if 'editing_job_id' in state:
                    text += f"\nOr send `skip` to keep the current filter: <code>{state['filter_type']}</code>"
            
                await progress_msg.edit_text(
                    text,
                    reply_markup=keyboard,
                    parse_mode=enums.ParseMode.HTML
                )
        
        except Exception as e:
            logger.error(f"Error checking target channel: {e}")
            await progress_msg.edit_text("❌ Error checking channel. Please try again.")
    
    async def handle_filter_selection(self, client: Client, callback_query: CallbackQuery, data: str):
        """Handle filter type selection"""
        user_id = callback_query.from_user.id
        filter_type = data.split("_")[1]  # media, text, or all
        
        state = self.db.get_user_state(user_id)
        if not state:
            await callback_query.answer("❌ Session expired. Please start over.", show_alert=True)
            return
        
        state["filter_type"] = filter_type
        state["step"] = "start_post"
        self.db.save_user_state(user_id, state)
        
        filter_names = {"media": "📷 Media Only", "text": "📝 Text Only", "all": "📋 All Posts"}
        
        text = f"""✅ Filter set to: <b>{filter_names[filter_type]}</b>

<b>Step 5:</b> Send the link of the FIRST post to forward
Example: <code>https://t.me/channelname/123</code>

This will be your starting point for forwarding.
        """
        if 'editing_job_id' in state:
            text += f"\nOr send `skip` to keep the current start post: <code>{state['start_post_id']}</code>"
        
        await callback_query.edit_message_text(text, parse_mode=enums.ParseMode.HTML)
    
    async def handle_start_post(self, client: Client, message: Message, state: dict):
        """Handle start post link"""
        post_link = message.text.strip()

        if post_link.lower() == 'skip' and 'editing_job_id' in state:
            state["step"] = "end_post"
            self.db.save_user_state(message.from_user.id, state)
            text = f"""✅ Start post kept as <code>{state['start_post_id']}</code>.

<b>Step 6:</b> Send the link of the LAST post to forward.
Or send `skip` to keep the current end post: <code>{state['end_post_id']}</code>
            """
            await message.reply_text(text, parse_mode=enums.ParseMode.HTML)
            return

        message_id = self.extract_message_id_from_link(post_link)
        
        if not message_id:
            await message.reply_text("❌ Invalid message link format. Please try again.")
            return
        
        state["start_post_id"] = message_id
        state["step"] = "end_post"
        self.db.save_user_state(message.from_user.id, state)
        
        text = f"""✅ Start post ID: <b>{message_id}</b>

<b>Step 6:</b> Send the link of the LAST post to forward
Example: <code>https://t.me/channelname/456</code>

This sets the range of posts to forward. You can use a very high number (like 999999) to include all future posts.
        """
        if 'editing_job_id' in state:
            text += f"\nOr send `skip` to keep the current end post: <code>{state['end_post_id']}</code>"
        
        await message.reply_text(text, parse_mode=enums.ParseMode.HTML)
    
    async def handle_end_post(self, client: Client, message: Message, state: dict):
        """Handle end post link"""
        post_link = message.text.strip()

        if post_link.lower() == 'skip' and 'editing_job_id' in state:
            state["step"] = "batch_size"
            self.db.save_user_state(message.from_user.id, state)
            text = f"""✅ End post kept as <code>{state['end_post_id']}</code>.

<b>Step 7:</b> Enter batch size (1-20).
Or send `skip` to keep the current batch size: <code>{state['batch_size']}</code>
            """
            await message.reply_text(text, parse_mode=enums.ParseMode.HTML)
            return
        
        # Allow "latest" or "all" as shortcuts for high number
        if post_link.lower() in ["latest", "all", "999999"]:
            message_id = 999999
        else:
            message_id = self.extract_message_id_from_link(post_link)
            if not message_id:
                await message.reply_text("❌ Invalid message link format. Please try again or send 'latest' for all posts.")
                return
        
        start_id = state["start_post_id"]
        if message_id < start_id and message_id != 999999:
            await message.reply_text("❌ End post ID must be greater than start post ID.")
            return
        
        state["end_post_id"] = message_id
        state["step"] = "batch_size"
        self.db.save_user_state(message.from_user.id, state)
        
        end_text = "All future posts" if message_id == 999999 else str(message_id)
        text = f"""✅ End post ID: <b>{end_text}</b>

<b>Step 7:</b> Enter batch size (1-20)
This is how many posts will be forwarded in each cycle.
Example: <code>5</code>
        """
        if 'editing_job_id' in state:
            text += f"\nOr send `skip` to keep the current batch size: <code>{state['batch_size']}</code>"
        
        await message.reply_text(text, parse_mode=enums.ParseMode.HTML)
    
    async def handle_batch_size(self, client: Client, message: Message, state: dict):
        """Handle batch size input"""
        batch_size_input = message.text.strip()

        if batch_size_input.lower() == 'skip' and 'editing_job_id' in state:
            state["step"] = "recurring_time"
            self.db.save_user_state(message.from_user.id, state)
            text = f"""✅ Batch size kept as <code>{state['batch_size']}</code>.

<b>Step 8:</b> Enter recurring time in minutes (1-1440).
Or send `skip` to keep the current recurring time: <code>{state['recurring_time']}</code>
            """
            await message.reply_text(text, parse_mode=enums.ParseMode.HTML)
            return

        try:
            batch_size = int(batch_size_input)
            if batch_size < 1 or batch_size > 20:
                raise ValueError()
        except ValueError:
            await message.reply_text("❌ Batch size must be a number between 1 and 20.")
            return
        
        state["batch_size"] = batch_size
        state["step"] = "recurring_time"
        self.db.save_user_state(message.from_user.id, state)
        
        text = f"""✅ Batch size: <b>{batch_size} posts</b>

<b>Step 8:</b> Enter recurring time in minutes (1-1440)
This is how often the bot will forward a new batch.
Example: <code>30</code> (every 30 minutes)
        """
        if 'editing_job_id' in state:
            text += f"\nOr send `skip` to keep the current recurring time: <code>{state['recurring_time']}</code>"
        
        await message.reply_text(text, parse_mode=enums.ParseMode.HTML)
    
    async def handle_recurring_time(self, client: Client, message: Message, state: dict):
        """Handle recurring time input"""
        recurring_time_input = message.text.strip()

        if recurring_time_input.lower() == 'skip' and 'editing_job_id' in state:
            state["step"] = "delete_time"
            self.db.save_user_state(message.from_user.id, state)
            text = f"""✅ Recurring time kept as <code>{state['recurring_time']}</code>.

<b>Step 9:</b> Enter delete time in minutes (0-10080).
Or send `skip` to keep the current delete time: <code>{state['delete_time']}</code>
            """
            await message.reply_text(text, parse_mode=enums.ParseMode.HTML)
            return

        try:
            recurring_time = int(recurring_time_input)
            if recurring_time < 1 or recurring_time > 1440:
                raise ValueError()
        except ValueError:
            await message.reply_text("❌ Recurring time must be between 1 and 1440 minutes.")
            return
        
        state["recurring_time"] = recurring_time
        state["step"] = "delete_time"
        self.db.save_user_state(message.from_user.id, state)
        
        text = f"""✅ Recurring time: <b>{recurring_time} minutes</b>

<b>Step 9:</b> Enter delete time in minutes (0-10080)
This is how long to keep forwarded posts before deleting them.
Use <code>0</code> to never delete posts.
Example: <code>60</code> (delete after 1 hour)
        """
        if 'editing_job_id' in state:
            text += f"\nOr send `skip` to keep the current delete time: <code>{state['delete_time']}</code>"
        
        await message.reply_text(text, parse_mode=enums.ParseMode.HTML)
    
    async def handle_delete_time(self, client: Client, message: Message, state: dict):
        """Handle delete time input"""
        delete_time_input = message.text.strip()

        if delete_time_input.lower() == 'skip' and 'editing_job_id' in state:
            state["step"] = "custom_caption"
            self.db.save_user_state(message.from_user.id, state)
            text = f"""✅ Delete time kept as <b>{state['delete_time']} minutes</b>.

<b>Step 10:</b> Enter custom caption (optional).
Or send `skip` to keep the current custom caption.
            """
            await message.reply_text(text, parse_mode=enums.ParseMode.HTML)
            return

        try:
            delete_time = int(delete_time_input)
            if delete_time < 0 or delete_time > 10080:
                raise ValueError()
        except ValueError:
            await message.reply_text("❌ Delete time must be between 0 and 10080 minutes.")
            return
        
        state["delete_time"] = delete_time
        state["step"] = "custom_caption"
        self.db.save_user_state(message.from_user.id, state)
        
        text = """✅ Delete time: <b>{delete_time} minutes</b>

<b>Step 10:</b> Enter custom caption (optional)
You can use HTML formatting:
• <code>&lt;b&gt;Bold&lt;/b&gt;</code>
• <code>&lt;i&gt;Italic&lt;/i&gt;</code>
• <code>&lt;u&gt;Underlined&lt;/u&gt;</code>
• <code>&lt;a href="link"&gt;Text&lt;/a&gt;</code>

Send <code>skip</code> to use original captions.
        """
        if 'editing_job_id' in state:
            text += "\nOr send `skip` to keep the current custom caption."
        
        await message.reply_text(text, parse_mode=enums.ParseMode.HTML)
    
    async def handle_custom_caption(self, client: Client, message: Message, state: dict):
        """Handle custom caption input"""
        caption = message.text.strip()
        
        if caption.lower() != "skip":
            state["custom_caption"] = caption
        
        state["step"] = "button_text"
        self.db.save_user_state(message.from_user.id, state)
        
        text = """✅ Custom caption saved!

<b>Step 11:</b> Enter button text (optional)
This will add an inline button to forwarded posts.
Send <code>skip</code> to not add a button.
        """
        if 'editing_job_id' in state:
            text += "\nOr send `skip` to keep the current button text."
        
        await message.reply_text(text, parse_mode=enums.ParseMode.HTML)
    
    async def handle_button_text(self, client: Client, message: Message, state: dict):
        """Handle button text input"""
        button_text = message.text.strip()
        
        if button_text.lower() == "skip":
            if 'editing_job_id' in state:
                await self.finalize_job(client, message, state)
                return
            else:
                # Finalize job without button
                await self.finalize_job(client, message, state)
                return
        
        state["button_text"] = button_text
        state["step"] = "button_url"
        self.db.save_user_state(message.from_user.id, state)
        
        text = f"""✅ Button text: <b>{button_text}</b>

<b>Step 12:</b> Enter button URL
Example: <code>https://t.me/yourchannel</code>
        """
        if 'editing_job_id' in state:
            text += f"\nOr send `skip` to keep the current button URL: <code>{state['button_url']}</code>"
        
        await message.reply_text(text, parse_mode=enums.ParseMode.HTML)
    
    async def handle_button_url(self, client: Client, message: Message, state: dict):
        """Handle button URL input"""
        button_url = message.text.strip()
        
        if button_url.lower() != 'skip':
            if not button_url.startswith(('http://', 'https://', 'tg://')):
                await message.reply_text("❌ Please enter a valid URL starting with http:// or https://")
                return
            state["button_url"] = button_url

        await self.finalize_job(client, message, state)
    
    async def finalize_job(self, client: Client, message: Message, state: dict):
        """Finalize and create or update the job"""
        user_id = message.from_user.id
        
        job_data = {
            'name': state['job_name'],
            'source': state['source_channel'],
            'target': state['target_channel'],
            'start_id': state['start_post_id'],
            'end_id': state['end_post_id'],
            'batch_size': state['batch_size'],
            'recurring_time': state['recurring_time'],
            'delete_time': state['delete_time'],
            'filter_type': state['filter_type'],
            'caption': state.get('custom_caption', ''),
            'button_text': state.get('button_text', ''),
            'button_url': state.get('button_url', '')
        }
        
        if "editing_job_id" in state:
            job_id = state["editing_job_id"]
            self.db.update_job(job_id, job_data)
            action_text = "Updated"
        else:
            job_id = self.db.create_job(user_id, job_data)
            action_text = "Created"

        # Clear user state
        self.db.clear_user_state(user_id)
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("▶️ Start Job", callback_data=f"job_start_{job_id}")],
            [InlineKeyboardButton("📋 My Jobs", callback_data="my_jobs")]
        ])
        
        delete_info = "Never delete" if job_data['delete_time'] == 0 else f"Delete after {job_data['delete_time']} min"
        end_info = "All future posts" if job_data['end_id'] == 999999 else str(job_data['end_id'])
        
        text = f"""🎉 <b>Job {action_text} Successfully!</b>

<b>📋 Job Details:</b>
• Name: <b>{job_data['name']}</b>
• Source: <b>{state['source_channel']}</b>
• Target: <b>{state['target_channel']}</b>
• Posts Range: <b>{job_data['start_id']} - {end_info}</b>
• Batch: <b>{job_data['batch_size']} posts every {job_data['recurring_time']} min</b>
• Filter: <b>{job_data['filter_type'].title()}</b>
• Delete: <b>{delete_info}</b>

Ready to start forwarding!
        """
        
        await message.reply_text(
            text,
            reply_markup=keyboard,
            parse_mode=enums.ParseMode.HTML
        )
    
    async def handle_job_action(self, client: Client, callback_query: CallbackQuery, data: str):
        """Handle job management actions"""
        try:
            action_parts = data.split("_")
            action = action_parts[1]
            job_id = action_parts[2]
            
            job = self.db.get_job(job_id)
            if not job:
                await callback_query.answer("❌ Job not found.", show_alert=True)
                return
            
            if action == "start":
                await self.start_job(client, callback_query, job_id)
            elif action == "stop":
                await self.stop_job(client, callback_query, job_id)
            elif action == "manage":
                await self.show_job_management(client, callback_query, job_id)
            elif action == "reset":
                await self.reset_job_progress_action(client, callback_query, job_id)
            elif action == "edit":
                await self.start_job_editing(client, callback_query, job_id)
            elif action == "delete":
                await self.delete_job_action(client, callback_query, job_id)
            
        except Exception as e:
            logger.error(f"Error in job action: {e}")
            await callback_query.answer("❌ An error occurred.", show_alert=True)
    
    async def show_job_management(self, client: Client, callback_query: CallbackQuery, job_id: int):
        """Show job management options"""
        job = self.db.get_job(job_id)
        if not job:
            await callback_query.answer("❌ Job not found.", show_alert=True)
            return
        
        status = "🟢 Active" if job['is_active'] else "🔴 Inactive"
        
        keyboard = []
        if job['is_active']:
            keyboard.append([InlineKeyboardButton("⏹️ Stop Job", callback_data=f"job_stop_{str(job['_id'])}")])
        else:
            keyboard.append([InlineKeyboardButton("▶️ Start Job", callback_data=f"job_start_{str(job['_id'])}")])
        
        keyboard.append([InlineKeyboardButton("🔄 Reset Progress", callback_data=f"job_reset_{str(job['_id'])}")])
        keyboard.extend([
            [
                InlineKeyboardButton("✏️ Edit Job", callback_data=f"job_edit_{str(job['_id'])}"),
                InlineKeyboardButton("🗑️ Delete Job", callback_data=f"job_delete_{str(job['_id'])}")
            ],
            [InlineKeyboardButton("🔙 Back to Jobs", callback_data="my_jobs")]
        ])
        
        delete_info = "Never" if job['delete_time'] == 0 else f"{job['delete_time']} min"
        end_info = "All future" if job['end_post_id'] == 999999 else str(job['end_post_id'])
        
        text = f"""⚙️ <b>Managing Job: {job['job_name']}</b>

<b>Status:</b> {status}
<b>Source:</b> {job['source_channel_id']}
<b>Target:</b> {job['target_channel_id']}
<b>Posts Range:</b> {job['start_post_id']} - {end_info}
<b>Batch Size:</b> {job['batch_size']} posts
<b>Frequency:</b> Every {job['recurring_time']} minutes
<b>Delete after:</b> {delete_info}
<b>Last Forwarded:</b> {job['last_forwarded_id']}
        """
        
        await callback_query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=enums.ParseMode.HTML
        )
    
    async def start_job(self, client: Client, callback_query: CallbackQuery, job_id: str):
        """Start a job"""
        job = self.db.get_job(job_id)
        if not job:
            await callback_query.answer("❌ Job not found.", show_alert=True)
            return
        
        if job['is_active']:
            await callback_query.answer("⚠️ Job is already running!", show_alert=True)
            return
        
        # Update job status
        self.db.update_job_status(job_id, True)
        
        # Start the job task
        if job_id not in self.active_jobs:
            self.active_jobs[job_id] = True
            self.job_locks[job_id] = asyncio.Lock()
            
            # Create task for job execution
            asyncio.create_task(self.run_job(client, job_id))
        
        await callback_query.edit_message_text(
            f"✅ Job <b>{job['job_name']}</b> started successfully!",
            parse_mode=enums.ParseMode.HTML
        )
    
    async def stop_job(self, client: Client, callback_query: CallbackQuery, job_id: str):
        """Stop a job"""
        job = self.db.get_job(job_id)
        if not job:
            await callback_query.answer("❌ Job not found.", show_alert=True)
            return
        
        # Update job status
        self.db.update_job_status(job_id, False)
        
        # Stop the job task
        if job_id in self.active_jobs:
            self.active_jobs[job_id] = False
        
        await callback_query.edit_message_text(
            f"⏹️ Job <b>{job['job_name']}</b> stopped successfully!",
            parse_mode=enums.ParseMode.HTML
        )

    async def reset_job_progress_action(self, client: Client, callback_query: CallbackQuery, job_id: str):
        """Handle resetting job progress"""
        job = self.db.get_job(job_id)
        if not job:
            await callback_query.answer("❌ Job not found.", show_alert=True)
            return
        
        if job['is_active']:
            await callback_query.answer("⚠️ Please stop the job before resetting its progress.", show_alert=True)
            return

        self.db.reset_job_progress(job_id, job['start_post_id'])
        
        await callback_query.edit_message_text(
            f"🔄 Progress for job <b>{job['job_name']}</b> has been reset. It will now start from message {job['start_post_id']}.",
            parse_mode=enums.ParseMode.HTML
        )
        # Optionally, navigate back to job management or my jobs
        await asyncio.sleep(2) # Give user time to read message
        await self.show_job_management(client, callback_query, job_id)

    async def start_job_editing(self, client: Client, callback_query: CallbackQuery, job_id: str):
        """Start the process of editing a job."""
        user_id = callback_query.from_user.id
        job = self.db.get_job(job_id)
        if not job:
            await callback_query.answer("❌ Job not found.", show_alert=True)
            return

        # Initialize user state for editing
        state = {
            "step": "job_name",
            "editing_job_id": job_id,
            "job_name": job['job_name'],
            "source_channel": job['source_channel_id'],
            "target_channel": job['target_channel_id'],
            "start_post_id": job['start_post_id'],
            "end_post_id": job['end_post_id'],
            "batch_size": job['batch_size'],
            "recurring_time": job['recurring_time'],
            "delete_time": job['delete_time'],
            "filter_type": job['filter_type'],
            "custom_caption": job['custom_caption'],
            "button_text": job['button_text'],
            "button_url": job['button_url']
        }
        self.db.save_user_state(user_id, state)

        text = f"""✏️ <b>Editing Job: {job['job_name']}</b>

Send a new name for your job or send `skip` to keep the current name.
Current name: <code>{job['job_name']}</code>
        """
        await callback_query.edit_message_text(text, parse_mode=enums.ParseMode.HTML)

    async def delete_job_action(self, client: Client, callback_query: CallbackQuery, job_id: str):
        """Handle deleting a job."""
        job = self.db.get_job(job_id)
        if not job:
            await callback_query.answer("❌ Job not found.", show_alert=True)
            return

        if job['is_active']:
            await callback_query.answer("⚠️ Please stop the job before deleting it.", show_alert=True)
            return

        self.db.delete_job(job_id)

        await callback_query.edit_message_text(
            f"🗑️ Job <b>{job['job_name']}</b> has been deleted.",
            parse_mode=enums.ParseMode.HTML
        )
        await asyncio.sleep(2)
        await self.show_user_jobs(client, callback_query)
    
    async def run_job(self, client: Client, job_id: int):
        """Main job execution loop"""
        logger.info(f"Starting job {job_id}")
        
        while job_id in self.active_jobs and self.active_jobs[job_id]:
            try:
                async with self.job_locks[job_id]:
                    job = self.db.get_job(job_id)
                    if not job or not job['is_active']:
                        break
                    
                    # Create a new aiohttp session for each cycle of the job
                    # This ensures the session is fresh and not prematurely closed
                    async with aiohttp.ClientSession() as session:
                        await self.process_job_batch(client, job, session)
                        
                        # Clean up old messages if delete_time > 0
                        if job['delete_time'] > 0:
                            await self.cleanup_old_messages(client, job, session)
            
                # Wait for next cycle only if we didn't reach the end of posts
                # If we reached the end, we might want to wait longer or stop
                if job['end_post_id'] != 999999 and job['last_forwarded_id'] >= job['end_post_id']:
                    logger.info(f"Job {job['id']}: Reached end of specified posts ({job['last_forwarded_id']}/{job['end_post_id']}). Pausing until new posts are available or job is reset.")
                    await asyncio.sleep(job['recurring_time'] * 60 * 2) # Wait longer if at end
                else:
                    await asyncio.sleep(job['recurring_time'] * 60)
                
            except FloodWait as e:
                logger.warning(f"FloodWait in job {job_id}: {e.value} seconds")
                await asyncio.sleep(e.value)
            except Exception as e:
                logger.error(f"Error in job {job_id}: {e}")
                await asyncio.sleep(60)  # Wait before retrying
        
        # Clean up
        if job_id in self.active_jobs:
            del self.active_jobs[job_id]
        if job_id in self.job_locks:
            del self.job_locks[job_id]
        
        logger.info(f"Job {job_id} stopped")
 
    async def process_job_batch(self, client: Client, job: dict, session: aiohttp.ClientSession):
        """Process a batch of messages for forwarding using raw API workaround"""
        current_message_id = max(job['last_forwarded_id'] + 1, job['start_post_id'])
        messages_to_forward = []
        last_checked_message_id = job['last_forwarded_id'] # Initialize with the last forwarded ID

        logger.info(f"Job {job['id']}: Starting batch search from message ID {current_message_id}")
        
        while len(messages_to_forward) < job['batch_size']:
            if job['end_post_id'] != 999999 and current_message_id > job['end_post_id']:
                logger.info(f"Job {job['id']}: Reached end of range ({current_message_id-1} vs {job['end_post_id']}). No more messages to process in this range.")
                break 

            try:
                # Step 1: Attempt to forward the message to a temporary location
                forward_url = f"https://api.telegram.org/bot{BOT_TOKEN}/forwardMessage"
                forward_params = {
                    'chat_id': job['target_channel_id'], # Forward to target to get message data
                    'from_chat_id': job['source_channel_id'],
                    'message_id': current_message_id
                }
                
                forward_response = await session.post(forward_url, data=forward_params)
                forward_data = await forward_response.json()

                last_checked_message_id = current_message_id # Always update this to the message we just tried to fetch

                if not forward_data.get('ok'):
                    # Message not found or cannot be forwarded (e.g., deleted, private, out of range)
                    logger.debug(f"Job {job['id']}: Message {current_message_id} not found or cannot be forwarded (error: {forward_data.get('description', 'Unknown')})")
                    current_message_id += 1
                    await asyncio.sleep(BATCH_DELAY) # Small delay to prevent hammering API
                    continue

                forwarded_msg_result = forward_data['result']
                temp_forwarded_msg_id = forwarded_msg_result['message_id']

                # Step 2: Immediately delete the temporary forwarded message
                delete_url = f"https://api.telegram.org/bot{BOT_TOKEN}/deleteMessage"
                delete_params = {
                    'chat_id': job['target_channel_id'],
                    'message_id': temp_forwarded_msg_id
                }
                await session.post(delete_url, data=delete_params) # Best effort delete, no need to check response
                
                # Step 3: Determine message type and apply filter
                message_type = self.get_message_type_from_raw_data(forwarded_msg_result)
                if self.message_matches_filter_raw(message_type, job['filter_type']):
                    messages_to_forward.append((current_message_id, forwarded_msg_result))
                    logger.debug(f"Job {job['id']}: Found matching message {current_message_id} (type: {message_type})")
                else:
                    logger.debug(f"Job {job['id']}: Message {current_message_id} (type: {message_type}) doesn't match filter {job['filter_type']}")

                current_message_id += 1
                await asyncio.sleep(BATCH_DELAY) # Small delay between checking messages
                
            except FloodWait as e:
                logger.warning(f"FloodWait while searching for messages in job {job['id']}: {e.value} seconds")
                await asyncio.sleep(e.value)
                # After flood wait, we should continue from the current_message_id
                continue
            except Exception as e:
                logger.error(f"Error searching for message {current_message_id} in job {job['id']}: {e}")
                current_message_id += 1 # Skip problematic message
                await asyncio.sleep(BATCH_DELAY)
                continue

        # After collecting the batch (or reaching end_post_id), process them
        forwarded_count = 0
        if messages_to_forward:
            logger.info(f"Job {job['id']}: Forwarding {len(messages_to_forward)} messages.")
            for original_id, msg_data in messages_to_forward:
                try:
                    # Use the session passed from run_job
                    sent_message_obj = await self.send_custom_message(session, job, msg_data)
                    if sent_message_obj:
                        self.db.add_forwarded_message(job['id'], original_id, sent_message_obj.id)
                        forwarded_count += 1
                        logger.info(f"Job {job['id']}: Successfully forwarded message {original_id}")
                    else:
                        logger.warning(f"Job {job['id']}: Failed to send custom message {original_id}")
                    await asyncio.sleep(FORWARD_DELAY)
                except FloodWait as e:
                    logger.warning(f"FloodWait during forwarding in job {job['id']}: {e.value} seconds")
                    await asyncio.sleep(e.value)
                    # If flood wait during forwarding, we might need to re-queue remaining messages
                    # For simplicity, we'll just stop this batch and update last_forwarded_id
                    break
                except Exception as e:
                    logger.error(f"Error forwarding message {original_id} in job {job['id']}: {e}")
                    continue

        # Update last_forwarded_id to the last message that was *checked*, not just forwarded.
        self.db.update_last_forwarded(job['id'], last_checked_message_id)
        logger.info(f"Job {job['id']}: Forwarded {forwarded_count} messages in this batch. Last checked message ID: {last_checked_message_id}")
    
    def get_message_type_from_raw_data(self, raw_msg_data: dict) -> str:
        """Determine message type from raw Telegram Bot API message data"""
        if 'photo' in raw_msg_data:
            return 'photo'
        elif 'video' in raw_msg_data:
            return 'video'
        elif 'document' in raw_msg_data:
            return 'document'
        elif 'audio' in raw_msg_data:
            return 'audio'
        elif 'voice' in raw_msg_data:
            return 'voice'
        elif 'video_note' in raw_msg_data:
            return 'video_note'
        elif 'animation' in raw_msg_data:
            return 'animation'
        elif 'sticker' in raw_msg_data:
            return 'sticker'
        elif 'text' in raw_msg_data:
            return 'text'
        else:
            return 'unknown'

    def message_matches_filter_raw(self, message_type: str, filter_type: str) -> bool:
        """Check if message matches the filter criteria using raw message type"""
        if filter_type == "all":
            return True
        elif filter_type == "media":
            media_types = ['photo', 'video', 'document', 'audio', 'voice', 'video_note', 'animation', 'sticker']
            return message_type in media_types
        elif filter_type == "text":
            return message_type == 'text'
        return False

    async def send_custom_message(self, session: aiohttp.ClientSession, job: dict, original_msg_data: dict):
        """Send message with custom caption and button using raw Bot API"""
        try:
            base_url = f"https://api.telegram.org/bot{BOT_TOKEN}"
            
            # Prepare caption
            caption = job['custom_caption'] if job['custom_caption'] else (
                original_msg_data.get('caption', '') or original_msg_data.get('text', '')
            )
            
            # Prepare reply markup
            reply_markup = None
            if job['button_text'] and job['button_url']:
                reply_markup = {
                    "inline_keyboard": [[{
                        "text": job['button_text'],
                        "url": job['button_url']
                    }]]
                }
            
            # Prepare common parameters
            params = {
                'chat_id': job['target_channel_id'],
                'parse_mode': 'HTML'
            }
            
            if reply_markup:
                params['reply_markup'] = json.dumps(reply_markup)
            
            # Send based on message type
            message_type = self.get_message_type_from_raw_data(original_msg_data)
            
            if message_type == 'photo':
                # Photos come as an array of sizes, pick the largest
                params['photo'] = original_msg_data['photo'][-1]['file_id']
                if caption:
                    params['caption'] = caption
                url = f"{base_url}/sendPhoto"
            
            elif message_type == 'video':
                params['video'] = original_msg_data['video']['file_id']
                if caption:
                    params['caption'] = caption
                url = f"{base_url}/sendVideo"
            
            elif message_type == 'document':
                params['document'] = original_msg_data['document']['file_id']
                if caption:
                    params['caption'] = caption
                url = f"{base_url}/sendDocument"
            
            elif message_type == 'audio':
                params['audio'] = original_msg_data['audio']['file_id']
                if caption:
                    params['caption'] = caption
                url = f"{base_url}/sendAudio"
            
            elif message_type == 'voice':
                params['voice'] = original_msg_data['voice']['file_id']
                if caption:
                    params['caption'] = caption
                url = f"{base_url}/sendVoice"
            
            elif message_type == 'animation':
                params['animation'] = original_msg_data['animation']['file_id']
                if caption:
                    params['caption'] = caption
                url = f"{base_url}/sendAnimation"
            
            elif message_type == 'sticker':
                params['sticker'] = original_msg_data['sticker']['file_id']
                url = f"{base_url}/sendSticker"
            
            elif message_type == 'video_note':
                params['video_note'] = original_msg_data['video_note']['file_id']
                url = f"{base_url}/sendVideoNote"
            
            elif message_type == 'text':
                params['text'] = caption if caption else original_msg_data.get('text', '')
                url = f"{base_url}/sendMessage"
            else:
                logger.warning(f"Unknown message type for sending: {message_type}")
                return None
            
            # Send the message using the provided session
            async with session.post(url, data=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get('ok'):
                        result = data.get('result', {})
                        # Create a simple message object with ID
                        class SimpleMessage:
                            def __init__(self, msg_id):
                                self.id = msg_id
                        return SimpleMessage(result.get('message_id'))
                else:
                    # Log the error for debugging
                    error_text = await response.text()
                    logger.error(f"Failed to send message: {response.status} - {error_text}")
                
            return None
            
        except Exception as e:
            logger.error(f"Error sending custom message: {e}")
            return None
    
    async def cleanup_old_messages(self, client: Client, job: dict, session: aiohttp.ClientSession):
        """Delete old forwarded messages using raw Bot API"""
        try:
            old_messages = self.db.get_old_forwarded_messages(job['id'], job['delete_time'])
        
            if not old_messages:
                return
            
            logger.info(f"Job {job['id']}: Cleaning up {len(old_messages)} old messages")
            
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/deleteMessage"
        
            deleted_count = 0
            for message_id in old_messages:
                try:
                    params = {
                        'chat_id': job['target_channel_id'],
                        'message_id': message_id
                    }
                
                    async with session.post(url, data=params) as response:
                        if response.status == 429:  # Rate limited
                            retry_after = int(response.headers.get('Retry-After', 1))
                            await asyncio.sleep(retry_after)
                        elif response.status == 200:
                            data = await response.json()
                            if data.get('ok'):
                                deleted_count += 1
                                logger.debug(f"Deleted old message {message_id}")
                    
                        await asyncio.sleep(DELETE_DELAY)
                    
                except Exception as e:
                    logger.error(f"Error deleting message {message_id}: {e}")
        
            logger.info(f"Job {job['id']}: Successfully deleted {deleted_count} old messages")
                
        except Exception as e:
            logger.error(f"Error in cleanup: {e}")
 
    async def test_channel_access(self, session: aiohttp.ClientSession, channel_id: Union[str, int]) -> bool:
        """Test if bot can access the channel using raw API"""
        try:
            # Use the passed session
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/getChat"
            params = {'chat_id': channel_id}
        
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get('ok', False)
                return False
            
        except Exception as e:
            logger.error(f"Error testing channel access: {e}")
            return False
    
    async def get_channel_info(self, session: aiohttp.ClientSession, client: Client, channel_id: Union[str, int]) -> Optional[dict]:
        """Get channel information using raw Bot API"""
        try:
            # Use the passed session
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/getChat"
            params = {'chat_id': channel_id}
        
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get('ok'):
                        chat = data.get('result', {})
                        return {
                            'id': chat.get('id'),
                            'title': chat.get('title', chat.get('first_name', 'Unknown')),
                            'type': chat.get('type', 'unknown'),
                            'username': chat.get('username', '')
                        }
                return None
                
        except Exception as e:
            logger.error(f"Error getting channel info: {e}")
            return None

    async def check_admin_status(self, session: aiohttp.ClientSession, client: Client, channel_id: Union[str, int]) -> bool:
        """Check if bot is admin in the channel using raw Bot API"""
        try:
            await asyncio.sleep(ADMIN_DELAY)  # Rate limiting
    
            # Use the passed session
            # Get bot user ID
            bot_url = f"https://api.telegram.org/bot{BOT_TOKEN}/getMe"
            async with session.get(bot_url) as response:
                if response.status != 200:
                    return False
                bot_data = await response.json()
                if not bot_data.get('ok'):
                    return False
                bot_id = bot_data['result']['id']
        
            # Check admin status using raw API
            admin_url = f"https://api.telegram.org/bot{BOT_TOKEN}/getChatMember"
            params = {
                'chat_id': channel_id,
                'user_id': bot_id
            }
        
            async with session.get(admin_url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get('ok'):
                        member = data.get('result', {})
                        status = member.get('status', '')
                        return status in ['administrator', 'creator']
                elif response.status == 400:
                    # Try alternative method for channels
                    try:
                        chat_url = f"https://api.telegram.org/bot{BOT_TOKEN}/getChat"
                        chat_params = {'chat_id': channel_id}
                        async with session.get(chat_url, params=chat_params) as chat_response:
                            if chat_response.status == 200:
                                chat_data = await chat_response.json()
                                return chat_data.get('ok', False)
                    except:
                        pass
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking admin status: {e}")
            return False
    
    def extract_channel_id(self, text: str) -> Optional[Union[str, int]]:
        """Extract channel ID from various formats"""
        text = text.strip()
        
        # If it's already a number (with or without -)
        if text.lstrip('-').isdigit():
            return int(text)
        
        # If it's a username (@channel)
        if text.startswith('@'):
            return text
        
        # If it's a t.me link
        if 't.me/' in text:
            username = text.split('t.me/')[-1].split('/')[0]
            return f"@{username}"
        
        return None
    
    def extract_message_id_from_link(self, link: str) -> Optional[int]:
        """Extract message ID from Telegram message link"""
        try:
            # Pattern for t.me/channel/message_id
            match = re.search(r'/(\d+)$', link)
            if match:
                return int(match.group(1))
        except:
            pass
        return None
    
    async def start(self):
        """Start the bot"""
        logger.info("Starting Autoposter Bot...")
        await self.app.start()
        logger.info("Bot started successfully!")
        
        # Keep the bot running
        await asyncio.Event().wait()
    
    async def stop(self):
        """Stop the bot"""
        logger.info("Stopping bot...")
        
        # Stop all active jobs
        for job_id in list(self.active_jobs.keys()):
            self.active_jobs[job_id] = False
        
        await self.app.stop()
        logger.info("Bot stopped.")

async def main():
    """Main function"""
    bot = AutoposterBot()
    
    try:
        await bot.start()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    finally:
        await bot.stop()

if __name__ == "__main__":
    asyncio.run(main())

