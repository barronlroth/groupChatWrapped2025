# Wrapped 2025

Your texting habits, exposed. A Spotify Wrapped-style visualization of your iMessage and WhatsApp history.

**[-> wrap2025.com](https://wrap2025.com)**

## Features

- **Total messages** - sent, received, per day
- **Top 5 contacts** - your inner circle
- **Texting personality** - based on your habits
- **Response time** - how fast you reply
- **3AM bestie** - late night conversations
- **Heating up** - growing relationships
- **Ghosted** - who stopped texting
- **Down bad** - who you simp for
- **Busiest day** - your most unhinged day
- **Who texts first** - conversation initiator %
- **Group chat stats** - your group chat activity overview
- **Top group chats** - your most active group conversations
- **Groupchat wrapped (single chat)** - pick one group chat + awards (e.g. most used emoji)
- **Contribution graph** - GitHub-style activity heatmap of your messaging throughout the year

## Installation

### iMessage Wrapped

#### 1. Download the script

```bash
curl -O https://raw.githubusercontent.com/kothari-nikunj/wrap2025/main/imessage_wrapped.py
```

#### 2. Grant Terminal access

The script needs to read your Messages database:

**System Settings -> Privacy & Security -> Full Disk Access -> Add Terminal**

(Or iTerm/Warp if you use those)

#### 3. Run it

```bash
python3 imessage_wrapped.py
```

### Groupchat Wrapped (iMessage, single chat)

Generates a Wrapped-style deck for one specific iMessage group chat (2025 YTD) with awards like “most used emoji”.

```bash
python3 groupchat_wrapped.py
```

“Potty Mouth” uses a small built-in wordlist (no third-party install). Disable it with `--no-profanity`:

```bash
python3 groupchat_wrapped.py --no-profanity
```

---

### WhatsApp Wrapped

#### 1. Download the script

```bash
curl -O https://raw.githubusercontent.com/kothari-nikunj/wrap2025/main/whatsapp_wrapped.py
```

#### 2. Grant Terminal access

The script needs to read your WhatsApp database:

**System Settings -> Privacy & Security -> Full Disk Access -> Add Terminal**

(Or iTerm/Warp if you use those)

#### 3. Run it

```bash
python3 whatsapp_wrapped.py
```

---

### Combined Wrapped (iMessage + WhatsApp)

Merges stats from both platforms into a single unified report.

#### 1. Download the script

```bash
curl -O https://raw.githubusercontent.com/kothari-nikunj/wrap2025/main/combined_wrapped.py
```

#### 2. Grant Terminal access

The script needs to read both message databases:

**System Settings -> Privacy & Security -> Full Disk Access -> Add Terminal**

(Or iTerm/Warp if you use those)

#### 3. Run it

```bash
python3 combined_wrapped.py
```

The combined script will:
- Analyze both iMessage and WhatsApp data
- Use your AddressBook to reconcile contacts across platforms
- Merge top contacts, group chats, and all other stats
- Show a platform breakdown slide
- Work even if you only have one platform installed

---

Your wrapped will open in your browser automatically.

## Options

```bash
# Use 2024 data instead of 2025
python3 imessage_wrapped.py --use-2024
python3 whatsapp_wrapped.py --use-2024
python3 combined_wrapped.py --use-2024

# Custom output filename
python3 imessage_wrapped.py -o my_wrapped.html
python3 whatsapp_wrapped.py -o my_wrapped.html
python3 combined_wrapped.py -o my_wrapped.html

# Groupchat Wrapped (single iMessage group)
python3 groupchat_wrapped.py --search "tennis"
python3 groupchat_wrapped.py -o my_groupchat_wrapped.html
python3 groupchat_wrapped.py --use-2024
python3 groupchat_wrapped.py --chat-id 123 --no-open
```

If you don't have enough 2025 messages yet, the script will automatically fall back to 2024.

## Privacy

**100% Local** - Your data never leaves your computer

- No servers, no uploads, no tracking
- No external dependencies (Python stdlib only)
- All analysis happens locally
- Output is a single HTML file

You can read the entire source code yourself.

## Requirements

- macOS (uses local message databases)
- Python 3 (pre-installed on macOS)
- Full Disk Access for Terminal
- For WhatsApp: WhatsApp desktop app installed with chat history

## How it works

### iMessage
The script reads your local `chat.db` (iMessage database) and `AddressBook` (Contacts) using SQLite queries.

### WhatsApp
The script reads your local `ChatStorage.sqlite` (WhatsApp database) using SQLite queries. WhatsApp stores contact names directly in the database.

### Combined
The combined script reads both databases and merges the data:
- Uses AddressBook contacts to reconcile names across platforms
- Combines message counts, response times, and other stats
- Merges top contacts (deduplicating by name when possible)
- Shows platform breakdown with message counts per platform
- Works even if only one platform is available

All scripts analyze your message patterns, resolve identifiers to contact names, and generate a self-contained HTML file with an interactive gallery.

## FAQ

**Q: Is this safe?**
A: Yes. The scripts only read local databases, write one HTML file, and make zero network requests. No data is sent anywhere.

**Q: Why do I need Full Disk Access?**
A: Apple protects message databases. Terminal needs permission to read them.

**Q: Can I run this on iOS?**
A: No, iOS doesn't allow access to message databases. macOS only.

**Q: The names are showing as phone numbers**
A: The script tries to match identifiers to contact names. Some may not resolve if the formatting differs.

**Q: Where is the WhatsApp database?**
A: WhatsApp stores its database at:
- `~/Library/Group Containers/group.net.whatsapp.WhatsApp.shared/ChatStorage.sqlite` (current version)
- `~/Library/Containers/com.whatsapp/Data/Library/Application Support/WhatsApp/ChatStorage.sqlite` (older versions)

## Credits

Made by [@nikunj](https://x.com/nikunj)

Not affiliated with Apple, Meta, Spotify, or WhatsApp.

## License

MIT
