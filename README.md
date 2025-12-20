# GroupChat Wrapped 2025 (iMessage)

Generates a Spotify Wrapped-style HTML “deck” for one specific iMessage group chat, with awards like most used emoji.

## Requirements

- macOS (reads the local Messages database)
- Python 3
- Full Disk Access for your terminal app:
  - **System Settings → Privacy & Security → Full Disk Access → add Terminal** (or iTerm/Warp)

## Run

```bash
python3 groupchat_wrapped.py
```

It will prompt you to pick a group chat, scan messages, and write an HTML file (then open it unless you disable that).

## Useful flags

```bash
# Pre-filter the chat picker list (case-insensitive)
python3 groupchat_wrapped.py --search "tennis"

# Skip the picker and analyze a specific chat id
python3 groupchat_wrapped.py --chat-id 123

# Analyze 2024 YTD instead of 2025 YTD
python3 groupchat_wrapped.py --use-2024

# Custom output filename, and don’t auto-open it
python3 groupchat_wrapped.py -o my_groupchat_wrapped.html --no-open

# Disable the Potty Mouth profanity counter
python3 groupchat_wrapped.py --no-profanity

# Debug utilities
python3 groupchat_wrapped.py --debug-tapbacks --debug-limit 25
python3 groupchat_wrapped.py --debug-emojis --debug-emoji-limit 20 --no-open
```

## Privacy

**100% local**: the script reads local databases, produces a single HTML file, and does not upload your data.

## Credits

This repo is a group-chat-focused fork of `kothari-nikunj/wrap2025`.

## License

MIT

