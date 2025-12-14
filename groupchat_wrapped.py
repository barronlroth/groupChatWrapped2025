#!/usr/bin/env python3
"""
Groupchat Wrapped 2025 (iMessage) - pick one group chat and generate a Spotify Wrapped-style summary.
Usage: python3 groupchat_wrapped.py
"""

import argparse, glob, html, os, re, sqlite3, subprocess, sys, threading, time
from collections import Counter, defaultdict
from datetime import datetime
from urllib.parse import urlparse

IMESSAGE_DB = os.path.expanduser("~/Library/Messages/chat.db")
ADDRESSBOOK_DIR = os.path.expanduser("~/Library/Application Support/AddressBook")

TS_2025 = 1735689600
TS_2024 = 1704067200
FIRST_PERSON_RE = re.compile(r"\b(i|me)\b", re.IGNORECASE)
WORD_RE = re.compile(r"\b[\w']+\b", re.UNICODE)
PROFANITY_CHUNK_RE = re.compile(r"[A-Za-z0-9@#$%*!?._'’+-]{2,}", re.UNICODE)
URL_RE = re.compile(r"(https?://[^\s<>()]+|www\.[^\s<>()]+)", re.IGNORECASE)
# Tapbacks use associated_message_type. 200x = add reaction, 300x = remove reaction.
TAPBACK_TYPE_LAUGH = 2003
UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.IGNORECASE)
# Conservative fallback for bare domains (no scheme/www). Keeps false positives down.
BARE_DOMAIN_RE = re.compile(
    r"(?i)\b([a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+"
    r"(com|net|org|edu|gov|io|co|ai|app|dev|me|gg|tv|xyz|info|biz)"
    r"([/][^\s<>()]*)?\b"
)


LEET_MAP = str.maketrans(
    {
        "@": "a",
        "4": "a",
        "$": "s",
        "5": "s",
        "0": "o",
        "1": "i",
        "!": "i",
        "3": "e",
        "7": "t",
    }
)

DEFAULT_PROFANITY_WORDS = {
    "ass",
    "asshole",
    "bastard",
    "bitch",
    "bullshit",
    "crap",
    "cunt",
    "damn",
    "dick",
    "dipshit",
    "douche",
    "douchebag",
    "fag",
    "faggot",
    "fuck",
    "goddamn",
    "hell",
    "jackass",
    "motherfucker",
    "piss",
    "pissed",
    "prick",
    "pussy",
    "shit",
    "slut",
    "twat",
    "wanker",
    "whore",
}


def _squash_repeats(s, max_run=2):
    if not s:
        return s
    out = []
    prev = s[0]
    run = 1
    out.append(prev)
    for ch in s[1:]:
        if ch == prev:
            run += 1
            if run <= max_run:
                out.append(ch)
        else:
            prev = ch
            run = 1
            out.append(ch)
    return "".join(out)


def normalize_profanity_token(token):
    if not token:
        return ""
    t = token.lower().replace("’", "'").translate(LEET_MAP)
    # Remove everything except letters and apostrophes, then trim apostrophes.
    t = re.sub(r"[^a-z']+", "", t).strip("'")
    if not t:
        return ""
    # Try both a mild and aggressive repeat squash.
    t2 = _squash_repeats(t, max_run=2)
    t1 = _squash_repeats(t, max_run=1)
    return t2, t1


def load_profanity_words():
    return set(DEFAULT_PROFANITY_WORDS)


def _canonical_profanity(candidate, profanity_words):
    if not candidate:
        return None
    if candidate in profanity_words:
        return candidate
    for suffix in ("ing", "in", "ed", "er", "ers", "es", "s"):
        if candidate.endswith(suffix) and len(candidate) - len(suffix) >= 3:
            base = candidate[: -len(suffix)]
            if base in profanity_words:
                return base
    return None


def extract_profanity_terms(text, profanity_words):
    terms = []
    if not text or not profanity_words:
        return terms
    for chunk in PROFANITY_CHUNK_RE.findall(text):
        n = normalize_profanity_token(chunk)
        if not n:
            continue
        t2, t1 = n
        # Avoid counting extremely short tokens to reduce false positives.
        for candidate in (t2, t1):
            if not candidate or len(candidate) < 3:
                continue
            canonical = _canonical_profanity(candidate, profanity_words)
            if canonical:
                terms.append(canonical)
                break
    return terms


def extract_links(text):
    if not text:
        return []
    links = []
    for raw in URL_RE.findall(text):
        url = raw.strip().rstrip('.,;:!?)"]}\'')
        if url.lower().startswith("www."):
            url = "http://" + url
        links.append(url)
    for m in BARE_DOMAIN_RE.finditer(text):
        url = m.group(0).strip().rstrip('.,;:!?)"]}\'')
        if url.lower().startswith("www."):
            url = "http://" + url
        elif not url.lower().startswith(("http://", "https://")):
            url = "http://" + url
        links.append(url)
    return links


def domain_from_url(url):
    try:
        parsed = urlparse(url)
        host = (parsed.netloc or "").lower()
        if host.startswith("www."):
            host = host[4:]
        return host or None
    except Exception:
        return None


def _decode_blob(blob):
    if blob is None:
        return ""
    if isinstance(blob, memoryview):
        blob = blob.tobytes()
    if isinstance(blob, bytes):
        try:
            return blob.decode("utf-8", errors="ignore")
        except Exception:
            return ""
    return str(blob)


def extract_links_from_message(text, attributed_body):
    links = []
    links.extend(extract_links(text or ""))
    blob_text = _decode_blob(attributed_body)
    if blob_text:
        links.extend(extract_links(blob_text))
    # De-dupe per-message to avoid double-counting text vs blob.
    return list(dict.fromkeys(links))


def load_group_chat_laugh_tapbacks(chat_id, ts_start):
    """
    Returns tapback messages that represent a "Laughed at" reaction within the chat.
    Each item includes who reacted (sender) and which message they reacted to (target_guid).
    """
    sql = """
        SELECT
            (m.date/1000000000+978307200) as ts,
            m.is_from_me,
            h.id as handle,
            m.text,
            m.guid,
            m.associated_message_guid,
            m.associated_message_type
        FROM message m
        JOIN chat_message_join cmj ON m.ROWID = cmj.message_id
        LEFT JOIN handle h ON m.handle_id = h.ROWID
        WHERE cmj.chat_id = ?
        AND (m.date/1000000000+978307200) > ?
        AND (
            (m.associated_message_guid IS NOT NULL AND m.associated_message_type IS NOT NULL)
            OR (m.text LIKE 'Laughed at %')
        )
        ORDER BY m.date
    """
    try:
        rows = q(sql, (chat_id, ts_start))
    except sqlite3.OperationalError:
        # Some schema variants may lack associated_message_* columns; fall back to text-only heuristic.
        rows = q(
            """
            SELECT
                (m.date/1000000000+978307200) as ts,
                m.is_from_me,
                h.id as handle,
                m.text,
                m.guid,
                NULL as associated_message_guid,
                NULL as associated_message_type
            FROM message m
            JOIN chat_message_join cmj ON m.ROWID = cmj.message_id
            LEFT JOIN handle h ON m.handle_id = h.ROWID
            WHERE cmj.chat_id = ?
            AND (m.date/1000000000+978307200) > ?
            AND (m.text LIKE 'Laughed at %')
            ORDER BY m.date
            """,
            (chat_id, ts_start),
        )

    tapbacks = []
    for ts, is_from_me, handle, text, guid, assoc_guid, assoc_type in rows:
        target_guid = str(assoc_guid).strip().lower() if assoc_guid else None
        # Ignore removed reactions (300x types). Identify "laugh" by type when possible,
        # but also fall back to the text prefix to avoid schema/locale mismatches.
        if assoc_type is not None:
            try:
                assoc_type_int = int(assoc_type)
            except Exception:
                assoc_type_int = None
            if assoc_type_int is None:
                continue
            if assoc_type_int >= 3000:
                continue
            if assoc_type_int != TAPBACK_TYPE_LAUGH and not (text or "").startswith("Laughed at"):
                continue
        else:
            # Fallback heuristic: message text starts with "Laughed at".
            if not (text or "").startswith("Laughed at"):
                continue

        if not target_guid:
            # If we only have text (older schema), we can't reliably attribute to a specific message.
            continue

        tapbacks.append(
            {
                "ts": int(ts or 0),
                "is_from_me": int(is_from_me or 0),
                "handle": handle,
                "text": text,
                "guid": guid,
                "target_guid": target_guid,
            }
        )
    return tapbacks


def resolve_message_authors_by_guid(chat_id, guids):
    guid_to_author = {}
    if not guids:
        return guid_to_author

    def norm(g):
        if not g:
            return None
        return str(g).strip().lower()

    guids = list(dict.fromkeys([norm(g) for g in guids if norm(g)]))
    chunk_size = 500
    for i in range(0, len(guids), chunk_size):
        chunk = guids[i : i + chunk_size]
        placeholders = ",".join(["?"] * len(chunk))
        rows = q(
            f"""
            SELECT
                lower(m.guid) as guid,
                m.is_from_me,
                h.id as handle
            FROM message m
            JOIN chat_message_join cmj ON m.ROWID = cmj.message_id
            LEFT JOIN handle h ON m.handle_id = h.ROWID
            WHERE cmj.chat_id = ?
            AND lower(m.guid) IN ({placeholders})
            """,
            (chat_id, *chunk),
        )
        for guid, is_from_me, handle in rows:
            guid_to_author[guid] = {"is_from_me": int(is_from_me or 0), "handle": handle}
    return guid_to_author


def normalize_message_guid(guid):
    if not guid:
        return None
    return str(guid).strip().lower()


def extract_guid_uuid(guid):
    if not guid:
        return None
    m = UUID_RE.search(str(guid))
    return m.group(0).lower() if m else None


def _truncate_one_line(s, n=120):
    if s is None:
        return ""
    s = str(s).replace("\n", " ").replace("\r", " ").strip()
    if len(s) <= n:
        return s
    return s[: n - 1] + "…"


def debug_tapbacks(chat_id, ts_start, contacts, limit=25):
    print("\n[debug] Tapbacks diagnostics")
    cols = [r[1] for r in q("PRAGMA table_info(message)")]
    has_assoc_guid = "associated_message_guid" in cols
    has_assoc_type = "associated_message_type" in cols
    has_guid = "guid" in cols
    print(
        f"[debug] message columns: guid={has_guid}, associated_message_guid={has_assoc_guid}, associated_message_type={has_assoc_type}"
    )

    # Count candidates in this chat.
    try:
        r = q(
            """
            SELECT
                SUM(CASE WHEN m.associated_message_guid IS NOT NULL THEN 1 ELSE 0 END) as assoc_guid_msgs,
                SUM(CASE WHEN m.associated_message_type IS NOT NULL THEN 1 ELSE 0 END) as assoc_type_msgs,
                SUM(CASE WHEN m.text LIKE 'Laughed at %' THEN 1 ELSE 0 END) as laughed_text
            FROM message m
            JOIN chat_message_join cmj ON m.ROWID = cmj.message_id
            WHERE cmj.chat_id = ?
            AND (m.date/1000000000+978307200) > ?
            """,
            (chat_id, ts_start),
        )
        if r and r[0]:
            print(
                f"[debug] candidates: associated_guid={int(r[0][0] or 0)}, associated_type={int(r[0][1] or 0)}, text='Laughed at'={int(r[0][2] or 0)}"
            )
    except sqlite3.OperationalError as e:
        print(f"[debug] candidate query failed: {e}")

    # Types distribution (if available).
    if has_assoc_type:
        try:
            rows = q(
                """
                SELECT m.associated_message_type, COUNT(*) c
                FROM message m
                JOIN chat_message_join cmj ON m.ROWID = cmj.message_id
                WHERE cmj.chat_id = ?
                AND (m.date/1000000000+978307200) > ?
                AND m.associated_message_type IS NOT NULL
                GROUP BY m.associated_message_type
                ORDER BY c DESC
                LIMIT 20
                """,
                (chat_id, ts_start),
            )
            if rows:
                print("[debug] top associated_message_type values:")
                for t, c in rows:
                    print(f"  - {t}: {c}")
        except sqlite3.OperationalError as e:
            print(f"[debug] type distribution query failed: {e}")

    # Sample tapback-ish rows.
    try:
        rows = q(
            """
            SELECT
                (m.date/1000000000+978307200) as ts,
                m.is_from_me,
                h.id as handle,
                m.associated_message_type,
                m.associated_message_guid,
                m.guid,
                m.text
            FROM message m
            JOIN chat_message_join cmj ON m.ROWID = cmj.message_id
            LEFT JOIN handle h ON m.handle_id = h.ROWID
            WHERE cmj.chat_id = ?
            AND (m.date/1000000000+978307200) > ?
            AND (
                (m.associated_message_guid IS NOT NULL AND m.associated_message_type IS NOT NULL)
                OR m.text LIKE 'Loved "%'
                OR m.text LIKE 'Liked "%'
                OR m.text LIKE 'Disliked "%'
                OR m.text LIKE 'Laughed at "%'
                OR m.text LIKE 'Emphasized "%'
                OR m.text LIKE 'Questioned "%'
            )
            ORDER BY m.date DESC
            LIMIT ?
            """,
            (chat_id, ts_start, limit),
        )
        if not rows:
            print("[debug] no tapback-like rows found with current heuristics.")
            return
        print(f"[debug] sample tapback-like rows (newest {len(rows)}):")
        for ts, is_from_me, handle, assoc_type, assoc_guid, guid, text in rows:
            sender = "You" if int(is_from_me or 0) == 1 else get_name(handle, contacts)
            print(
                f"  - {datetime.fromtimestamp(int(ts or 0)).strftime('%Y-%m-%d %H:%M')} | {sender} | type={assoc_type} | target={_truncate_one_line(assoc_guid, 44)} | text={_truncate_one_line(text, 90)}"
            )

        # How many of these targets resolve to messages in the chat?
        msgs = load_group_chat_messages(chat_id, ts_start)
        guid_to_author = {}
        uuid_to_author = {}
        for m in msgs:
            author = "You" if int(m.get("is_from_me") or 0) == 1 else get_name(m.get("handle"), contacts)
            g = normalize_message_guid(m.get("guid"))
            if g:
                guid_to_author[g] = author
            u = extract_guid_uuid(m.get("guid"))
            if u:
                uuid_to_author[u] = author

        targets = [normalize_message_guid(r[4]) for r in rows if r[4]]
        targets = [t for t in targets if t]
        unique_targets = list(dict.fromkeys(targets))
        resolved_full = 0
        resolved_uuid = 0
        for t in unique_targets:
            if t in guid_to_author:
                resolved_full += 1
                continue
            u = extract_guid_uuid(t)
            if u and u in uuid_to_author:
                resolved_uuid += 1

        print(f"[debug] target guid resolvable: full={resolved_full}/{len(unique_targets)}, uuid={resolved_uuid}/{len(unique_targets)}")
        if unique_targets:
            t0 = unique_targets[0]
            a0 = guid_to_author.get(t0)
            if not a0:
                u0 = extract_guid_uuid(t0)
                if u0:
                    a0 = uuid_to_author.get(u0)
            if a0:
                print(f"[debug] example target resolves: {_truncate_one_line(t0, 32)} -> {a0}")
    except sqlite3.OperationalError as e:
        print(f"[debug] sample tapback query failed: {e}")

class Spinner:
    def __init__(self, message=""):
        self.message = message
        self.spinning = False
        self.thread = None
        self.frames = ["⣾", "⣽", "⣻", "⢿", "⡿", "⣟", "⣯", "⣷"]

    def spin(self):
        i = 0
        while self.spinning:
            frame = self.frames[i % len(self.frames)]
            print(f"\r    {frame} {self.message}", end="", flush=True)
            time.sleep(0.1)
            i += 1

    def start(self, message=None):
        if message:
            self.message = message
        self.spinning = True
        self.thread = threading.Thread(target=self.spin)
        self.thread.start()

    def stop(self, final_message=None):
        self.spinning = False
        if self.thread:
            self.thread.join()
        if final_message:
            print(f"\r    ✓ {final_message}".ljust(60))
        else:
            print()


def check_access():
    if not os.path.exists(IMESSAGE_DB):
        print("\n[FATAL] Messages database not found. This only works on macOS with Messages enabled.")
        sys.exit(1)
    try:
        conn = sqlite3.connect(IMESSAGE_DB)
        conn.execute("SELECT 1 FROM message LIMIT 1")
        conn.close()
    except Exception:
        print("\n⚠️  ACCESS DENIED")
        print("   System Settings → Privacy & Security → Full Disk Access → Add Terminal")
        try:
            subprocess.run(
                [
                    "open",
                    "x-apple.systempreferences:com.apple.preference.security?Privacy_AllFiles",
                ],
                check=False,
            )
        except Exception:
            pass
        sys.exit(1)


def q(sql, params=()):
    conn = sqlite3.connect(IMESSAGE_DB)
    try:
        return conn.execute(sql, params).fetchall()
    finally:
        conn.close()


def extract_contacts():
    contacts = {}
    db_paths = glob.glob(
        os.path.join(ADDRESSBOOK_DIR, "Sources", "*", "AddressBook-v22.abcddb")
    )
    main_db = os.path.join(ADDRESSBOOK_DIR, "AddressBook-v22.abcddb")
    if os.path.exists(main_db):
        db_paths.append(main_db)

    for db_path in db_paths:
        try:
            conn = sqlite3.connect(db_path)
            people = {}
            for row in conn.execute(
                "SELECT ROWID, ZFIRSTNAME, ZLASTNAME FROM ZABCDRECORD "
                "WHERE ZFIRSTNAME IS NOT NULL OR ZLASTNAME IS NOT NULL"
            ):
                name = f"{row[1] or ''} {row[2] or ''}".strip()
                if name:
                    people[row[0]] = name

            for owner, phone in conn.execute(
                "SELECT ZOWNER, ZFULLNUMBER FROM ZABCDPHONENUMBER WHERE ZFULLNUMBER IS NOT NULL"
            ):
                if owner in people:
                    name = people[owner]
                    digits = re.sub(r"\D", "", str(phone))
                    if digits:
                        contacts[digits] = name
                        if len(digits) >= 10:
                            contacts[digits[-10:]] = name
                        if len(digits) >= 7:
                            contacts[digits[-7:]] = name
                        if len(digits) == 11 and digits.startswith("1"):
                            contacts[digits[1:]] = name

            for owner, email in conn.execute(
                "SELECT ZOWNER, ZADDRESS FROM ZABCDEMAILADDRESS WHERE ZADDRESS IS NOT NULL"
            ):
                if owner in people:
                    contacts[email.lower().strip()] = people[owner]
            conn.close()
        except Exception:
            pass
    return contacts


def get_name(handle, contacts):
    if not handle:
        return "Unknown"
    if "@" in handle:
        lookup = handle.lower().strip()
        if lookup in contacts:
            return contacts[lookup]
        return handle.split("@")[0]
    digits = re.sub(r"\D", "", str(handle))
    if digits in contacts:
        return contacts[digits]
    if len(digits) == 11 and digits.startswith("1") and digits[1:] in contacts:
        return contacts[digits[1:]]
    if len(digits) >= 10 and digits[-10:] in contacts:
        return contacts[digits[-10:]]
    if len(digits) >= 7 and digits[-7:] in contacts:
        return contacts[digits[-7:]]
    return handle


def _is_tapback_text(text):
    return bool(
        text.startswith('Loved "')
        or text.startswith('Liked "')
        or text.startswith('Disliked "')
        or text.startswith('Laughed at "')
        or text.startswith('Emphasized "')
        or text.startswith('Questioned "')
    )


EMOJI_BASE_RANGES = [
    (0x1F1E6, 0x1F1FF),  # flags
    (0x1F300, 0x1F5FF),  # symbols & pictographs
    (0x1F600, 0x1F64F),  # emoticons
    (0x1F680, 0x1F6FF),  # transport & map
    (0x1F700, 0x1F77F),  # alchemical symbols
    (0x1F780, 0x1F7FF),  # geometric shapes ext
    (0x1F800, 0x1F8FF),  # supplemental arrows-c
    (0x1F900, 0x1F9FF),  # supplemental symbols & pictographs
    (0x1FA00, 0x1FA6F),  # symbols & pictographs ext-a (part 1)
    (0x1FA70, 0x1FAFF),  # symbols & pictographs ext-a (part 2)
    (0x2600, 0x26FF),  # misc symbols
    (0x2700, 0x27BF),  # dingbats
]

VS16 = 0xFE0F
ZWJ = 0x200D
KEYCAP = 0x20E3
SKIN_TONE_START = 0x1F3FB
SKIN_TONE_END = 0x1F3FF
TAG_START = 0xE0020
TAG_END = 0xE007F


def _is_emoji_base(cp):
    for start, end in EMOJI_BASE_RANGES:
        if start <= cp <= end:
            return True
    return False


def extract_emojis(text):
    emojis = []
    i = 0
    n = len(text)

    while i < n:
        cp = ord(text[i])

        # Flags (regional indicator pairs)
        if 0x1F1E6 <= cp <= 0x1F1FF and i + 1 < n:
            cp2 = ord(text[i + 1])
            if 0x1F1E6 <= cp2 <= 0x1F1FF:
                emojis.append(text[i : i + 2])
                i += 2
                continue

        # Keycap sequences: [0-9#*] + VS16? + KEYCAP
        if text[i] in "0123456789#*" and i + 1 < n:
            j = i + 1
            if j < n and ord(text[j]) == VS16:
                j += 1
            if j < n and ord(text[j]) == KEYCAP:
                emojis.append(text[i : j + 1])
                i = j + 1
                continue

        if not _is_emoji_base(cp):
            i += 1
            continue

        j = i + 1
        while j < n:
            next_cp = ord(text[j])

            # Variation selector
            if next_cp == VS16:
                j += 1
                continue

            # Skin tone modifier
            if SKIN_TONE_START <= next_cp <= SKIN_TONE_END:
                j += 1
                continue

            # Tag sequences (subdivision flags). Keep until cancel tag (E007F).
            if TAG_START <= next_cp <= TAG_END:
                j += 1
                while j < n and ord(text[j]) != TAG_END:
                    j += 1
                if j < n:
                    j += 1
                continue

            # ZWJ sequences: emoji + ZWJ + emoji + ...
            if next_cp == ZWJ and j + 1 < n and _is_emoji_base(ord(text[j + 1])):
                j += 1  # include ZWJ
                j += 1  # include next base emoji
                continue

            break

        emojis.append(text[i:j])
        i = j

    return emojis


def get_group_chat_participants(chat_id):
    rows = q(
        """
        SELECT h.id
        FROM chat_handle_join chj
        JOIN handle h ON h.ROWID = chj.handle_id
        WHERE chj.chat_id = ?
        ORDER BY h.id
        """,
        (chat_id,),
    )
    return [r[0] for r in rows if r and r[0]]


def get_group_chat_summaries(ts_start, limit=60):
    rows = q(
        """
        WITH chat_participants AS (
            SELECT chat_id, COUNT(*) as participant_count
            FROM chat_handle_join
            GROUP BY chat_id
        ),
        group_chats AS (
            SELECT chat_id, participant_count
            FROM chat_participants
            WHERE participant_count >= 2
        ),
        group_messages AS (
            SELECT cmj.chat_id as chat_id, m.ROWID as msg_id, m.date as date
            FROM chat_message_join cmj
            JOIN message m ON m.ROWID = cmj.message_id
            WHERE cmj.chat_id IN (SELECT chat_id FROM group_chats)
            AND (m.date/1000000000+978307200) > ?
        )
        SELECT
            c.ROWID as chat_id,
            c.display_name as display_name,
            gc.participant_count as participant_count,
            COUNT(gm.msg_id) as msg_count,
            MAX(gm.date/1000000000+978307200) as last_ts
        FROM chat c
        JOIN group_chats gc ON c.ROWID = gc.chat_id
        JOIN group_messages gm ON c.ROWID = gm.chat_id
        GROUP BY c.ROWID
        ORDER BY msg_count DESC, last_ts DESC
        LIMIT ?
        """,
        (ts_start, limit),
    )

    chats = []
    for chat_id, display_name, participant_count, msg_count, last_ts in rows:
        chats.append(
            {
                "chat_id": int(chat_id),
                "display_name": display_name,
                "participant_count": int(participant_count or 0),
                "msg_count": int(msg_count or 0),
                "last_ts": int(last_ts or 0),
            }
        )
    return chats


def get_chat_display_name(chat_id):
    rows = q("SELECT display_name FROM chat WHERE ROWID = ?", (chat_id,))
    return rows[0][0] if rows and rows[0] else None


def is_group_chat(chat_id):
    rows = q(
        "SELECT COUNT(*) FROM chat_handle_join WHERE chat_id = ?",
        (chat_id,),
    )
    return bool(rows and rows[0] and int(rows[0][0] or 0) >= 2)


def _format_last_active(ts):
    if not ts:
        return "—"
    try:
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
    except Exception:
        return "—"


def _build_chat_label(chat, contacts):
    if chat.get("display_name"):
        return str(chat["display_name"])
    participants = get_group_chat_participants(chat["chat_id"])
    names = [get_name(h, contacts) for h in participants[:2]]
    extra = max(0, len(participants) - len(names))
    if names:
        return f"{', '.join(names)} +{extra}" if extra else ", ".join(names)
    return f"Chat {chat['chat_id']}"


def pick_group_chat(ts_start, year, contacts, limit=60, initial_search=None):
    all_chats = get_group_chat_summaries(ts_start, limit=limit)
    if not all_chats:
        print(f"\n[FATAL] No active group chats found for {year} yet.")
        sys.exit(1)

    for c in all_chats:
        c["label"] = _build_chat_label(c, contacts)
    filtered = all_chats
    if initial_search:
        term = str(initial_search).strip().lower()
        if term:
            matches = [c for c in all_chats if term in c["label"].lower()]
            if matches:
                filtered = matches
    while True:
        print(f"\nPick a group chat ({year} YTD):")
        print("   tip: type /tennis to search, or press Enter to refresh\n")

        labels = [(c["label"], c) for c in filtered[:30]]
        name_width = max(12, min(46, max(len(lbl) for lbl, _ in labels)))

        header = f"  #  {'Chat'.ljust(name_width)}  Msgs   Ppl  Last"
        print(header)
        print("  " + "-" * (len(header) - 2))
        for idx, (label, chat) in enumerate(labels, 1):
            chat_name = (label[: name_width - 1] + "…") if len(label) > name_width else label
            print(
                f"  {str(idx).rjust(2)}  {chat_name.ljust(name_width)}  "
                f"{str(chat['msg_count']).rjust(4)}  {str(chat['participant_count']).rjust(3)}  "
                f"{_format_last_active(chat['last_ts'])}"
            )

        choice = input("\nSelect #, or /search, or q: ").strip()
        if not choice:
            filtered = all_chats
            continue
        if choice.lower() in {"q", "quit", "exit"}:
            sys.exit(0)
        if choice.startswith("/"):
            term = choice[1:].strip().lower()
            if not term:
                filtered = all_chats
                continue
            filtered = [
                c for c in all_chats if term in c["label"].lower()
            ]
            if not filtered:
                print("    (no matches)")
                filtered = all_chats
            continue
        if choice.isdigit():
            idx = int(choice)
            if 1 <= idx <= len(labels):
                return labels[idx - 1][1]
        print("    (invalid input)")


def load_group_chat_messages(chat_id, ts_start):
    sql_with_attr = """
        SELECT
            m.ROWID,
            (m.date/1000000000+978307200) as ts,
            m.is_from_me,
            h.id as handle,
            m.text,
            m.guid,
            m.attributedBody
        FROM message m
        JOIN chat_message_join cmj ON m.ROWID = cmj.message_id
        LEFT JOIN handle h ON m.handle_id = h.ROWID
        WHERE cmj.chat_id = ?
        AND (m.date/1000000000+978307200) > ?
        ORDER BY m.date
    """
    sql_without_attr = """
        SELECT
            m.ROWID,
            (m.date/1000000000+978307200) as ts,
            m.is_from_me,
            h.id as handle,
            m.text,
            m.guid,
            NULL as attributedBody
        FROM message m
        JOIN chat_message_join cmj ON m.ROWID = cmj.message_id
        LEFT JOIN handle h ON m.handle_id = h.ROWID
        WHERE cmj.chat_id = ?
        AND (m.date/1000000000+978307200) > ?
        ORDER BY m.date
    """
    try:
        rows = q(sql_with_attr, (chat_id, ts_start))
    except sqlite3.OperationalError:
        rows = q(sql_without_attr, (chat_id, ts_start))
    msgs = []
    for _rowid, ts, is_from_me, handle, text, guid, attributed_body in rows:
        msgs.append(
            {
                "ts": int(ts or 0),
                "is_from_me": int(is_from_me or 0),
                "handle": handle,
                "text": text,
                "guid": guid,
                "attributed_body": attributed_body,
            }
        )
    return msgs


def analyze_group_chat(chat, ts_start, year, contacts, profanity_words):
    chat_id = chat["chat_id"]
    participants = get_group_chat_participants(chat_id)
    participant_names = [get_name(h, contacts) for h in participants]
    chat_name = _build_chat_label(chat, contacts)

    msgs = load_group_chat_messages(chat_id, ts_start)
    last_ts = msgs[-1]["ts"] if msgs else int(chat.get("last_ts") or 0)

    # Build a fast GUID → author index for tapback resolution.
    guid_to_author_name = {}
    uuid_to_author_name = {}

    sender_msg_counts = Counter()
    sender_emoji_counts = Counter()
    sender_unique_emoji = defaultdict(set)
    emoji_counts = Counter()
    sender_first_person_counts = Counter()
    sender_word_counts = Counter()
    sender_profanity_counts = Counter()
    sender_profanity_term_counts = defaultdict(Counter)
    sender_link_counts = Counter()
    sender_link_domain_counts = defaultdict(Counter)
    sender_laugh_received_counts = Counter()

    analyzed_text_msgs = 0
    for m in msgs:
        sender = "You" if m["is_from_me"] == 1 else get_name(m["handle"], contacts)
        text = m["text"] or ""

        msg_guid = normalize_message_guid(m.get("guid"))
        if msg_guid:
            guid_to_author_name[msg_guid] = sender
        msg_uuid = extract_guid_uuid(m.get("guid"))
        if msg_uuid:
            uuid_to_author_name[msg_uuid] = sender

        if text and _is_tapback_text(text):
            continue

        sender_msg_counts[sender] += 1

        if not text:
            continue
        cleaned = text.replace("\uFFFC", "").strip()
        if not cleaned:
            continue

        analyzed_text_msgs += 1

        words = WORD_RE.findall(cleaned)
        if words:
            sender_word_counts[sender] += len(words)
            sender_first_person_counts[sender] += len(FIRST_PERSON_RE.findall(cleaned))

        links = extract_links_from_message(cleaned, m.get("attributed_body"))
        if links:
            sender_link_counts[sender] += len(links)
            for url in links:
                host = domain_from_url(url)
                if host:
                    sender_link_domain_counts[sender].update([host])

        if profanity_words:
            prof_terms = extract_profanity_terms(cleaned, profanity_words)
            if prof_terms:
                sender_profanity_counts[sender] += len(prof_terms)
                sender_profanity_term_counts[sender].update(prof_terms)

        ems = extract_emojis(cleaned)
        if not ems:
            continue

        emoji_counts.update(ems)
        sender_emoji_counts[sender] += len(ems)
        for e in ems:
            sender_unique_emoji[sender].add(e)

    top_emojis = emoji_counts.most_common(10)
    most_used_emoji = top_emojis[0] if top_emojis else None

    yapper = sender_msg_counts.most_common(1)[0] if sender_msg_counts else ("—", 0)
    emoji_mvp = sender_emoji_counts.most_common(1)[0] if sender_emoji_counts else ("—", 0)
    unique_emoji_mvp = (
        max(sender_unique_emoji.items(), key=lambda kv: len(kv[1]))
        if sender_unique_emoji
        else ("—", set())
    )

    # "Narcissist" = first-person usage rate (I/me) per 100 words, with a small minimum sample size.
    narcissist = ("—", 0, 0, 0)  # (name, i_me_count, words, per_100_words)
    if sender_word_counts:
        eligible = [
            (name, int(sender_first_person_counts.get(name, 0)), int(words))
            for name, words in sender_word_counts.items()
            if int(words) >= 30
        ]
        if eligible:
            name, fp_count, words = max(
                eligible,
                key=lambda t: (t[1] / max(t[2], 1.0), t[1], t[2]),
            )
            narcissist = (name, fp_count, words, round((fp_count / max(words, 1)) * 100))
        else:
            if sender_first_person_counts:
                name, fp_count = sender_first_person_counts.most_common(1)[0]
                words = int(sender_word_counts.get(name, 0))
                narcissist = (name, int(fp_count), words, round((fp_count / max(words, 1)) * 100) if words else 0)

    potty_mouth = ("—", 0)
    if profanity_words and sender_profanity_counts:
        potty_mouth = sender_profanity_counts.most_common(1)[0]

    link_spammer = ("—", 0)
    if sender_link_counts:
        link_spammer = sender_link_counts.most_common(1)[0]

    # Class clown = whose messages get the most "Laughed at" tapbacks.
    laugh_tapbacks = load_group_chat_laugh_tapbacks(chat_id, ts_start)
    for tb in laugh_tapbacks:
        target_guid = normalize_message_guid(tb["target_guid"])
        if not target_guid:
            continue
        author_name = guid_to_author_name.get(target_guid)
        if not author_name:
            target_uuid = extract_guid_uuid(target_guid)
            if target_uuid:
                author_name = uuid_to_author_name.get(target_uuid)
        if author_name:
            sender_laugh_received_counts[author_name] += 1

    class_clown = ("—", 0)
    if sender_laugh_received_counts:
        class_clown = sender_laugh_received_counts.most_common(1)[0]

    return {
        "year": year,
        "ts_start": ts_start,
        "chat_id": chat_id,
        "chat_name": chat_name,
        "participants": participant_names,
        "participant_count": len(participant_names),
        "total_messages": len(msgs),
        "last_active": _format_last_active(last_ts),
        "messages_analyzed": len(msgs),
        "text_messages_analyzed": analyzed_text_msgs,
        "sender_msg_counts": sender_msg_counts,
        "emoji_counts": emoji_counts,
        "top_emojis": top_emojis,
        "most_used_emoji": most_used_emoji,
        "sender_emoji_counts": sender_emoji_counts,
        "yapper": yapper,
        "emoji_mvp": emoji_mvp,
        "unique_emoji_mvp": (unique_emoji_mvp[0], len(unique_emoji_mvp[1])),
        "sender_first_person_counts": sender_first_person_counts,
        "sender_word_counts": sender_word_counts,
        "narcissist": narcissist,
        "sender_profanity_counts": sender_profanity_counts,
        "sender_profanity_term_counts": sender_profanity_term_counts,
        "potty_mouth": potty_mouth,
        "profanity_available": bool(profanity_words),
        "sender_link_counts": sender_link_counts,
        "sender_link_domain_counts": sender_link_domain_counts,
        "link_spammer": link_spammer,
        "sender_laugh_received_counts": sender_laugh_received_counts,
        "class_clown": class_clown,
    }


def _escape(s):
    return html.escape(str(s), quote=True)


def gen_html(d, output_path):
    chat_name = _escape(d["chat_name"])
    year = int(d["year"])
    total_msgs = int(d["total_messages"])
    ppl = int(d["participant_count"])

    participants_preview = ", ".join(d["participants"][:6])
    if len(d["participants"]) > 6:
        participants_preview += f" +{len(d['participants']) - 6}"
    participants_preview = _escape(participants_preview) if participants_preview else "—"

    top_emojis = d["top_emojis"]
    if top_emojis:
        emoji_rows = "".join(
            f'<div class="rank-item"><span class="rank-emoji">{_escape(e)}</span>'
            f'<span class="rank-count">{c:,}</span></div>'
            for e, c in top_emojis[:5]
        )
        most_emoji, most_emoji_count = top_emojis[0]
    else:
        emoji_rows = '<div class="muted">no emojis found (yall are so serious)</div>'
        most_emoji, most_emoji_count = "—", 0

    yapper_name, yapper_count = d["yapper"]
    emoji_mvp_name, emoji_mvp_count = d["emoji_mvp"]
    unique_name, unique_count = d["unique_emoji_mvp"]
    narc_name, narc_fp_count, narc_words, narc_rate = d.get("narcissist", ("—", 0, 0, 0))
    profanity_available = bool(d.get("profanity_available"))
    potty_name, potty_count = d.get("potty_mouth", ("—", 0))
    potty_fave = "—"
    term_counts = d.get("sender_profanity_term_counts", {})
    if term_counts and potty_name in term_counts and term_counts[potty_name]:
        potty_fave = term_counts[potty_name].most_common(1)[0][0]

    potty_rows = ""
    if not profanity_available:
        potty_name, potty_count, potty_fave = "—", 0, "—"
        potty_rows = '<div class="muted">potty mouth disabled</div>'
    else:
        potty_top = []
        spc = d.get("sender_profanity_counts")
        if spc:
            potty_top = spc.most_common(5)
        if potty_top:
            potty_rows_parts = []
            for name, count in potty_top:
                fave = "—"
                if term_counts and name in term_counts and term_counts[name]:
                    fave = term_counts[name].most_common(1)[0][0]
                potty_rows_parts.append(
                    f'<div class="rank-item"><span class="rank-name">{_escape(name)}<span class="muted"> — {_escape(fave)}</span></span>'
                    f'<span class="rank-count">{int(count):,}</span></div>'
                )
            potty_rows = "".join(potty_rows_parts)
        else:
            potty_rows = '<div class="muted">no profane words found</div>'

    link_name, link_count = d.get("link_spammer", ("—", 0))
    link_domain_counts = d.get("sender_link_domain_counts", {})
    link_fave = "—"
    if link_domain_counts and link_name in link_domain_counts and link_domain_counts[link_name]:
        link_fave = link_domain_counts[link_name].most_common(1)[0][0]

    link_top = []
    slc = d.get("sender_link_counts")
    if slc:
        link_top = slc.most_common(5)
    if link_top:
        link_rows_parts = []
        for name, count in link_top:
            fave = "—"
            if link_domain_counts and name in link_domain_counts and link_domain_counts[name]:
                fave = link_domain_counts[name].most_common(1)[0][0]
            link_rows_parts.append(
                f'<div class="rank-item"><span class="rank-name">{_escape(name)}<span class="muted"> — {_escape(fave)}</span></span>'
                f'<span class="rank-count">{int(count):,}</span></div>'
            )
        link_rows = "".join(link_rows_parts)
    else:
        link_rows = '<div class="muted">no links found (touch grass)</div>'

    clown_name, clown_count = d.get("class_clown", ("—", 0))
    laugh_counts = d.get("sender_laugh_received_counts")
    if laugh_counts:
        clown_top = laugh_counts.most_common(5)
        clown_rows = "".join(
            f'<div class="rank-item"><span class="rank-name">{_escape(name)}</span>'
            f'<span class="rank-count">{int(count):,}</span></div>'
            for name, count in clown_top
        )
    else:
        clown_rows = '<div class="muted">no “Laughed at” tapbacks found</div>'

    html_out = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Groupchat Wrapped {year}</title>
<style>
:root {{
  --bg: #0a0a12;
  --card: #12121f;
  --text: #f0f0f0;
  --muted: #8b93a7;
  --green: #4ade80;
  --yellow: #fbbf24;
  --red: #f87171;
  --pink: #f472b6;
  --cyan: #22d3ee;
  --orange: #fb923c;
  --purple: #a78bfa;
  --border: rgba(255,255,255,0.08);
}}
* {{ box-sizing: border-box; }}
html, body {{ height: 100%; }}
body {{
  margin: 0;
  background: var(--bg);
  color: var(--text);
  font-family: -apple-system, BlinkMacSystemFont, "SF Pro Display", "SF Pro Text", "Segoe UI", Inter, Roboto, Arial, sans-serif;
  overflow: hidden;
}}
.slides {{ height: 100%; width: 100%; position: relative; }}
.slide {{
  position: absolute;
  inset: 0;
  display: none;
  padding: 56px 28px;
  align-items: center;
  justify-content: center;
  text-align: center;
}}
.slide.active {{ display: flex; }}
.card {{
  width: min(560px, 92vw);
  background: var(--card);
  border: 1px solid var(--border);
  border-radius: 24px;
  padding: 28px 22px;
  box-shadow: 0 24px 80px rgba(0,0,0,0.55);
}}
.label {{
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace;
  color: var(--muted);
  letter-spacing: 0.14em;
  font-size: 12px;
  text-transform: uppercase;
}}
h1 {{
  margin: 14px 0 8px;
  font-size: 42px;
  line-height: 1.05;
}}
.subtitle {{
  color: var(--muted);
  margin: 0;
}}
.big-number {{
  font-size: 64px;
  margin: 10px 0;
  font-weight: 800;
}}
.green {{ color: var(--green); }}
.red {{ color: var(--red); }}
.pink {{ color: var(--pink); }}
.cyan {{ color: var(--cyan); }}
.orange {{ color: var(--orange); }}
.purple {{ color: var(--purple); }}
.stat-row {{
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 12px;
  margin-top: 18px;
}}
.stat {{
  background: rgba(255,255,255,0.04);
  border: 1px solid rgba(255,255,255,0.06);
  border-radius: 14px;
  padding: 12px 12px;
}}
.stat .k {{ color: var(--muted); font-size: 12px; letter-spacing: 0.08em; text-transform: uppercase; }}
.stat .v {{ font-size: 20px; font-weight: 700; margin-top: 4px; }}
.rank-list {{
  margin-top: 18px;
  display: flex;
  flex-direction: column;
  gap: 10px;
}}
.rank-item {{
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 14px 14px;
  border-radius: 16px;
  border: 1px solid rgba(255,255,255,0.08);
  background: rgba(255,255,255,0.03);
}}
.rank-emoji {{
  font-size: 28px;
}}
.rank-count {{
  color: var(--muted);
  font-weight: 700;
}}
.big-emoji {{
  font-size: 84px;
  margin: 14px 0 6px;
}}
.award-icon {{
  font-size: 44px;
  line-height: 1;
  margin: 12px 0 8px;
}}
.muted {{
  color: var(--muted);
}}
.hint {{
  margin-top: 18px;
  color: var(--muted);
  font-size: 12px;
}}
.progress {{
  position: fixed;
  left: 50%;
  transform: translateX(-50%);
  bottom: 14px;
  color: var(--muted);
  font-size: 12px;
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace;
  user-select: none;
}}
</style>
</head>
<body>
<div class="slides" id="slides">
  <section class="slide active">
    <div class="card">
      <div class="label">// groupchat wrapped</div>
      <h1>{chat_name}</h1>
      <p class="subtitle">{year} • {ppl} people</p>
      <p class="subtitle" style="margin-top:10px">{participants_preview}</p>
      <div class="hint">click / tap / → to continue</div>
    </div>
  </section>

  <section class="slide">
    <div class="card">
      <div class="label">// total damage</div>
      <div class="big-number green">{total_msgs:,}</div>
      <div class="muted">messages in {year} ytd</div>
      <div class="stat-row">
        <div class="stat"><div class="k">last active</div><div class="v">{_escape(d['last_active'])}</div></div>
        <div class="stat"><div class="k">analyzed</div><div class="v">{int(d['messages_analyzed']):,}</div></div>
      </div>
      <div class="hint">tap for the fun stuff</div>
    </div>
  </section>

	  <section class="slide">
	    <div class="card">
	      <div class="label">// yapper of the year</div>
	      <div class="award-icon">🗣️</div>
	      <div class="big-number orange">{_escape(yapper_name)}</div>
	      <div class="muted">{int(yapper_count):,} messages</div>
	      <div class="hint">respectfully: log off</div>
	    </div>
	  </section>

	  <section class="slide">
	    <div class="card">
	      <div class="label">// main character energy</div>
	      <div class="award-icon">🎬</div>
	      <div class="big-number purple">{_escape(narc_name)}</div>
	      <div class="muted">{int(narc_fp_count):,} “I/me” • {int(narc_rate)} per 100 words</div>
	      <div class="stat-row">
	        <div class="stat"><div class="k">words</div><div class="v">{int(narc_words):,}</div></div>
	        <div class="stat"><div class="k">diagnosis</div><div class="v">NARCISSIST</div></div>
      </div>
      <div class="hint">it’s giving autobiography</div>
    </div>
  </section>

	  <section class="slide">
	    <div class="card">
	      <div class="label">// potty mouth</div>
	      <div class="award-icon">🧼</div>
	      <div class="big-number red">{_escape(potty_name)}</div>
	      <div class="muted">{int(potty_count):,} swears • favorite: “{_escape(potty_fave)}”</div>
	      <div class="rank-list">{potty_rows}</div>
	      <div class="hint">soap is on the way</div>
	    </div>
	  </section>

	  <section class="slide">
	    <div class="card">
	      <div class="label">// link spammer</div>
	      <div class="award-icon">🔗</div>
	      <div class="big-number cyan">{_escape(link_name)}</div>
	      <div class="muted">{int(link_count):,} links • favorite: {_escape(link_fave)}</div>
	      <div class="rank-list">{link_rows}</div>
	      <div class="hint">source: trust me bro</div>
	    </div>
	  </section>

	  <section class="slide">
	    <div class="card">
	      <div class="label">// class clown</div>
	      <div class="award-icon">🤡</div>
	      <div class="big-number yellow">{_escape(clown_name)}</div>
	      <div class="muted">{int(clown_count):,} “haha” tapbacks received</div>
	      <div class="rank-list">{clown_rows}</div>
	      <div class="hint">stand up. get it?</div>
	    </div>
	  </section>

  <section class="slide">
    <div class="card">
      <div class="label">// most used emoji</div>
      <div class="big-emoji">{_escape(most_emoji)}</div>
      <div class="muted">{int(most_emoji_count):,} times</div>
      <div class="rank-list">{emoji_rows}</div>
    </div>
  </section>

	  <section class="slide">
	    <div class="card">
	      <div class="label">// emoji mvp</div>
	      <div class="award-icon">✨</div>
	      <div class="big-number pink">{_escape(emoji_mvp_name)}</div>
	      <div class="muted">{int(emoji_mvp_count):,} emojis deployed</div>
	      <div class="stat-row">
	        <div class="stat"><div class="k">unique emoji</div><div class="v">{_escape(unique_name)} ({int(unique_count)})</div></div>
	        <div class="stat"><div class="k">text msgs</div><div class="v">{int(d['text_messages_analyzed']):,}</div></div>
      </div>
      <div class="hint">done • press esc to quit</div>
    </div>
  </section>
</div>
<div class="progress" id="progress"></div>
<script>
(function() {{
  const slides = Array.from(document.querySelectorAll('.slide'));
  const progressEl = document.getElementById('progress');
  let i = 0;

  function render() {{
    slides.forEach((s, idx) => s.classList.toggle('active', idx === i));
    progressEl.textContent = `${{i+1}} / ${{slides.length}}`;
  }}

  function next() {{
    if (i < slides.length - 1) {{ i++; render(); }}
  }}

  function prev() {{
    if (i > 0) {{ i--; render(); }}
  }}

  document.addEventListener('click', next);
  document.addEventListener('keydown', (e) => {{
    if (e.key === 'ArrowRight' || e.key === ' ') return next();
    if (e.key === 'ArrowLeft') return prev();
    if (e.key === 'Escape') window.close();
  }});

  render();
}})();
</script>
</body>
</html>
"""

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_out)
    return output_path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", "-o")
    parser.add_argument("--limit", type=int, default=200, help="How many chats to list")
    parser.add_argument("--search", help="Pre-filter chat list (case-insensitive)")
    parser.add_argument("--use-2024", action="store_true", help="Analyze 2024 YTD instead of 2025 YTD")
    parser.add_argument("--chat-id", type=int, help="Skip picker and analyze this chat ID")
    parser.add_argument("--no-open", action="store_true", help="Don't open the HTML automatically")
    parser.add_argument("--no-profanity", action="store_true", help="Disable the Potty Mouth profanity counter")
    parser.add_argument("--debug-tapbacks", action="store_true", help="Print tapback diagnostics for the selected chat")
    parser.add_argument("--debug-limit", type=int, default=25, help="Rows to print for --debug-tapbacks")
    args = parser.parse_args()

    ts_start = TS_2024 if args.use_2024 else TS_2025
    year = 2024 if args.use_2024 else 2025
    output_path = args.output or f"groupchat_wrapped_{year}.html"

    print("\n" + "=" * 54)
    print(f"  Groupchat WRAPPED {year} (iMessage) | local only")
    print("=" * 54 + "\n")

    print("[*] Checking access...")
    check_access()
    print("    ✓ OK")

    spinner = Spinner()
    print("[*] Loading contacts...")
    spinner.start("Indexing AddressBook...")
    contacts = extract_contacts()
    spinner.stop(f"{len(contacts)} indexed")

    print("[*] Choosing group chat...")
    if args.chat_id:
        if not is_group_chat(args.chat_id):
            print(f"\n[FATAL] chat_id={args.chat_id} is not a group chat (or not found).")
            sys.exit(1)
        chat = {
            "chat_id": args.chat_id,
            "display_name": get_chat_display_name(args.chat_id),
        }
    else:
        chat = pick_group_chat(ts_start, year, contacts, limit=args.limit, initial_search=args.search)

    if args.debug_tapbacks:
        debug_tapbacks(chat["chat_id"], ts_start, contacts, limit=args.debug_limit)

    print("[*] Analyzing chat...")
    spinner.start("Reading messages...")
    profanity_words = set() if args.no_profanity else load_profanity_words()
    data = analyze_group_chat(chat, ts_start, year, contacts, profanity_words)
    spinner.stop(f"{data['messages_analyzed']:,} messages scanned")

    print("[*] Generating report...")
    spinner.start("Building HTML...")
    gen_html(data, output_path)
    spinner.stop(f"Saved to {output_path}")

    if not args.no_open:
        subprocess.run(["open", output_path], check=False)
    print("\n  Done! Click through your groupchat wrapped.\n")


if __name__ == "__main__":
    main()
