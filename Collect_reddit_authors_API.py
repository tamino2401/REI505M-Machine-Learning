# improved_reddit_collector.py
import requests
import csv
import time
import calendar
import random
from datetime import datetime, timezone

# ========== CONFIG ==========
OUTPUT_CSV = "reddit_authors_posts_full_debug.csv"

SUBREDDITS = [
    "politics", "worldpolitics", "PoliticalDiscussion", "Conservative",
    "Liberal", "dsa", "socialism", "Anarchism",
    "collegerepublicans", "Ask_Politics"
]

START_YEAR = 2016
END_YEAR = 2021  # inclusive
TARGET_UNIQUE_AUTHORS = 5000
MIN_POSTS_PER_AUTHOR = 20
MAX_POSTS_PER_AUTHOR = 100
PAGE_SIZE = 100          # PullPush allows up to 100
REQUEST_DELAY = 1.0      # seconds between requests
MAX_RETRIES = 5

# safety: how many attempts to try sampling year/subreddit windows before giving up
MAX_ATTEMPTS_AUTHOR_COLLECTION = 5000

# CSV fields
FIELDS = [
    "subreddit", "id", "score", "numReplies", "author", "title", "text",
    "is_self", "domain", "url", "permalink", "upvote_ratio", "date_created"
]

# candidate keys for varying API responses
TEXT_KEYS = ["selftext", "self_text", "body", "text", "raw_text", "content"]
TITLE_KEYS = ["title", "post_title", "link_title"]
NUM_COMMENTS_KEYS = ["num_comments", "numReplies", "num_replies", "comments"]
SCORE_KEYS = ["score", "ups", "points"]
CREATED_KEYS = ["created_utc", "created", "created_at", "timestamp"]

# ========== HELPERS ==========
def to_unix(year, month=1, day=1, hour=0, minute=0, second=0):
    return calendar.timegm(datetime(year, month, day, hour, minute, second).timetuple())

def safe_get(d, candidates, default=None):
    for k in candidates:
        if k in d and d[k] not in (None, "", []):
            return d[k]
    return default

def extract_post_info(post):
    created_val = safe_get(post, CREATED_KEYS)
    try:
        created_iso = datetime.fromtimestamp(int(created_val), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S") if created_val else ""
    except Exception:
        created_iso = str(created_val) if created_val else ""

    text_val = safe_get(post, TEXT_KEYS, "")
    title_val = safe_get(post, TITLE_KEYS, "")

    return {
        "subreddit": post.get("subreddit") or post.get("subreddit_name") or "",
        "id": post.get("id") or post.get("post_id") or "",
        "score": safe_get(post, SCORE_KEYS, None),
        "numReplies": int(safe_get(post, NUM_COMMENTS_KEYS, 0) or 0),
        "author": post.get("author") or post.get("username") or "",
        "title": title_val or "",
        "text": text_val or "",
        "is_self": post.get("is_self", bool(text_val)),
        "domain": post.get("domain") or "",
        "url": post.get("url") or "",
        "permalink": post.get("permalink") or f"/r/{post.get('subreddit','')}/comments/{post.get('id','')}",
        "upvote_ratio": post.get("upvote_ratio") or None,
        "date_created": created_iso
    }

def fetch_posts(subreddit=None, author=None, after_ts=None, before_ts=None, size=PAGE_SIZE, use_filter_link=True):
    """
    Query PullPush. If use_filter_link is True, include filter=link param (may or may not be supported).
    Returns raw list (unfiltered), or empty list on persistent failure.
    """
    url = "https://api.pullpush.io/reddit/search/submission/"
    params = {"size": size, "sort": "asc", "sort_type": "created_utc"}
    if use_filter_link:
        params["filter"] = "link"
    if subreddit:
        params["subreddit"] = subreddit
    if author:
        params["author"] = author
    if after_ts:
        params["after"] = after_ts
    if before_ts:
        params["before"] = before_ts

    retries = 0
    while retries <= MAX_RETRIES:
        try:
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            posts = data.get("data") or data.get("results") or data.get("posts") or []
            if isinstance(posts, dict):
                posts = posts.get("children") or posts.get("items") or []
            return posts
        except requests.exceptions.RequestException as e:
            retries += 1
            wait = 2 ** retries
            print(f"[fetch_posts] Request error for subreddit={subreddit}, author={author}: {e}. Retry {retries}/{MAX_RETRIES} after {wait}s...")
            time.sleep(wait)
    print(f"[fetch_posts] Giving up for subreddit={subreddit}, author={author} after {MAX_RETRIES} retries.")
    return []

# ========== MAIN ==========
def main():
    print("Starting improved_reddit_collector.py")
    print(f"Target unique authors: {TARGET_UNIQUE_AUTHORS}, sampling years {START_YEAR}-{END_YEAR}")
    print(f"Subreddits: {SUBREDDITS}")
    print("Will log progress and API response counts to help debug empty outputs.\n")

    collected_authors = set()
    total_collected = 0
    years = list(range(START_YEAR, END_YEAR + 1))
    attempts = 0

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=FIELDS)
        writer.writeheader()

        # 1) collect authors (randomized across years and subs)
        while len(collected_authors) < TARGET_UNIQUE_AUTHORS and attempts < MAX_ATTEMPTS_AUTHOR_COLLECTION:
            attempts += 1
            year = random.choice(years)
            after_ts = to_unix(year, 1, 1)
            before_ts = to_unix(year, 12, 31, 23, 59, 59)
            sub = random.choice(SUBREDDITS)

            if attempts % 100 == 0:
                print(f"[author collection] Attempt #{attempts}, collected so far: {len(collected_authors)}")

            # Try with the filter parameter first; if no results, try again without the filter
            posts = fetch_posts(subreddit=sub, after_ts=after_ts, before_ts=before_ts, size=PAGE_SIZE, use_filter_link=True)
            print(f"[author collection] year={year}, subreddit={sub}, raw returned={len(posts)} (filter=link)")
            if not posts:
                posts = fetch_posts(subreddit=sub, after_ts=after_ts, before_ts=before_ts, size=PAGE_SIZE, use_filter_link=False)
                print(f"[author collection] retry without filter: raw returned={len(posts)}")

            # If API returned nothing, go to next attempt
            if not posts:
                time.sleep(REQUEST_DELAY)
                continue

            # Show a small sample of the raw keys for inspecting shape (only on first few attempts)
            if attempts <= 3:
                print("[author collection] Sample post keys:", list(posts[0].keys()) if posts else "no posts")

            # Filter client-side to ensure we only take submissions (have a title)
            posts_with_title = [p for p in posts if safe_get(p, TITLE_KEYS)]
            print(f"[author collection] after title-filter: {len(posts_with_title)} posts")

            for post in posts_with_title:
                author = post.get("author")
                if not author or author in ("[deleted]", "AutoModerator"):
                    continue
                if author in collected_authors:
                    continue
                row = extract_post_info(post)
                writer.writerow(row)
                collected_authors.add(author)
                total_collected += 1
                if total_collected % 50 == 0:
                    print(f"[author collection] Total unique authors collected: {total_collected}")
                if total_collected >= TARGET_UNIQUE_AUTHORS:
                    break

            time.sleep(REQUEST_DELAY)

        if len(collected_authors) < TARGET_UNIQUE_AUTHORS:
            print(f"[author collection] Stopping after {attempts} attempts. Collected {len(collected_authors)} authors (target {TARGET_UNIQUE_AUTHORS}).")
        else:
            print(f"[author collection] Completed. Collected {len(collected_authors)} authors in {attempts} attempts.")

        # 2) collect 20-100 posts per collected author (any subreddit). If author list is empty, exit early.
        if not collected_authors:
            print("[main] No authors collected. Exiting to avoid creating empty dataset.")
            return

        print("\nStarting per-author post collection (20-100 posts each).")
        author_list = list(collected_authors)
        for idx, author in enumerate(author_list, 1):
            num_posts_target = random.randint(MIN_POSTS_PER_AUTHOR, MAX_POSTS_PER_AUTHOR)
            author_posts_collected = 0
            after_ts = to_unix(START_YEAR, 1, 1)
            before_ts = to_unix(END_YEAR, 12, 31, 23, 59, 59)
            author_attempts = 0

            # fetch pages until we get enough or exhaust attempts
            while author_posts_collected < num_posts_target and author_attempts < 200:
                author_attempts += 1
                posts = fetch_posts(author=author, after_ts=after_ts, before_ts=before_ts, size=PAGE_SIZE, use_filter_link=True)
                print(f"[author posts] author={author} attempt={author_attempts} raw_returned={len(posts)} (filter=link)")
                if not posts:
                    posts = fetch_posts(author=author, after_ts=after_ts, before_ts=before_ts, size=PAGE_SIZE, use_filter_link=False)
                    print(f"[author posts] retry no-filter raw_returned={len(posts)}")
                if not posts:
                    break

                # ensure submissions only (title present)
                posts = [p for p in posts if safe_get(p, TITLE_KEYS)]
                print(f"[author posts] after title-filter={len(posts)}")

                if not posts:
                    # advance the after_ts a bit randomly to explore other windows
                    after_ts += 60 * 60 * 24 * 7  # advance 7 days
                    time.sleep(REQUEST_DELAY)
                    continue

                # shuffle to sample randomly
                random.shuffle(posts)

                for post in posts:
                    if author_posts_collected >= num_posts_target:
                        break
                    row = extract_post_info(post)
                    writer.writerow(row)
                    author_posts_collected += 1

                # advance cursor to last post returned to page forward
                last_created = posts[-1].get("created_utc") or posts[-1].get("created") or None
                if last_created:
                    try:
                        after_ts = int(last_created) + 1
                    except Exception:
                        # if it's not numeric, break to avoid infinite loop
                        break

                time.sleep(REQUEST_DELAY)

            print(f"[{idx}/{len(author_list)}] Collected {author_posts_collected} posts for author {author}")

    print("\n=== finished script ===")
    print(f"Output CSV: {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
