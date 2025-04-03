from kafka import KafkaProducer
import asyncio
import websockets
import json
import requests
import os
from datetime import datetime
from dotenv import load_dotenv
from websockets.exceptions import ConnectionClosedError


# Load credentials from .env
load_dotenv()
BSKY_USERNAME = os.getenv("ATP_EMAIL")
BSKY_PASSWORD = os.getenv("ATP_PASSWORD")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = "bluesky_posts"

# Target media DIDs
TARGET_DIDS = {
    "did:plc:u6mkbcgviwlbhuwqirmhcgu3": "elpais.com",
    "did:plc:humoyyfleayy76szpd5nqmw5": "catalannews.com",
    "did:plc:ylspytpr7posthjximkqea4n": "acn.cat",
    "did:plc:reloczc52dp4zqwymggpepoz": "vilaweb.cat",
    "did:plc:pfyxqe5aivngt44dqojq6pyq": "igualadanews.bsky.social",
    "did:plc:7y5fmnz4ftxjobz6domosi56": "negg47.bsky.social"
}

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Get JWT token
def obtener_token():
    try:
        res = requests.post(
            "https://bsky.social/xrpc/com.atproto.server.createSession",
            json={"identifier": BSKY_USERNAME, "password": BSKY_PASSWORD}
        )
        res.raise_for_status()
        return res.json()["accessJwt"]
    except Exception as e:
        print(f"Login error: {e}")
        return None

# Get extra post details
def obtener_detalles_extra(uri, token):
    if not token:
        print("Token unavailable, cannot fetch post details.")
        return {}

    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://bsky.social/xrpc/app.bsky.feed.getPostThread?uri={uri}"

    try:
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        data = r.json()
        post = data.get("thread", {}).get("post", {})
        author = post.get("author", {})
        return {
            "likes": post.get("likeCount", 0),
            "reposts": post.get("repostCount", 0),
            "replies": post.get("replyCount", 0),
            "author_handle": author.get("handle"),
            "author_displayName": author.get("displayName"),
            "author_avatar": author.get("avatar"),
        }
    except requests.exceptions.HTTPError as e:
        if r.status_code == 401:
            print("Token expired or invalid. Retrying...")
            new_token = obtener_token()
            return obtener_detalles_extra(uri, new_token)
        print(f"Error fetching post details: {e}")
        return {}
    except Exception as e:
        print(f"Unexpected error fetching details: {e}")
        return {}

# WebSocket connection to Jetstream stream
async def listen_bluesky_jetstream():
    while True:
        token = obtener_token()
        if not token:
            print("Could not obtain token. Retrying in 15 seconds...")
            await asyncio.sleep(15)
            continue

        wanted_dids_query = "&".join([f"wantedDids={did}" for did in TARGET_DIDS])
        url = f"wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post&{wanted_dids_query}"

        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=60) as websocket:
                print(f"Connected to Jetstream. Listening for posts from:\n{TARGET_DIDS}\n")

                while True:
                    try:
                        message = await websocket.recv()
                        data = json.loads(message)
                        post_did = data.get("did")

                        if post_did in TARGET_DIDS and "commit" in data:
                            commit_data = data["commit"]
                            if commit_data.get("operation") != "create":
                                continue
                            if commit_data.get("collection") != "app.bsky.feed.post":
                                continue

                            post_record = commit_data.get("record", {})
                            post_uri = f"at://{post_did}/app.bsky.feed.post/{commit_data.get('rkey')}"
                            timestamp = post_record.get("createdAt", datetime.utcnow().isoformat())
                            text = post_record.get("text", "[No Content]")
                            source = TARGET_DIDS[post_did]

                            # Get extended details (author, metrics)
                            extra = obtener_detalles_extra(post_uri, token)

                            # Get images
                            media_urls = []
                            embed = post_record.get("embed")
                            if embed:
                                embed_type = embed.get("$type")
                                if embed_type == "app.bsky.embed.images":
                                    media_urls = [img.get("fullsize") for img in embed.get("images", [])]
                                elif embed_type == "app.bsky.embed.recordWithMedia":
                                    media = embed.get("media", {})
                                    if media.get("$type") == "app.bsky.embed.images":
                                        media_urls = [img.get("fullsize") for img in media.get("images", [])]
                                elif embed_type == "app.bsky.embed.external":
                                    media_urls.append(embed.get("external", {}).get("thumb"))

                            # Create JSON for Kafka
                            post_json = {
                                "timestamp": timestamp,
                                "text": text,
                                "uri": post_uri,
                                "source": source,
                                "media_urls": media_urls,
                                "author": {
                                    "handle": extra.get("author_handle"),
                                    "displayName": extra.get("author_displayName"),
                                    "avatar": extra.get("author_avatar")
                                },
                                "metrics": {
                                    "likes": extra.get("likes"),
                                    "reposts": extra.get("reposts"),
                                    "replies": extra.get("replies")
                                }
                            }

                            print(f"Sending post from {source} to Kafka ({TOPIC})")
                            producer.send(TOPIC, post_json)

                    except json.JSONDecodeError:
                        print("JSON decoding error")
                    except Exception as e:
                        print(f"Unexpected error processing message: {e}")

        except ConnectionClosedError as e:
            print(f"WebSocket closed (1011): {e}")
        except Exception as e:
            print(f"Error connecting WebSocket: {e}")

        print("Retrying connection in 10 seconds...\n")
        await asyncio.sleep(10)


async def main():
    while True:
        try:
            await listen_bluesky_jetstream()
        except ConnectionClosedError as e:
            print(f"WebSocket unexpectedly closed: {e}")
            print("Reconnecting in 10 seconds...\n")
            await asyncio.sleep(10)
        except Exception as e:
            print(f"Unexpected error: {e}")
            print("Retrying in 10 seconds...\n")
            await asyncio.sleep(10)


# Run
asyncio.run(main())
