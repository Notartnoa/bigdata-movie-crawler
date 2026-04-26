import requests
import pymongo
import hashlib
import json
from bs4 import BeautifulSoup
from datetime import datetime, timezone

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["film_crawling"]
collection = db["movies"]

collection.create_index([("title", pymongo.TEXT)])
collection.create_index([("src", pymongo.ASCENDING)])
collection.create_index([("entities", pymongo.ASCENDING)])

URL = "https://www.imdb.com/chart/top/"


def clean_text(text):
    if not text:
        return ""
    return " ".join(text.split())


def scrape_imdb_movies():
    try:
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept-Language": "en-US,en;q=0.9"
        }

        response = requests.get(URL, headers=headers, timeout=15)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")

        # IMDb biasanya menyimpan data awal di script JSON-LD.
        script_tag = soup.find("script", type="application/ld+json")

        if not script_tag:
            print("Data JSON-LD IMDb tidak ditemukan. Struktur halaman mungkin berubah.")
            return

        data = json.loads(script_tag.string)

        movies = data.get("itemListElement", [])

        if not movies:
            print("Daftar film tidak ditemukan.")
            return

        inserted_count = 0

        for item in movies:
            movie = item.get("item", {})

            title = clean_text(movie.get("name", ""))
            movie_url = movie.get("url", "")
            description = clean_text(movie.get("description", ""))

            rank = item.get("position")

            if not title:
                continue

            raw_id = f"imdb_website|{title}|{rank}"
            hashed_id = hashlib.sha1(raw_id.encode()).hexdigest()

            current_time_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

            document = {
                "_id": hashed_id,
                "src": "imdb_website",
                "fmt": "html",
                "ts": current_time_utc,
                "title": title,
                "text": description,
                "entities": ["film", "imdb", "top_250"],
                "kv": {
                    "rank": rank,
                    "source_url": URL,
                    "movie_url": movie_url
                }
            }

            collection.update_one(
                {"_id": document["_id"]},
                {"$set": document},
                upsert=True
            )

            inserted_count += 1
            print(f"Disimpan dari IMDb: #{rank} {title}")

        print(f"\nSelesai! Berhasil menyimpan {inserted_count} data film dari IMDb.")

    except Exception as e:
        print(f"Terjadi kesalahan saat scraping IMDb: {e}")


if __name__ == "__main__":
    scrape_imdb_movies()