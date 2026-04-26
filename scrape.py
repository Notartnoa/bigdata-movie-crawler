import requests
import pymongo
import hashlib
from bs4 import BeautifulSoup
from datetime import datetime, timezone

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["film_crawling"]
collection = db["movies"]

collection.create_index([("title", pymongo.TEXT)])
collection.create_index([("src", pymongo.ASCENDING)])
collection.create_index([("entities", pymongo.ASCENDING)])

URL = "https://www.boxofficemojo.com/chart/ww_top_lifetime_gross/"


def clean_text(text):
    if not text:
        return ""
    return " ".join(text.split())


def scrape_and_store_boxoffice_movies():
    try:
        headers = {
            "User-Agent": "Mozilla/5.0"
        }

        response = requests.get(URL, headers=headers, timeout=15)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        table = soup.find("table")

        if table is None:
            print("Tabel tidak ditemukan. Cek kembali struktur halaman Box Office Mojo.")
            return

        rows = table.find_all("tr")
        inserted_count = 0

        for row in rows[1:]:
            columns = row.find_all("td")

            if len(columns) < 7:
                continue

            rank = clean_text(columns[0].get_text())
            title = clean_text(columns[1].get_text())
            worldwide_gross = clean_text(columns[2].get_text())
            domestic_gross = clean_text(columns[3].get_text())
            domestic_percent = clean_text(columns[4].get_text())
            foreign_gross = clean_text(columns[5].get_text())
            foreign_percent = clean_text(columns[6].get_text())
            year = clean_text(columns[7].get_text()) if len(columns) > 7 else ""

            link_tag = columns[1].find("a")
            movie_url = ""

            if link_tag and link_tag.get("href"):
                movie_url = "https://www.boxofficemojo.com" + link_tag.get("href")

            if not title:
                continue

            raw_id = f"boxofficemojo_website|{rank}|{title}|{year}"
            hashed_id = hashlib.sha1(raw_id.encode()).hexdigest()
            current_time_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

            document = {
                "_id": hashed_id,
                "src": "boxofficemojo_website",
                "fmt": "html",
                "ts": current_time_utc,
                "title": title,
                "text": f"Film {title} berada pada peringkat {rank} dalam daftar Top Lifetime Grosses Worldwide di Box Office Mojo.",
                "entities": ["film", "box_office", "website"],
                "kv": {
                    "rank": rank,
                    "worldwide_lifetime_gross": worldwide_gross,
                    "domestic_lifetime_gross": domestic_gross,
                    "domestic_percent": domestic_percent,
                    "foreign_lifetime_gross": foreign_gross,
                    "foreign_percent": foreign_percent,
                    "year": year,
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
            print(f"Disimpan dari website: {rank}. {title} ({year})")

        print(f"\nSelesai! Berhasil menyimpan {inserted_count} data film dari Box Office Mojo.")

    except Exception as e:
        print(f"Terjadi kesalahan saat scraping Box Office Mojo: {e}")


if __name__ == "__main__":
    scrape_and_store_boxoffice_movies()