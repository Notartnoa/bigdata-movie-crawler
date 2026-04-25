import requests
import pymongo
import hashlib
from datetime import datetime, timezone

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["film_crawling"]
collection = db["movies"]

collection.create_index([("title", pymongo.TEXT)])
collection.create_index([("entities", pymongo.ASCENDING)])
print("Berhasil membuat index pencarian judul dan genre.")

API_KEY = "d5b9d125b1f4fc60062509602f76f477"
URL = f"https://api.themoviedb.org/3/movie/popular?api_key={API_KEY}&language=id-ID&page=1"

def fetch_and_store_movies():
    try:
        response = requests.get(URL)
        data = response.json()
        
        if "results" not in data:
            print("Gagal mengambil data dari API.")
            return

        movies = data["results"]
        inserted_count = 0

        for movie in movies:
            raw_id = f"tmdb_api|{movie['id']}"
            hashed_id = hashlib.sha1(raw_id.encode()).hexdigest()            
            current_time_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            genre_list = [f"Genre_{gid}" for gid in movie.get("genre_ids", [])]

            document = {
                "_id": hashed_id, 
                "src": "tmdb_api", 
                "fmt": "json", 
                "ts": current_time_utc, 
                "title": movie.get("title"), 
                "text": movie.get("overview"), 
                "entities": genre_list, 
                "kv": { 
                    "popularity": movie.get("popularity"),
                    "vote_average": movie.get("vote_average"),
                    "vote_count": movie.get("vote_count"),
                    "release_date": movie.get("release_date")
                }
            }

            collection.update_one({"_id": document["_id"]}, {"$set": document}, upsert=True)
            inserted_count += 1
            print(f"Disimpan: {document['title']}")

        print(f"\nSelesai! Berhasil menyimpan {inserted_count} film ke database.")

    except Exception as e:
        print(f"Terjadi kesalahan: {e}")

if __name__ == "__main__":
    fetch_and_store_movies()