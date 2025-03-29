# app.py
from flask import Flask, request, jsonify, render_template
import requests
from bs4 import BeautifulSoup
import imagehash
from PIL import Image
import io
import sqlite3
import os
import time
import threading
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import re

app = Flask(__name__)

# Database setup
def init_db():
    conn = sqlite3.connect('nhentai_db.sqlite')
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS galleries (
        id INTEGER PRIMARY KEY,
        title TEXT,
        thumbnail_url TEXT,
        page_count INTEGER,
        tags TEXT,
        phash TEXT,
        last_updated TIMESTAMP
    )
    ''')
    conn.commit()
    conn.close()

# Initialize database
init_db()

# Function to scrape nhentai
def scrape_gallery(gallery_id):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # Add random delay to avoid rate limiting
        time.sleep(np.random.uniform(1, 3))
        
        # Request the gallery page
        url = f"https://nhentai.net/g/{gallery_id}/"
        response = requests.get(url, headers=headers)
        
        if response.status_code != 200:
            return None
            
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract title
        title_element = soup.select_one('#info h1')
        title = title_element.text if title_element else f"Gallery {gallery_id}"
        
        # Extract thumbnail
        thumbnail = soup.select_one('#cover img')
        thumbnail_url = thumbnail['data-src'] if thumbnail and 'data-src' in thumbnail.attrs else thumbnail['src'] if thumbnail else ""
        
        # Extract page count - Fixed to properly find and parse page count
        pages_element = soup.select_one('#info div.pages')
        if pages_element:
            page_count_match = re.search(r'(\d+)', pages_element.text)
            page_count = int(page_count_match.group(1)) if page_count_match else 0
        else:
            # Fallback method
            pages_info = soup.select('#info div')
            for div in pages_info:
                if 'pages' in div.text.lower():
                    page_count_match = re.search(r'(\d+)', div.text)
                    page_count = int(page_count_match.group(1)) if page_count_match else 0
                    break
            else:
                page_count = 0
        
        # Extract tags
        tag_elements = soup.select('#tags .tag')
        tags = [tag.select_one('.name').text for tag in tag_elements if tag.select_one('.name')]
        tags_str = ','.join(tags)
        
        # Download thumbnail for hashing
        if thumbnail_url:
            img_response = requests.get(thumbnail_url, headers=headers)
            if img_response.status_code == 200:
                img = Image.open(io.BytesIO(img_response.content))
                phash = str(imagehash.phash(img))
            else:
                phash = ""
        else:
            phash = ""
        
        # Store in database
        conn = sqlite3.connect('nhentai_db.sqlite')
        cursor = conn.cursor()
        cursor.execute('''
        INSERT OR REPLACE INTO galleries (id, title, thumbnail_url, page_count, tags, phash, last_updated)
        VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
        ''', (gallery_id, title, thumbnail_url, page_count, tags_str, phash))
        conn.commit()
        conn.close()
        
        return {
            'id': gallery_id,
            'title': title,
            'thumbnail_url': thumbnail_url,
            'page_count': page_count,
            'tags': tags,
            'phash': phash
        }
    except Exception as e:
        print(f"Error scraping gallery {gallery_id}: {e}")
        return None

# Function to update database
def update_database(start_id=1, end_id=400000, max_workers=5):
    def process_batch(batch):
        for gallery_id in batch:
            try:
                scrape_gallery(gallery_id)
                print(f"Processed gallery {gallery_id}")
            except Exception as e:
                print(f"Error processing gallery {gallery_id}: {e}")
    
    # Split the work into batches
    all_ids = list(range(start_id, end_id + 1))
    batch_size = 100
    batches = [all_ids[i:i + batch_size] for i in range(0, len(all_ids), batch_size)]
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for batch in batches:
            executor.submit(process_batch, batch)

# Function to find matches by image
def find_by_image(image_file, limit=10):
    # Open and hash the uploaded image
    img = Image.open(image_file)
    query_hash = imagehash.phash(img)
    
    # Query the database for similar hashes
    conn = sqlite3.connect('nhentai_db.sqlite')
    cursor = conn.cursor()
    cursor.execute("SELECT id, title, thumbnail_url, page_count, tags, phash FROM galleries")
    results = []
    
    for row in cursor.fetchall():
        gallery_id, title, thumbnail_url, page_count, tags_str, phash_str = row
        if not phash_str:
            continue
        
        # Calculate hash difference
        db_hash = imagehash.hex_to_hash(phash_str)
        diff = query_hash - db_hash
        
        if diff < 15:  # Threshold for similarity
            results.append({
                'id': gallery_id,
                'title': title,
                'thumbnail_url': thumbnail_url,
                'page_count': page_count,
                'tags': tags_str.split(','),
                'url': f"https://nhentai.net/g/{gallery_id}/",
                'similarity': 100 - (diff * 100 / 64)  # Convert diff to percentage
            })
    
    conn.close()
    
    # Sort by similarity (highest first)
    results.sort(key=lambda x: x['similarity'], reverse=True)
    return results[:limit]

# Routes
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/search', methods=['POST'])
def search():
    if 'image' not in request.files:
        return jsonify({'error': 'No image provided'}), 400
        
    file = request.files['image']
    if file.filename == '':
        return jsonify({'error': 'No image selected'}), 400
        
    results = find_by_image(file)
    return jsonify(results)

@app.route('/api/update_db', methods=['POST'])
def update_db_endpoint():
    data = request.get_json()
    start_id = data.get('start_id', 1)
    end_id = data.get('end_id', 5000)  # Limit range for safety
    
    # Start background update
    thread = threading.Thread(target=update_database, args=(start_id, end_id))
    thread.daemon = True
    thread.start()
    
    return jsonify({'message': f'Database update started for IDs {start_id} to {end_id}'})

if __name__ == '__main__':
    app.run(debug=True)