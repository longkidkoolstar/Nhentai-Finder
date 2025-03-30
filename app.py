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
    # Original galleries table
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
    # New table for individual pages
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS pages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        gallery_id INTEGER,
        page_number INTEGER,
        image_url TEXT,
        phash TEXT,
        FOREIGN KEY (gallery_id) REFERENCES galleries(id),
        UNIQUE(gallery_id, page_number)
    )
    ''')
    conn.commit()
    conn.close()

# Initialize database
init_db()
def scrape_gallery_pages(gallery_id, page_count, headers):
    conn = sqlite3.connect('nhentai_db.sqlite')
    cursor = conn.cursor()
    
    for page_num in range(1, page_count + 1):
        try:
            # Add random delay between page requests
            time.sleep(np.random.uniform(1.5, 3))
            
            # Get the page HTML first
            page_html_url = f"https://nhentai.net/g/{gallery_id}/{page_num}/"
            page_response = requests.get(page_html_url, headers=headers)
            
            if page_response.status_code != 200:
                print(f"Failed to load page {page_num} HTML for gallery {gallery_id}")
                continue
                
            # Parse the HTML to find the actual image URL
            page_soup = BeautifulSoup(page_response.text, 'html.parser')
            img_element = page_soup.select_one('#image-container img')
            
            if not img_element or 'src' not in img_element.attrs:
                print(f"Could not find image element for page {page_num} of gallery {gallery_id}")
                continue
                
            image_url = img_element['src']
            
            # Now download the image and process it
            # Use a different session with a longer timeout
            session = requests.Session()
            image_response = session.get(image_url, headers=headers, timeout=30)
            
            if image_response.status_code == 200:
                page_img = Image.open(io.BytesIO(image_response.content))
                page_phash = str(imagehash.phash(page_img))
                
                # Store page info in database
                cursor.execute('''
                INSERT OR REPLACE INTO pages (gallery_id, page_number, image_url, phash)
                VALUES (?, ?, ?, ?)
                ''', (gallery_id, page_num, image_url, page_phash))
                
                print(f"Processed page {page_num} of gallery {gallery_id}")
            else:
                print(f"Failed to download image for page {page_num} of gallery {gallery_id}")
                
        except Exception as e:
            print(f"Error processing page {page_num} of gallery {gallery_id}: {e}")
    
    conn.commit()
    conn.close()

# Function to scrape nhentai
def scrape_gallery(gallery_id, include_pages=True):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Referer': 'https://nhentai.net/'
        }
        
        # Add random delay to avoid rate limiting
        time.sleep(np.random.uniform(2, 4))
        
        # Request the gallery page
        url = f"https://nhentai.net/g/{gallery_id}/"
        response = requests.get(url, headers=headers)
        
        if response.status_code != 200:
            return None
            
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract gallery info as before
        title_element = soup.select_one('#info h1')
        title = title_element.text if title_element else f"Gallery {gallery_id}"
        
        thumbnail = soup.select_one('#cover img')
        thumbnail_url = thumbnail['data-src'] if thumbnail and 'data-src' in thumbnail.attrs else thumbnail['src'] if thumbnail else ""
        
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
        
        tag_elements = soup.select('#tags .tag')
        tags = [tag.select_one('.name').text for tag in tag_elements if tag.select_one('.name')]
        tags_str = ','.join(tags)
        
        # Process thumbnail
        phash = ""
        if thumbnail_url:
            img_response = requests.get(thumbnail_url, headers=headers)
            if img_response.status_code == 200:
                img = Image.open(io.BytesIO(img_response.content))
                phash = str(imagehash.phash(img))
        
        # Store gallery in database
        conn = sqlite3.connect('nhentai_db.sqlite')
        cursor = conn.cursor()
        cursor.execute('''
        INSERT OR REPLACE INTO galleries (id, title, thumbnail_url, page_count, tags, phash, last_updated)
        VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
        ''', (gallery_id, title, thumbnail_url, page_count, tags_str, phash))
        conn.commit()
        conn.close()
        
        # Now process pages in a separate function if include_pages is True
        if page_count > 0 and include_pages:
            scrape_gallery_pages(gallery_id, page_count, headers)
        
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
def update_database(start_id=1, end_id=400000, max_workers=8, include_pages=True):  # Added include_pages parameter
    def process_batch(batch):
        for gallery_id in batch:
            try:
                # Modified scrape_gallery call to include the include_pages parameter
                gallery = scrape_gallery(gallery_id, include_pages)
                print(f"Processed gallery {gallery_id}")
                # Add a delay between galleries to avoid rate limiting
                time.sleep(np.random.uniform(3, 5))
            except Exception as e:
                print(f"Error processing gallery {gallery_id}: {e}")
    
    # Split the work into batches
    all_ids = list(range(start_id, end_id + 1))
    batch_size = 50  # Smaller batch size
    batches = [all_ids[i:i + batch_size] for i in range(0, len(all_ids), batch_size)]
    
    # Process batches in sequence rather than in parallel to avoid too many connections
    for batch in batches:
        process_batch(batch)

# Function to find matches by image
def find_by_image(image_file, limit=10, include_pages=True):
    # Open and hash the uploaded image
    img = Image.open(image_file)
    query_hash = imagehash.phash(img)
    
    # Query the database for similar hashes
    conn = sqlite3.connect('nhentai_db.sqlite')
    cursor = conn.cursor()
    
    results = []
    
    # First check gallery thumbnails
    cursor.execute("SELECT id, title, thumbnail_url, page_count, tags, phash FROM galleries")
    
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
                'similarity': 100 - (diff * 100 / 64),  # Convert diff to percentage
                'matched_type': 'thumbnail'
            })
    
    # Then check individual pages if requested
    if include_pages:
        cursor.execute("""
        SELECT p.gallery_id, p.page_number, p.image_url, p.phash, 
               g.title, g.thumbnail_url, g.page_count, g.tags
        FROM pages p
        JOIN galleries g ON p.gallery_id = g.id
        """)
        
        for row in cursor.fetchall():
            gallery_id, page_number, image_url, phash_str, title, thumbnail_url, page_count, tags_str = row
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
                    'url': f"https://nhentai.net/g/{gallery_id}/{page_number}/",
                    'similarity': 100 - (diff * 100 / 64),  # Convert diff to percentage
                    'matched_type': 'page',
                    'page_number': page_number,
                    'image_url': image_url
                })
    
    conn.close()
    
    # Sort by similarity (highest first)
    results.sort(key=lambda x: x['similarity'], reverse=True)
    
    # Remove duplicates (keeping highest similarity match)
    unique_results = []
    seen_ids = set()
    
    for result in results:
        if result['id'] not in seen_ids:
            unique_results.append(result)
            seen_ids.add(result['id'])
        
        if len(unique_results) >= limit:
            break
    
    return unique_results

    @app.route('/api/status', methods=['GET'])
    def get_status():
        conn = sqlite3.connect('nhentai_db.sqlite')
        cursor = conn.cursor()
        
    # Get gallery count
    cursor.execute("SELECT COUNT(*) FROM galleries")
    gallery_count = cursor.fetchone()[0]
    
    # Get page count
    cursor.execute("SELECT COUNT(*) FROM pages")
    page_count = cursor.fetchone()[0]
    
    # Get recent updates
    cursor.execute("""
    SELECT id, title, last_updated 
    FROM galleries 
    ORDER BY last_updated DESC 
    LIMIT 10
    """)
    recent_updates = [
        {'id': row[0], 'title': row[1], 'timestamp': row[2]}
        for row in cursor.fetchall()
    ]
    
    conn.close()
    
    return jsonify({
        'gallery_count': gallery_count,
        'page_count': page_count,
        'recent_updates': recent_updates
    })

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
    
    include_pages = request.form.get('search_pages', 'true').lower() == 'true'
    results = find_by_image(file, include_pages=include_pages)
    return jsonify(results)

@app.route('/api/update_db', methods=['POST'])
def update_db_endpoint():
    data = request.get_json()
    start_id = data.get('start_id', 1)
    end_id = data.get('end_id', 5000)  # Limit range for safety
    include_pages = data.get('include_pages', True)
    
    # Start background update
    thread = threading.Thread(target=update_database, args=(start_id, end_id, 3, include_pages))
    thread.daemon = True
    thread.start()
    
    return jsonify({'message': f'Database update started for IDs {start_id} to {end_id}, including pages: {include_pages}'})

if __name__ == '__main__':
    app.run(debug=True)