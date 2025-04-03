from flask import Flask, request, jsonify, render_template
import aiohttp
import asyncio
from bs4 import BeautifulSoup
import imagehash
from PIL import Image
import io
import sqlite3
import os
import time
import threading
import numpy as np
import re
from functools import lru_cache

app = Flask(__name__)

# Database connection pool
def get_db_connection():
    conn = sqlite3.connect('nhentai_db.sqlite')
    conn.row_factory = sqlite3.Row
    return conn

# Database setup
def init_db():
    conn = get_db_connection()
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
    # Pages table
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
    # Add indexes for faster queries
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_gallery_id ON pages (gallery_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_phash ON galleries (phash)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_page_phash ON pages (phash)')
    
    conn.commit()
    conn.close()

# Initialize database
init_db()

# Rate limiter class
class RateLimiter:
    def __init__(self, requests_per_minute=25):  # Increased from 20
        self.request_times = []
        self.requests_per_minute = requests_per_minute
        self.lock = asyncio.Lock()
    
    async def wait_if_needed(self):
        async with self.lock:
            current_time = time.time()
            # Remove old requests from tracking
            self.request_times = [t for t in self.request_times if current_time - t < 45]  # Reduced from 60
            
            if len(self.request_times) >= self.requests_per_minute:
                # Calculate wait time to stay under limit
                oldest_request = min(self.request_times)
                wait_time = 30 - (current_time - oldest_request)  # Reduced from 60
                if wait_time > 0:
                    await asyncio.sleep(wait_time)
            
            # Add current request to tracking
            self.request_times.append(time.time())

# Create global rate limiter
rate_limiter = RateLimiter(requests_per_minute=15)

# Create aiohttp session for connection pooling
async def get_session():
    return aiohttp.ClientSession(
        headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Referer': 'https://nhentai.net/'
        },
        timeout=aiohttp.ClientTimeout(total=30)
    )

# LRU cache for page URLs to reduce database hits
@lru_cache(maxsize=1000)
def get_cached_page_url(gallery_id, page_number):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT image_url FROM pages WHERE gallery_id = ? AND page_number = ?", 
        (gallery_id, page_number)
    )
    result = cursor.fetchone()
    conn.close()
    return result['image_url'] if result else None

async def scrape_gallery_page(session, gallery_id, page_num):
    await rate_limiter.wait_if_needed()
    
    try:
        # Check if we already have this page cached
        cached_url = get_cached_page_url(gallery_id, page_num)
        if cached_url:
            # If we have the URL but not the hash, we can fetch just the image
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT phash FROM pages WHERE gallery_id = ? AND page_number = ?", 
                (gallery_id, page_num)
            )
            result = cursor.fetchone()
            conn.close()
            
            if result and result['phash']:
                print(f"Using cached data for page {page_num} of gallery {gallery_id}")
                return True
            
            image_url = cached_url
        else:
            # Get the page HTML first
            page_html_url = f"https://nhentai.net/g/{gallery_id}/{page_num}/"
            async with session.get(page_html_url) as page_response:
                if page_response.status != 200:
                    print(f"Failed to load page {page_num} HTML for gallery {gallery_id}")
                    return False
                    
                page_html = await page_response.text()
                
            # Parse the HTML to find the actual image URL
            page_soup = BeautifulSoup(page_html, 'html.parser')
            img_element = page_soup.select_one('#image-container img')
            
            if not img_element or 'src' not in img_element.attrs:
                print(f"Could not find image element for page {page_num} of gallery {gallery_id}")
                return False
                
            image_url = img_element['src']
        
        # Now download the image and process it
        async with session.get(image_url) as image_response:
            if image_response.status == 200:
                image_data = await image_response.read()
                for attempt in range(3):  # retry up to 3 times
                    try:
                        page_img = Image.open(io.BytesIO(image_data))
                        page_phash = str(imagehash.phash(page_img))
                        
                        # Store page info in database
                        conn = get_db_connection()
                        cursor = conn.cursor()
                        cursor.execute('''
                        INSERT OR REPLACE INTO pages (gallery_id, page_number, image_url, phash)
                        VALUES (?, ?, ?, ?)
                        ''', (gallery_id, page_num, image_url, page_phash))
                        conn.commit()
                        conn.close()
                        
                        print(f"Processed page {page_num} of gallery {gallery_id}")
                        return True
                    except IOError as e:
                        if "truncated" in str(e):
                            # Log the error and retry
                            print(f"Error processing page {page_num} of gallery {gallery_id} (attempt {attempt + 1}/3): {e}")
                            await asyncio.sleep(1)  # wait 1 second before retrying
                        else:
                            # Raise the exception
                            raise
                else:
                    # Log the error and continue processing
                    print(f"Error processing page {page_num} of gallery {gallery_id}: failed after 3 retries")
                    return False
            else:
                print(f"Failed to download image for page {page_num} of gallery {gallery_id}")
                return False

    except Exception as e:
            print(f"Error processing page {page_num} of gallery {gallery_id}: {e}")
            return False

async def scrape_gallery_pages(session, gallery_id, page_count):
    # Process pages in batches of 5
    batch_size = 5
    for i in range(0, page_count, batch_size):
        batch_end = min(i + batch_size, page_count + 1)
        tasks = []
        for page_num in range(i + 1, batch_end):
            tasks.append(scrape_gallery_page(session, gallery_id, page_num))
        
        # Wait for batch to complete
        await asyncio.gather(*tasks)

async def scrape_gallery(session, gallery_id, include_pages=True):
    try:
        await rate_limiter.wait_if_needed()
        
        # Check if we already have this gallery and it was updated recently
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT last_updated FROM galleries WHERE id = ?", 
            (gallery_id,)
        )
        result = cursor.fetchone()
        conn.close()
        
        if result:
            last_updated = result['last_updated']
            # If updated in the last 24 hours, skip
            if last_updated and (time.time() - time.mktime(time.strptime(last_updated, '%Y-%m-%d %H:%M:%S'))) < 86400:
                print(f"Skipping gallery {gallery_id} - recently updated")
                return None
        
        # Request the gallery page
        url = f"https://nhentai.net/g/{gallery_id}/"
        async with session.get(url) as response:
            if response.status != 200:
                return None
                
            html = await response.text()
            
        soup = BeautifulSoup(html, 'html.parser')
        
        # Extract gallery info
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
            async with session.get(thumbnail_url) as img_response:
                if img_response.status == 200:
                    img_data = await img_response.read()
                    img = Image.open(io.BytesIO(img_data))
                    phash = str(imagehash.phash(img))
        
        # Store gallery in database
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('''
        INSERT OR REPLACE INTO galleries (id, title, thumbnail_url, page_count, tags, phash, last_updated)
        VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
        ''', (gallery_id, title, thumbnail_url, page_count, tags_str, phash))
        conn.commit()
        conn.close()
        
        # Now process pages if include_pages is True
        if page_count > 0 and include_pages:
            await scrape_gallery_pages(session, gallery_id, page_count)
        
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

async def process_gallery_batch(batch, include_pages=True, retry_count=3, delay=1):
    async with await get_session() as session:
        for gallery_id in batch:
            for attempt in range(retry_count + 1):
                try:
                    await scrape_gallery(session, gallery_id, include_pages)
                    print(f"Processed gallery {gallery_id}")
                    break
                except Exception as e:
                    if "truncated" in str(e):
                        # Log the error and retry
                        print(f"Error processing gallery {gallery_id} (attempt {attempt + 1}/{retry_count + 1}): {e}")
                        await asyncio.sleep(delay)
                    else:
                        # Raise the exception
                        raise
            else:
                # Log the error and continue processing
                print(f"Error processing gallery {gallery_id}: failed after {retry_count} retries")

async def update_database_async(start_id=1, end_id=400000, include_pages=True):
    # Split the work into batches
    all_ids = list(range(start_id, end_id + 1))
    batch_size = 10  # Process 10 galleries at a time
    batches = [all_ids[i:i + batch_size] for i in range(0, len(all_ids), batch_size)]
    
    # Process batches in parallel, limited to 3 batches at a time
    semaphore = asyncio.Semaphore(3)
    
    async def process_with_semaphore(batch):
        async with semaphore:
            await process_gallery_batch(batch, include_pages)
    
    tasks = [process_with_semaphore(batch) for batch in batches]
    await asyncio.gather(*tasks)

# Function to find matches by image
def find_by_image(image_file, limit=10, include_pages=True):
    # Open and hash the uploaded image
    img = Image.open(image_file)
    query_hash = imagehash.phash(img)
    
    # Query the database for similar hashes
    conn = get_db_connection()
    cursor = conn.cursor()
    
    results = []
    
    # First check gallery thumbnails - use parameterized query
    cursor.execute("SELECT id, title, thumbnail_url, page_count, tags, phash FROM galleries WHERE phash IS NOT NULL")
    
    for row in cursor.fetchall():
        gallery_id, title, thumbnail_url, page_count, tags_str, phash_str = row
        
        # Calculate hash difference
        db_hash = imagehash.hex_to_hash(phash_str)
        diff = query_hash - db_hash
        
        if diff < 15:  # Threshold for similarity
            results.append({
                'id': gallery_id,
                'title': title,
                'thumbnail_url': thumbnail_url,
                'page_count': page_count,
                'tags': tags_str.split(',') if tags_str else [],
                'url': f"https://nhentai.net/g/{gallery_id}/",
                'similarity': 100 - (diff * 100 / 64),  # Convert diff to percentage
                'matched_type': 'thumbnail'
            })
    
    # Then check individual pages if requested - use more efficient query
    if include_pages:
        cursor.execute("""
        SELECT p.gallery_id, p.page_number, p.image_url, p.phash, 
               g.title, g.thumbnail_url, g.page_count, g.tags
        FROM pages p
        JOIN galleries g ON p.gallery_id = g.id
        WHERE p.phash IS NOT NULL
        """)
        
        for row in cursor.fetchall():
            gallery_id, page_number, image_url, phash_str, title, thumbnail_url, page_count, tags_str = row
            
            # Calculate hash difference
            db_hash = imagehash.hex_to_hash(phash_str)
            diff = query_hash - db_hash
            
            if diff < 15:  # Threshold for similarity
                results.append({
                    'id': gallery_id,
                    'title': title,
                    'thumbnail_url': thumbnail_url,
                    'page_count': page_count,
                    'tags': tags_str.split(',') if tags_str else [],
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
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get gallery count
    cursor.execute("SELECT COUNT(*) as count FROM galleries")
    gallery_count = cursor.fetchone()['count']
    
    # Get page count
    cursor.execute("SELECT COUNT(*) as count FROM pages")
    page_count = cursor.fetchone()['count']
    
    # Get recent updates
    cursor.execute("""
    SELECT id, title, last_updated 
    FROM galleries 
    ORDER BY last_updated DESC 
    LIMIT 10
    """)
    recent_updates = [
        {'id': row['id'], 'title': row['title'], 'timestamp': row['last_updated']}
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

def update_database_wrapper(start_id, end_id, include_pages):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(update_database_async(start_id, end_id, include_pages))
    loop.close()

@app.route('/api/update_db', methods=['POST'])
def update_db_endpoint():
    data = request.get_json()
    start_id = data.get('start_id', 1)
    end_id = data.get('end_id', 5000)  # Limit range for safety
    include_pages = data.get('include_pages', True)
    
    # Start background update in a separate thread
    thread = threading.Thread(target=update_database_wrapper, args=(start_id, end_id, include_pages))
    thread.daemon = True
    thread.start()
    
    return jsonify({'message': f'Database update started for IDs {start_id} to {end_id}, including pages: {include_pages}'})

if __name__ == '__main__':
    app.run(debug=True)