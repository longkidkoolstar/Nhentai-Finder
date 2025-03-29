# app.py
from flask import Flask, request, jsonify, render_template, current_app
import requests
from bs4 import BeautifulSoup
import imagehash
from PIL import Image, ImageFile
import io

# Configure PIL to handle truncated images
ImageFile.LOAD_TRUNCATED_IMAGES = True
import sqlite3
import os
import time
import threading
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
import functools
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database setup with connection pooling
def get_db_connection():
    conn = sqlite3.connect('nhentai_db.sqlite', timeout=30.0)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Enable WAL mode for better concurrency
    cursor.execute('PRAGMA journal_mode=WAL')
    
    # Add indexes for faster queries
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
    
    # Create indexes for faster lookups
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_pages_gallery_id ON pages(gallery_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_galleries_phash ON galleries(phash)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_pages_phash ON pages(phash)')
    
    conn.commit()
    conn.close()

# Create a requests session with retries
def create_session():
    session = requests.Session()
    retries = Retry(
        total=10,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=['HEAD', 'GET', 'OPTIONS'],
        respect_retry_after_header=True
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=25, pool_maxsize=25)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    session.headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Referer': 'https://nhentai.net/'
    }
    return session

# Initialize database
init_db()

# Cache for gallery data
gallery_cache = {}

# Optimized function to scrape gallery pages
def scrape_gallery_pages(gallery_id, page_count, session=None):
    if session is None:
        session = create_session()
    
    # Use a thread pool for parallel page scraping
    def process_page(page_num):
        try:
            # Add small delay between requests
            time.sleep(np.random.uniform(0.5, 1.5))
            
            # Get the page HTML
            page_html_url = f"https://nhentai.net/g/{gallery_id}/{page_num}/"
            page_response = session.get(page_html_url, timeout=15)
            
            if page_response.status_code != 200:
                logger.warning(f"Failed to load page {page_num} HTML for gallery {gallery_id}")
                return None
                
            # Parse the HTML to find the image URL
            page_soup = BeautifulSoup(page_response.text, 'html.parser')
            img_element = page_soup.select_one('#image-container img')
            
            if not img_element or 'src' not in img_element.attrs:
                logger.warning(f"Could not find image element for page {page_num} of gallery {gallery_id}")
                return None
                
            image_url = img_element['src']
            
            # Download the image and process it
            image_response = session.get(image_url, timeout=20)
            
            if image_response.status_code == 200:
                try:
                    # Try to open the image with PIL's built-in truncated image handling
                    page_img = Image.open(io.BytesIO(image_response.content))
                    # Force image loading to detect truncation issues early
                    page_img.load()
                    page_phash = str(imagehash.phash(page_img))
                    
                    return (gallery_id, page_num, image_url, page_phash)
                except OSError as img_err:
                    # Handle truncated image errors specifically
                    if "truncated" in str(img_err).lower():
                        logger.warning(f"Truncated image detected for page {page_num} of gallery {gallery_id}, attempting recovery")
                        try:
                            # Try to recover using a more permissive approach
                            from PIL import ImageFile
                            ImageFile.LOAD_TRUNCATED_IMAGES = True
                            page_img = Image.open(io.BytesIO(image_response.content))
                            page_img.load()
                            page_phash = str(imagehash.phash(page_img))
                            logger.info(f"Successfully recovered truncated image for page {page_num} of gallery {gallery_id}")
                            return (gallery_id, page_num, image_url, page_phash)
                        except Exception as recovery_err:
                            logger.error(f"Failed to recover truncated image for page {page_num} of gallery {gallery_id}: {recovery_err}")
                            return None
                    else:
                        # Re-raise other image errors
                        raise
            else:
                logger.warning(f"Failed to download image for page {page_num} of gallery {gallery_id}")
                return None
                
        except Exception as e:
            logger.error(f"Error processing page {page_num} of gallery {gallery_id}: {e}")
            return None
    
    # Use a thread pool to process pages in parallel
    results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(process_page, page_num): page_num for page_num in range(1, page_count + 1)}
        
        for future in as_completed(futures):
            result = future.result()
            if result:
                results.append(result)
    
    # Batch insert the results into the database
    if results:
        conn = get_db_connection()
        conn.executemany(
            'INSERT OR REPLACE INTO pages (gallery_id, page_number, image_url, phash) VALUES (?, ?, ?, ?)',
            results
        )
        conn.commit()
        conn.close()
        logger.info(f"Processed {len(results)} pages for gallery {gallery_id}")

# Function to scrape nhentai with caching
def scrape_gallery(gallery_id, include_pages=True, session=None):
    # Check cache first
    if gallery_id in gallery_cache:
        logger.info(f"Using cached data for gallery {gallery_id}")
        gallery_data = gallery_cache[gallery_id]
        
        # If we need pages but they weren't fetched last time, fetch them now
        if include_pages and gallery_data.get('page_count', 0) > 0 and not gallery_data.get('pages_fetched', False):
            scrape_gallery_pages(gallery_id, gallery_data['page_count'], session)
            gallery_data['pages_fetched'] = True
            
        return gallery_data
    
    try:
        if session is None:
            session = create_session()
        
        # Add small delay to avoid rate limiting
        time.sleep(np.random.uniform(1, 2))
        
        # Request the gallery page
        url = f"https://nhentai.net/g/{gallery_id}/"
        response = session.get(url, timeout=15)
        
        if response.status_code != 200:
            return None
            
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract gallery info
        title_element = soup.select_one('#info h1')
        title = title_element.text if title_element else f"Gallery {gallery_id}"
        
        thumbnail = soup.select_one('#cover img')
        thumbnail_url = thumbnail['data-src'] if thumbnail and 'data-src' in thumbnail.attrs else thumbnail['src'] if thumbnail else ""
        
        # Extract page count
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
            img_response = session.get(thumbnail_url, timeout=15)
            if img_response.status_code == 200:
                try:
                    # Try to open the image with PIL's built-in truncated image handling
                    img = Image.open(io.BytesIO(img_response.content))
                    # Force image loading to detect truncation issues early
                    img.load()
                    phash = str(imagehash.phash(img))
                except OSError as img_err:
                    # Handle truncated image errors specifically
                    if "truncated" in str(img_err).lower():
                        logger.warning(f"Truncated thumbnail image detected for gallery {gallery_id}, attempting recovery")
                        try:
                            # Try to recover using a more permissive approach
                            from PIL import ImageFile
                            ImageFile.LOAD_TRUNCATED_IMAGES = True
                            img = Image.open(io.BytesIO(img_response.content))
                            img.load()
                            phash = str(imagehash.phash(img))
                            logger.info(f"Successfully recovered truncated thumbnail image for gallery {gallery_id}")
                        except Exception as recovery_err:
                            logger.error(f"Failed to recover truncated thumbnail image for gallery {gallery_id}: {recovery_err}")
                    else:
                        # Log other image errors
                        logger.error(f"Error processing thumbnail for gallery {gallery_id}: {img_err}")
                        # Continue without a phash
        
        # Store gallery in database
        conn = get_db_connection()
        conn.execute('''
        INSERT OR REPLACE INTO galleries (id, title, thumbnail_url, page_count, tags, phash, last_updated)
        VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
        ''', (gallery_id, title, thumbnail_url, page_count, tags_str, phash))
        conn.commit()
        conn.close()
        
        # Create gallery data dictionary
        gallery_data = {
            'id': gallery_id,
            'title': title,
            'thumbnail_url': thumbnail_url,
            'page_count': page_count,
            'tags': tags,
            'phash': phash,
            'pages_fetched': False
        }
        
        # Cache the gallery data
        gallery_cache[gallery_id] = gallery_data
        
        # Now process pages in a separate function if include_pages is True
        if page_count > 0 and include_pages:
            scrape_gallery_pages(gallery_id, page_count, session)
            gallery_data['pages_fetched'] = True
        
        return gallery_data
    except Exception as e:
        logger.error(f"Error scraping gallery {gallery_id}: {e}")
        return None

# Flag to control database update process
update_running = False

# Improved function to update database
def update_database(start_id=1, end_id=400000, max_workers=8, include_pages=True):
    global update_running
    update_running = True
    # Create a session to be reused
    session = create_session()
    
    # Track progress
    total_galleries = end_id - start_id + 1
    processed = 0
    successful = 0
    
    def process_batch(batch_ids):

        nonlocal processed, successful
        results = []
        
        # Process galleries in parallel within each batch
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(scrape_gallery, gid, include_pages, session): gid for gid in batch_ids}
            
            for future in as_completed(futures):
                gid = futures[future]
                try:
                    result = future.result()
                    processed += 1
                    if result:
                        successful += 1
                        
                    # Log progress
                    if processed % 10 == 0:
                        progress = (processed / total_galleries) * 100
                        logger.info(f"Progress: {processed}/{total_galleries} ({progress:.1f}%) galleries processed. Success rate: {successful}/{processed}")
                        
                except Exception as e:
                    logger.error(f"Error in gallery {gid}: {e}")
    
    # Split the work into batches
    all_ids = list(range(start_id, end_id + 1))
    batch_size = 25  # Smaller batch size
    batches = [all_ids[i:i + batch_size] for i in range(0, len(all_ids), batch_size)]
    
    # Process batches with some parallelism but not too much
    with ThreadPoolExecutor(max_workers=3) as executor:
        for i in range(0, len(batches), 3):  # Process 3 batches at a time
            # Check if we should stop
            if not update_running:
                logger.info("Database update stopped by user")
                break
                
            batch_group = batches[i:i+3]
            futures = [executor.submit(process_batch, batch) for batch in batch_group]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Batch processing error: {e}")
            
            # Small delay between batch groups
            time.sleep(5)
    
    logger.info(f"Database update complete. Processed {processed} galleries with {successful} successful.")

# Improved function to find matches by image
def find_by_image(image_file, limit=10, include_pages=True):
    # Open and hash the uploaded image
    img = Image.open(image_file)
    query_hash = imagehash.phash(img)
    
    # Prepare an in-memory cache for results
    results = []
    
    # Use a single connection for all queries
    conn = get_db_connection()
    
    # First check gallery thumbnails with optimized query
    # Use a threshold in the SQL query itself to reduce the number of candidates
    cursor = conn.execute("""
    SELECT id, title, thumbnail_url, page_count, tags, phash 
    FROM galleries 
    WHERE phash IS NOT NULL
    """)
    
    for row in cursor:
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
    
    # Then check individual pages if requested
    if include_pages:
        cursor = conn.execute("""
        SELECT p.gallery_id, p.page_number, p.image_url, p.phash, 
               g.title, g.thumbnail_url, g.page_count, g.tags
        FROM pages p
        JOIN galleries g ON p.gallery_id = g.id
        WHERE p.phash IS NOT NULL
        """)
        
        for row in cursor:
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
    
    # Get gallery count
    gallery_count = conn.execute("SELECT COUNT(*) FROM galleries").fetchone()[0]
    
    # Get page count
    page_count = conn.execute("SELECT COUNT(*) FROM pages").fetchone()[0]
    
    # Get recent updates
    cursor = conn.execute("""
    SELECT id, title, last_updated 
    FROM galleries 
    ORDER BY last_updated DESC 
    LIMIT 10
    """)
    
    recent_updates = [
        {'id': row[0], 'title': row[1], 'timestamp': row[2]}
        for row in cursor
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
    thread = threading.Thread(target=update_database, args=(start_id, end_id, 5, include_pages))
    thread.daemon = True
    thread.start()
    
    return jsonify({'message': f'Database update started for IDs {start_id} to {end_id}, including pages: {include_pages}'})

@app.route('/api/stop_update', methods=['POST'])
def stop_update_endpoint():
    global update_running
    update_running = False
    return jsonify({'message': 'Database update stopping... It may take a few seconds to complete current batch.'})

if __name__ == '__main__':
    # Use a production WSGI server like gunicorn in production
    app.run(debug=True, threaded=True)
