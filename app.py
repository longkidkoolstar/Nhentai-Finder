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

# Flag to control database update process
update_running = False

# Gallery cache
gallery_cache = {}

# Session pool
session_pool = []
session_pool_lock = threading.Lock()

# Database connection pool
class DBConnectionPool:
    def __init__(self, max_size):
        self.max_size = max_size
        self.connections = []
        self.in_use = set()
        self.lock = threading.Lock()
        
    def get_connection(self):
        with self.lock:
            if len(self.connections) < self.max_size:
                conn = sqlite3.connect('nhentai_db.sqlite', timeout=30.0, check_same_thread=False)
                conn.row_factory = sqlite3.Row
                conn.execute('PRAGMA journal_mode=WAL')
                conn.execute('PRAGMA synchronous=NORMAL')
                conn.execute('PRAGMA cache_size=10000')
                conn.execute('PRAGMA temp_store=MEMORY')
                self.connections.append(conn)
                self.in_use.add(conn)
                return conn
            else:
                for conn in self.connections:
                    if conn not in self.in_use:
                        self.in_use.add(conn)
                        return conn
                # All connections in use, wait and retry
                time.sleep(0.1)
                return self.get_connection()
                
    def release_connection(self, conn):
        with self.lock:
            if conn in self.in_use:
                self.in_use.remove(conn)

# Create a global connection pool
db_pool = DBConnectionPool(max_size=30)

# Standard database connection function for backward compatibility
def get_db_connection():
    conn = sqlite3.connect('nhentai_db.sqlite', timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=NORMAL')
    conn.execute('PRAGMA cache_size=10000')
    conn.execute('PRAGMA temp_store=MEMORY')
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
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_galleries_title ON galleries(title)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_pages_gallery_id_page_number ON pages(gallery_id, page_number)')
    
    conn.commit()
    conn.close()

# Initialize database
init_db()

# Session management with connection pooling
def create_session_pool(pool_size=20):
    global session_pool
    with session_pool_lock:
        session_pool = []
        for _ in range(pool_size):
            session = requests.Session()
            retries = Retry(
                total=10,
                backoff_factor=0.5,  # Reduced backoff factor
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=['HEAD', 'GET', 'OPTIONS'],
                respect_retry_after_header=True
            )
            adapter = HTTPAdapter(max_retries=retries, pool_connections=50, pool_maxsize=50)
            session.mount('http://', adapter)
            session.mount('https://', adapter)
            session.headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Referer': 'https://nhentai.net/'
            }
            session_pool.append(session)
    return session_pool

# Get a session from the pool
def get_session():
    with session_pool_lock:
        if not session_pool:
            create_session_pool()
        return np.random.choice(session_pool)

# Initialize session pool
create_session_pool()

# Create a requests session with retries (legacy function for backward compatibility)
def create_session():
    session = requests.Session()
    retries = Retry(
        total=10,
        backoff_factor=0.5,
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

# Optimized function to scrape gallery pages
def scrape_gallery_pages_optimized(gallery_id, page_count):
    # Use a thread pool for parallel page scraping
    def process_page(page_num):
        try:
            # Get a session from the pool
            session = get_session()
            
            # Add small delay between requests with more randomization and lower average
            time.sleep(np.random.uniform(0.1, 0.8))
            
            # Get the page HTML
            page_html_url = f"https://nhentai.net/g/{gallery_id}/{page_num}/"
            page_response = session.get(page_html_url, timeout=10)
            
            if page_response.status_code != 200:
                logger.warning(f"Failed to load page {page_num} HTML for gallery {gallery_id}")
                return None
                
            # Parse the HTML to find the image URL - optimize with CSS selector
            page_soup = BeautifulSoup(page_response.text, 'html.parser')
            img_element = page_soup.select_one('#image-container img')
            
            if not img_element or 'src' not in img_element.attrs:
                logger.warning(f"Could not find image element for page {page_num} of gallery {gallery_id}")
                return None
                
            image_url = img_element['src']
            
            # Download the image and process it
            image_response = session.get(image_url, timeout=15)
            
            if image_response.status_code == 200:
                try:
                    # Optimize image handling
                    page_img = Image.open(io.BytesIO(image_response.content))
                    # Calculate hash more efficiently
                    page_phash = str(imagehash.phash(page_img, hash_size=8))
                    
                    return (gallery_id, page_num, image_url, page_phash)
                except Exception as img_err:
                    logger.error(f"Error processing image for page {page_num} of gallery {gallery_id}: {img_err}")
                    return None
            else:
                logger.warning(f"Failed to download image for page {page_num} of gallery {gallery_id}")
                return None
                
        except Exception as e:
            logger.error(f"Error processing page {page_num} of gallery {gallery_id}: {e}")
            return None
    
    # Use a thread pool to process pages in parallel - increased max_workers
    results = []
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(process_page, page_num): page_num for page_num in range(1, page_count + 1)}
        
        for future in as_completed(futures):
            result = future.result()
            if result:
                results.append(result)
    
    # Batch insert the results into the database
    if results:
        conn = db_pool.get_connection()
        cursor = conn.cursor()
        cursor.execute('BEGIN TRANSACTION')
        try:
            cursor.executemany(
                'INSERT OR REPLACE INTO pages (gallery_id, page_number, image_url, phash) VALUES (?, ?, ?, ?)',
                results
            )
            conn.commit()
            logger.info(f"Processed {len(results)} pages for gallery {gallery_id}")
        except Exception as e:
            conn.rollback()
            logger.error(f"Database error when inserting pages for gallery {gallery_id}: {e}")
        finally:
            db_pool.release_connection(conn)

# Legacy function for backward compatibility
def scrape_gallery_pages(gallery_id, page_count, session=None):
    return scrape_gallery_pages_optimized(gallery_id, page_count)

# Optimized function to scrape nhentai gallery
def scrape_gallery_optimized(gallery_id, include_pages=True):
    # Check cache first
    if gallery_id in gallery_cache:
        logger.info(f"Using cached data for gallery {gallery_id}")
        gallery_data = gallery_cache[gallery_id]
        
        # If we need pages but they weren't fetched last time, fetch them now
        if include_pages and gallery_data.get('page_count', 0) > 0 and not gallery_data.get('pages_fetched', False):
            threading.Thread(
                target=scrape_gallery_pages_optimized, 
                args=(gallery_id, gallery_data['page_count'])
            ).start()
            gallery_data['pages_fetched'] = True
            
        return gallery_data
    
    try:
        # Get a session from the pool
        session = get_session()
        
        # Add small delay to avoid rate limiting
        time.sleep(np.random.uniform(0.2, 1.0))
        
        # Request the gallery page
        url = f"https://nhentai.net/g/{gallery_id}/"
        response = session.get(url, timeout=10)
        
        if response.status_code != 200:
            logger.warning(f"Failed to load gallery {gallery_id}, status code: {response.status_code}")
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
            img_response = session.get(thumbnail_url, timeout=10)
            if img_response.status_code == 200:
                try:
                    # Try to open the image with PIL's built-in truncated image handling
                    img = Image.open(io.BytesIO(img_response.content))
                    # Force image loading to detect truncation issues early
                    img.load()
                    phash = str(imagehash.phash(img, hash_size=8))
                except Exception as img_err:
                    logger.error(f"Error processing thumbnail for gallery {gallery_id}: {img_err}")
        
        # Store gallery in database
        conn = db_pool.get_connection()
        try:
            conn.execute('''
            INSERT OR REPLACE INTO galleries (id, title, thumbnail_url, page_count, tags, phash, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
            ''', (gallery_id, title, thumbnail_url, page_count, tags_str, phash))
            conn.commit()
        except Exception as e:
            logger.error(f"Database error when inserting gallery {gallery_id}: {e}")
        finally:
            db_pool.release_connection(conn)
        
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
        
        # Now process pages in a separate thread if include_pages is True
        if page_count > 0 and include_pages:
            threading.Thread(
                target=scrape_gallery_pages_optimized, 
                args=(gallery_id, page_count)
            ).start()
            gallery_data['pages_fetched'] = True
        
        return gallery_data
    except Exception as e:
        logger.error(f"Error scraping gallery {gallery_id}: {e}")
        return None

# Legacy function for backward compatibility
def scrape_gallery(gallery_id, include_pages=True, session=None):
    return scrape_gallery_optimized(gallery_id, include_pages)

# Optimized update database function
def update_database_optimized(start_id=1, end_id=400000, concurrent_galleries=20, include_pages=True):
    global update_running
    update_running = True
    
    # Ensure session pool is initialized
    if len(session_pool) < concurrent_galleries:
        create_session_pool(pool_size=concurrent_galleries * 2)
    
    # Track progress
    total_galleries = end_id - start_id + 1
    processed = 0
    successful = 0
    
    # Progress tracking
    progress_lock = threading.Lock()
    
    # Use a semaphore to limit concurrent requests
    semaphore = threading.Semaphore(concurrent_galleries)
    
    def process_gallery(gallery_id):
        nonlocal processed, successful
        
        try:
            with semaphore:
                # Add randomized delay to avoid rate limiting
                time.sleep(np.random.uniform(0.2, 1.0))
                
                result = scrape_gallery_optimized(gallery_id, include_pages)
                
                with progress_lock:
                    processed += 1
                    if result:
                        successful += 1
                    
                    # Log progress
                    if processed % 20 == 0:
                        progress = (processed / total_galleries) * 100
                        logger.info(f"Progress: {processed}/{total_galleries} ({progress:.1f}%) galleries processed. Success rate: {successful}/{processed}")
                
                return result
                
        except Exception as e:
            with progress_lock:
                processed += 1
            logger.error(f"Error in gallery {gallery_id}: {e}")
            return None
    
    # Process galleries in parallel with dynamic rate limiting
    def process_range():
        with ThreadPoolExecutor(max_workers=concurrent_galleries) as executor:
            futures = []
            
            # Submit gallery IDs in batches to avoid overwhelming the executor
            batch_size = 100
            for batch_start in range(start_id, end_id + 1, batch_size):
                batch_end = min(batch_start + batch_size - 1, end_id)
                
                if not update_running:
                    logger.info("Database update stopped by user")
                    break
                
                # Submit jobs for this batch
                batch_futures = []
                for gid in range(batch_start, batch_end + 1):
                    batch_futures.append(executor.submit(process_gallery, gid))
                
                # Wait for this batch to complete
                for future in as_completed(batch_futures):
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"Unexpected error in gallery processing thread: {e}")
    
    process_range()
    logger.info(f"Database update complete. Processed {processed} galleries with {successful} successful.")

# Legacy function for backward compatibility
def update_database(start_id=1, end_id=400000, max_workers=8, include_pages=True):
    return update_database_optimized(start_id, end_id, max_workers, include_pages)

# Optimized find_by_image function
def find_by_image_optimized(image_file, limit=10, include_pages=True):
    start_time = time.time()
    
    # Open and hash the uploaded image more efficiently
    img = Image.open(image_file)
    query_hash = imagehash.phash(img, hash_size=8)
    
    # Use an optimized query approach with indexing
    conn = get_db_connection()
    conn.execute("PRAGMA cache_size=10000")  # Increase cache size
    
    # Prepare results
    results = []
    
    # First get potential thumbnail matches using a fast pre-filtering query
    cursor = conn.execute("""
    SELECT id, title, thumbnail_url, page_count, tags, phash 
    FROM galleries 
    WHERE phash IS NOT NULL
    """)
    
    # Process thumbnail matches
    for row in cursor:
        gallery_id, title, thumbnail_url, page_count, tags_str, phash_str = row
        
        if not phash_str:
            continue
            
        # Calculate hash difference
        try:
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
        except Exception as e:
            logger.error(f"Error comparing hash for gallery {gallery_id}: {e}")
    
    # Then check individual pages if requested
    if include_pages:
        # Use an optimized query with JOIN and pre-filtering
        cursor = conn.execute("""
        SELECT p.gallery_id, p.page_number, p.image_url, p.phash, 
               g.title, g.thumbnail_url, g.page_count, g.tags
        FROM pages p
        JOIN galleries g ON p.gallery_id = g.id
        WHERE p.phash IS NOT NULL
        """)
        
        # Process page matches
        for row in cursor:
            gallery_id, page_number, image_url, phash_str, title, thumbnail_url, page_count, tags_str = row
            
            if not phash_str:
                continue
                
            # Calculate hash difference
            try:
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
            except Exception as e:
                logger.error(f"Error comparing hash for page {page_number} of gallery {gallery_id}: {e}")
    
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
    
    end_time = time.time()
    logger.info(f"Image search completed in {end_time - start_time:.2f}s, found {len(unique_results)} results")
    
    return unique_results

# Legacy function for backward compatibility
def find_by_image(image_file, limit=10, include_pages=True):
    return find_by_image_optimized(image_file, limit, include_pages)

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
    limit = int(request.form.get('limit', 10))
    
    results = find_by_image_optimized(file, limit, include_pages)
    return jsonify(results)

@app.route('/api/update_db', methods=['POST'])
def update_db_endpoint():
    data = request.get_json()
    start_id = data.get('start_id', 1)
    end_id = data.get('end_id', 400000)  # Limit range for safety
    concurrent_galleries = data.get('concurrent_galleries', 20)
    include_pages = data.get('include_pages', True)
    
    # Start background update
    thread = threading.Thread(
        target=update_database_optimized, 
        args=(start_id, end_id, concurrent_galleries, include_pages)
    )
    thread.daemon = True
    thread.start()
    
    return jsonify({
        'message': f'Database update started for IDs {start_id} to {end_id}, '
                  f'with {concurrent_galleries} concurrent galleries, including pages: {include_pages}'
    })

@app.route('/api/stop_update', methods=['POST'])
def stop_update_endpoint():
    global update_running
    update_running = False
    return jsonify({'message': 'Database update stopping... It may take a few seconds to complete current batch.'})

@app.route('/api/optimize_db', methods=['POST'])
def optimize_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Run VACUUM to rebuild the database file
        cursor.execute('VACUUM')
        
        # Analyze tables for query optimization
        cursor.execute('ANALYZE')
        
        # Optimize WAL checkpoint
        cursor.execute('PRAGMA wal_checkpoint(FULL)')
        
        # Optimize cache
        cursor.execute('PRAGMA cache_size=10000')
        cursor.execute('PRAGMA temp_store=MEMORY')
        
        # Add missing indexes if any
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_galleries_title ON galleries(title)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_pages_gallery_id_page_number ON pages(gallery_id, page_number)')
        
        conn.commit()
        return jsonify({'message': 'Database optimization completed successfully'})
    except Exception as e:
        conn.rollback()
        return jsonify({'error': f'Database optimization failed: {str(e)}'}), 500
    finally:
        conn.close()

@app.route('/api/clear_cache', methods=['POST'])
def clear_cache():
    global gallery_cache
    gallery_cache = {}
    return jsonify({'message': 'Cache cleared successfully'})

@app.route('/api/refresh_sessions', methods=['POST'])
def refresh_sessions():
    # Recreate the session pool
    pool_size = request.json.get('pool_size', 30)
    create_session_pool(pool_size)
    return jsonify({'message': f'Session pool refreshed with {pool_size} sessions'})

@app.route('/api/bulk_fetch', methods=['POST'])
def bulk_fetch():
    data = request.get_json()
    gallery_ids = data.get('gallery_ids', [])
    include_pages = data.get('include_pages', True)
    
    if not gallery_ids:
        return jsonify({'error': 'No gallery IDs provided'}), 400
    
    # Limit the number of galleries to prevent abuse
    if len(gallery_ids) > 100:
        return jsonify({'error': 'Too many gallery IDs. Maximum is 100 at a time'}), 400
    
    # Start background fetch
    def fetch_galleries():
        results = []
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = {executor.submit(scrape_gallery_optimized, gid, include_pages): gid for gid in gallery_ids}
            
            for future in as_completed(futures):
                gid = futures[future]
                try:
                    result = future.result()
                    if result:
                        results.append(result)
                except Exception as e:
                    logger.error(f"Error fetching gallery {gid}: {e}")
        
        logger.info(f"Bulk fetch completed. Fetched {len(results)} out of {len(gallery_ids)} galleries.")
    
    thread = threading.Thread(target=fetch_galleries)
    thread.daemon = True
    thread.start()
    
    return jsonify({'message': f'Bulk fetch started for {len(gallery_ids)} galleries'})

if __name__ == '__main__':
    # Use a production WSGI server like gunicorn in production
    app.run(debug=True, threaded=True)