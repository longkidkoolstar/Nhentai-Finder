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
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
import re
import functools
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import multiprocessing
import queue
import random
import cachetools
import ujson as json  # Faster JSON processing
from lru import LRU  # More efficient LRU cache

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Flag to control database update process
update_running = False

# Gallery cache - upgraded to LRU cache with 10,000 item capacity
gallery_cache = LRU(10000)

# Session pool
session_pool = []
session_pool_lock = threading.Lock()

# CPU count for optimal thread allocation
CPU_COUNT = multiprocessing.cpu_count()
MAX_WORKERS = CPU_COUNT * 4  # Optimal thread count for I/O bound operations

# Database connection pool
class DBConnectionPool:
    def __init__(self, max_size):
        self.max_size = max_size
        self.connections = []
        self.in_use = set()
        self.lock = threading.Lock()
        self.connection_queue = queue.Queue()
        
    def get_connection(self):
        with self.lock:
            if len(self.connections) < self.max_size:
                conn = sqlite3.connect('nhentai_db.sqlite', timeout=30.0, check_same_thread=False)
                conn.row_factory = sqlite3.Row
                conn.execute('PRAGMA journal_mode=WAL')
                conn.execute('PRAGMA synchronous=NORMAL')
                conn.execute('PRAGMA cache_size=50000')  # Increased cache size
                conn.execute('PRAGMA temp_store=MEMORY')
                conn.execute('PRAGMA mmap_size=30000000000')  # Memory-mapped I/O for faster access
                self.connections.append(conn)
                self.in_use.add(conn)
                return conn
            else:
                for conn in self.connections:
                    if conn not in self.in_use:
                        self.in_use.add(conn)
                        return conn
                # All connections in use, wait and retry with exponential backoff
                try:
                    return self.connection_queue.get(timeout=0.1)
                except queue.Empty:
                    time.sleep(0.05)
                    return self.get_connection()
                
    def release_connection(self, conn):
        with self.lock:
            if conn in self.in_use:
                self.in_use.remove(conn)
                try:
                    self.connection_queue.put(conn, block=False)
                except queue.Full:
                    pass  # Queue is full, just release the connection

# Create a global connection pool with more connections
db_pool = DBConnectionPool(max_size=CPU_COUNT * 8)

# Standard database connection function for backward compatibility
def get_db_connection():
    conn = sqlite3.connect('nhentai_db.sqlite', timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=NORMAL')
    conn.execute('PRAGMA cache_size=50000')  # Increased cache size
    conn.execute('PRAGMA temp_store=MEMORY')
    conn.execute('PRAGMA mmap_size=30000000000')  # Memory-mapped I/O for faster access
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
def create_session_pool(pool_size=MAX_WORKERS):
    global session_pool
    with session_pool_lock:
        session_pool = []
        
        # List of different user agents to rotate
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Safari/605.1.15',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36 Edg/94.0.992.47',
            'Mozilla/5.0 (iPhone; CPU iPhone OS 14_7_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Mobile/15E148 Safari/604.1',
            'Mozilla/5.0 (iPad; CPU OS 14_7_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Mobile/15E148 Safari/604.1',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:92.0) Gecko/20100101 Firefox/92.0',
            # Added more user agents for better rotation
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:95.0) Gecko/20100101 Firefox/95.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:95.0) Gecko/20100101 Firefox/95.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36 Edg/96.0.1054.62'
        ]
        
        for i in range(pool_size):
            session = requests.Session()
            retries = Retry(
                total=20,  # Increased total retries
                backoff_factor=2.0,  # More aggressive exponential backoff
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=['HEAD', 'GET', 'OPTIONS'],
                respect_retry_after_header=True
            )
            adapter = HTTPAdapter(
                max_retries=retries, 
                pool_connections=50,  # Increased connection pool
                pool_maxsize=50,      # Increased max size
                pool_block=False      # Non-blocking pool
            )
            session.mount('http://', adapter)
            session.mount('https://', adapter)
            
            # Rotate user agents
            user_agent = user_agents[i % len(user_agents)]
            session.headers = {
                'User-Agent': user_agent,
                'Referer': 'https://nhentai.net/',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Cache-Control': 'max-age=0',
                'DNT': '1'  # Do Not Track header
            }
            session_pool.append(session)
    return session_pool

# Get a session from the pool with intelligent selection
def get_session():
    with session_pool_lock:
        if not session_pool or len(session_pool) < 5:
            create_session_pool()
        # Use weighted random selection to distribute load
        weights = np.ones(len(session_pool))
        return np.random.choice(session_pool, p=weights/weights.sum())

# Initialize session pool
create_session_pool()

# Create a requests session with retries (legacy function for backward compatibility)
def create_session():
    session = requests.Session()
    retries = Retry(
        total=20,
        backoff_factor=2.0,
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
    return session

# Optimized function to scrape gallery pages with adaptive rate limiting
def scrape_gallery_pages_optimized(gallery_id, page_count):
    # Use a thread pool for parallel page scraping
    def process_page(page_num):
        try:
            # Get a session from the pool
            session = get_session()
            
            # Adaptive delay based on page number to distribute load
            delay = np.random.uniform(0.05, 0.5) * (1 + (page_num % 3) * 0.1)
            time.sleep(delay)
            
            # Get the page HTML
            page_html_url = f"https://nhentai.net/g/{gallery_id}/{page_num}/"
            page_response = session.get(page_html_url, timeout=15)
            
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
            image_response = session.get(image_url, timeout=20)
            
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
    
    # Use a thread pool to process pages in parallel - dynamic worker count
    results = []
    max_workers = min(MAX_WORKERS, page_count)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Process pages in batches to avoid overwhelming the server
        batch_size = 10
        for batch_start in range(1, page_count + 1, batch_size):
            batch_end = min(batch_start + batch_size - 1, page_count)
            
            # Submit batch of pages
            futures = {executor.submit(process_page, page_num): page_num 
                      for page_num in range(batch_start, batch_end + 1)}
            
            # Process completed futures
            for future in as_completed(futures):
                result = future.result()
                if result:
                    results.append(result)
            
            # Small delay between batches to avoid rate limiting
            if batch_end < page_count:
                time.sleep(np.random.uniform(0.5, 1.5))
    
    # Batch insert the results into the database
    if results:
        conn = db_pool.get_connection()
        cursor = conn.cursor()
        cursor.execute('BEGIN TRANSACTION')
        try:
            # Use a larger batch size for better performance
            batch_size = 100
            for i in range(0, len(results), batch_size):
                batch = results[i:i+batch_size]
                cursor.executemany(
                    'INSERT OR REPLACE INTO pages (gallery_id, page_number, image_url, phash) VALUES (?, ?, ?, ?)',
                    batch
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

# Optimized function to scrape nhentai gallery with caching
def scrape_gallery_optimized(gallery_id, include_pages=True):
    # Check cache first
    if gallery_id in gallery_cache:
        logger.info(f"Using cached data for gallery {gallery_id}")
        gallery_data = gallery_cache[gallery_id]
        
        # If we need pages but they weren't fetched last time, fetch them now
        if include_pages and gallery_data.get('page_count', 0) > 0 and not gallery_data.get('pages_fetched', False):
            threading.Thread(
                target=scrape_gallery_pages_optimized, 
                args=(gallery_id, gallery_data['page_count']),
                daemon=True
            ).start()
            gallery_data['pages_fetched'] = True
            
        return gallery_data
    
    try:
        # Get a session from the pool
        session = get_session()
        
        # Add small delay to avoid rate limiting
        time.sleep(np.random.uniform(0.1, 0.5))
        
        # Request the gallery page
        url = f"https://nhentai.net/g/{gallery_id}/"
        response = session.get(url, timeout=15)
        
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
            img_response = session.get(thumbnail_url, timeout=15)
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
                args=(gallery_id, page_count),
                daemon=True
            ).start()
            gallery_data['pages_fetched'] = True
        
        return gallery_data
    except Exception as e:
        logger.error(f"Error scraping gallery {gallery_id}: {e}")
        return None

# Legacy function for backward compatibility
def scrape_gallery(gallery_id, include_pages=True, session=None):
    return scrape_gallery_optimized(gallery_id, include_pages)

# Optimized update database function with work queue and adaptive rate limiting
def update_database_optimized(start_id=1, end_id=400000, concurrent_galleries=MAX_WORKERS, include_pages=True):
    global update_running
    update_running = True
    
    # Ensure session pool is initialized with more connections
    optimal_pool_size = concurrent_galleries * 3  # Increased pool size for better throughput
    if len(session_pool) < optimal_pool_size:
        create_session_pool(pool_size=optimal_pool_size)
    
    # Track progress
    total_galleries = end_id - start_id + 1
    processed = 0
    successful = 0
    
    # Progress tracking with atomic operations
    progress_lock = threading.Lock()
    
    # Create a larger work queue for better load distribution
    work_queue = queue.Queue(maxsize=concurrent_galleries * 10)  # Increased queue size
    
    # Advanced adaptive rate limiting with multiple tiers
    rate_limit_data = {
        'windows': {
            'short': [],  # 5-second window
            'medium': [], # 30-second window
            'long': []    # 60-second window
        },
        'limits': {
            'short': 50,   # Max requests per 5 seconds
            'medium': 200, # Max requests per 30 seconds
            'long': 300    # Max requests per 60 seconds
        },
        'timeframes': {
            'short': 5,
            'medium': 30,
            'long': 60
        }
    }
    rate_limit_lock = threading.Lock()
    
    # Dynamic backoff parameters
    backoff_data = {
        'consecutive_failures': 0,
        'backoff_multiplier': 1.0,
        'max_backoff': 10.0,
        'success_streak': 0
    }
    backoff_lock = threading.Lock()
    
    def advanced_rate_limiter():
        with rate_limit_lock:
            current_time = time.time()
            delay = 0
            
            # Update all time windows
            for window_type in rate_limit_data['windows']:
                timeframe = rate_limit_data['timeframes'][window_type]
                window = rate_limit_data['windows'][window_type]
                
                # Remove timestamps older than the timeframe
                while window and window[0] < current_time - timeframe:
                    window.pop(0)
                
                # Calculate how close we are to the limit (as a percentage)
                limit = rate_limit_data['limits'][window_type]
                usage_percent = len(window) / limit
                
                # Apply progressive delay based on usage percentage
                if usage_percent > 0.8:
                    tier_delay = np.random.uniform(0.5, 2.0) * (usage_percent - 0.8) * 10
                    delay = max(delay, tier_delay)
            
            # Apply dynamic backoff for consecutive failures
            with backoff_lock:
                if backoff_data['consecutive_failures'] > 0:
                    failure_delay = min(
                        backoff_data['consecutive_failures'] * backoff_data['backoff_multiplier'],
                        backoff_data['max_backoff']
                    )
                    delay = max(delay, failure_delay)
                
                # Reduce backoff if we have a success streak
                if backoff_data['success_streak'] > 10 and backoff_data['backoff_multiplier'] > 1.0:
                    backoff_data['backoff_multiplier'] = max(1.0, backoff_data['backoff_multiplier'] * 0.9)
                    backoff_data['success_streak'] = 0
            
            # Apply calculated delay
            if delay > 0:
                time.sleep(delay)
            
            # Add current timestamp to all windows
            for window_type in rate_limit_data['windows']:
                rate_limit_data['windows'][window_type].append(current_time)
    
    def process_gallery(gallery_id):
        nonlocal processed, successful
        
        try:
            # Apply advanced rate limiting
            advanced_rate_limiter()
            
            result = scrape_gallery_optimized(gallery_id, include_pages)
            
            with progress_lock:
                processed += 1
                if result:
                    successful += 1
                    # Update success streak for dynamic backoff
                    with backoff_lock:
                        backoff_data['success_streak'] += 1
                        backoff_data['consecutive_failures'] = 0
                
                # Log progress with more detailed information
                if processed % 20 == 0:
                    progress = (processed / total_galleries) * 100
                    current_rate = len(rate_limit_data['windows']['medium']) / 30  # requests per second
                    logger.info(
                        f"Progress: {processed}/{total_galleries} ({progress:.1f}%) galleries processed. "
                        f"Success rate: {successful}/{processed} ({(successful/processed*100):.1f}%). "
                        f"Current rate: {current_rate:.2f} req/s"
                    )
            
            return result
                
        except Exception as e:
            with progress_lock:
                processed += 1
            
            # Update failure count for dynamic backoff
            with backoff_lock:
                backoff_data['consecutive_failures'] += 1
                backoff_data['success_streak'] = 0
                # Increase backoff multiplier on consecutive failures
                if backoff_data['consecutive_failures'] > 5:
                    backoff_data['backoff_multiplier'] = min(
                        backoff_data['backoff_multiplier'] * 1.5,
                        backoff_data['max_backoff']
                    )
            
            logger.error(f"Error in gallery {gallery_id}: {e}")
            return None
    
    # Worker function to process galleries from the queue with adaptive behavior
    def worker():
        while update_running:
            try:
                gallery_id = work_queue.get(timeout=1)
                process_gallery(gallery_id)
                work_queue.task_done()
                
                # Adaptive sleep based on queue size to prevent overwhelming
                queue_size = work_queue.qsize()
                queue_capacity = work_queue.maxsize
                if queue_size < queue_capacity * 0.2:
                    # Queue is getting empty, no need to slow down
                    pass
                elif queue_size > queue_capacity * 0.8:
                    # Queue is very full, slow down a bit
                    time.sleep(0.1)
                
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Worker error: {e}")
    
    # Dynamic worker scaling based on system load
    def monitor_and_scale_workers(initial_workers, min_workers, max_workers):
        workers = []
        current_worker_count = initial_workers
        
        # Start initial workers
        for _ in range(initial_workers):
            t = threading.Thread(target=worker, daemon=True)
            t.start()
            workers.append(t)
        
        # Monitor and scale
        while update_running and not work_queue.empty():
            try:
                # Check system load
                system_load = 0.5  # Default value
                try:
                    import psutil
                    system_load = psutil.cpu_percent(interval=1) / 100.0
                except ImportError:
                    pass
                
                # Check rate limit status
                with rate_limit_lock:
                    rate_usage = len(rate_limit_data['windows']['medium']) / rate_limit_data['limits']['medium']
                
                # Calculate target worker count based on load and rate limit
                if system_load > 0.9 or rate_usage > 0.9:
                    # System is overloaded or close to rate limit, reduce workers
                    target_workers = max(min_workers, int(current_worker_count * 0.8))
                elif system_load < 0.7 and rate_usage < 0.7:
                    # System has capacity and we're not close to rate limit, add workers
                    target_workers = min(max_workers, int(current_worker_count * 1.2) + 1)
                else:
                    # Maintain current count
                    target_workers = current_worker_count
                
                # Adjust worker count if needed
                if target_workers > current_worker_count:
                    # Add workers
                    for _ in range(target_workers - current_worker_count):
                        t = threading.Thread(target=worker, daemon=True)
                        t.start()
                        workers.append(t)
                    logger.info(f"Scaled up workers from {current_worker_count} to {target_workers}")
                    current_worker_count = target_workers
                
                # We don't actively remove workers, just let them complete naturally
                
                # Sleep before next check
                time.sleep(5)
                
            except Exception as e:
                logger.error(f"Scaling error: {e}")
                time.sleep(5)
        
        return workers
    
    # Start the worker scaling monitor in a separate thread
    min_workers = max(4, CPU_COUNT)
    max_workers = max(concurrent_galleries, CPU_COUNT * 8)  # Allow more workers for better throughput
    initial_workers = min(concurrent_galleries, (min_workers + max_workers) // 2)
    
    scaling_thread = threading.Thread(
        target=monitor_and_scale_workers,
        args=(initial_workers, min_workers, max_workers),
        daemon=True
    )
    scaling_thread.start()
    
    # Producer function to fill the work queue with smart batching
    try:
        # Prioritize certain ID ranges that are more likely to exist
        # This is based on the observation that certain ID ranges have higher success rates
        
        # Define priority tiers with different batch sizes
        priority_ranges = [
            # High priority: recent IDs (more likely to exist)
            {'start': max(start_id, end_id - 50000), 'end': end_id, 'batch_size': 100, 'shuffle': True},
            # Medium priority: middle range
            {'start': max(start_id, end_id - 200000), 'end': end_id - 50000, 'batch_size': 500, 'shuffle': True},
            # Low priority: older IDs
            {'start': start_id, 'end': min(end_id - 200000, end_id), 'batch_size': 1000, 'shuffle': True}
        ]
        
        # Process each priority range
        for priority_range in priority_ranges:
            range_start = priority_range['start']
            range_end = priority_range['end']
            batch_size = priority_range['batch_size']
            should_shuffle = priority_range['shuffle']
            
            if range_start >= range_end:
                continue
                
            logger.info(f"Processing ID range {range_start} to {range_end} with batch size {batch_size}")
            
            # Submit gallery IDs in batches
            for batch_start in range(range_start, range_end + 1, batch_size):
                batch_end = min(batch_start + batch_size - 1, range_end)
                
                if not update_running:
                    logger.info("Database update stopped by user")
                    break
                
                # Create batch and optionally shuffle for better distribution
                batch_ids = list(range(batch_start, batch_end + 1))
                if should_shuffle:
                    random.shuffle(batch_ids)
                
                # Add IDs to work queue with backpressure handling
                for gid in batch_ids:
                    while update_running:
                        try:
                            # Use shorter timeout for more responsive queue management
                            work_queue.put(gid, timeout=0.5)
                            break
                        except queue.Full:
                            # Queue is full, wait before retrying
                            time.sleep(0.2)
                            continue
                
                # Small delay between batches to allow queue processing to start
                if batch_end < range_end:
                    time.sleep(0.5)
        
        # Wait for queue to be processed with progress reporting
        last_processed = 0
        while not work_queue.empty() and update_running:
            with progress_lock:
                if processed != last_processed:
                    remaining = work_queue.qsize()
                    logger.info(f"Waiting for {remaining} remaining items in queue. Processed so far: {processed}")
                    last_processed = processed
            time.sleep(5)
        
        # Final wait for queue to be fully processed
        work_queue.join()
        
    finally:
        # Cleanup
        update_running = False
        
        # Wait for scaling thread to finish
        scaling_thread.join(timeout=2)
    
    logger.info(f"Database update complete. Processed {processed} galleries with {successful} successful ({(successful/processed*100 if processed else 0):.1f}%).")

# Legacy function for backward compatibility
def update_database(start_id=1, end_id=400000, max_workers=8, include_pages=True):
    return update_database_optimized(start_id, end_id, max_workers, include_pages)

# Optimized find_by_image function with parallel processing
def find_by_image_optimized(image_file, limit=10, include_pages=True):
    start_time = time.time()
    
    # Open and hash the uploaded image more efficiently
    img = Image.open(image_file)
    query_hash = imagehash.phash(img, hash_size=8)
    
    # Use an optimized query approach with indexing
    conn = get_db_connection()
    conn.execute("PRAGMA cache_size=50000")  # Increase cache size
    
    # Prepare results
    results = []
    
    # Process thumbnail matches in parallel
    def process_thumbnails():
        local_results = []
        # First get potential thumbnail matches using a fast pre-filtering query
        cursor = conn.execute("""
        SELECT id, title, thumbnail_url, page_count, tags, phash 
        FROM galleries 
        WHERE phash IS NOT NULL
        """)
        
        rows = cursor.fetchall()
        
        def process_row(row):
            gallery_id, title, thumbnail_url, page_count, tags_str, phash_str = row
            
            if not phash_str:
                return None
                
            # Calculate hash difference
            try:
                db_hash = imagehash.hex_to_hash(phash_str)
                diff = query_hash - db_hash
                
                if diff < 15:  # Threshold for similarity
                    return {
                        'id': gallery_id,
                        'title': title,
                        'thumbnail_url': thumbnail_url,
                        'page_count': page_count,
                        'tags': tags_str.split(',') if tags_str else [],
                        'url': f"https://nhentai.net/g/{gallery_id}/",
                        'similarity': 100 - (diff * 100 / 64),  # Convert diff to percentage
                        'matched_type': 'thumbnail'
                    }
            except Exception as e:
                logger.error(f"Error comparing hash for gallery {gallery_id}: {e}")
            
            return None
        
        # Process rows in parallel
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for result in executor.map(process_row, rows):
                if result:
                    local_results.append(result)
        
        return local_results
    
    # Process page matches in parallel
    def process_pages():
        if not include_pages:
            return []
            
        local_results = []
        # Use an optimized query with JOIN and pre-filtering
        cursor = conn.execute("""
        SELECT p.gallery_id, p.page_number, p.image_url, p.phash, 
               g.title, g.thumbnail_url, g.page_count, g.tags
        FROM pages p
        JOIN galleries g ON p.gallery_id = g.id
        WHERE p.phash IS NOT NULL
        """)
        
        rows = cursor.fetchall()
        
        def process_row(row):
            gallery_id, page_number, image_url, phash_str, title, thumbnail_url, page_count, tags_str = row
            
            if not phash_str:
                return None
                
            # Calculate hash difference
            try:
                db_hash = imagehash.hex_to_hash(phash_str)
                diff = query_hash - db_hash
                
                if diff < 15:  # Threshold for similarity
                    return {
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
                    }
            except Exception as e:
                logger.error(f"Error comparing hash for page {page_number} of gallery {gallery_id}: {e}")
            
            return None
        
        # Process rows in parallel
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for result in executor.map(process_row, rows):
                if result:
                    local_results.append(result)
        
        return local_results
    
    # Run both processes in parallel
    with ThreadPoolExecutor(max_workers=2) as executor:
        thumbnail_future = executor.submit(process_thumbnails)
        pages_future = executor.submit(process_pages)
        
        # Collect results
        results.extend(thumbnail_future.result())
        results.extend(pages_future.result())
    
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

# Optimized database status function
@app.route('/api/status', methods=['GET'])
def get_status():
    conn = get_db_connection()
    
    # Get gallery count and page count in a single query for efficiency
    cursor = conn.execute("""
    SELECT 
        (SELECT COUNT(*) FROM galleries) as gallery_count,
        (SELECT COUNT(*) FROM pages) as page_count
    """)
    
    result = cursor.fetchone()
    gallery_count = result['gallery_count']
    page_count = result['page_count']
    
    # Get recent updates with optimized query
    cursor = conn.execute("""
    SELECT id, title, last_updated 
    FROM galleries 
    ORDER BY last_updated DESC 
    LIMIT 10
    """)
    
    recent_updates = [
        {'id': row['id'], 'title': row['title'], 'timestamp': row['last_updated']}
        for row in cursor
    ]
    
    conn.close()
    
    # Use faster JSON serialization
    return app.response_class(
        response=json.dumps({
            'gallery_count': gallery_count,
            'page_count': page_count,
            'recent_updates': recent_updates
        }),
        mimetype='application/json'
    )

# Routes
@app.route('/')
def index():
    return render_template('index.html')

# Optimized search endpoint with memory caching
search_cache = cachetools.TTLCache(maxsize=100, ttl=300)  # Cache results for 5 minutes

@app.route('/api/search', methods=['POST'])
def search():
    if 'image' not in request.files:
        return jsonify({'error': 'No image provided'}), 400
        
    file = request.files['image']
    if file.filename == '':
        return jsonify({'error': 'No image selected'}), 400
    
    include_pages = request.form.get('search_pages', 'true').lower() == 'true'
    limit = int(request.form.get('limit', 10))
    
    # Generate a cache key based on image content
    file_content = file.read()
    cache_key = hash(file_content)
    file.seek(0)  # Reset file pointer
    
    # Check cache
    if cache_key in search_cache:
        logger.info(f"Using cached search results for {file.filename}")
        return jsonify(search_cache[cache_key])
    
    # Perform search
    results = find_by_image_optimized(file, limit, include_pages)
    
    # Cache results
    search_cache[cache_key] = results
    
    return jsonify(results)

@app.route('/api/update_db', methods=['POST'])
def update_db_endpoint():
    data = request.get_json()
    start_id = data.get('start_id', 1)
    end_id = data.get('end_id', 564933)  # Limit range for safety
    concurrent_galleries = data.get('concurrent_galleries', MAX_WORKERS)
    include_pages = data.get('include_pages', True)
    
    # Start background update
    thread = threading.Thread(
        target=update_database_optimized, 
        args=(start_id, end_id, concurrent_galleries, include_pages),
        daemon=True
    )
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

# Optimized database optimization endpoint
@app.route('/api/optimize_db', methods=['POST'])
def optimize_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Run optimizations in a specific order for best results
        
        # First analyze tables for query optimization
        cursor.execute('ANALYZE')
        
        # Optimize indexes
        cursor.execute('PRAGMA optimize')
        
        # Optimize WAL checkpoint
        cursor.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        
        # Optimize cache and memory settings
        cursor.execute('PRAGMA cache_size=50000')
        cursor.execute('PRAGMA temp_store=MEMORY')
        cursor.execute('PRAGMA mmap_size=30000000000')
        
        # Add or rebuild missing indexes
        cursor.execute('REINDEX')
        
        # Add missing indexes if any
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_galleries_title ON galleries(title)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_pages_gallery_id_page_number ON pages(gallery_id, page_number)')
        
        # Finally run VACUUM to rebuild the database file
        cursor.execute('VACUUM')
        
        conn.commit()
        return jsonify({'message': 'Database optimization completed successfully'})
    except Exception as e:
        conn.rollback()
        return jsonify({'error': f'Database optimization failed: {str(e)}'}), 500
    finally:
        conn.close()

@app.route('/api/clear_cache', methods=['POST'])
def clear_cache():
    global gallery_cache, search_cache
    gallery_cache.clear()  # Clear LRU cache
    search_cache.clear()   # Clear TTL cache
    return jsonify({'message': 'All caches cleared successfully'})

@app.route('/api/refresh_sessions', methods=['POST'])
def refresh_sessions():
    # Recreate the session pool
    pool_size = request.json.get('pool_size', MAX_WORKERS * 2)
    create_session_pool(pool_size)
    return jsonify({'message': f'Session pool refreshed with {pool_size} sessions'})

# Optimized bulk fetch with work queue
@app.route('/api/bulk_fetch', methods=['POST'])
def bulk_fetch():
    data = request.get_json()
    gallery_ids = data.get('gallery_ids', [])
    include_pages = data.get('include_pages', True)
    
    if not gallery_ids:
        return jsonify({'error': 'No gallery IDs provided'}), 400
    
    # Limit the number of galleries to prevent abuse
    if len(gallery_ids) > 500:  # Increased limit for high-performance mode
        return jsonify({'error': 'Too many gallery IDs. Maximum is 500 at a time'}), 400
    
    # Create a work queue for better load distribution
    work_queue = queue.Queue()
    for gid in gallery_ids:
        work_queue.put(gid)
    
    # Track progress
    total = len(gallery_ids)
    completed = 0
    completed_lock = threading.Lock()
    
    # Worker function
    def worker():
        nonlocal completed
        while True:
            try:
                gid = work_queue.get(block=False)
            except queue.Empty:
                break
                
            try:
                # Add adaptive delay to avoid rate limiting
                time.sleep(np.random.uniform(0.1, 0.5))
                scrape_gallery_optimized(gid, include_pages)
                
                with completed_lock:
                    completed += 1
                    if completed % 10 == 0:
                        logger.info(f"Bulk fetch progress: {completed}/{total}")
                        
            except Exception as e:
                logger.error(f"Error fetching gallery {gid}: {e}")
            finally:
                work_queue.task_done()
    
    # Start worker threads - use dynamic number based on request size
    worker_count = min(MAX_WORKERS, len(gallery_ids))
    threads = []
    for _ in range(worker_count):
        t = threading.Thread(target=worker, daemon=True)
        t.start()
        threads.append(t)
    
    # Start a monitoring thread
    def monitor():
        work_queue.join()
        logger.info(f"Bulk fetch completed. Processed {completed} out of {total} galleries.")
    
    monitor_thread = threading.Thread(target=monitor, daemon=True)
    monitor_thread.start()
    
    return jsonify({
        'message': f'Bulk fetch started for {len(gallery_ids)} galleries with {worker_count} workers'
    })

# New endpoint for advanced database statistics
@app.route('/api/db_stats', methods=['GET'])
def get_db_stats():
    conn = get_db_connection()
    
    try:
        stats = {}
        
        # Get total counts
        cursor = conn.execute("""
        SELECT 
            (SELECT COUNT(*) FROM galleries) as gallery_count,
            (SELECT COUNT(*) FROM pages) as page_count,
            (SELECT COUNT(DISTINCT gallery_id) FROM pages) as galleries_with_pages
        """)
        basic_stats = cursor.fetchone()
        stats['gallery_count'] = basic_stats['gallery_count']
        stats['page_count'] = basic_stats['page_count']
        stats['galleries_with_pages'] = basic_stats['galleries_with_pages']
        
        # Get database file size
        db_path = 'nhentai_db.sqlite'
        if os.path.exists(db_path):
            stats['db_size_mb'] = os.path.getsize(db_path) / (1024 * 1024)
        
        # Get tag statistics
        cursor = conn.execute("""
        SELECT tags FROM galleries WHERE tags IS NOT NULL AND tags != ''
        """)
        
        tag_counts = {}
        for row in cursor:
            tags = row['tags'].split(',')
            for tag in tags:
                tag = tag.strip()
                if tag:
                    tag_counts[tag] = tag_counts.get(tag, 0) + 1
        
        # Get top 20 tags
        top_tags = sorted(tag_counts.items(), key=lambda x: x[1], reverse=True)[:20]
        stats['top_tags'] = [{'tag': tag, 'count': count} for tag, count in top_tags]
        
        # Get recent activity
        cursor = conn.execute("""
        SELECT date(last_updated) as update_date, COUNT(*) as count
        FROM galleries
        WHERE last_updated IS NOT NULL
        GROUP BY update_date
        ORDER BY update_date DESC
        LIMIT 7
        """)
        
        stats['recent_activity'] = [
            {'date': row['update_date'], 'count': row['count']}
            for row in cursor
        ]
        
        return jsonify(stats)
    except Exception as e:
        logger.error(f"Error getting database stats: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

# New endpoint for system performance monitoring
@app.route('/api/system_stats', methods=['GET'])
def get_system_stats():
    stats = {
        'cpu_count': CPU_COUNT,
        'max_workers': MAX_WORKERS,
        'session_pool_size': len(session_pool),
        'gallery_cache_size': len(gallery_cache),
        'search_cache_size': len(search_cache),
        'search_cache_info': {
            'hits': search_cache.hits,
            'misses': search_cache.misses,
            'maxsize': search_cache.maxsize,
            'ttl': search_cache.ttl
        }
    }
    
    # Add memory usage if psutil is available
    try:
        import psutil
        process = psutil.Process(os.getpid())
        stats['memory_usage_mb'] = process.memory_info().rss / (1024 * 1024)
        stats['cpu_percent'] = process.cpu_percent(interval=0.1)
        stats['system_memory'] = {
            'total': psutil.virtual_memory().total / (1024 * 1024 * 1024),
            'available': psutil.virtual_memory().available / (1024 * 1024 * 1024),
            'percent': psutil.virtual_memory().percent
        }
    except ImportError:
        stats['memory_info'] = 'psutil not available'
    
    return jsonify(stats)

if __name__ == '__main__':
    # Use a production WSGI server like gunicorn in production
    from waitress import serve
    
    # Create a larger thread pool for the server
    serve(app, host='127.0.0.1', port=5000, threads=CPU_COUNT*4)