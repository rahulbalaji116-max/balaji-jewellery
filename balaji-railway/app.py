"""
app.py — Sri Balaji Jewellers (Railway Production)
Single server that runs both the main website and admin panel.

URLs:
  /          → Main customer website
  /admin     → Admin panel (PIN protected)
  /api/* → Shared API
  /events    → SSE stream for real-time sync
"""
import os, json, uuid, base64, queue, threading
from datetime import datetime
from functools import wraps
from flask import (Flask, render_template, request, jsonify,
                   session, Response, redirect, url_for)
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__, template_folder='templates', static_folder='static')
app.secret_key = os.environ.get('SECRET_KEY', 'balaji_secret_change_me')

# ─── CONFIG ───
OWNER_PIN    = os.environ.get('OWNER_PIN', '1234')
DATABASE_URL = os.environ.get('DATABASE_URL')          # set by Railway Postgres plugin
REDIS_URL    = os.environ.get('REDIS_URL')             # set by Railway Redis plugin

# ─── IMAGE STORAGE ───
# On Railway, local filesystem is ephemeral so we store images as base64 in DB.

# ─── SSE BUS ───
_listeners: list[queue.Queue] = []
_lock = threading.Lock()

def _setup_redis():
    if not REDIS_URL:
        return None
    try:
        import redis
        r = redis.from_url(REDIS_URL)
        r.ping()
        return r
    except Exception as e:
        print(f"[SSE] Redis not available: {e} — using in-process bus")
        return None

_redis = _setup_redis()

def subscribe() -> queue.Queue:
    q: queue.Queue = queue.Queue(maxsize=100)
    with _lock:
        _listeners.append(q)
    return q

def unsubscribe(q: queue.Queue):
    with _lock:
        try:
            _listeners.remove(q)
        except ValueError:
            pass

def broadcast(event: str, data: dict):
    msg = f"event: {event}\ndata: {json.dumps(data)}\n\n"
    if _redis:
        try:
            _redis.publish('balaji_sse', msg)
            return
        except Exception:
            pass
    # In-process fallback
    with _lock:
        dead = []
        for q in _listeners:
            try:
                q.put_nowait(msg)
            except queue.Full:
                dead.append(q)
        for q in dead:
            _listeners.remove(q)

def _redis_subscriber():
    import redis as redis_lib
    r = redis_lib.from_url(REDIS_URL)
    pubsub = r.pubsub()
    pubsub.subscribe('balaji_sse')
    for message in pubsub.listen():
        if message['type'] == 'message':
            msg = message['data'].decode()
            with _lock:
                dead = []
                for q in _listeners:
                    try:
                        q.put_nowait(msg)
                    except queue.Full:
                        dead.append(q)
                for q in dead:
                    _listeners.remove(q)

if _redis:
    t = threading.Thread(target=_redis_subscriber, daemon=True)
    t.start()

# ─── DATABASE ───
def get_db():
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    return conn

def init_db():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute('''
                CREATE TABLE IF NOT EXISTS products (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    metal TEXT NOT NULL,
                    type TEXT NOT NULL,
                    price TEXT NOT NULL,
                    "desc" TEXT,
                    img_data TEXT,
                    emoji TEXT,
                    created_at TIMESTAMP DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS orders (
                    id TEXT PRIMARY KEY,
                    timestamp TEXT,
                    name TEXT,
                    phone TEXT,
                    address TEXT,
                    note TEXT,
                    items TEXT,
                    total INTEGER,
                    status TEXT DEFAULT 'new',
                    order_type TEXT DEFAULT 'cart'
                );
            ''')
            cur.execute('SELECT COUNT(*) FROM products')
            count = cur.fetchone()['count']
            if count == 0:
                defaults = [
                    ('Traditional Gold Necklace', 'gold', 'necklace', '48,500', '22K gold with temple design, 8g', '📿'),
                    ('Silver Choker Set', 'silver', 'necklace', '3,200', 'Pure 925 silver, oxidised finish', '🔮'),
                    ('Diamond-cut Gold Ring', 'gold', 'ring', '18,000', '18K gold couple ring', '💍'),
                    ('Silver Toe Ring Pair', 'silver', 'ring', '850', 'Traditional silver toe rings', '✨'),
                    ('Gold Jhumka Earrings', 'gold', 'earring', '12,400', 'Hanging jhumka with kundan work', '🌸'),
                    ('Silver Kada Bangle', 'silver', 'bangle', '2,500', 'Solid handcrafted silver bangle', '⭕'),
                    ('Gold Mangalsutra Chain', 'gold', 'chain', '54,000', '22K daily wear chain, 10g', '⛓️'),
                    ('Silver Drop Earrings', 'silver', 'earring', '1,800', 'Elegant silver with pearl drop', '🪷'),
                ]
                for d in defaults:
                    cur.execute(
                        'INSERT INTO products (name,metal,type,price,"desc",emoji) VALUES (%s,%s,%s,%s,%s,%s)',
                        d
                    )
        conn.commit()

try:
    init_db()
    print("✅ Database initialised")
except Exception as e:
    print(f"⚠️  DB init error: {e}")

# ─── AUTH ───
def owner_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('owner_logged_in'):
            return jsonify({'error': 'Unauthorized'}), 401
        return f(*args, **kwargs)
    return decorated

def admin_page_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('owner_logged_in'):
            return redirect('/admin/login')
        return f(*args, **kwargs)
    return decorated

# ════════════════════════════════
#  MAIN SITE ROUTES
# ════════════════════════════════
@app.route('/')
def index():
    return render_template('index.html')

# ════════════════════════════════
#  ADMIN ROUTES
# ════════════════════════════════
@app.route('/admin')
@admin_page_required
def admin_panel():
    return render_template('admin.html')

@app.route('/admin/login')
def admin_login_page():
    if session.get('owner_logged_in'):
        return redirect('/admin')
    return render_template('admin_login.html')

# ════════════════════════════════
#  PRODUCTS API
# ════════════════════════════════
@app.route('/api/products')
def get_products():
    metal = request.args.get('metal')
    type_ = request.args.get('type')
    with get_db() as conn:
        with conn.cursor() as cur:
            if metal:
                cur.execute('SELECT * FROM products WHERE metal=%s ORDER BY id DESC', (metal,))
            elif type_:
                cur.execute('SELECT * FROM products WHERE type=%s ORDER BY id DESC', (type_,))
            else:
                cur.execute('SELECT * FROM products ORDER BY id DESC')
            rows = cur.fetchall()
    result = []
    for r in rows:
        p = dict(r)
        p['has_image'] = bool(p.get('img_data'))
        result.append(p)
    return jsonify(result)

@app.route('/api/products/<int:pid>/image')
def product_image(pid):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT img_data FROM products WHERE id=%s', (pid,))
            row = cur.fetchone()
    if not row or not row['img_data']:
        return '', 404
    img_data = row['img_data']
    header, encoded = img_data.split(',', 1)
    mime = header.split(':')[1].split(';')[0]
    img_bytes = base64.b64decode(encoded)
    return Response(img_bytes, mimetype=mime,
                    headers={'Cache-Control': 'public, max-age=86400'})

@app.route('/api/products', methods=['POST'])
@owner_required
def add_product():
    data = request.json
    name   = data.get('name', '').strip()
    metal  = data.get('metal', 'gold')
    type_  = data.get('type', 'other')
    price  = data.get('price', '').strip()
    desc   = data.get('desc', '').strip()
    img_data = data.get('img_data')
    emoji_map = {'necklace':'📿','ring':'💍','earring':'🌸','bangle':'⭕','chain':'⛓️','other':'💎'}
    emoji = emoji_map.get(type_, '💎')

    if not name or not price:
        return jsonify({'error': 'Name and price required'}), 400

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'INSERT INTO products (name,metal,type,price,"desc",img_data,emoji) VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING *',
                (name, metal, type_, price, desc, img_data, emoji)
            )
            prod = dict(cur.fetchone())
        conn.commit()

    prod['has_image'] = bool(prod.get('img_data'))
    prod_out = {k: v for k, v in prod.items() if k != 'img_data'}
    broadcast('product_added', prod_out)
    return jsonify(prod_out), 201

@app.route('/api/products/<int:pid>', methods=['PUT'])
@owner_required
def update_product(pid):
    data = request.json
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT * FROM products WHERE id=%s', (pid,))
            prod = cur.fetchone()
            if not prod:
                return jsonify({'error': 'Not found'}), 404
            prod = dict(prod)

            name     = data.get('name', prod['name']).strip()
            metal    = data.get('metal', prod['metal'])
            type_    = data.get('type', prod['type'])
            price    = data.get('price', prod['price']).strip()
            desc     = data.get('desc', prod['desc'] or '').strip()
            img_data = data.get('img_data') or prod.get('img_data')
            emoji_map = {'necklace':'📿','ring':'💍','earring':'🌸','bangle':'⭕','chain':'⛓️','other':'💎'}
            emoji = emoji_map.get(type_, '💎')

            cur.execute(
                'UPDATE products SET name=%s,metal=%s,type=%s,price=%s,"desc"=%s,img_data=%s,emoji=%s WHERE id=%s RETURNING *',
                (name, metal, type_, price, desc, img_data, emoji, pid)
            )
            updated = dict(cur.fetchone())
        conn.commit()

    updated['has_image'] = bool(updated.get('img_data'))
    out = {k: v for k, v in updated.items() if k != 'img_data'}
    broadcast('product_updated', out)
    return jsonify(out)

@app.route('/api/products/<int:pid>', methods=['DELETE'])
@owner_required
def delete_product(pid):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT id FROM products WHERE id=%s', (pid,))
            if not cur.fetchone():
                return jsonify({'error': 'Not found'}), 404
            cur.execute('DELETE FROM products WHERE id=%s', (pid,))
        conn.commit()
    broadcast('product_deleted', {'id': pid})
    return jsonify({'ok': True})

# ════════════════════════════════
#  ORDERS API
# ════════════════════════════════
@app.route('/api/orders', methods=['POST'])
def place_order():
    data = request.json or {}
    order_type = data.get('order_type', 'cart')
    order_id = ('CORD-' if order_type == 'custom' else 'ORD-') + str(int(datetime.now().timestamp() * 1000))
    
    items_raw = data.get('items', [])
    
    # 🎯 FIX: Bundle custom uploaded image directly into items dict if custom structure is sent
    if order_type == 'custom' and data.get('custom_image'):
        items_raw = {'custom_image': data.get('custom_image')}

    items_json = json.dumps(items_raw) if isinstance(items_raw, (list, dict)) else str(items_raw)

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'INSERT INTO orders (id,timestamp,name,phone,address,note,items,total,status,order_type) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                (order_id, datetime.now().isoformat(), data.get('name'), data.get('phone'),
                 data.get('address',''), data.get('note',''),
                 items_json, data.get('total',0), 'new', order_type)
            )
        conn.commit()

    order_payload = {
        'id': order_id,
        'timestamp': datetime.now().isoformat(),
        'name': data.get('name'),
        'phone': data.get('phone'),
        'address': data.get('address',''),
        'note': data.get('note',''),
        'items': items_raw,
        'total': data.get('total',0),
        'status': 'new',
        'order_type': order_type
    }
    broadcast('order_placed', order_payload)
    return jsonify({'ok': True, 'id': order_id})

@app.route('/api/orders')
@owner_required
def get_orders():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT * FROM orders ORDER BY timestamp DESC')
            rows = cur.fetchall()
    orders = []
    for r in rows:
        o = dict(r)
        try:
            o['items'] = json.loads(o['items'])
        except Exception:
            o['items'] = []
        orders.append(o)
    return jsonify(orders)

@app.route('/api/orders/<order_id>/status', methods=['PATCH'])
@owner_required
def update_order_status(order_id):
    status = request.json.get('status')
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute('UPDATE orders SET status=%s WHERE id=%s', (status, order_id))
        conn.commit()
    broadcast('order_updated', {'id': order_id, 'status': status})
    return jsonify({'ok': True})

# ════════════════════════════════
#  AUTH API
# ════════════════════════════════
@app.route('/api/owner/login', methods=['POST'])
def owner_login():
    pin = request.json.get('pin', '')
    if pin == OWNER_PIN:
        session['owner_logged_in'] = True
        return jsonify({'ok': True})
    return jsonify({'error': 'Wrong PIN'}), 401

@app.route('/api/owner/logout', methods=['POST'])
def owner_logout():
    session.pop('owner_logged_in', None)
    return jsonify({'ok': True})

@app.route('/api/owner/status')
def owner_status():
    return jsonify({'logged_in': bool(session.get('owner_logged_in'))})

# ════════════════════════════════
#  SSE STREAM
# ════════════════════════════════
@app.route('/events')
def sse_stream():
    q = subscribe()
    def stream():
        yield "event: connected\\ndata: {}\\n\\n"
        try:
            while True:
                try:
                    msg = q.get(timeout=25)
                    yield msg
                except Exception:
                    yield ": heartbeat\\n\\n"
        finally:
            unsubscribe(q)
    return Response(stream(), mimetype='text/event-stream',
                    headers={'Cache-Control':'no-cache','X-Accel-Buffering':'no'})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
