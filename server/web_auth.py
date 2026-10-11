"""Google owner sign-in and revocable, fixed-expiry guest access."""

import hashlib
import json
import os
import re
from pathlib import Path
import secrets
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass
from urllib.parse import quote, urlsplit

from authlib.integrations.base_client.errors import OAuthError
from authlib.integrations.starlette_client import OAuth
from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse, RedirectResponse
import httpx
from joserfc.errors import JoseError
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.sessions import SessionMiddleware


ROOT = Path(__file__).resolve().parent.parent
GUEST_SECONDS = 24 * 60 * 60
OWNER_SECONDS = 7 * GUEST_SECONDS


def safe_next(value: str) -> str:
    """Only navigate to local dashboard paths, never another origin or auth route."""
    try:
        parsed = urlsplit(value)
    except ValueError:
        return '/'
    if (not value.startswith('/') or value.startswith('//') or parsed.netloc
            or parsed.scheme or '\\' in value or any(ord(c) < 32 for c in value)
            or parsed.path.startswith(('/auth', '/login', '/share'))):
        return '/'
    return value


@dataclass(frozen=True)
class AuthConfig:
    enabled: bool
    public_url: str
    owner_email: str
    secret: str
    client_id: str
    client_secret: str
    database: Path

    @classmethod
    def from_environment(cls):
        path = Path(os.getenv('WEB_AUTH_CONFIG_FILE', str(ROOT / '.web_auth.json')))
        values = json.loads(path.read_text()) if path.exists() else {}

        def setting(key, default=''):
            return os.getenv('WEB_AUTH_' + key.upper(), values.get(key, default))

        public_url = setting('public_url').rstrip('/')
        if public_url:
            parsed = urlsplit(public_url)
            if (parsed.scheme != 'https' or not parsed.netloc or parsed.path
                    or parsed.query or parsed.fragment or parsed.username or parsed.password):
                raise ValueError('WEB_AUTH_PUBLIC_URL must be an HTTPS origin without a path')
        return cls(
            enabled=str(setting('enabled', True)).lower() not in ('false', '0'),
            public_url=public_url,
            owner_email=setting('owner_email').strip().lower(),
            secret=setting('secret'),
            client_id=setting('client_id'),
            client_secret=setting('client_secret'),
            database=Path(setting('database', str(ROOT / '.web_auth.sqlite3'))),
        )

    @property
    def ready(self):
        return bool(self.public_url and self.owner_email and len(self.secret) >= 32
                    and self.client_id and self.client_secret)


class ShareStore:
    def __init__(self, path):
        self.path = path

    @contextmanager
    def connect(self):
        # Create privately from the first write, including in a fresh deployment.
        fd = os.open(self.path, os.O_CREAT | os.O_RDWR, 0o600)
        os.close(fd)
        connection = sqlite3.connect(self.path, timeout=10)
        connection.row_factory = sqlite3.Row
        try:
            with connection:
                connection.execute('''CREATE TABLE IF NOT EXISTS shares (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    token_hash TEXT UNIQUE NOT NULL,
                    created_at INTEGER NOT NULL,
                    expires_at INTEGER NOT NULL,
                    revoked INTEGER NOT NULL DEFAULT 0
                )''')
                connection.execute('''CREATE TABLE IF NOT EXISTS members (
                    email TEXT PRIMARY KEY,
                    created_at INTEGER NOT NULL
                )''')
                yield connection
        finally:
            connection.close()

    def create(self):
        token = secrets.token_urlsafe(32)
        now = int(time.time())
        with self.connect() as db:
            cursor = db.execute(
                'INSERT INTO shares (token_hash, created_at, expires_at) VALUES (?, ?, ?)',
                (hashlib.sha256(token.encode()).hexdigest(), now, now + GUEST_SECONDS),
            )
            return cursor.lastrowid, token, now + GUEST_SECONDS

    def find(self, *, token=None, share_id=None):
        with self.connect() as db:
            if token is not None:
                row = db.execute('SELECT * FROM shares WHERE token_hash = ?',
                                 (hashlib.sha256(token.encode()).hexdigest(),)).fetchone()
            else:
                row = db.execute('SELECT * FROM shares WHERE id = ?', (share_id,)).fetchone()
        return dict(row) if row and not row['revoked'] and row['expires_at'] > time.time() else None

    def listing(self):
        with self.connect() as db:
            return [dict(row) for row in db.execute(
                'SELECT id, created_at, expires_at, revoked FROM shares ORDER BY id DESC LIMIT 100')]

    def revoke(self, share_id):
        with self.connect() as db:
            db.execute('UPDATE shares SET revoked = 1 WHERE id = ?', (share_id,))

    def is_member(self, email):
        with self.connect() as db:
            return db.execute('SELECT 1 FROM members WHERE email = ?', (email,)).fetchone() is not None

    def members(self):
        with self.connect() as db:
            return [dict(row) for row in db.execute('SELECT email, created_at FROM members ORDER BY email')]

    def add_member(self, email):
        with self.connect() as db:
            db.execute('INSERT OR IGNORE INTO members (email, created_at) VALUES (?, ?)',
                       (email, int(time.time())))

    def remove_member(self, email):
        with self.connect() as db:
            db.execute('DELETE FROM members WHERE email = ?', (email,))


class AccessMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        config = request.app.state.auth_config
        principal = None
        if config.enabled and config.ready:
            user = request.session.get('user', {})
            if (user.get('role') == 'owner' and user.get('email') == config.owner_email
                    and user.get('sub') and user.get('expires_at', 0) > time.time()):
                principal = user
            elif (user.get('role') == 'member' and user.get('sub')
                    and user.get('expires_at', 0) > time.time()
                    and request.app.state.share_store.is_member(user.get('email'))):
                principal = user
            elif user.get('role') == 'guest':
                share = request.app.state.share_store.find(share_id=user.get('share_id'))
                if share and share['expires_at'] == user.get('expires_at'):
                    principal = user
        request.state.principal = principal
        public = request.url.path in ('/login', '/auth/google', '/auth/callback', '/health')
        public = public or request.url.path.startswith('/share/')
        if config.enabled and not public and principal is None:
            if request.url.path.startswith(('/api/', '/devices', '/openapi.json')):
                response = JSONResponse({'detail': 'Sign in or use a valid share link'}, status_code=401)
            else:
                target = request.url.path + ('?' + request.url.query if request.url.query else '')
                response = RedirectResponse('/login?next=' + quote(target, safe=''), status_code=303)
        else:
            response = await call_next(request)
        if config.enabled:
            response.headers['Cache-Control'] = 'no-store'
            response.headers['Referrer-Policy'] = 'no-referrer'
            response.headers['X-Frame-Options'] = 'DENY'
        return response


def require_owner(request):
    user = request.state.principal
    if not user or user.get('role') != 'owner':
        raise HTTPException(403, 'Only the owner can manage access')


def csrf_token(request):
    if 'csrf' not in request.session:
        request.session['csrf'] = secrets.token_urlsafe(32)
    return request.session['csrf']


async def check_csrf(request):
    form = await request.form()
    expected = request.session.get('csrf', '')
    supplied = str(form.get('csrf', ''))
    if not expected or not secrets.compare_digest(expected, supplied):
        raise HTTPException(403, 'Invalid form token; reload this page and try again')
    origin = request.headers.get('origin')
    if origin and origin != request.app.state.auth_config.public_url:
        raise HTTPException(403, 'Invalid request origin')


def install_auth(app, templates, config=None):
    config = config or AuthConfig.from_environment()
    app.state.auth_config = config
    app.state.share_store = ShareStore(config.database)
    app.add_middleware(AccessMiddleware)
    app.add_middleware(
        SessionMiddleware, secret_key=config.secret or secrets.token_urlsafe(32),
        session_cookie='engagement_session', max_age=OWNER_SECONDS,
        same_site='lax', https_only=True,
    )
    oauth = OAuth()
    if config.ready:
        oauth.register(
            name='google', client_id=config.client_id, client_secret=config.client_secret,
            server_metadata_url='https://accounts.google.com/.well-known/openid-configuration',
            client_kwargs={'scope': 'openid email', 'code_challenge_method': 'S256'},
        )
    app.state.google_oauth = oauth

    def access_page(request, new_link=None, expires_at=None):
        return templates.TemplateResponse('access.html', {
            'request': request, 'csrf': csrf_token(request), 'new_link': new_link,
            'expires_at': expires_at, 'shares': app.state.share_store.listing(),
            'now': time.time(), 'members': app.state.share_store.members(),
            'owner_email': config.owner_email,
        })

    @app.get('/login', include_in_schema=False)
    def login(request: Request, next: str = '/'):
        if request.state.principal:
            return RedirectResponse(safe_next(next), status_code=303)
        return templates.TemplateResponse('login.html', {
            'request': request, 'ready': config.ready and config.enabled,
            'next': safe_next(next), 'message': None,
        })

    @app.get('/auth/google', include_in_schema=False)
    async def google_login(request: Request, next: str = '/'):
        if not config.enabled or not config.ready:
            raise HTTPException(503, 'Google sign-in has not been configured')
        request.session['next'] = safe_next(next)
        return await app.state.google_oauth.google.authorize_redirect(
            request, config.public_url + '/auth/callback', prompt='select_account',
        )

    @app.get('/auth/callback', include_in_schema=False)
    async def google_callback(request: Request):
        if not config.enabled or not config.ready:
            raise HTTPException(503, 'Google sign-in has not been configured')
        try:
            # Authlib verifies state, nonce, signature, audience, issuer and expiry.
            token = await app.state.google_oauth.google.authorize_access_token(request)
        except (OAuthError, JoseError, httpx.HTTPError, ValueError):
            request.session.clear()
            raise HTTPException(400, 'Sign-in could not be verified. Please try again.')
        user = token.get('userinfo', {})
        email = user.get('email', '').lower()
        if (user.get('email_verified') is not True or not user.get('sub')
                or not (email == config.owner_email or app.state.share_store.is_member(email))):
            request.session.clear()
            return templates.TemplateResponse('login.html', {
                'request': request, 'ready': True, 'next': '/',
                'message': 'This Google account does not have access. Ask the owner for a share link.',
            }, status_code=403)
        target = safe_next(request.session.get('next', '/'))
        request.session.clear()
        request.session['user'] = {
            'role': 'owner' if email == config.owner_email else 'member',
            'email': email, 'sub': user['sub'],
            'expires_at': int(time.time()) + OWNER_SECONDS,
        }
        return RedirectResponse(target, status_code=303)

    @app.get('/share/{token}', include_in_schema=False)
    def redeem_share(request: Request, token: str):
        share = app.state.share_store.find(token=token) if config.enabled and config.ready else None
        if not share:
            return templates.TemplateResponse('login.html', {
                'request': request, 'ready': config.ready, 'next': '/',
                'message': 'This share link has expired, was revoked, or is invalid. Ask the owner for a new link.',
            }, status_code=410)
        if not request.state.principal or request.state.principal.get('role') == 'guest':
            request.session.clear()
            request.session['user'] = {
                'role': 'guest', 'share_id': share['id'], 'expires_at': share['expires_at'],
            }
        # Remove the bearer token from the address bar before rendering any data.
        return RedirectResponse('/', status_code=303)

    @app.get('/access', include_in_schema=False)
    def access(request: Request):
        require_owner(request)
        return access_page(request)

    @app.post('/access/links', include_in_schema=False)
    async def create_link(request: Request):
        require_owner(request)
        await check_csrf(request)
        _, token, expires_at = app.state.share_store.create()
        return access_page(request, config.public_url + '/share/' + token, expires_at)

    @app.post('/access/links/{share_id}/revoke', include_in_schema=False)
    async def revoke_link(request: Request, share_id: int):
        require_owner(request)
        await check_csrf(request)
        app.state.share_store.revoke(share_id)
        return RedirectResponse('/access', status_code=303)

    @app.post('/access/members', include_in_schema=False)
    async def add_member(request: Request):
        require_owner(request)
        await check_csrf(request)
        email = str((await request.form()).get('email', '')).strip().lower()
        if len(email) > 254 or not re.fullmatch(r'[^\s@]+@[^\s@]+\.[^\s@]+', email):
            raise HTTPException(400, 'Enter a valid Google account email address')
        if email != config.owner_email:
            app.state.share_store.add_member(email)
        return RedirectResponse('/access', status_code=303)

    @app.post('/access/members/remove', include_in_schema=False)
    async def remove_member(request: Request):
        require_owner(request)
        await check_csrf(request)
        email = str((await request.form()).get('email', '')).strip().lower()
        app.state.share_store.remove_member(email)
        return RedirectResponse('/access', status_code=303)

    @app.post('/auth/logout', include_in_schema=False)
    async def logout(request: Request):
        await check_csrf(request)
        request.session.clear()
        return RedirectResponse('/login', status_code=303)
