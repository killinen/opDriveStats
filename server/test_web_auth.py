import base64
from dataclasses import replace
import json
from pathlib import Path
import re
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, patch
from urllib.parse import parse_qs, urlsplit

from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from itsdangerous import TimestampSigner
from joserfc import jwt
from joserfc.jwk import RSAKey

from server.app import templates
from server.web_auth import AuthConfig, GUEST_SECONDS, install_auth, safe_next


class WebAuthTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.config = AuthConfig(
            True, 'https://stats.example.com', 'owner@gmail.com', 's' * 48,
            'test-client', 'test-client-secret', Path(self.directory.name) / 'auth.sqlite3',
        )
        self.app = self.make_app(self.config)
        self.client = self.make_client(self.app)

    def tearDown(self):
        self.client.close()
        self.directory.cleanup()

    def make_app(self, config):
        app = FastAPI()
        install_auth(app, templates, config)

        @app.get('/')
        def home(request: Request):
            return {'role': request.state.principal['role']}

        @app.get('/api/data')
        def data():
            return {'private': True}

        @app.get('/health')
        def health():
            return {'status': 'ok'}

        return app

    def make_client(self, app=None):
        return TestClient(app or self.app, base_url=self.config.public_url, follow_redirects=False)

    def owner_login(self, claims=None):
        claims = claims or {'email': self.config.owner_email, 'email_verified': True, 'sub': 'owner-123'}
        with patch.object(self.app.state.google_oauth.google, 'authorize_access_token',
                          AsyncMock(return_value={'userinfo': claims})):
            return self.client.get('/auth/callback?code=test&state=test')

    def form_token(self):
        response = self.client.get('/access')
        return re.search(r'name="csrf" value="([^"]+)"', response.text).group(1)

    def test_every_dashboard_and_data_route_requires_access(self):
        for path in ('/', '/stats', '/stats_mod', '/compare', '/total_stats', '/total_stats/style4', '/docs', '/redoc'):
            with self.subTest(path=path):
                response = self.client.get(path)
                self.assertEqual(response.status_code, 303)
                self.assertTrue(response.headers['location'].startswith('/login?next='))
        for path in ('/api/data', '/api/compare', '/api/total_stats', '/devices', '/devices/abc', '/openapi.json'):
            with self.subTest(path=path):
                self.assertEqual(self.client.get(path).status_code, 401)
        self.assertEqual(self.client.get('/health').json(), {'status': 'ok'})

    def test_owner_access_and_secure_cookie(self):
        response = self.owner_login()
        self.assertEqual(response.status_code, 303)
        cookie = response.headers['set-cookie'].lower()
        for flag in ('httponly', 'secure', 'samesite=lax'):
            self.assertIn(flag, cookie)
        self.assertEqual(self.client.get('/').json(), {'role': 'owner'})
        self.assertEqual(self.client.get('/access').status_code, 200)

    def test_disallowed_and_unverified_google_accounts_are_rejected(self):
        for email, verified, sub in [('other@gmail.com', True, 'other'),
                                     (self.config.owner_email, False, 'owner'),
                                     (self.config.owner_email, True, '')]:
            self.assertEqual(self.owner_login({'email': email, 'email_verified': verified, 'sub': sub}).status_code, 403)
            self.assertEqual(self.client.get('/api/data').status_code, 401)

    def test_callback_without_matching_state_rejected_by_authlib(self):
        self.assertEqual(self.client.get('/auth/callback?code=forged&state=forged').status_code, 400)
        self.assertEqual(self.client.get('/api/data').status_code, 401)

    def test_google_flow_verifies_signed_id_token(self):
        key = RSAKey.generate_key(2048, parameters={'kid': 'test-key'})
        google = self.app.state.google_oauth.google
        google.server_metadata.update({
            '_loaded_at': time.time(), 'issuer': 'https://accounts.google.com',
            'authorization_endpoint': 'https://accounts.google.com/o/oauth2/v2/auth',
            'token_endpoint': 'https://oauth2.googleapis.com/token',
            'jwks': {'keys': [key.as_dict(private=False)]},
            'id_token_signing_alg_values_supported': ['RS256'],
        })
        response = self.client.get('/auth/google?next=/api/data')
        params = parse_qs(urlsplit(response.headers['location']).query)
        self.assertEqual(params['redirect_uri'], [self.config.public_url + '/auth/callback'])
        self.assertEqual(params['code_challenge_method'], ['S256'])
        claims = {
            'iss': 'https://accounts.google.com', 'aud': self.config.client_id,
            'sub': 'owner-123', 'email': self.config.owner_email, 'email_verified': True,
            'iat': int(time.time()), 'exp': int(time.time()) + 300, 'nonce': params['nonce'][0],
        }
        signed = jwt.encode({'alg': 'RS256', 'kid': 'test-key'}, claims, key)
        with patch.object(google, 'fetch_access_token', AsyncMock(return_value={'id_token': signed})):
            callback = self.client.get('/auth/callback', params={'code': 'test', 'state': params['state'][0]})
        self.assertEqual(callback.status_code, 303)
        self.assertEqual(callback.headers['location'], '/api/data')
        self.assertEqual(self.client.get('/api/data').status_code, 200)

    def test_forged_id_token_signature_rejected(self):
        google = self.app.state.google_oauth.google
        key = RSAKey.generate_key(2048, parameters={'kid': 'test-key'})
        wrong_key = RSAKey.generate_key(2048, parameters={'kid': 'test-key'})
        google.server_metadata.update({
            '_loaded_at': time.time(), 'issuer': 'https://accounts.google.com',
            'authorization_endpoint': 'https://accounts.google.com/o/oauth2/v2/auth',
            'jwks': {'keys': [key.as_dict(private=False)]},
        })
        response = self.client.get('/auth/google')
        params = parse_qs(urlsplit(response.headers['location']).query)
        signed = jwt.encode({'alg': 'RS256', 'kid': 'test-key'}, {
            'iss': 'https://accounts.google.com', 'aud': self.config.client_id,
            'sub': 'owner-123', 'email': self.config.owner_email, 'email_verified': True,
            'iat': int(time.time()), 'exp': int(time.time()) + 300, 'nonce': params['nonce'][0],
        }, wrong_key)
        with patch.object(google, 'fetch_access_token', AsyncMock(return_value={'id_token': signed})):
            callback = self.client.get('/auth/callback', params={'code': 'test', 'state': params['state'][0]})
        self.assertEqual(callback.status_code, 400)
        self.assertEqual(self.client.get('/api/data').status_code, 401)

    def test_owner_creates_guest_link_and_guest_cannot_manage_access(self):
        self.owner_login()
        response = self.client.post('/access/links', data={'csrf': self.form_token()})
        self.assertEqual(response.status_code, 200)
        link = re.search(r'id="share-link" readonly value="([^"]+)"', response.text).group(1)
        share = self.app.state.share_store.listing()[0]
        self.assertEqual(share['expires_at'] - share['created_at'], GUEST_SECONDS)
        with self.make_client() as guest:
            redeemed = guest.get(link)
            self.assertEqual(redeemed.status_code, 303)
            self.assertEqual(redeemed.headers['location'], '/')
            self.assertEqual(guest.get('/').json(), {'role': 'guest'})
            self.assertEqual(guest.get('/api/data').status_code, 200)
            self.assertEqual(guest.get('/access').status_code, 403)
            self.assertEqual(guest.post('/access/links').status_code, 403)
            self.assertEqual(guest.post('/access/links/1/revoke').status_code, 403)

    def test_expiry_also_ends_existing_guest_session(self):
        _, token, expires = self.app.state.share_store.create()
        self.client.get('/share/' + token)
        self.assertEqual(self.client.get('/api/data').status_code, 200)
        with patch('server.web_auth.time.time', return_value=expires):
            self.assertEqual(self.client.get('/api/data').status_code, 401)
            self.assertEqual(self.client.get('/share/' + token).status_code, 410)

    def test_revocation_ends_link_and_existing_guest_session(self):
        self.owner_login()
        csrf = self.form_token()
        share_id, token, _ = self.app.state.share_store.create()
        with self.make_client() as guest:
            guest.get('/share/' + token)
            self.assertEqual(guest.get('/api/data').status_code, 200)
            response = self.client.post(f'/access/links/{share_id}/revoke', data={'csrf': csrf})
            self.assertEqual(response.status_code, 303)
            self.assertEqual(guest.get('/api/data').status_code, 401)
            self.assertEqual(guest.get('/share/' + token).status_code, 410)

    def test_csrf_required_for_mutations_and_logout(self):
        self.owner_login()
        for path in ('/access/links', '/access/links/1/revoke', '/access/members', '/access/members/remove', '/auth/logout'):
            self.assertEqual(self.client.post(path).status_code, 403)
        token = self.form_token()
        self.assertEqual(self.client.post('/access/links', data={'csrf': token},
                                        headers={'Origin': 'https://evil.example'}).status_code, 403)
        self.assertEqual(self.client.post('/auth/logout', data={'csrf': token}).status_code, 303)
        self.assertEqual(self.client.get('/api/data').status_code, 401)

    def test_owner_grants_google_account_and_removes_active_session(self):
        self.owner_login()
        csrf = self.form_token()
        response = self.client.post('/access/members', data={'csrf': csrf, 'email': ' Friend@Gmail.com '})
        self.assertEqual(response.status_code, 303)
        self.assertTrue(self.app.state.share_store.is_member('friend@gmail.com'))
        with self.make_client() as member:
            with patch.object(self.app.state.google_oauth.google, 'authorize_access_token',
                              AsyncMock(return_value={'userinfo': {
                                  'email': 'friend@gmail.com', 'email_verified': True, 'sub': 'friend-123',
                              }})):
                self.assertEqual(member.get('/auth/callback').status_code, 303)
            self.assertEqual(member.get('/').json(), {'role': 'member'})
            self.assertEqual(member.get('/api/data').status_code, 200)
            self.assertEqual(member.get('/access').status_code, 403)
            self.assertEqual(member.post('/access/members').status_code, 403)
            response = self.client.post('/access/members/remove', data={'csrf': csrf, 'email': 'friend@gmail.com'})
            self.assertEqual(response.status_code, 303)
            self.assertEqual(member.get('/api/data').status_code, 401)
        self.assertEqual(self.client.get('/').json(), {'role': 'owner'})

    def test_member_grant_validates_email_and_owner_cannot_be_removed(self):
        self.owner_login()
        csrf = self.form_token()
        for value in ('no-email', 'a@@b.com', 'a @gmail.com'):
            self.assertEqual(self.client.post('/access/members', data={'csrf': csrf, 'email': value}).status_code, 400)
        self.client.post('/access/members/remove', data={'csrf': csrf, 'email': self.config.owner_email})
        self.assertEqual(self.client.get('/').json(), {'role': 'owner'})

    def test_forged_session_rejected(self):
        body = base64.b64encode(json.dumps({'user': {
            'role': 'owner', 'email': self.config.owner_email, 'sub': 'owner',
            'expires_at': time.time() + 1000,
        }}).encode())
        self.client.cookies.set('engagement_session', TimestampSigner('wrong-key').sign(body).decode())
        self.assertEqual(self.client.get('/api/data').status_code, 401)

    def test_incomplete_configuration_fails_closed(self):
        app = self.make_app(replace(self.config, client_secret=''))
        with self.make_client(app) as client:
            self.assertEqual(client.get('/api/data').status_code, 401)
            self.assertEqual(client.get('/auth/google').status_code, 503)
            self.assertIn('Private access is being set up', client.get('/login').text)

    def test_tokens_are_hashed_and_links_survive_restart(self):
        _, token, _ = self.app.state.share_store.create()
        with self.app.state.share_store.connect() as db:
            self.assertNotEqual(db.execute('SELECT token_hash FROM shares').fetchone()[0], token)
        restarted = self.make_app(self.config)
        with self.make_client(restarted) as client:
            self.assertEqual(client.get('/share/' + token).status_code, 303)
            self.assertEqual(client.get('/api/data').status_code, 200)

    def test_security_headers_and_invalid_links(self):
        response = self.client.get('/share/invalid')
        self.assertEqual(response.status_code, 410)
        self.assertEqual(response.headers['cache-control'], 'no-store')
        self.assertEqual(response.headers['referrer-policy'], 'no-referrer')

    def test_return_path_cannot_redirect_off_site(self):
        for value in ('https://evil.example', '//evil.example', '//[', '/\\evil.example', '/login', '/auth/google', '/share/token', '/\nevil'):
            self.assertEqual(safe_next(value), '/')
        self.assertEqual(safe_next('/compare?device=abc'), '/compare?device=abc')


if __name__ == '__main__':
    unittest.main()
