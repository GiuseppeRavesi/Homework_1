# data_collector/opensky_client.py
import os
import requests
from datetime import datetime, timedelta
from time import time
from circuit_breaker import CircuitBreaker, CircuitBreakerOpen

class OpenSkyClient:
    def __init__(self, client_id=None, client_secret=None):
        self.api_url = "https://opensky-network.org/api"
        self.auth_url = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
        self.client_id = client_id or os.getenv("CLIENT_ID")
        self.client_secret = client_secret or os.getenv("CLIENT_SECRET")
        self.token = None
        self.token_expires_at = 0
        if not self.client_id or not self.client_secret:
            raise RuntimeError("OpenSky credentials not found in environment (CLIENT_ID, CLIENT_SECRET)")
        self.authenticate()
        self.circuit_breaker = CircuitBreaker()

    def authenticate(self):
        payload = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret
        }
        try:
            r = requests.post(self.auth_url, data=payload, timeout=15)
            r.raise_for_status()
            j = r.json()
            self.token = j.get("access_token")
            # il campo expires_in è solitamente presente: ttl in secondi
            expires_in = j.get("expires_in", 3600)
            self.token_expires_at = int(time()) + int(expires_in) - 30
            print("[OpenSky] Token ottenuto, scade in", expires_in, "s", flush=True)
        except Exception as e:
            print("[OpenSky] Errore autenticazione:", str(e), flush=True)
            self.token = None
            self.token_expires_at = 0

    def _ensure_token(self):
        if not self.token or int(time()) >= self.token_expires_at:
            self.authenticate()

    def get_headers(self):
        self._ensure_token()
        if not self.token:
            return {}
        return {"Authorization": f"Bearer {self.token}"}

    def _call_api(self, path, params=None):
        self.circuit_breaker.before_call()
        url = f"{self.api_url}{path}"
        try:
            r = requests.get(url, params=params, headers=self.get_headers(), timeout=30)
            if r.status_code == 200:
                self.circuit_breaker.on_success()
                return r.json()
            elif r.status_code == 401:
                # token scaduto o revocato
                self.authenticate()
                r = requests.get(url, params=params, headers=self.get_headers(), timeout=30)
                if r.status_code == 200:
                    return r.json()
                return []
            elif r.status_code == 404:
                return []
            else:
                print(f"[OpenSky] API error {r.status_code}: {r.text}", flush=True)
                return []
        except Exception as e:
            self.circuit_breaker.on_failure()
            print(f"[OpenSky] Request error: {e}", flush=True)
            return []

    def get_departures(self, airport_icao, begin_timestamp=None, end_timestamp=None):
        if not begin_timestamp:
            begin_timestamp = int((datetime.utcnow() - timedelta(hours=24)).timestamp())
        if not end_timestamp:
            end_timestamp = int(datetime.utcnow().timestamp())
        params = {"airport": airport_icao, "begin": begin_timestamp, "end": end_timestamp}
        return self._call_api("/flights/departure", params=params)

    def get_arrivals(self, airport_icao, begin_timestamp=None, end_timestamp=None):
        if not begin_timestamp:
            begin_timestamp = int((datetime.utcnow() - timedelta(hours=24)).timestamp())
        if not end_timestamp:
            end_timestamp = int(datetime.utcnow().timestamp())
        params = {"airport": airport_icao, "begin": begin_timestamp, "end": end_timestamp}
        return self._call_api("/flights/arrival", params=params)

    def get_flights_for_airport(self, airport_icao, begin_timestamp=None, end_timestamp=None):
        departures = self.get_departures(airport_icao, begin_timestamp, end_timestamp)
        arrivals = self.get_arrivals(airport_icao, begin_timestamp, end_timestamp)
        return {"departures": departures or [], "arrivals": arrivals or [], "total": (len(departures or []) + len(arrivals or []))}
