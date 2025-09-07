import hashlib
import hmac
import os

def generate_token(secret: str, worker_id: str) -> str:
	"""Genera un token HMAC para autenticación de workers"""
	return hmac.new(secret.encode(), worker_id.encode(), hashlib.sha256).hexdigest()

def verify_token(secret: str, worker_id: str, token: str) -> bool:
	"""Verifica un token HMAC recibido"""
	expected = generate_token(secret, worker_id)
	return hmac.compare_digest(expected, token)

def generate_random_secret(length: int = 32) -> str:
	"""Genera un secreto aleatorio seguro"""
	return os.urandom(length).hex()
