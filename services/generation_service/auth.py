from datetime import datetime, timedelta, timezone
from typing import Any

import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from passlib.context import CryptContext
from sqlalchemy import select
from sqlalchemy.orm import Session

from db import get_db
from models import User

# NOTE:
# passlib+bcrypt backend is currently unstable in our runtime
# (bcrypt backend initialization fails and raises ValueError even for short passwords).
# Use PBKDF2-SHA256 to keep password hashing reliable and secure.
password_context = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")
bearer_scheme = HTTPBearer()


def hash_password(password: str) -> str:
    return password_context.hash(password)


def verify_password(password: str, password_hash: str) -> bool:
    return password_context.verify(password, password_hash)


def create_access_token(config: dict[str, Any], user_id: int) -> str:
    auth_config = config.get("auth", {})
    secret = auth_config.get("jwt_secret")
    algorithm = auth_config.get("jwt_algorithm", "HS256")
    ttl_minutes = int(auth_config.get("token_ttl_minutes", 1440))
    if not secret:
        raise RuntimeError("auth.jwt_secret is required")

    now = datetime.now(timezone.utc)
    payload = {
        "sub": str(user_id),
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=ttl_minutes)).timestamp()),
    }
    return jwt.encode(payload, secret, algorithm=algorithm)


def decode_access_token(config: dict[str, Any], token: str) -> dict[str, Any]:
    auth_config = config.get("auth", {})
    secret = auth_config.get("jwt_secret")
    algorithm = auth_config.get("jwt_algorithm", "HS256")
    if not secret:
        raise RuntimeError("auth.jwt_secret is required")

    try:
        return jwt.decode(token, secret, algorithms=[algorithm])
    except jwt.PyJWTError as error:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
        ) from error


def get_current_user_dependency(config: dict[str, Any]):
    def _get_current_user(
        credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
        db: Session = Depends(get_db),
    ) -> User:
        token = credentials.credentials
        payload = decode_access_token(config=config, token=token)
        user_id = payload.get("sub")
        if user_id is None:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token payload")

        user = db.execute(select(User).where(User.id == int(user_id))).scalar_one_or_none()
        if user is None:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        return user

    return _get_current_user
