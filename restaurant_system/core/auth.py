import hashlib
import secrets
from typing import Optional, Dict

def hash_password(password: str, salt: Optional[str] = None) -> tuple[str, str]:
    """สร้าง Password Hash ที่ปลอดภัยด้วย Salt"""
    if not salt:
        salt = secrets.token_hex(16)
    hashed = hashlib.sha256((password + salt).encode('utf-8')).hexdigest()
    return hashed, salt

def verify_password(password: str, hashed: str, salt: str) -> bool:
    check_hash, _ = hash_password(password, salt)
    return secrets.compare_digest(check_hash, hashed)

def create_access_token(user_id: str, role: str) -> str:
    """สร้าง Session Token แบบรวดเร็วและปลอดภัย"""
    token = secrets.token_urlsafe(32)
    return f"{user_id}:{role}:{token}"

def parse_token(token_str: str) -> Optional[Dict[str, str]]:
    try:
        parts = token_str.split(":")
        if len(parts) >= 2:
            return {"user_id": parts[0], "role": parts[1]}
        return None
    except Exception:
        return None

def check_permission(user_role: str, allowed_roles: list[str]) -> bool:
    """ตรวจสอบสิทธิ์ตามเกณฑ์ RBAC"""
    if "all" in allowed_roles:
        return True
    return user_role in allowed_roles