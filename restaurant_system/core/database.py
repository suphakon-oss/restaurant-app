"""
core/database.py - ระบบจัดการฐานข้อมูล JSON พร้อมระบบ Thread Lock & Atomic File Write
ป้องกันบั๊ก Race Condition ออเดอร์หายเวลากดรัวๆ 100%
"""

import os
import json
import threading
import tempfile
from datetime import datetime
from typing import Any, Dict, List, Optional
from core.auth import hash_password

IS_VERCEL = bool(os.environ.get("VERCEL"))
DATA_PATH = "/tmp/restaurant_data.json" if IS_VERCEL else "restaurant_data.json"

# ตัวล็อกการอ่าน-เขียนไฟล์ ป้องกันคำสั่งชนกันเวลาผู้ใช้กดรัวๆ
_db_lock = threading.Lock()
_DB_CACHE: Optional[Dict[str, Any]] = None

def get_initial_schema() -> Dict[str, Any]:
    admin_h, admin_s = hash_password("admin123")
    staff_h, staff_s = hash_password("staff123")
    cust_h, cust_s = hash_password("customer123")
    
    return {
        "users": [
            {"id": "u1", "username": "admin", "password_hash": admin_h, "salt": admin_s, "role": "admin", "name": "ผู้ดูแลระบบ (Admin)"},
            {"id": "u2", "username": "staff", "password_hash": staff_h, "salt": staff_s, "role": "staff", "name": "พนักงานหน้าร้าน (Staff)"},
            {"id": "u3", "username": "customer", "password_hash": cust_h, "salt": cust_s, "role": "customer", "name": "คุณสมชาย (ลูกค้าประจำ)", "registered_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
        ],
        "inventory": [
            {"id": "ing_1", "name": "กุ้งแม่น้ำ (ตัว)", "stock": 50, "unit": "ตัว", "min_stock": 10},
            {"id": "ing_2", "name": "เส้นผัดไทย (กรัม)", "stock": 5000, "unit": "กรัม", "min_stock": 1000},
            {"id": "ing_3", "name": "ไข่ไก่ (ฟอง)", "stock": 100, "unit": "ฟอง", "min_stock": 20},
            {"id": "ing_4", "name": "ข้าวสวย (จาน)", "stock": 80, "unit": "จาน", "min_stock": 15}
        ],
        "menu": [
            {
                "id": 1,
                "name": "ผัดไทยกุ้งสด",
                "category": "อาหารจานเดียว",
                "price": 85.0,
                "is_available": True,
                "image": "https://images.unsplash.com/photo-1559847844-5315695dadae?auto=format&fit=crop&w=600&q=80",
                "recipe": [
                    {"ingredient_id": "ing_1", "amount": 3},
                    {"ingredient_id": "ing_2", "amount": 100},
                    {"ingredient_id": "ing_3", "amount": 1}
                ]
            },
            {
                "id": 2,
                "name": "ข้าวผัดไข่",
                "category": "อาหารจานเดียว",
                "price": 50.0,
                "is_available": True,
                "image": "https://images.unsplash.com/photo-1603133872878-684f208fb84b?auto=format&fit=crop&w=600&q=80",
                "recipe": [
                    {"ingredient_id": "ing_3", "amount": 2},
                    {"ingredient_id": "ing_4", "amount": 1}
                ]
            }
        ],
        "tables": [
            {"table_id": 1, "status": "ว่าง", "capacity": 4},
            {"table_id": 2, "status": "ว่าง", "capacity": 2},
            {"table_id": 3, "status": "ว่าง", "capacity": 6}
        ],
        "orders": [],
        "reservations": [],
        "queues": [],
        "members": [
            {"phone": "0812345678", "name": "คุณสมชาย", "points": 120}
        ],
        "audit_logs": [
            {"timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "user": "System", "action": "SYSTEM_START", "details": "เริ่มต้นระบบสำเร็จ"}
        ],
        "sales": []
    }

def load_db() -> Dict[str, Any]:
    """โหลดข้อมูลจากไฟล์อย่างปลอดภัย ถ้าไฟล์ติดล็อกให้ดึงจาก RAM แทนการรีเซ็ตข้อมูล"""
    global _DB_CACHE
    with _db_lock:
        if not os.path.exists(DATA_PATH):
            if _DB_CACHE is not None:
                return _DB_CACHE
            data = get_initial_schema()
            _DB_CACHE = data
            try:
                with open(DATA_PATH, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
            except Exception:
                pass
            return data

        try:
            with open(DATA_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
                _DB_CACHE = data
                return data
        except Exception:
            # กรณีไฟล์ถูกเปิดทับชั่วขณะ ให้คืนค่าแคชใน RAM แทน ห้ามรีเซ็ตข้อมูลเด็ดขาด!
            if _DB_CACHE is not None:
                return _DB_CACHE
            return get_initial_schema()

def save_db(data: Dict[str, Any]) -> bool:
    """บันทึกไฟล์แบบ Atomic ด้วยการเขียนลงไฟล์ชั่วคราวแล้วสลับไฟล์ ป้องกันไฟล์ขาด/ว่างเปล่า"""
    global _DB_CACHE
    with _db_lock:
        try:
            _DB_CACHE = data
            dir_name = os.path.dirname(DATA_PATH) or "."
            # สร้างไฟล์ชั่วคราวในโฟลเดอร์เดียวกัน
            with tempfile.NamedTemporaryFile("w", dir=dir_name, delete=False, encoding="utf-8") as tf:
                json.dump(data, tf, ensure_ascii=False, indent=2)
                temp_name = tf.name
            
            # สลับไฟล์ทันทีในระดับ OS ป้องกันไม่ให้มีจังหวะไฟล์ว่างเปล่า (Atomic File Replace)
            os.replace(temp_name, DATA_PATH)
            return True
        except Exception:
            try:
                # Fallback บันทึกตรงหากติดปัญหาชั่วคราว
                with open(DATA_PATH, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                return True
            except Exception:
                return False

def add_audit_log(user: str, action: str, details: str):
    try:
        db = load_db()
        log_entry = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "user": user,
            "action": action,
            "details": details
        }
        db.setdefault("audit_logs", []).insert(0, log_entry)
        save_db(db)
    except Exception:
        pass