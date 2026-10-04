"""
core/database.py - ระบบจัดการฐานข้อมูล 10 เมนูหลัก พร้อมรูปภาพอาหารตรงปกจาก Wikimedia Commons
"""

import os
import json
import time
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional
from core.auth import hash_password

IS_VERCEL = bool(os.environ.get("VERCEL"))
DATA_PATH = "/tmp/restaurant_data.json" if IS_VERCEL else "restaurant_data.json"
BACKUP_PATH = DATA_PATH + ".bak"

_db_lock = threading.Lock()
_DB_CACHE: Optional[Dict[str, Any]] = None

def get_initial_schema() -> Dict[str, Any]:
    admin_h, admin_s = hash_password("admin123")
    staff_h, staff_s = hash_password("staff123")
    cust_h, cust_s = hash_password("customer123")

    inventory = [
        {"id": "ing_1", "name": "หมูสับ", "stock": 10000, "unit": "กรัม", "min_stock": 1000},
        {"id": "ing_2", "name": "เนื้อไก่", "stock": 10000, "unit": "กรัม", "min_stock": 1000},
        {"id": "ing_3", "name": "หมูกรอบ", "stock": 5000, "unit": "กรัม", "min_stock": 500},
        {"id": "ing_4", "name": "หมูชิ้น", "stock": 8000, "unit": "กรัม", "min_stock": 1000},
        {"id": "ing_5", "name": "กุ้งสด", "stock": 500, "unit": "ตัว", "min_stock": 50},
        {"id": "ing_6", "name": "ปลากะพง", "stock": 40, "unit": "ตัว", "min_stock": 5},
        {"id": "ing_7", "name": "ไข่ไก่", "stock": 300, "unit": "ฟอง", "min_stock": 30},
        {"id": "ing_8", "name": "ข้าวสวย", "stock": 200, "unit": "จาน", "min_stock": 20},
        {"id": "ing_9", "name": "เส้นใหญ่", "stock": 6000, "unit": "กรัม", "min_stock": 600},
        {"id": "ing_10", "name": "ใบกะเพรา", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_11", "name": "กระเทียม", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_12", "name": "พริกสด", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_13", "name": "ผักคะน้า", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_14", "name": "ผักบุ้งจีน", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_15", "name": "มะเขือเทศ", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_16", "name": "มะละกอขูด", "stock": 4000, "unit": "กรัม", "min_stock": 400},
        {"id": "ing_17", "name": "ถั่วฝักยาว", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_18", "name": "มะเขือเปราะ", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_19", "name": "เห็ดฟาง", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_20", "name": "ข่า ตะไคร้ ใบมะกรูด", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_21", "name": "มะนาว", "stock": 200, "unit": "ลูก", "min_stock": 20},
        {"id": "ing_22", "name": "กะทิ", "stock": 5000, "unit": "มล.", "min_stock": 500},
        {"id": "ing_23", "name": "พริกแกงเขียวหวาน", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_24", "name": "น้ำมันหอย", "stock": 5000, "unit": "มล.", "min_stock": 500},
        {"id": "ing_25", "name": "น้ำปลา", "stock": 5000, "unit": "มล.", "min_stock": 500},
        {"id": "ing_26", "name": "ซีอิ๊วดำ", "stock": 5000, "unit": "มล.", "min_stock": 500},
        {"id": "ing_27", "name": "ซีอิ๊วขาว", "stock": 5000, "unit": "มล.", "min_stock": 500},
        {"id": "ing_28", "name": "เต้าเจี้ยว", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_29", "name": "น้ำตาลทราย", "stock": 5000, "unit": "กรัม", "min_stock": 500},
        {"id": "ing_30", "name": "ถั่วลิสงและกุ้งแห้ง", "stock": 3000, "unit": "กรัม", "min_stock": 300}
    ]

    menu = [
        {"id": 1, "name": "กะเพราหมูสับ", "category": "ผัด / จานเดียว", "price": 65.0, "is_available": True, "image": "https://upload.wikimedia.org/wikipedia/commons/thumb/6/69/2017_0426_Mu_krop_phat_kaphrao_khai_dao_rat_khao_in_Ayutthaya.jpg/640px-2017_0426_Mu_krop_phat_kaphrao_khai_dao_rat_khao_in_Ayutthaya.jpg", "recipe": [{"ingredient_id": "ing_1", "amount": 100}, {"ingredient_id": "ing_10", "amount": 30}, {"ingredient_id": "ing_11", "amount": 20}, {"ingredient_id": "ing_12", "amount": 20}, {"ingredient_id": "ing_24", "amount": 15}, {"ingredient_id": "ing_25", "amount": 15}]},
        {"id": 2, "name": "ไก่กระเทียม", "category": "ผัด / จานเดียว", "price": 60.0, "is_available": True, "image": "https://upload.wikimedia.org/wikipedia/commons/thumb/2/24/Kai_thot_takhrai.jpg/640px-Kai_thot_takhrai.jpg", "recipe": [{"ingredient_id": "ing_2", "amount": 100}, {"ingredient_id": "ing_11", "amount": 30}, {"ingredient_id": "ing_26", "amount": 10}, {"ingredient_id": "ing_24", "amount": 15}, {"ingredient_id": "ing_25", "amount": 15}]},
        {"id": 3, "name": "คะน้าหมูกรอบ", "category": "ผัด / จานเดียว", "price": 75.0, "is_available": True, "image": "https://upload.wikimedia.org/wikipedia/commons/thumb/1/12/Phat_khana_mu_krop.jpg/640px-Phat_khana_mu_krop.jpg", "recipe": [{"ingredient_id": "ing_3", "amount": 80}, {"ingredient_id": "ing_13", "amount": 50}, {"ingredient_id": "ing_11", "amount": 20}, {"ingredient_id": "ing_12", "amount": 20}, {"ingredient_id": "ing_24", "amount": 15}, {"ingredient_id": "ing_28", "amount": 20}]},
        {"id": 4, "name": "ผัดผักบุ้งไฟแดง", "category": "ผัด / จานเดียว", "price": 60.0, "is_available": True, "image": "https://upload.wikimedia.org/wikipedia/commons/thumb/1/19/Pak_boong_fai_daeng.jpg/640px-Pak_boong_fai_daeng.jpg", "recipe": [{"ingredient_id": "ing_14", "amount": 80}, {"ingredient_id": "ing_12", "amount": 20}, {"ingredient_id": "ing_11", "amount": 20}, {"ingredient_id": "ing_28", "amount": 20}, {"ingredient_id": "ing_24", "amount": 15}]},
        {"id": 5, "name": "ข้าวผัดหมู / ไก่", "category": "ผัด / จานเดียว", "price": 60.0, "is_available": True, "image": "https://upload.wikimedia.org/wikipedia/commons/thumb/7/72/Khao_Phat_Kung.jpg/640px-Khao_Phat_Kung.jpg", "recipe": [{"ingredient_id": "ing_8", "amount": 1}, {"ingredient_id": "ing_4", "amount": 80}, {"ingredient_id": "ing_7", "amount": 1}, {"ingredient_id": "ing_15", "amount": 30}, {"ingredient_id": "ing_13", "amount": 30}, {"ingredient_id": "ing_27", "amount": 15}]},
        {"id": 6, "name": "ผัดซีอิ๊วเส้นใหญ่หมู", "category": "ผัด / จานเดียว", "price": 65.0, "is_available": True, "image": "https://upload.wikimedia.org/wikipedia/commons/thumb/3/34/Sriwan_KuayTiow.JPG/640px-Sriwan_KuayTiow.JPG", "recipe": [{"ingredient_id": "ing_9", "amount": 150}, {"ingredient_id": "ing_4", "amount": 80}, {"ingredient_id": "ing_13", "amount": 40}, {"ingredient_id": "ing_7", "amount": 1}, {"ingredient_id": "ing_26", "amount": 15}, {"ingredient_id": "ing_27", "amount": 15}]},
        {"id": 7, "name": "ต้มยำกุ้งน้ำข้น", "category": "แกง / ต้ม", "price": 180.0, "is_available": True, "image": "https://upload.wikimedia.org/wikipedia/commons/thumb/e/e8/Tom_yam_kung_maenam.jpg/640px-Tom_yam_kung_maenam.jpg", "recipe": [{"ingredient_id": "ing_5", "amount": 4}, {"ingredient_id": "ing_19", "amount": 40}, {"ingredient_id": "ing_20", "amount": 40}, {"ingredient_id": "ing_12", "amount": 20}, {"ingredient_id": "ing_21", "amount": 1}, {"ingredient_id": "ing_25", "amount": 20}]},
        {"id": 8, "name": "แกงเขียวหวานไก่", "category": "แกง / ต้ม", "price": 110.0, "is_available": True, "image": "https://upload.wikimedia.org/wikipedia/commons/thumb/e/e5/Thai_green_chicken_curry_and_roti.jpg/640px-Thai_green_chicken_curry_and_roti.jpg", "recipe": [{"ingredient_id": "ing_2", "amount": 100}, {"ingredient_id": "ing_18", "amount": 40}, {"ingredient_id": "ing_23", "amount": 25}, {"ingredient_id": "ing_22", "amount": 50}, {"ingredient_id": "ing_25", "amount": 15}]},
        {"id": 9, "name": "ปลากะพงทอดน้ำปลา", "category": "ทอด / ย่าง / อบ", "price": 320.0, "is_available": True, "image": "https://upload.wikimedia.org/wikipedia/commons/thumb/0/0d/Pla_nin_thot.jpg/640px-Pla_nin_thot.jpg", "recipe": [{"ingredient_id": "ing_6", "amount": 1}, {"ingredient_id": "ing_25", "amount": 30}, {"ingredient_id": "ing_29", "amount": 20}]},
        {"id": 10, "name": "ส้มตำไทย", "category": "ยำ / ตำ / ลาบ", "price": 60.0, "is_available": True, "image": "https://upload.wikimedia.org/wikipedia/commons/thumb/a/a3/Somtam_kaiyang_khaoniaow.jpg/640px-Somtam_kaiyang_khaoniaow.jpg", "recipe": [{"ingredient_id": "ing_16", "amount": 100}, {"ingredient_id": "ing_15", "amount": 30}, {"ingredient_id": "ing_17", "amount": 30}, {"ingredient_id": "ing_30", "amount": 20}, {"ingredient_id": "ing_12", "amount": 20}, {"ingredient_id": "ing_21", "amount": 1}, {"ingredient_id": "ing_25", "amount": 15}]}
    ]

    return {
        "schema_version": 5,
        "users": [
            {"id": "u1", "username": "admin", "password_hash": admin_h, "salt": admin_s, "role": "admin", "name": "ผู้ดูแลระบบ (Admin)"},
            {"id": "u2", "username": "staff", "password_hash": staff_h, "salt": staff_s, "role": "staff", "name": "พนักงานหน้าร้าน (Staff)"},
            {"id": "u3", "username": "customer", "password_hash": cust_h, "salt": cust_s, "role": "customer", "name": "คุณสมชาย (ลูกค้าประจำ)", "registered_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
        ],
        "inventory": inventory,
        "menu": menu,
        "tables": [
            {"table_id": 1, "status": "ว่าง", "capacity": 4},
            {"table_id": 2, "status": "ว่าง", "capacity": 2},
            {"table_id": 3, "status": "ว่าง", "capacity": 6},
            {"table_id": 4, "status": "ว่าง", "capacity": 4},
            {"table_id": 5, "status": "ว่าง", "capacity": 8}
        ],
        "orders": [],
        "reservations": [],
        "queues": [],
        "members": [{"phone": "0812345678", "name": "คุณสมชาย", "points": 150}],
        "audit_logs": [{"timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "user": "System", "action": "SYSTEM_START", "details": "เริ่มต้นระบบสำเร็จ"}],
        "sales": []
    }

def load_db() -> Dict[str, Any]:
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

        for _ in range(5):
            try:
                with open(DATA_PATH, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    
                    # อัปเกรดรูปภาพเมนูให้ตรงปกทันที โดยไม่แตะต้อง users, sales, หรือ logs เดิม
                    if data.get("schema_version", 0) < 5:
                        init_data = get_initial_schema()
                        data["menu"] = init_data["menu"]
                        data["inventory"] = init_data["inventory"]
                        data["schema_version"] = 5
                        with open(DATA_PATH, "w", encoding="utf-8") as fw:
                            json.dump(data, fw, ensure_ascii=False, indent=2)

                    _DB_CACHE = data
                    return data
            except Exception:
                time.sleep(0.05)

        if os.path.exists(BACKUP_PATH):
            try:
                with open(BACKUP_PATH, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    _DB_CACHE = data
                    return data
            except Exception:
                pass

        if _DB_CACHE is not None:
            return _DB_CACHE
        return get_initial_schema()

def save_db(data: Dict[str, Any]) -> bool:
    global _DB_CACHE
    with _db_lock:
        try:
            _DB_CACHE = data
            if os.path.exists(DATA_PATH):
                try:
                    with open(DATA_PATH, "r", encoding="utf-8") as src, open(BACKUP_PATH, "w", encoding="utf-8") as dst:
                        dst.write(src.read())
                except Exception:
                    pass

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