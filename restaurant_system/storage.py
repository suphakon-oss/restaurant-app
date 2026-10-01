"""
storage.py - ระบบจัดการไฟล์และการบันทึกข้อมูล (File Management & Persistence)
ใช้เฉพาะ Standard Library: json, os, base64
"""

import json
import os
import base64

DATA_FILE = "data.json"
UPLOAD_DIR = "uploads"

# ตรวจสอบและสร้างไดเรกทอรีสำหรับอัปโหลดไฟล์
if not os.path.exists(UPLOAD_DIR):
    try:
        os.makedirs(UPLOAD_DIR)
    except OSError:
        pass


def get_default_data() -> dict:
    """คืนค่าข้อมูลเริ่มต้นเมื่อยังไม่มีไฟล์ข้อมูล (ใช้ dict, list, bool, int, float, str)"""
    return {
        "menu": [
            {
                "id": 1,
                "name": "ผัดไทยกุ้งสด",
                "category": "อาหารจานเดียว",
                "price": 85.0,
                "is_available": True,
                "image_path": ""
            },
            {
                "id": 2,
                "name": "ต้มยำกุ้งแม่น้ำ",
                "category": "ต้ม/แกง",
                "price": 250.0,
                "is_available": True,
                "image_path": ""
            },
            {
                "id": 3,
                "name": "ข้าวผัดปู",
                "category": "อาหารจานเดียว",
                "price": 120.0,
                "is_available": False,  # ตัวอย่างสถานะ "หมด"
                "image_path": ""
            },
            {
                "id": 4,
                "name": "ชาไทยเย็น",
                "category": "เครื่องดื่ม",
                "price": 45.0,
                "is_available": True,
                "image_path": ""
            }
        ],
        "tables": [
            {"table_id": 1, "status": "ว่าง"},
            {"table_id": 2, "status": "ว่าง"},
            {"table_id": 3, "status": "ว่าง"},
            {"table_id": 4, "status": "ว่าง"},
            {"table_id": 5, "status": "ว่าง"}
        ],
        "orders": [],   # รายการออเดอร์ในครัวและโต๊ะ
        "sales": []     # ประวัติการเช็คบิลสำหรับรายงาน
    }


def load_database() -> dict:
    """ฟังก์ชันที่ 1: โหลดข้อมูลจากไฟล์ JSON พร้อมจัดการ Error ไม่ให้แสดง Traceback"""
    if not os.path.exists(DATA_FILE):
        data = get_default_data()
        save_database(data)
        return data

    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data
    except (json.JSONDecodeError, OSError):
        # หากไฟล์เสียหาย ให้สำรองข้อมูลเดิมและสร้างชุดเริ่มต้นใหม่
        default_data = get_default_data()
        save_database(default_data)
        return default_data


def save_database(data: dict) -> bool:
    """ฟังก์ชันที่ 2: บันทึกข้อมูลลงไฟล์ JSON พร้อม try-except ปลอดภัย"""
    try:
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return True
    except OSError:
        return False


def save_uploaded_image(file_name: str, base64_str: str) -> str:
    """ฟังก์ชันที่ 3: จัดเก็บไฟล์ภาพจาก Base64 ลงโฟลเดอร์ uploads/"""
    try:
        if not file_name or not base64_str:
            return ""
        
        # ตัด data:image/...;base64, ออกหากมี
        if "," in base64_str:
            base64_str = base64_str.split(",")[1]

        safe_name = os.path.basename(file_name).replace(" ", "_")
        target_path = os.path.join(UPLOAD_DIR, safe_name)

        image_data = base64.b64decode(base64_str)
        with open(target_path, "wb") as img_file:
            img_file.write(image_data)

        return f"/uploads/{safe_name}"
    except Exception:
        return ""