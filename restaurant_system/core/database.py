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
        {"id": "ing_1", "name": "กระดูกอ่อนหมู", "stock": 5000, "unit": "กรัม", "min_stock": 500},
        {"id": "ing_2", "name": "กระชาย", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_3", "name": "กระเทียม", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_4", "name": "กระเทียมดอง", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_5", "name": "กระเทียมเจียว", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_6", "name": "กะทิ", "stock": 5000, "unit": "มล.", "min_stock": 500},
        {"id": "ing_7", "name": "กะปิ", "stock": 4000, "unit": "กรัม", "min_stock": 400},
        {"id": "ing_8", "name": "กุ้งสด", "stock": 500, "unit": "ตัว", "min_stock": 50},
        {"id": "ing_9", "name": "กุ้งแห้ง", "stock": 2000, "unit": "กรัม", "min_stock": 200},
        {"id": "ing_10", "name": "กุ้งแม่น้ำ", "stock": 200, "unit": "ตัว", "min_stock": 20},
        {"id": "ing_11", "name": "ข่า", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_12", "name": "ข้าวคั่ว", "stock": 4000, "unit": "กรัม", "min_stock": 400},
        {"id": "ing_13", "name": "ข้าวสวย", "stock": 200, "unit": "จาน", "min_stock": 20},
        {"id": "ing_14", "name": "ข้าวโพดอ่อน", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_15", "name": "ขิง", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_16", "name": "ขึ้นฉ่าย", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_17", "name": "คอหมู", "stock": 5000, "unit": "กรัม", "min_stock": 500},
        {"id": "ing_18", "name": "งาขาว", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_19", "name": "ชะอม", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_20", "name": "ซีอิ๊วขาว", "stock": 5000, "unit": "มล.", "min_stock": 500},
        {"id": "ing_21", "name": "ซีอิ๊วดำ", "stock": 5000, "unit": "มล.", "min_stock": 500},
        {"id": "ing_22", "name": "ซอสปรุงรส", "stock": 5000, "unit": "มล.", "min_stock": 500},
        {"id": "ing_23", "name": "ซอสบาร์บีคิว", "stock": 5000, "unit": "มล.", "min_stock": 500},
        {"id": "ing_24", "name": "ซอสมะเขือเทศ", "stock": 5000, "unit": "มล.", "min_stock": 500},
        {"id": "ing_25", "name": "ซุปก้อน", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_26", "name": "ดอกกะหล่ำ", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_27", "name": "ตะไคร้", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_28", "name": "ต้นหอม", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_29", "name": "ถั่วฝักยาว", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_30", "name": "ถั่วลิสง", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_31", "name": "นมข้นจืด", "stock": 5000, "unit": "มล.", "min_stock": 500},
        {"id": "ing_32", "name": "น้ำตาล", "stock": 4000, "unit": "กรัม", "min_stock": 400},
        {"id": "ing_33", "name": "น้ำตาลปี๊บ", "stock": 4000, "unit": "กรัม", "min_stock": 400},
        {"id": "ing_34", "name": "น้ำปลา", "stock": 5000, "unit": "มล.", "min_stock": 500},
        {"id": "ing_35", "name": "น้ำปลาร้า", "stock": 5000, "unit": "มล.", "min_stock": 500},
        {"id": "ing_36", "name": "น้ำพริกเผา", "stock": 5000, "unit": "มล.", "min_stock": 500},
        {"id": "ing_37", "name": "น้ำมะขามเปียก", "stock": 5000, "unit": "มล.", "min_stock": 500},
        {"id": "ing_38", "name": "น้ำมันพืช", "stock": 5000, "unit": "มล.", "min_stock": 500},
        {"id": "ing_39", "name": "น้ำมันหอย", "stock": 5000, "unit": "มล.", "min_stock": 500},
        {"id": "ing_40", "name": "บวบ", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_41", "name": "บล็อคโคลี่", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_42", "name": "ปลากะพง", "stock": 40, "unit": "ตัว", "min_stock": 5},
        {"id": "ing_43", "name": "ปลาดุกย่าง", "stock": 60, "unit": "ตัว", "min_stock": 10},
        {"id": "ing_44", "name": "ปลาทับทิม", "stock": 40, "unit": "ตัว", "min_stock": 5},
        {"id": "ing_45", "name": "ปลาริวกิว", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_46", "name": "ปลาสำลี", "stock": 40, "unit": "ตัว", "min_stock": 5},
        {"id": "ing_47", "name": "ปลาหมึก", "stock": 5000, "unit": "กรัม", "min_stock": 500},
        {"id": "ing_48", "name": "ปลาช่อน", "stock": 50, "unit": "ตัว", "min_stock": 5},
        {"id": "ing_49", "name": "ปีกไก่", "stock": 200, "unit": "ชิ้น", "min_stock": 20},
        {"id": "ing_50", "name": "ผงปาปริก้า", "stock": 4000, "unit": "กรัม", "min_stock": 400},
        {"id": "ing_51", "name": "ผักกาดขาว", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_52", "name": "ผักคะน้า", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_53", "name": "ผักชีฝรั่ง", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_54", "name": "ผักบุ้งจีน", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_55", "name": "พริก", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_56", "name": "พริกชี้ฟ้า", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_57", "name": "พริกป่น", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_58", "name": "พริกแกงคั่ว", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_59", "name": "พริกแกงป่า", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_60", "name": "พริกแกงมัสมั่น", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_61", "name": "พริกแกงส้ม", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_62", "name": "พริกแกงเขียวหวาน", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_63", "name": "พริกแกงเผ็ด", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_64", "name": "พริกไทย", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_65", "name": "พริกไทยดำ", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_66", "name": "พริกไทยอ่อน", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_67", "name": "พริกแห้ง", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_68", "name": "ฟักทอง", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_69", "name": "มะขามเปียก", "stock": 5000, "unit": "มล.", "min_stock": 500},
        {"id": "ing_70", "name": "มะเขือเทศ", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_71", "name": "มะเขือเปราะ", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_72", "name": "มะนาว", "stock": 200, "unit": "ลูก", "min_stock": 20},
        {"id": "ing_73", "name": "มะระจีน", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_74", "name": "มะละกอ", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_75", "name": "มะม่วงดิบ", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_76", "name": "มันเทศ", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_77", "name": "ยี่หร่า", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_78", "name": "ยอดมะพร้าว", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_79", "name": "วุ้นเส้น", "stock": 4000, "unit": "กรัม", "min_stock": 400},
        {"id": "ing_80", "name": "สะระแหน่", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_81", "name": "สับปะรด", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_82", "name": "หน่อไม้", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_83", "name": "หมูกรอบ", "stock": 5000, "unit": "กรัม", "min_stock": 500},
        {"id": "ing_84", "name": "หมูชิ้น", "stock": 8000, "unit": "กรัม", "min_stock": 1000},
        {"id": "ing_85", "name": "หมูสับ", "stock": 10000, "unit": "กรัม", "min_stock": 1000},
        {"id": "ing_86", "name": "หมูสามชั้น", "stock": 6000, "unit": "กรัม", "min_stock": 600},
        {"id": "ing_87", "name": "หมูหมัก", "stock": 5000, "unit": "กรัม", "min_stock": 500},
        {"id": "ing_88", "name": "หมูยอ", "stock": 4000, "unit": "กรัม", "min_stock": 400},
        {"id": "ing_89", "name": "หอมเจียว", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_90", "name": "หอมแดง", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_91", "name": "หอมใหญ่", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_92", "name": "หอยขม", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_93", "name": "เกลือ", "stock": 4000, "unit": "กรัม", "min_stock": 400},
        {"id": "ing_94", "name": "เนย", "stock": 4000, "unit": "กรัม", "min_stock": 400},
        {"id": "ing_95", "name": "เป็ดย่าง", "stock": 4000, "unit": "กรัม", "min_stock": 400},
        {"id": "ing_96", "name": "เม็ดมะม่วงหิมพานต์", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_97", "name": "เต้าหู้ไข่", "stock": 100, "unit": "หลอด", "min_stock": 10},
        {"id": "ing_98", "name": "เต้าเจี้ยว", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_99", "name": "เนื้อปลากราย", "stock": 4000, "unit": "กรัม", "min_stock": 400},
        {"id": "ing_100", "name": "เนื้อวัว", "stock": 6000, "unit": "กรัม", "min_stock": 500},
        {"id": "ing_101", "name": "เนื้อหมู", "stock": 6000, "unit": "กรัม", "min_stock": 500},
        {"id": "ing_102", "name": "เนื้อไก่", "stock": 10000, "unit": "กรัม", "min_stock": 1000},
        {"id": "ing_103", "name": "เห็ดฟาง", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_104", "name": "เห็ดหอม", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_105", "name": "เส้นปลา", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_106", "name": "เส้นมาม่า", "stock": 150, "unit": "ซอง", "min_stock": 20},
        {"id": "ing_107", "name": "เส้นใหญ่", "stock": 6000, "unit": "กรัม", "min_stock": 600},
        {"id": "ing_108", "name": "แครอท", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_109", "name": "แป้งทอดกรอบ", "stock": 4000, "unit": "กรัม", "min_stock": 400},
        {"id": "ing_110", "name": "แป้งมัน", "stock": 4000, "unit": "กรัม", "min_stock": 400},
        {"id": "ing_111", "name": "ไข่ชะอม", "stock": 100, "unit": "ชิ้น", "min_stock": 10},
        {"id": "ing_112", "name": "ไข่เค็มแดง", "stock": 150, "unit": "ฟอง", "min_stock": 20},
        {"id": "ing_113", "name": "ไข่ไก่", "stock": 300, "unit": "ฟอง", "min_stock": 30},
        {"id": "ing_114", "name": "ใบกะเพรา", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_115", "name": "ใบชะพลู", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_116", "name": "ใบมะกรูด", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_117", "name": "ใบแมงลัก", "stock": 3000, "unit": "กรัม", "min_stock": 300},
        {"id": "ing_118", "name": "ใบโหระพา", "stock": 3000, "unit": "กรัม", "min_stock": 300}
    ]

    menu = [
        {"id": 1, "name": "กะเพราหมูสับ", "category": "ผัด / จานเดียว", "price": 65.0, "is_available": True, "image": "https://images.unsplash.com/photo-1563379091339-03b21ab4a4f8?w=600&auto=format&fit=crop&q=80", "recipe": [{"ingredient_id": "ing_85", "amount": 100}, {"ingredient_id": "ing_114", "amount": 30}, {"ingredient_id": "ing_55", "amount": 30}, {"ingredient_id": "ing_3", "amount": 30}, {"ingredient_id": "ing_39", "amount": 15}, {"ingredient_id": "ing_34", "amount": 15}, {"ingredient_id": "ing_32", "amount": 10}]},
        {"id": 2, "name": "ไก่กระเทียม", "category": "ผัด / จานเดียว", "price": 60.0, "is_available": True, "image": "https://images.unsplash.com/photo-1604908176997-125f25cc6f3d?w=600&auto=format&fit=crop&q=80", "recipe": [{"ingredient_id": "ing_102", "amount": 100}, {"ingredient_id": "ing_3", "amount": 30}, {"ingredient_id": "ing_21", "amount": 15}, {"ingredient_id": "ing_39", "amount": 15}, {"ingredient_id": "ing_34", "amount": 15}, {"ingredient_id": "ing_64", "amount": 30}]},
        {"id": 3, "name": "คะน้าหมูกรอบ", "category": "ผัด / จานเดียว", "price": 75.0, "is_available": True, "image": "https://images.unsplash.com/photo-1546069901-ba9599a7e63c?w=600&auto=format&fit=crop&q=80", "recipe": [{"ingredient_id": "ing_83", "amount": 80}, {"ingredient_id": "ing_52", "amount": 30}, {"ingredient_id": "ing_3", "amount": 30}, {"ingredient_id": "ing_55", "amount": 30}, {"ingredient_id": "ing_39", "amount": 15}, {"ingredient_id": "ing_98", "amount": 30}, {"ingredient_id": "ing_32", "amount": 10}]},
        {"id": 4, "name": "ผัดบุ้งไฟแดง", "category": "ผัด / จานเดียว", "price": 60.0, "is_available": True, "image": "https://images.unsplash.com/photo-1540420773420-3366772f4999?w=600&auto=format&fit=crop&q=80", "recipe": [{"ingredient_id": "ing_54", "amount": 30}, {"ingredient_id": "ing_55", "amount": 30}, {"ingredient_id": "ing_3", "amount": 30}, {"ingredient_id": "ing_98", "amount": 30}, {"ingredient_id": "ing_39", "amount": 15}, {"ingredient_id": "ing_20", "amount": 15}]},
        {"id": 5, "name": "ผัดพริกแกงหมู", "category": "ผัด / จานเดียว", "price": 65.0, "is_available": True, "image": "https://images.unsplash.com/photo-1598515214211-89d3c73ae83b?w=600&auto=format&fit=crop&q=80", "recipe": [{"ingredient_id": "ing_84", "amount": 100}, {"ingredient_id": "ing_63", "amount": 25}, {"ingredient_id": "ing_29", "amount": 30}, {"ingredient_id": "ing_116", "amount": 30}, {"ingredient_id": "ing_34", "amount": 15}, {"ingredient_id": "ing_33", "amount": 10}]},
        {"id": 6, "name": "ราดหน้าหมูนุ่ม", "category": "ผัด / จานเดียว", "price": 65.0, "is_available": True, "image": "https://images.unsplash.com/photo-1569718212165-3a8278d5f624?w=600&auto=format&fit=crop&q=80", "recipe": [{"ingredient_id": "ing_107", "amount": 150}, {"ingredient_id": "ing_87", "amount": 100}, {"ingredient_id": "ing_52", "amount": 30}, {"ingredient_id": "ing_98", "amount": 30}, {"ingredient_id": "ing_110", "amount": 10}, {"ingredient_id": "ing_4", "amount": 30}]},
        {"id": 7, "name": "ผัดฉ่าทะเล", "category": "ผัด / จานเดียว", "price": 150.0, "is_available": True, "image": "https://images.unsplash.com/photo-1534422298391-e4f8c172dddb?w=600&auto=format&fit=crop&q=80", "recipe": [{"ingredient_id": "ing_8", "amount": 4}, {"ingredient_id": "ing_47", "amount": 80}, {"ingredient_id": "ing_2", "amount": 30}, {"ingredient_id": "ing_66", "amount": 30}, {"ingredient_id": "ing_118", "amount": 30}, {"ingredient_id": "ing_56", "amount": 30}, {"ingredient_id": "ing_116", "amount": 30}]},
        {"id": 8, "name": "ปลาหมึกผัดไข่เค็ม", "category": "ผัด / จานเดียว", "price": 140.0, "is_available": True, "image": "https://images.unsplash.com/photo-1565557623262-b51c2513a641?w=600&auto=format&fit=crop&q=80", "recipe": [{"ingredient_id": "ing_47", "amount": 80}, {"ingredient_id": "ing_112", "amount": 1}, {"ingredient_id": "ing_91", "amount": 30}, {"ingredient_id": "ing_28", "amount": 30}, {"ingredient_id": "ing_56", "amount": 30}, {"ingredient_id": "ing_22", "amount": 15}]},
        {"id": 9, "name": "ข้าวผัดหมู / ไก่", "category": "ผัด / จานเดียว", "price": 60.0, "is_available": True, "image": "https://images.unsplash.com/photo-1603133872878-684f208fb84b?w=600&auto=format&fit=crop&q=80", "recipe": [{"ingredient_id": "ing_13", "amount": 1}, {"ingredient_id": "ing_84", "amount": 100}, {"ingredient_id": "ing_113", "amount": 1}, {"ingredient_id": "ing_70", "amount": 30}, {"ingredient_id": "ing_52", "amount": 30}, {"ingredient_id": "ing_91", "amount": 30}, {"ingredient_id": "ing_28", "amount": 30}]},
        {"id": 10, "name": "ผัดมาม่าขี้เมา", "category": "ผัด / จานเดียว", "price": 65.0, "is_available": True, "image": "https://images.unsplash.com/photo-1612927601601-6638404737ce?w=600&auto=format&fit=crop&q=80", "recipe": [{"ingredient_id": "ing_106", "amount": 1}, {"ingredient_id": "ing_85", "amount": 100}, {"ingredient_id": "ing_114", "amount": 30}, {"ingredient_id": "ing_66", "amount": 30}, {"ingredient_id": "ing_2", "amount": 30}, {"ingredient_id": "ing_55", "amount": 30}]},
        {"id": 11, "name": "ผัดผักรวมมิตร", "category": "ผัด / จานเดียว", "price": 70.0, "is_available": True, "image": "https://images.unsplash.com/photo-1512621776951-a57141f2eefd?w=600&auto=format&fit=crop&q=80", "recipe": [{"ingredient_id": "ing_41", "amount": 30}, {"ingredient_id": "ing_108", "amount": 30}, {"ingredient_id": "ing_26", "amount": 30}, {"ingredient_id": "ing_14", "amount": 30}, {"ingredient_id": "ing_8", "amount": 4}, {"ingredient_id": "ing_39", "amount": 15}]},
        {"id": 12, "name": "ไก่ผัดเม็ดมะม่วงหิมพานต์", "category": "ผัด / จานเดียว", "price": 120.0, "is_available": True, "image": "https://images.unsplash.com/photo-1525755662778-989d0524087e?w=600&auto=format&fit=crop&q=80", "recipe": [{"ingredient_id": "ing_102", "amount": 100}, {"ingredient_id": "ing_96", "amount": 30}, {"ingredient_id": "ing_91", "amount": 30}, {"ingredient_id": "ing_28", "amount": 30}, {"ingredient_id": "ing_67", "amount": 30}, {"ingredient_id": "ing_36", "amount": 15}]},
        {"id": 13, "name": "ผัดซีอิ๊วเส้นใหญ่", "category": "ผัด / จานเดียว", "price": 65.0, "is_available": True, "image": "https://images.unsplash.com/photo-1559847844-5315695dadae?w=600&auto=format&fit=crop&q=80", "recipe": [{"ingredient_id": "ing_107", "amount": 150}, {"ingredient_id": "ing_84", "amount": 100}, {"ingredient_id": "ing_52", "amount": 30}, {"ingredient_id": "ing_113", "amount": 1}, {"ingredient_id": "ing_21", "amount": 15}, {"ingredient_id": "ing_20", "amount": 15}, {"ingredient_id": "ing_32", "amount": 10}]},
        {"id": 14, "name": "หมูสับผัดพริกเกลือ", "category": "ผัด / จานเดียว", "price": 70.0, "is_available": True, "image": "https://images.unsplash.com/photo-1567620832903-9fc6debc209f?w=600&auto=format&fit=crop&q=80", "recipe": [{"ingredient_id": "ing_85", "amount": 100}, {"ingredient_id": "ing_3", "amount": 30}, {"ingredient_id": "ing_55", "amount": 30}, {"ingredient_id": "ing_93", "amount": 10}, {"ingredient_id": "ing_28", "amount": 30}]},
        {"id": 15, "name": "บล็อคโคลี่ผัดกุ้ง", "category": "ผัด / จานเดียว", "price": 90.0, "is_available": True, "image": "https://images.unsplash.com/photo-1589302168068-964664d93dc0?w=600&auto=format&fit=crop&q=80", "recipe": [{"ingredient_id": "ing_41", "amount": 30}, {"ingredient_id": "ing_8", "amount": 4}, {"ingredient_id": "ing_3", "amount": 30}, {"ingredient_id": "ing_39", "amount": 15}, {"ingredient_id": "ing_20", "amount": 15}]},
        {"id": 16, "name": "แกงเขียวหวานไก่", "category": "แกง / ต้ม", "price": 110.0, "is_available": True, "image": "https://images.unsplash.com/photo-1455619452474-d2be8b1e70cd?w=600&auto=format&fit=crop&q=80", "recipe": [{"ingredient_id": "ing_102", "amount": 100}, {"ingredient_id": "ing_71", "amount": 30}, {"ingredient_id": "ing_62", "amount": 25}, {"ingredient_id": "ing_6", "amount": 15}, {"ingredient_id": "ing_118", "amount": 30}, {"ingredient_id": "ing_116", "amount": 30}]},
        {"id": 17, "name": "แกงส้มชะอมกุ้ง", "category": "แกง / ต้ม", "price": 130.0, "is_available": True, "image": "https://images.unsplash.com/photo-1548943487-a2e4e43b4853?w=600&auto=format&fit=crop&q=80", "recipe": [{"ingredient_id": "ing_8", "amount": 4}, {"ingredient_id": "ing_19", "amount": 30}, {"ingredient_id": "ing_113", "amount": 1}, {"ingredient_id": "ing_61", "amount": 25}, {"ingredient_id": "ing_37", "amount": 15}, {"ingredient_id": "ing_33", "amount": 10}, {"ingredient_id": "ing_34", "amount": 15}]},
        {"id": 18, "name": "ต้มยำกุ้ง", "category": "แกง / ต้ม", "price": 180.0, "is_available": True, "image": "https://images.unsplash.com/photo-1547592166-23ac45744acd?w=600&auto=format&fit=crop&q=80", "recipe": [{"ingredient_id": "ing_10", "amount": 2}, {"ingredient_id": "ing_103", "amount": 30}, {"ingredient_id": "ing_11", "amount": 30}, {"ingredient_id": "ing_27", "amount": 30}, {"ingredient_id": "ing_116", "amount": 30}, {"ingredient_id": "ing_55", "amount": 30}, {"ingredient_id": "ing_31", "amount": 15}, {"ingredient_id": "ing_72", "amount": 1}]},
        {"id": 19, "name": "แกงจืดเต้าหู้หมูสับ", "category": "แกง / ต้ม", "price": 85.0, "is_available": True, "image": "https://images.unsplash.com/photo-1547592180-85f173990554?w=600&auto=format&fit=crop&q=80", "recipe": [{"ingredient_id": "ing_85", "amount": 100}, {"ingredient_id": "ing_97", "amount": 1}, {"ingredient_id": "ing_51", "amount": 30}, {"ingredient_id": "ing_79", "amount": 60}, {"ingredient_id": "ing_5", "amount": 30}]},
        {"id": 20, "name": "แกงมัสมั่นเนื้อ", "category": "แกง / ต้ม", "price": 160.0, "is_available": True, "image": "https://images.unsplash.com/photo-1588166524941-3bf61a9c41db?w=600&auto=format&fit=crop&q=80", "recipe": [{"ingredient_id": "ing_100", "amount": 100}, {"ingredient_id": "ing_76", "amount": 30}, {"ingredient_id": "ing_91", "amount": 30}, {"ingredient_id": "ing_30", "amount": 30}, {"ingredient_id": "ing_60", "amount": 25}, {"ingredient_id": "ing_6", "amount": 15}, {"ingredient_id": "ing_33", "amount": 10}]},
        {"id": 21, "name": "ต้มยำปลาช่อน", "category": "แกง / ต้ม", "price": 160.0, "is_available": True, "image": "https://images.unsplash.com/photo-1594756201799-9b936d501235?w=600&auto=format&fit=crop&q=80", "recipe": [{"ingredient_id": "ing_48", "amount": 1}, {"ingredient_id": "ing_11", "amount": 30}, {"ingredient_id": "ing_27", "amount": 30}, {"ingredient_id": "ing_116", "amount": 30}, {"ingredient_id": "ing_90", "amount": 30}, {"ingredient_id": "ing_118", "amount": 30}, {"ingredient_id": "ing_67", "amount": 30}]},
        {"id": 22, "name": "แกงเลียงฟักทอง", "category": "แกง / ต้ม", "price": 95.0, "is_available": True, "image": "https://images.unsplash.com/photo-1518779578993-ec3579fee39f?w=600&auto=format&fit=crop&q=80", "recipe": [{"ingredient_id": "ing_68", "amount": 30}, {"ingredient_id": "ing_40", "amount": 30}, {"ingredient_id": "ing_117", "amount": 30}, {"ingredient_id": "ing_8", "amount": 4}, {"ingredient_id": "ing_65", "amount": 30}, {"ingredient_id": "ing_90", "amount": 30}, {"ingredient_id": "ing_7", "amount": 10}]},
        {"id": 23, "name": "แกงป่าหมู", "category": "แกง / ต้ม", "price": 100.0, "is_available": True, "image": "https://images.unsplash.com/photo-1543339308-43e59d6b73a6?w=600&auto=format&fit=crop&q=80", "recipe": [{"ingredient_id": "ing_84", "amount": 100}, {"ingredient_id": "ing_59", "amount": 25}, {"ingredient_id": "ing_71", "amount": 30}, {"ingredient_id": "ing_29", "amount": 30}, {"ingredient_id": "ing_82", "amount": 30}, {"ingredient_id": "ing_114", "amount": 30}]},
        {"id": 24, "name": "แกงส้มยอดมะพร้าวปลาริวกิว", "category": "แกง / ต้ม", "price": 150.0, "is_available": True, "image": "https://images.unsplash.com/photo-1505253716362-afaea1d3d1af?w=600&auto=format&fit=crop&q=80", "recipe": [{"ingredient_id": "ing_45", "amount": 100}, {"ingredient_id": "ing_78", "amount": 30}, {"ingredient_id": "ing_61", "amount": 25}, {"ingredient_id": "ing_37", "amount": 15}]},
        {"id": 25, "name": "แกงจืดมะระยัดไส้", "category": "แกง / ต้ม", "price": 90.0, "is_available": True, "image": "https://images.unsplash.com/photo-1547592180-85f173990554?w=600&auto=format&fit=crop&q=80", "recipe": [{"ingredient_id": "ing_73", "amount": 30}, {"ingredient_id": "ing_85", "amount": 100}, {"ingredient_id": "ing_79", "amount": 60}, {"ingredient_id": "ing_104", "amount": 30}, {"ingredient_id": "ing_25", "amount": 30}]},
        {"id": 26, "name": "แกงรัญจวน", "category": "แกง / ต้ม", "price": 140.0, "is_available": True, "image": "https://images.unsplash.com/photo-1559847844-5315695dadae?w=600&auto=format&fit=crop&q=80", "recipe": [{"ingredient_id": "ing_100", "amount": 100}, {"ingredient_id": "ing_27", "amount": 30}, {"ingredient_id": "ing_90", "amount": 30}, {"ingredient_id": "ing_55", "amount": 30}, {"ingredient_id": "ing_7", "amount": 10}, {"ingredient_id": "ing_118", "amount": 30}, {"ingredient_id": "ing_72", "amount": 1}]},
        {"id": 27, "name": "ต้มแซ่บกระดูกอ่อน", "category": "แกง / ต้ม", "price": 120.0, "is_available": True, "image": "https://images.unsplash.com/photo-1574484284002-952d92456975?w=600&auto=format&fit=crop&q=80", "recipe": [{"ingredient_id": "ing_1", "amount": 100}, {"ingredient_id": "ing_11", "amount": 30}, {"ingredient_id": "ing_27", "amount": 30}, {"ingredient_id": "ing_116", "amount": 30}, {"ingredient_id": "ing_90", "amount": 30}, {"ingredient_id": "ing_53", "amount": 30}, {"ingredient_id": "ing_67", "amount": 30}]},
        {"id": 28, "name": "แกงเผ็ดเป็ดย่าง", "category": "แกง / ต้ม", "price": 160.0, "is_available": True, "image": "https://images.unsplash.com/photo-1519708227418-c8fd9a32b7a2?w=600&auto=format&fit=crop&q=80", "recipe": [{"ingredient_id": "ing_95", "amount": 100}, {"ingredient_id": "ing_70", "amount": 30}, {"ingredient_id": "ing_81", "amount": 30}, {"ingredient_id": "ing_63", "amount": 25}, {"ingredient_id": "ing_6", "amount": 15}, {"ingredient_id": "ing_118", "amount": 30}]},
        {"id": 29, "name": "แกงคั่วหอยขม", "category": "แกง / ต้ม", "price": 130.0, "is_available": True, "image": "https://images.unsplash.com/photo-1504674900247-0877df9cc836?w=600&auto=format&fit=crop&q=80", "recipe": [{"ingredient_id": "ing_92", "amount": 80}, {"ingredient_id": "ing_115", "amount": 30}, {"ingredient_id": "ing_58", "amount": 25}, {"ingredient_id": "ing_6", "amount": 15}, {"ingredient_id": "ing_34", "amount": 15}, {"ingredient_id": "ing_33", "amount": 10}]},
        {"id": 30, "name": "ต้มโคล้งปลาดุกย่าง", "category": "แกง / ต้ม", "price": 130.0, "is_available": True, "image": "https://images.unsplash.com/photo-1541832676-9b763b0239ab?w=600&auto=format&fit=crop&q=80", "recipe": [{"ingredient_id": "ing_43", "amount": 1}, {"ingredient_id": "ing_103", "amount": 30}, {"ingredient_id": "ing_11", "amount": 30}, {"ingredient_id": "ing_27", "amount": 30}, {"ingredient_id": "ing_116", "amount": 30}, {"ingredient_id": "ing_67", "amount": 30}, {"ingredient_id": "ing_37", "amount": 15}]},
        {"id": 31, "name": "ปลากะพงทอดน้ำปลา", "category": "ทอด / ย่าง / อบ", "price": 320.0, "is_available": True, "image": "https://images.unsplash.com/photo-1519708227418-c8fd9a32b7a2?w=600&auto=format&fit=crop&q=80", "recipe": [{"ingredient_id": "ing_42", "amount": 1}, {"ingredient_id": "ing_34", "amount": 15}, {"ingredient_id": "ing_33", "amount": 10}, {"ingredient_id": "ing_75", "amount": 30}]},
        {"id": 32, "name": "หมูสามชั้นทอดน้ำปลา", "category": "ทอด / ย่าง / อบ", "price": 110.0, "is_available": True, "image": "https://images.unsplash.com/photo-1626082927389-6cd097cdc6ec?w=600&auto=format&fit=crop&q=80", "recipe": [{"ingredient_id": "ing_86", "amount": 80}, {"ingredient_id": "ing_34", "amount": 15}, {"ingredient_id": "ing_64", "amount": 30}, {"ingredient_id": "ing_109", "amount": 10}]},
        {"id": 33, "name": "ไก่ทอดหาดใหญ่", "category": "ทอด / ย่าง / อบ", "price": 85.0, "is_available": True, "image": "https://images.unsplash.com/photo-1626082896492-766af4eb6501?w=600&auto=format&fit=crop&q=80", "recipe": [{"ingredient_id": "ing_102", "amount": 100}, {"ingredient_id": "ing_89", "amount": 30}, {"ingredient_id": "ing_3", "amount": 30}, {"ingredient_id": "ing_64", "amount": 30}, {"ingredient_id": "ing_77", "amount": 30}, {"ingredient_id": "ing_109", "amount": 10}]},
        {"id": 34, "name": "ไข่เจียวหมูสับ", "category": "ทอด / ย่าง / อบ", "price": 55.0, "is_available": True, "image": "https://images.unsplash.com/photo-1525351484163-7529414344d8?w=600&auto=format&fit=crop&q=80", "recipe": [{"ingredient_id": "ing_113", "amount": 1}, {"ingredient_id": "ing_85", "amount": 100}, {"ingredient_id": "ing_28", "amount": 30}, {"ingredient_id": "ing_34", "amount": 15}, {"ingredient_id": "ing_38", "amount": 15}]},
        {"id": 35, "name": "ปลาทับทิมทอดสามรส", "category": "ทอด / ย่าง / อบ", "price": 260.0, "is_available": True, "image": "https://images.unsplash.com/photo-1534939561126-855b8675edd7?w=600&auto=format&fit=crop&q=80", "recipe": [{"ingredient_id": "ing_44", "amount": 1}, {"ingredient_id": "ing_3", "amount": 30}, {"ingredient_id": "ing_56", "amount": 30}, {"ingredient_id": "ing_37", "amount": 15}, {"ingredient_id": "ing_24", "amount": 15}, {"ingredient_id": "ing_32", "amount": 10}]},
        {"id": 36, "name": "ปีกไก่นิวออร์ลีน", "category": "ทอด / ย่าง / อบ", "price": 120.0, "is_available": True, "image": "https://images.unsplash.com/photo-1527477378406-89680327f29b?w=600&auto=format&fit=crop&q=80", "recipe": [{"ingredient_id": "ing_49", "amount": 4}, {"ingredient_id": "ing_23", "amount": 15}, {"ingredient_id": "ing_24", "amount": 15}, {"ingredient_id": "ing_50", "amount": 10}, {"ingredient_id": "ing_94", "amount": 10}]},
        {"id": 37, "name": "ทอดมันปลากราย", "category": "ทอด / ย่าง / อบ", "price": 100.0, "is_available": True, "image": "https://images.unsplash.com/photo-1541832676-9b763b0239ab?w=600&auto=format&fit=crop&q=80", "recipe": [{"ingredient_id": "ing_99", "amount": 100}, {"ingredient_id": "ing_63", "amount": 25}, {"ingredient_id": "ing_29", "amount": 30}, {"ingredient_id": "ing_116", "amount": 30}, {"ingredient_id": "ing_113", "amount": 1}]},
        {"id": 38, "name": "หมูแดดเดียว", "category": "ทอด / ย่าง / อบ", "price": 95.0, "is_available": True, "image": "https://images.unsplash.com/photo-1544025162-d76694265947?w=600&auto=format&fit=crop&q=80", "recipe": [{"ingredient_id": "ing_101", "amount": 100}, {"ingredient_id": "ing_20", "amount": 15}, {"ingredient_id": "ing_32", "amount": 10}, {"ingredient_id": "ing_18", "amount": 30}]},
        {"id": 39, "name": "กุ้งทอดซอสมะขาม", "category": "ทอด / ย่าง / อบ", "price": 180.0, "is_available": True, "image": "https://images.unsplash.com/photo-1559847844-5315695dadae?w=600&auto=format&fit=crop&q=80", "recipe": [{"ingredient_id": "ing_10", "amount": 2}, {"ingredient_id": "ing_37", "amount": 15}, {"ingredient_id": "ing_33", "amount": 10}, {"ingredient_id": "ing_89", "amount": 30}, {"ingredient_id": "ing_67", "amount": 30}]},
        {"id": 40, "name": "ปลาสำลีทอดยำมะม่วง", "category": "ทอด / ย่าง / อบ", "price": 280.0, "is_available": True, "image": "https://images.unsplash.com/photo-1534939561126-855b8675edd7?w=600&auto=format&fit=crop&q=80", "recipe": [{"ingredient_id": "ing_46", "amount": 1}, {"ingredient_id": "ing_75", "amount": 30}, {"ingredient_id": "ing_90", "amount": 30}, {"ingredient_id": "ing_30", "amount": 30}, {"ingredient_id": "ing_55", "amount": 30}, {"ingredient_id": "ing_34", "amount": 15}]},
        {"id": 41, "name": "คอหมูย่าง", "category": "ทอด / ย่าง / อบ", "price": 95.0, "is_available": True, "image": "https://images.unsplash.com/photo-1555939594-58d7cb561ad1?w=600&auto=format&fit=crop&q=80", "recipe": [{"ingredient_id": "ing_17", "amount": 100}, {"ingredient_id": "ing_12", "amount": 10}, {"ingredient_id": "ing_57", "amount": 30}, {"ingredient_id": "ing_34", "amount": 15}, {"ingredient_id": "ing_72", "amount": 1}, {"ingredient_id": "ing_33", "amount": 10}]},
        {"id": 42, "name": "เต้าหู้ทรงเครื่องทอด", "category": "ทอด / ย่าง / อบ", "price": 85.0, "is_available": True, "image": "https://images.unsplash.com/photo-1546069901-ba9599a7e63c?w=600&auto=format&fit=crop&q=80", "recipe": [{"ingredient_id": "ing_97", "amount": 1}, {"ingredient_id": "ing_85", "amount": 100}, {"ingredient_id": "ing_108", "amount": 30}, {"ingredient_id": "ing_104", "amount": 30}, {"ingredient_id": "ing_39", "amount": 15}]},
        {"id": 43, "name": "กุ้งอบวุ้นเส้น", "category": "ทอด / ย่าง / อบ", "price": 160.0, "is_available": True, "image": "https://images.unsplash.com/photo-1563379091339-03b21ab4a4f8?w=600&auto=format&fit=crop&q=80", "recipe": [{"ingredient_id": "ing_79", "amount": 60}, {"ingredient_id": "ing_8", "amount": 4}, {"ingredient_id": "ing_86", "amount": 80}, {"ingredient_id": "ing_16", "amount": 30}, {"ingredient_id": "ing_15", "amount": 30}, {"ingredient_id": "ing_39", "amount": 15}, {"ingredient_id": "ing_21", "amount": 15}]},
        {"id": 44, "name": "ไข่ลูกเขย", "category": "ทอด / ย่าง / อบ", "price": 65.0, "is_available": True, "image": "https://images.unsplash.com/photo-1582878826629-29b7ad1cdc43?w=600&auto=format&fit=crop&q=80", "recipe": [{"ingredient_id": "ing_113", "amount": 1}, {"ingredient_id": "ing_37", "amount": 15}, {"ingredient_id": "ing_33", "amount": 10}, {"ingredient_id": "ing_34", "amount": 15}, {"ingredient_id": "ing_89", "amount": 30}]},
        {"id": 45, "name": "ปลาช่อนลุยสวน", "category": "ทอด / ย่าง / อบ", "price": 280.0, "is_available": True, "image": "https://images.unsplash.com/photo-1519708227418-c8fd9a32b7a2?w=600&auto=format&fit=crop&q=80", "recipe": [{"ingredient_id": "ing_48", "amount": 1}, {"ingredient_id": "ing_27", "amount": 30}, {"ingredient_id": "ing_90", "amount": 30}, {"ingredient_id": "ing_15", "amount": 30}, {"ingredient_id": "ing_72", "amount": 1}, {"ingredient_id": "ing_55", "amount": 30}, {"ingredient_id": "ing_80", "amount": 30}]},
        {"id": 46, "name": "ยำวุ้นเส้นรวมมิตร", "category": "ยำ / ตำ / ลาบ", "price": 110.0, "is_available": True, "image": "https://images.unsplash.com/photo-1540420773420-3366772f4999?w=600&auto=format&fit=crop&q=80", "recipe": [{"ingredient_id": "ing_79", "amount": 60}, {"ingredient_id": "ing_85", "amount": 100}, {"ingredient_id": "ing_8", "amount": 4}, {"ingredient_id": "ing_47", "amount": 80}, {"ingredient_id": "ing_16", "amount": 30}, {"ingredient_id": "ing_91", "amount": 30}, {"ingredient_id": "ing_72", "amount": 1}, {"ingredient_id": "ing_55", "amount": 30}, {"ingredient_id": "ing_34", "amount": 15}]},
        {"id": 47, "name": "ส้มตำไทย", "category": "ยำ / ตำ / ลาบ", "price": 60.0, "is_available": True, "image": "https://images.unsplash.com/photo-1569718212165-3a8278d5f624?w=600&auto=format&fit=crop&q=80", "recipe": [{"ingredient_id": "ing_74", "amount": 30}, {"ingredient_id": "ing_70", "amount": 30}, {"ingredient_id": "ing_29", "amount": 30}, {"ingredient_id": "ing_9", "amount": 15}, {"ingredient_id": "ing_30", "amount": 30}, {"ingredient_id": "ing_55", "amount": 30}, {"ingredient_id": "ing_72", "amount": 1}, {"ingredient_id": "ing_35", "amount": 15}]},
        {"id": 48, "name": "ยำหมูยอไข่แดงเค็ม", "category": "ยำ / ตำ / ลาบ", "price": 120.0, "is_available": True, "image": "https://images.unsplash.com/photo-1546069901-ba9599a7e63c?w=600&auto=format&fit=crop&q=80", "recipe": [{"ingredient_id": "ing_88", "amount": 80}, {"ingredient_id": "ing_112", "amount": 1}, {"ingredient_id": "ing_91", "amount": 30}, {"ingredient_id": "ing_16", "amount": 30}, {"ingredient_id": "ing_72", "amount": 1}, {"ingredient_id": "ing_34", "amount": 15}, {"ingredient_id": "ing_32", "amount": 10}, {"ingredient_id": "ing_55", "amount": 30}]},
        {"id": 49, "name": "ยำปลาดุกฟู", "category": "ยำ / ตำ / ลาบ", "price": 110.0, "is_available": True, "image": "https://images.unsplash.com/photo-1534422298391-e4f8c172dddb?w=600&auto=format&fit=crop&q=80", "recipe": [{"ingredient_id": "ing_43", "amount": 1}, {"ingredient_id": "ing_75", "amount": 30}, {"ingredient_id": "ing_90", "amount": 30}, {"ingredient_id": "ing_30", "amount": 30}, {"ingredient_id": "ing_55", "amount": 30}, {"ingredient_id": "ing_34", "amount": 15}, {"ingredient_id": "ing_72", "amount": 1}]},
        {"id": 50, "name": "ลาบหมู", "category": "ยำ / ตำ / ลาบ", "price": 75.0, "is_available": True, "image": "https://images.unsplash.com/photo-1555939594-58d7cb561ad1?w=600&auto=format&fit=crop&q=80", "recipe": [{"ingredient_id": "ing_85", "amount": 100}, {"ingredient_id": "ing_12", "amount": 10}, {"ingredient_id": "ing_57", "amount": 30}, {"ingredient_id": "ing_90", "amount": 30}, {"ingredient_id": "ing_28", "amount": 30}, {"ingredient_id": "ing_53", "amount": 30}, {"ingredient_id": "ing_80", "amount": 30}, {"ingredient_id": "ing_72", "amount": 1}, {"ingredient_id": "ing_34", "amount": 15}]}
    ]

    return {
        "schema_version": 3,
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
        "members": [
            {"phone": "0812345678", "name": "คุณสมชาย", "points": 150}
        ],
        "audit_logs": [
            {"timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "user": "System", "action": "SYSTEM_START", "details": "เริ่มต้นระบบสำเร็จ พร้อม 50 เมนูอาหารและ 118 วัตถุดิบ"}
        ],
        "sales": []
    }

def load_db() -> Dict[str, Any]:
    """โหลดข้อมูลจากไฟล์อย่างปลอดภัย โดยไม่ล้างข้อมูลผู้ใช้หรือยอดขายทิ้งเด็ดขาด"""
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

        # มีไฟล์อยู่แล้ว: พยายามอ่านไฟล์ซ้ำสูงสุด 5 ครั้งเพื่อป้องกันปัญหาการล็อกไฟล์ชั่วขณะ
        for _ in range(5):
            try:
                with open(DATA_PATH, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    
                    # อัปเกรดเฉพาะเมนูและวัตถุดิบครั้งแรก โดยห้ามแตะต้อง users, sales, audit_logs เดิมเด็ดขาด!
                    if data.get("schema_version", 0) < 3 or not data.get("inventory"):
                        init_data = get_initial_schema()
                        data["menu"] = init_data["menu"]
                        data["inventory"] = init_data["inventory"]
                        data["schema_version"] = 3
                        # บันทึกการอัปเกรด
                        with open(DATA_PATH, "w", encoding="utf-8") as fw:
                            json.dump(data, fw, ensure_ascii=False, indent=2)
                    
                    _DB_CACHE = data
                    return data
            except Exception:
                time.sleep(0.05)

        # หากอ่านไฟล์หลักไม่ได้ ให้ลองอ่านจากไฟล์สำรอง .bak
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
    """บันทึกไฟล์พร้อมทำสำเนา .bak อัตโนมัติ ป้องกันข้อมูลสูญหาย"""
    global _DB_CACHE
    with _db_lock:
        try:
            _DB_CACHE = data
            # สำเนาไฟล์เดิมไว้ก่อนเซฟเสมอ
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