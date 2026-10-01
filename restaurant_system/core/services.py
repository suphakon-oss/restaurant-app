"""
core/services.py - รวมตรรกะทางธุรกิจทั้งหมดของระบบ (Business Logic Layer)
รองรับทั้งฟีเจอร์พื้นฐานเดิม และฟีเจอร์ขั้นสูงสำหรับ Vercel
"""

from datetime import datetime
from typing import Dict, Any, List, Tuple
from core.database import load_db, save_db, add_audit_log

# ค่าคงที่ใช้อ้างอิง (Tuple)
RATES: tuple = (0.07, 0.10)  # (VAT 7%, Service Charge 10%)
TABLE_STATUSES: tuple = ("ว่าง", "มีลูกค้า", "รอเช็คบิล")
KITCHEN_STATUSES: tuple = ("รอทำ", "กำลังทำ", "เสิร์ฟแล้ว", "ยกเลิก")


# ================= 1. ฟังก์ชันเดิมจากรอบแรก (ปรับปรุงเข้ากับ DB ใหม่) =================

def get_unique_categories(menu_list: list) -> list:
    """ดึงหมวดหมู่ที่ไม่ซ้ำกันโดยใช้ Set"""
    category_set: set = set()
    for item in menu_list:
        if isinstance(item, dict) and "category" in item:
            category_set.add(str(item["category"]).strip())
    return sorted(list(category_set))


def calculate_bill(items: list, discount_percent: float = 0.0) -> tuple:
    """
    คำนวณยอดเงินรวม ส่วนลด เซอร์วิสชาร์จ 10% และ VAT 7%
    คืนค่าเป็น Tuple: (subtotal, discount_amount, service_charge, vat, net_total)
    """
    subtotal: float = 0.0
    for order_item in items:
        if not (order_item.get("status") == "ยกเลิก"):
            # รองรับทั้งคีย์ unit_price และ price
            price: float = float(order_item.get("unit_price", order_item.get("price", 0.0)))
            qty: int = int(order_item.get("qty", 1))
            subtotal += price * qty

    if discount_percent < 0.0 or discount_percent > 100.0:
        discount_percent = 0.0

    discount_amount: float = round(subtotal * (discount_percent / 100.0), 2)
    after_discount: float = subtotal - discount_amount

    if after_discount > 0.0 and len(items) > 0:
        vat_rate, sc_rate = RATES[0], RATES[1]
        service_charge: float = round(after_discount * sc_rate, 2)
        vat: float = round((after_discount + service_charge) * vat_rate, 2)
        net_total: float = round(after_discount + service_charge + vat, 2)
    else:
        service_charge = 0.0
        vat = 0.0
        net_total = 0.0

    return (subtotal, discount_amount, service_charge, vat, net_total)


def generate_sales_report(target_date: str = "") -> dict:
    """สรุปยอดขายรายวัน และ 5 อันดับเมนูขายดี"""
    db = load_db()
    sales = db.get("sales", [])

    if not target_date:
        target_date = datetime.now().strftime("%Y-%m-%d")

    total_revenue: float = 0.0
    total_bills: int = 0
    item_sales_counter: dict = {}

    for bill in sales:
        bill_time = str(bill.get("timestamp", ""))
        if bill_time.startswith(target_date):
            total_revenue += float(bill.get("net_total", 0.0))
            total_bills += 1
            for item in bill.get("items", []):
                name = item.get("name", "ไม่ระบุ")
                qty = int(item.get("qty", 0))
                item_sales_counter[name] = item_sales_counter.get(name, 0) + qty

    best_sellers = sorted(item_sales_counter.items(), key=lambda x: x[1], reverse=True)

    return {
        "date": target_date,
        "total_revenue": round(total_revenue, 2),
        "total_bills": total_bills,
        "best_sellers": [{"name": k, "qty": v} for k, v in best_sellers[:5]]
    }


# ================= 2. ฟังก์ชันส่วนเสริมใหม่ (ฟีเจอร์ขั้นสูง) =================

def deduct_stock_by_recipe(menu_id: int, qty: int, actor: str) -> Tuple[bool, str]:
    """ตัดสต็อกวัตถุดิบอัตโนมัติตามสูตรอาหารเมื่อห้องครัวรับออเดอร์"""
    db = load_db()
    menu_item = next((m for m in db.get("menu", []) if m["id"] == menu_id), None)
    if not menu_item or not menu_item.get("recipe"):
        return True, "ไม่มีสูตรอาหาร ไม่ต้องตัดสต็อก"

    # 1. ตรวจสอบว่าวัตถุดิบพอหรือไม่ก่อน
    for ing in menu_item["recipe"]:
        item = next((i for i in db.get("inventory", []) if i["id"] == ing["ingredient_id"]), None)
        if not item or item["stock"] < (ing["amount"] * qty):
            item_name = item["name"] if item else "ไม่ทราบชื่อ"
            return False, f"วัตถุดิบ '{item_name}' ไม่เพียงพอ (คงเหลือ: {item['stock'] if item else 0})"

    # 2. ตัดสต็อกจริงและลง Audit Log
    for ing in menu_item["recipe"]:
        for item in db["inventory"]:
            if item["id"] == ing["ingredient_id"]:
                deducted = ing["amount"] * qty
                item["stock"] -= deducted
                add_audit_log(actor, "DEDUCT_STOCK", f"ตัดวัตถุดิบ {item['name']} จำนวน {deducted} {item['unit']}")

    save_db(db)
    return True, "ตัดสต็อกวัตถุดิบตามสูตรสำเร็จ"


def move_table(from_table: int, to_table: int, actor: str) -> Tuple[bool, str]:
    """ย้ายโต๊ะ: โอนรายการอาหารทั้งหมดไปยังโต๊ะใหม่"""
    db = load_db()
    t_target = next((t for t in db["tables"] if t["table_id"] == to_table), None)
    if not t_target:
        return False, "ไม่พบหมายเลขโต๊ะปลายทาง"
    if t_target["status"] != "ว่าง":
        return False, f"โต๊ะ {to_table} มีลูกค้าอยู่แล้ว ไม่สามารถย้ายไปได้"

    orders_moved = 0
    for o in db.get("orders", []):
        if o["table_id"] == from_table and o["status"] != "ยกเลิก":
            o["table_id"] = to_table
            orders_moved += 1

    for t in db["tables"]:
        if t["table_id"] == from_table:
            t["status"] = "ว่าง"
        elif t["table_id"] == to_table:
            t["status"] = "มีลูกค้า"

    add_audit_log(actor, "MOVE_TABLE", f"ย้ายรายการจากโต๊ะ {from_table} ไปโต๊ะ {to_table} ({orders_moved} รายการ)")
    save_db(db)
    return True, f"ย้ายจากโต๊ะ {from_table} ไปโต๊ะ {to_table} เรียบร้อย"


def merge_tables(source_tables: List[int], target_table: int, actor: str) -> Tuple[bool, str]:
    """รวมโต๊ะ: รวมออเดอร์ของหลายโต๊ะมารวมไว้ที่โต๊ะเป้าหมาย"""
    db = load_db()
    for o in db.get("orders", []):
        if o["table_id"] in source_tables and o["status"] != "ยกเลิก":
            o["table_id"] = target_table

    for t in db["tables"]:
        if t["table_id"] in source_tables and t["table_id"] != target_table:
            t["status"] = "ว่าง"
        elif t["table_id"] == target_table:
            t["status"] = "มีลูกค้า"

    add_audit_log(actor, "MERGE_TABLE", f"รวมโต๊ะ {source_tables} เข้าสู่โต๊ะ {target_table}")
    save_db(db)
    return True, f"รวมโต๊ะเข้าสู่โต๊ะ {target_table} เรียบร้อย"


def process_loyalty_points(phone: str, net_amount: float) -> int:
    """สะสมแต้มสมาชิก: ยอดใช้จ่ายทุก 10 บาท ได้รับ 1 แต้ม"""
    if not phone:
        return 0
    db = load_db()
    earned_points = int(net_amount // 10)
    
    for m in db.get("members", []):
        if m["phone"] == phone:
            m["points"] += earned_points
            save_db(db)
            return m["points"]

    # ถ้ายังไม่เคยเป็นสมาชิก สมัครให้อัตโนมัติทันที
    db.setdefault("members", []).append({
        "phone": phone,
        "name": "ลูกค้าทั่วไป",
        "points": earned_points
    })
    save_db(db)
    return earned_points


def paginate_and_sort(items: list, search: str = "", search_field: str = "name",
                      sort_by: str = "", order: str = "asc", page: int = 1, limit: int = 10) -> dict:
    """ฟังก์ชันกลางสำหรับ ค้นหา กรอง เรียงลำดับ และแบ่งหน้า (Pagination)"""
    filtered = items
    if search:
        filtered = [x for x in items if search.lower() in str(x.get(search_field, "")).lower()]

    if sort_by:
        filtered = sorted(filtered, key=lambda x: x.get(sort_by, 0), reverse=(order == "desc"))

    total = len(filtered)
    start = (page - 1) * limit
    paged_items = filtered[start:start + limit]

    return {
        "items": paged_items,
        "total": total,
        "page": page,
        "limit": limit,
        "total_pages": (total + limit - 1) // limit if limit > 0 else 1
    }