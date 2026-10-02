"""
api/index.py - REST API, RBAC Authentication, Real-Time SSE and Modern Responsive UI
อัปเดต: รองรับรูปภาพเมนูทั้งแบบ URL และ Upload ไฟล์, Live Preview, แสดงรูปในเมนูและหน้าสั่ง
"""

import json
import asyncio
import re
from datetime import datetime
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel, Field

from core.database import load_db, save_db, add_audit_log
from core.auth import verify_password, hash_password, create_access_token
import core.services as services

app = FastAPI(title="Smart Restaurant Pro", docs_url="/api/docs", openapi_url="/api/openapi.json")

# Broadcast Queue สำหรับระบบ Real-time (Server-Sent Events)
event_subscribers: List[asyncio.Queue] = []

async def broadcast_event(event_type: str, data: dict):
    message = json.dumps({"type": event_type, "data": data, "time": datetime.now().isoformat()})
    for q in list(event_subscribers):
        try:
            await q.put(message)
        except Exception:
            pass

# ================= 1. Schemas สำหรับตรวจสอบข้อมูล (Validation) =================

class LoginRequest(BaseModel):
    username: str
    password: str

class RegisterRequest(BaseModel):
    username: str
    password: str
    name: str = Field(..., min_length=2)

class TableCreateRequest(BaseModel):
    table_id: Optional[int] = None
    capacity: int = Field(default=4, gt=0)

class MenuRecipeItem(BaseModel):
    ingredient_id: str
    amount: float = Field(..., gt=0)

class MenuCreateRequest(BaseModel):
    name: str = Field(..., min_length=2)
    category: str = Field(default="อาหารจานเดียว")
    price: float = Field(..., ge=0)
    is_available: bool = True
    image: Optional[str] = ""  # รองรับทั้ง URL และ Base64 Data URL
    recipe: List[MenuRecipeItem] = []

class OrderOption(BaseModel):
    spiciness: Optional[str] = "ปกติ"
    egg: Optional[str] = "ไม่ใส่"
    size: Optional[str] = "ธรรมดา"

class PlaceOrderItem(BaseModel):
    menu_id: int
    qty: int = Field(default=1, gt=0)
    options: Optional[OrderOption] = None

class CustomerOrderRequest(BaseModel):
    table_id: int
    items: List[PlaceOrderItem]

class CheckoutRequest(BaseModel):
    table_id: int
    discount_percent: float = Field(default=0.0, ge=0.0, le=100.0)
    member_phone: Optional[str] = None
    split_type: Optional[str] = "full"
    split_count: Optional[int] = 1

class MoveTableRequest(BaseModel):
    from_table: int
    to_table: int

class MergeTableRequest(BaseModel):
    source_tables: List[int]
    target_table: int

class QueueRequest(BaseModel):
    name: str = Field(..., min_length=2)
    party_size: int = Field(..., gt=0)

class ReservationRequest(BaseModel):
    name: str = Field(..., min_length=2)
    phone: str = Field(..., min_length=9)
    date: str
    time: str
    party_size: int = Field(..., gt=0)

class InventoryItemRequest(BaseModel):
    name: str = Field(..., min_length=2)
    stock: float = Field(..., ge=0)
    unit: str = Field(..., min_length=1)
    min_stock: float = Field(default=10, ge=0)

# ================= 2. Authentication APIs (RBAC & บังคับลูกค้าทั่วไป) =================

@app.post("/api/auth/login")
async def login(req: LoginRequest):
    if not re.match(r"^[a-zA-Z0-9]+$", req.username):
        raise HTTPException(status_code=400, detail="ชื่อผู้ใช้ต้องเป็นตัวอักษรภาษาอังกฤษและตัวเลขเท่านั้น")

    if len(req.password) < 6:
        raise HTTPException(status_code=400, detail="รหัสผ่านต้องมีความยาวอย่างน้อย 6 ตัวอักษร")

    db = load_db()
    user = next((u for u in db.get("users", []) if u["username"] == req.username), None)
    if not user or not verify_password(req.password, user["password_hash"], user["salt"]):
        raise HTTPException(status_code=401, detail="ชื่อผู้ใช้หรือรหัสผ่านไม่ถูกต้อง")

    token = create_access_token(user["id"], user["role"])
    add_audit_log(user["username"], "LOGIN", f"เข้าสู่ระบบในฐานะ {user['role']}")
    return {"token": token, "role": user["role"], "name": user["name"], "username": user["username"]}

@app.post("/api/auth/register")
async def register(req: RegisterRequest):
    if not re.match(r"^[a-zA-Z0-9]+$", req.username):
        raise HTTPException(status_code=400, detail="ชื่อผู้ใช้ต้องเป็นตัวอักษรภาษาอังกฤษและตัวเลขเท่านั้น")

    if len(req.password) < 6:
        raise HTTPException(status_code=400, detail="รหัสผ่านต้องมีความยาวอย่างน้อย 6 ตัวอักษร")

    db = load_db()
    if any(u["username"] == req.username for u in db.get("users", [])):
        raise HTTPException(status_code=400, detail="ชื่อผู้ใช้นี้มีอยู่ในระบบแล้ว")

    h, s = hash_password(req.password)
    new_user = {
        "id": f"u_{len(db.get('users', [])) + 1}",
        "username": req.username,
        "password_hash": h,
        "salt": s,
        "role": "customer",
        "name": req.name
    }
    db.setdefault("users", []).append(new_user)
    save_db(db)
    add_audit_log("System", "REGISTER_USER", f"สมัครสมาชิกลูกค้า: {req.username}")
    return {"success": True, "message": "ลงทะเบียนสมาชิกลูกค้าสำเร็จ"}

# ================= 3. Real-Time Server-Sent Events (SSE) =================

@app.get("/api/realtime")
async def sse_notifications():
    queue: asyncio.Queue = asyncio.Queue()
    event_subscribers.append(queue)

    async def event_generator():
        try:
            while True:
                data = await queue.get()
                yield f"data: {data}\n\n"
        except asyncio.CancelledError:
            if queue in event_subscribers:
                event_subscribers.remove(queue)

    return StreamingResponse(event_generator(), media_type="text/event-stream")

# ================= 4. เมนูอาหาร & CRUD (พร้อมจัดการรูปภาพ) =================

@app.get("/api/menu")
def get_menus(search: str = "", category: str = "", sort_by: str = "id", order: str = "asc", page: int = 1, limit: int = 50):
    db = load_db()
    items = db.get("menu", [])
    if category:
        items = [m for m in items if m.get("category") == category]

    result = services.paginate_and_sort(items, search=search, search_field="name", sort_by=sort_by, order=order, page=page, limit=limit)
    result["categories"] = services.get_unique_categories(db.get("menu", []))
    return result

@app.post("/api/menu")
def create_menu(req: MenuCreateRequest):
    """เพิ่มเมนูใหม่พร้อมบันทึกรูปภาพ (URL หรือ Base64)"""
    db = load_db()
    new_id = max([m["id"] for m in db.get("menu", [])], default=0) + 1
    new_menu = {
        "id": new_id,
        "name": req.name,
        "category": req.category,
        "price": req.price,
        "is_available": req.is_available,
        "image": req.image or "",
        "recipe": [r.dict() for r in req.recipe]
    }
    db.setdefault("menu", []).append(new_menu)
    add_audit_log("Staff", "CREATE_MENU", f"เพิ่มเมนูใหม่: {req.name} ({req.price} ฿)")
    save_db(db)
    return {"success": True, "menu": new_menu}

@app.delete("/api/menu/{menu_id}")
def delete_menu(menu_id: int):
    db = load_db()
    menu = next((m for m in db.get("menu", []) if m["id"] == menu_id), None)
    if not menu:
        raise HTTPException(status_code=404, detail="ไม่พบเมนู")

    db["menu"] = [m for m in db["menu"] if m["id"] != menu_id]
    add_audit_log("Staff", "DELETE_MENU", f"ลบเมนู: {menu['name']}")
    save_db(db)
    return {"success": True, "message": f"ลบเมนู '{menu['name']}' เรียบร้อย"}

@app.post("/api/menu/{menu_id}/toggle")
def toggle_menu_availability(menu_id: int):
    db = load_db()
    menu = next((m for m in db.get("menu", []) if m["id"] == menu_id), None)
    if not menu:
        raise HTTPException(status_code=404, detail="ไม่พบเมนู")

    menu["is_available"] = not menu.get("is_available", True)
    status_str = "พร้อมขาย" if menu["is_available"] else "หมด"
    add_audit_log("Staff", "TOGGLE_MENU", f"เปลี่ยนสถานะเมนู '{menu['name']}' เป็น {status_str}")
    save_db(db)
    return {"success": True, "is_available": menu["is_available"]}

# ================= 5. จัดการโต๊ะ (Tables, Move, Merge) =================

@app.get("/api/tables")
def get_tables():
    db = load_db()
    return db.get("tables", [])

@app.post("/api/tables")
def create_table(req: TableCreateRequest):
    db = load_db()
    tables = db.setdefault("tables", [])
    new_id = req.table_id if req.table_id else (max([t["table_id"] for t in tables], default=0) + 1)
    
    if any(t["table_id"] == new_id for t in tables):
        raise HTTPException(status_code=400, detail=f"โต๊ะหมายเลข {new_id} มีอยู่ในระบบแล้ว")

    new_table = {
        "table_id": new_id,
        "status": "ว่าง",
        "capacity": req.capacity
    }
    tables.append(new_table)
    add_audit_log("Staff", "ADD_TABLE", f"เพิ่มโต๊ะใหม่ หมายเลข {new_id} ({req.capacity} ที่นั่ง)")
    save_db(db)
    return {"success": True, "table": new_table}

@app.delete("/api/tables/{table_id}")
def delete_table(table_id: int):
    db = load_db()
    tables = db.get("tables", [])
    table = next((t for t in tables if t["table_id"] == table_id), None)
    if not table:
        raise HTTPException(status_code=404, detail="ไม่พบโต๊ะ")
    if table.get("status") != "ว่าง":
        raise HTTPException(status_code=400, detail="ไม่สามารถลบโต๊ะที่มีลูกค้าหรือรอเช็คบิลได้")

    db["tables"] = [t for t in tables if t["table_id"] != table_id]
    add_audit_log("Admin", "DELETE_TABLE", f"ลบโต๊ะหมายเลข {table_id}")
    save_db(db)
    return {"success": True, "message": f"ลบโต๊ะหมายเลข {table_id} สำเร็จ"}

@app.post("/api/table/move")
def move_table_route(req: MoveTableRequest):
    ok, msg = services.move_table(req.from_table, req.to_table, "Staff")
    if not ok:
        raise HTTPException(status_code=400, detail=msg)
    return {"success": True, "message": msg}

@app.post("/api/table/merge")
def merge_table_route(req: MergeTableRequest):
    ok, msg = services.merge_tables(req.source_tables, req.target_table, "Staff")
    if not ok:
        raise HTTPException(status_code=400, detail=msg)
    return {"success": True, "message": msg}

# ================= 6. สั่งอาหาร & จอครัว (ตัดสต็อกทั้งตอนเริ่มทำและเสิร์ฟเสร็จ) =================

@app.get("/api/orders")
def get_orders(table_id: Optional[int] = None):
    db = load_db()
    orders = db.get("orders", [])
    if table_id is not None:
        orders = [o for o in orders if o["table_id"] == table_id]
    return orders

@app.post("/api/customer/order")
async def customer_order(req: CustomerOrderRequest):
    db = load_db()
    table = next((t for t in db.get("tables", []) if t["table_id"] == req.table_id), None)
    if not table:
        raise HTTPException(status_code=404, detail="ไม่พบหมายเลขโต๊ะ")

    created = []
    for it in req.items:
        menu_item = next((m for m in db.get("menu", []) if m["id"] == it.menu_id), None)
        if not menu_item or not menu_item.get("is_available", True):
            continue

        extra_price = 0.0
        if it.options:
            if it.options.egg in ("ไข่ดาว", "ไข่เจียว"): extra_price += 10.0
            if it.options.size == "พิเศษ": extra_price += 15.0

        order_id = len(db.get("orders", [])) + 1
        new_order = {
            "order_id": order_id,
            "table_id": req.table_id,
            "menu_id": it.menu_id,
            "name": menu_item["name"],
            "image": menu_item.get("image", ""),
            "unit_price": menu_item["price"] + extra_price,
            "qty": it.qty,
            "options": it.options.dict() if it.options else {},
            "status": "รอทำ",
            "stock_deducted": False,
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        db.setdefault("orders", []).append(new_order)
        created.append(new_order)

    table["status"] = "มีลูกค้า"
    save_db(db)
    await broadcast_event("NEW_ORDER", {"table_id": req.table_id, "count": len(created)})
    return {"success": True, "message": "ส่งรายการเข้าห้องครัวเรียบร้อย", "orders": created}

@app.post("/api/kitchen/{order_id}/status")
async def update_kitchen_order(order_id: int, status: str = Query(...)):
    db = load_db()
    order = next((o for o in db.get("orders", []) if o["order_id"] == order_id), None)
    if not order:
        raise HTTPException(status_code=404, detail="ไม่พบรายการออเดอร์")

    # ตัดสต็อกเมื่อเปลี่ยนสถานะเป็น "กำลังทำ" หรือ "เสิร์ฟแล้ว" (ถ้ายังไม่เคยตัด)
    if status in ("กำลังทำ", "เสิร์ฟแล้ว") and not order.get("stock_deducted", False):
        ok, msg = services.deduct_stock_in_db(db, order["menu_id"], order["qty"], "Kitchen")
        if not ok:
            raise HTTPException(status_code=400, detail=msg)
        order["stock_deducted"] = True

    order["status"] = status
    save_db(db)
    await broadcast_event("ORDER_STATUS_UPDATE", {"order_id": order_id, "status": status})
    await broadcast_event("STOCK_UPDATE", {})
    return {"success": True, "status": status}

# ================= 7. สต็อกวัตถุดิบ (Inventory CRUD) =================

@app.get("/api/inventory")
def get_inventory():
    db = load_db()
    return db.get("inventory", [])

@app.post("/api/inventory")
def add_inventory_item(req: InventoryItemRequest):
    db = load_db()
    new_id = f"ing_{len(db.get('inventory', [])) + 1}"
    item = {
        "id": new_id,
        "name": req.name,
        "stock": req.stock,
        "unit": req.unit,
        "min_stock": req.min_stock
    }
    db.setdefault("inventory", []).append(item)
    add_audit_log("Admin", "ADD_INVENTORY", f"เพิ่มวัตถุดิบ: {req.name} ({req.stock} {req.unit})")
    save_db(db)
    return {"success": True, "item": item}

@app.put("/api/inventory/{item_id}")
def update_inventory_stock(item_id: str, added_stock: float = Query(...)):
    db = load_db()
    item = next((i for i in db.get("inventory", []) if i["id"] == item_id), None)
    if not item:
        raise HTTPException(status_code=404, detail="ไม่พบวัตถุดิบ")

    item["stock"] = round(float(item["stock"]) + added_stock, 2)
    add_audit_log("Staff", "RESTOCK", f"เติมสต็อก {item['name']} เพิ่ม {added_stock} {item['unit']} (คงเหลือ: {item['stock']})")
    save_db(db)
    return {"success": True, "item": item}

# ================= 8. ระบบคิว & จองโต๊ะ =================

@app.get("/api/queues")
def get_queues():
    db = load_db()
    return db.get("queues", [])

@app.post("/api/queue/ticket")
async def issue_queue(req: QueueRequest):
    db = load_db()
    q_num = f"Q{len(db.get('queues', [])) + 1:02d}"
    ticket = {
        "queue_id": q_num,
        "name": req.name,
        "party_size": req.party_size,
        "status": "รอเรียก",
        "created_at": datetime.now().strftime("%H:%M:%S")
    }
    db.setdefault("queues", []).append(ticket)
    save_db(db)
    await broadcast_event("QUEUE_UPDATE", ticket)
    return ticket

@app.post("/api/queue/{queue_id}/status")
async def update_queue_status(queue_id: str, status: str = Query(...)):
    db = load_db()
    q = next((q for q in db.get("queues", []) if q["queue_id"] == queue_id), None)
    if not q:
        raise HTTPException(status_code=404, detail="ไม่พบคิว")
    q["status"] = status
    save_db(db)
    await broadcast_event("QUEUE_CALLED", q)
    return {"success": True, "queue": q}

@app.get("/api/reservations")
def get_reservations():
    db = load_db()
    return db.get("reservations", [])

@app.post("/api/reservations")
def create_reservation(req: ReservationRequest):
    db = load_db()
    res_entry = req.dict()
    res_entry["id"] = len(db.get("reservations", [])) + 1
    res_entry["status"] = "จองสำเร็จ"
    db.setdefault("reservations", []).append(res_entry)
    add_audit_log("Staff", "RESERVATION", f"จองโต๊ะ: {req.name} ({req.date} {req.time})")
    save_db(db)
    return {"success": True, "reservation": res_entry}

# ================= 9. เช็คบิล / ใบเสร็จ / สะสมแต้ม =================

@app.post("/api/checkout")
def checkout_order(req: CheckoutRequest):
    db = load_db()
    orders = [o for o in db.get("orders", []) if o["table_id"] == req.table_id and o.get("status") != "ยกเลิก"]
    if not orders:
        raise HTTPException(status_code=400, detail="ไม่มีรายการอาหารสำหรับชำระเงิน")

    subtotal, disc_amt, sc, vat, net = services.calculate_bill(orders, req.discount_percent)

    earned_pts = 0
    if req.member_phone:
        earned_pts = services.process_loyalty_points(req.member_phone, net)

    split_info = None
    if req.split_type == "split_even" and req.split_count and req.split_count > 1:
        split_info = {
            "split_count": req.split_count,
            "amount_per_person": round(net / req.split_count, 2)
        }

    receipt = {
        "receipt_id": f"REC-{datetime.now().strftime('%Y%m%d%H%M%S')}",
        "table_id": req.table_id,
        "items": orders,
        "subtotal": subtotal,
        "discount_percent": req.discount_percent,
        "discount_amount": disc_amt,
        "service_charge": sc,
        "vat": vat,
        "net_total": net,
        "split_info": split_info,
        "member_phone": req.member_phone,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    db.setdefault("sales", []).append(receipt)
    db["orders"] = [o for o in db.get("orders", []) if o["table_id"] != req.table_id]

    for t in db.get("tables", []):
        if t["table_id"] == req.table_id:
            t["status"] = "ว่าง"

    add_audit_log("Cashier", "CHECKOUT", f"เช็คบิลโต๊ะ {req.table_id} สุทธิ {net} ฿")
    save_db(db)
    return {"success": True, "receipt": receipt, "earned_points": earned_pts}

# ================= 10. Dashboard & Audit Logs =================

@app.get("/api/dashboard")
def get_dashboard():
    db = load_db()
    total_sales = sum(s.get("net_total", 0.0) for s in db.get("sales", []))
    occupied_tables = sum(1 for t in db.get("tables", []) if t.get("status") != "ว่าง")
    low_stock = [i for i in db.get("inventory", []) if i.get("stock", 0) <= i.get("min_stock", 10)]
    report = services.generate_sales_report()

    return {
        "total_revenue": round(total_sales, 2),
        "total_bills": len(db.get("sales", [])),
        "occupied_tables": occupied_tables,
        "total_tables": len(db.get("tables", [])),
        "low_stock_alerts": low_stock,
        "best_sellers": report.get("best_sellers", []),
        "recent_logs": db.get("audit_logs", [])[:10]
    }

@app.get("/api/logs")
def get_audit_logs(page: int = 1, limit: int = 15):
    db = load_db()
    logs = db.get("audit_logs", [])
    return services.paginate_and_sort(logs, sort_by="", page=page, limit=limit)

# ================= 11. UI พร้อมหน้า Login & แยกสิทธิ์ (Full SPA) =================

@app.get("/", response_class=HTMLResponse)
def index():
    return """<!DOCTYPE html>
<html lang="th">
<head>
  <meta charset="UTF-8">
  <title>RESTRO PRO - Smart Restaurant Management</title>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <script src="https://cdn.tailwindcss.com"></script>
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
  <style>
    @media print {
      body * { visibility: hidden; }
      #printable-receipt, #printable-receipt * { visibility: visible; }
      #printable-receipt { position: absolute; left: 0; top: 0; width: 100%; border: none !important; }
    }
    .custom-scroll::-webkit-scrollbar { width: 6px; height: 6px; }
    .custom-scroll::-webkit-scrollbar-thumb { background: #cbd5e1; border-radius: 4px; }
  </style>
</head>
<body class="bg-slate-100 text-slate-800 antialiased font-sans">

  <!-- Toast Notification Box -->
  <div id="toast-container" class="fixed top-5 right-5 z-50 space-y-2 pointer-events-none"></div>

  <!-- Modal แสดง QR Code ประจำโต๊ะ -->
  <div id="qr-modal" class="fixed inset-0 bg-black/60 z-50 hidden flex items-center justify-center p-4">
    <div class="bg-white rounded-2xl max-w-sm w-full p-6 text-center space-y-4 shadow-2xl">
      <h3 id="qr-modal-title" class="font-extrabold text-xl text-slate-800">QR Code สั่งอาหาร</h3>
      <p class="text-xs text-slate-500">สแกนเพื่อสั่งอาหารและระบุตัวเลือกได้ทันที</p>
      <div class="bg-slate-50 p-4 rounded-xl flex justify-center">
        <img id="qr-modal-img" src="" alt="Table QR" class="w-48 h-48 border rounded-lg shadow-sm">
      </div>
      <button onclick="document.getElementById('qr-modal').classList.add('hidden')" class="w-full bg-slate-800 hover:bg-slate-900 text-white font-bold py-2 rounded-xl text-sm transition">ปิดหน้าต่าง</button>
    </div>
  </div>

  <!-- ================= Modal สำหรับเพิ่มเมนูอาหารใหม่ (พร้อมจัดการรูปภาพ) ================= -->
  <div id="add-menu-modal" class="fixed inset-0 bg-black/60 z-50 hidden flex items-center justify-center p-4">
    <div class="bg-white rounded-3xl max-w-lg w-full p-6 shadow-2xl space-y-4 max-h-[92vh] overflow-y-auto custom-scroll">
      <div class="flex justify-between items-center border-b pb-3">
        <h3 class="font-extrabold text-lg text-slate-800">🍽️ เพิ่มเมนูอาหารใหม่</h3>
        <button onclick="document.getElementById('add-menu-modal').classList.add('hidden')" class="text-slate-400 hover:text-slate-600"><i class="fa-solid fa-xmark text-lg"></i></button>
      </div>
      <div class="space-y-3">
        <div>
          <label class="text-xs font-bold text-slate-600 block mb-1">ชื่อเมนูอาหาร</label>
          <input type="text" id="new-menu-name" placeholder="เช่น กะเพราหมูกรอบ" class="w-full border rounded-xl p-2.5 text-sm outline-none">
        </div>
        <div class="grid grid-cols-2 gap-3">
          <div>
            <label class="text-xs font-bold text-slate-600 block mb-1">หมวดหมู่</label>
            <input type="text" id="new-menu-cat" placeholder="เช่น อาหารจานเดียว" value="อาหารจานเดียว" class="w-full border rounded-xl p-2.5 text-sm outline-none">
          </div>
          <div>
            <label class="text-xs font-bold text-slate-600 block mb-1">ราคา (บาท)</label>
            <input type="number" id="new-menu-price" step="0.5" placeholder="0.00" class="w-full border rounded-xl p-2.5 text-sm outline-none font-bold text-blue-600">
          </div>
        </div>

        <!-- ตัวเลือกรูปภาพ (อัปโหลด หรือ URL) -->
        <div class="border rounded-2xl p-3 bg-slate-50/70 space-y-2">
          <label class="text-xs font-bold text-slate-700 block">🖼️ รูปภาพเมนูอาหาร</label>
          <div class="flex gap-2 text-xs">
            <button type="button" onclick="setImgMode('url')" id="btn-mode-url" class="flex-1 py-1.5 rounded-lg font-bold bg-blue-600 text-white transition">ใส่ลิงก์ (URL)</button>
            <button type="button" onclick="setImgMode('upload')" id="btn-mode-upload" class="flex-1 py-1.5 rounded-lg font-bold bg-slate-200 text-slate-700 transition">อัปโหลดไฟล์</button>
          </div>

          <!-- ช่องใส่ URL -->
          <div id="box-img-url">
            <input type="url" id="new-menu-img-url" oninput="previewImage()" placeholder="https://example.com/food.jpg" class="w-full border rounded-xl p-2 text-xs bg-white outline-none">
          </div>

          <!-- ช่องอัปโหลดไฟล์ -->
          <div id="box-img-upload" class="hidden">
            <input type="file" id="new-menu-img-file" accept="image/*" onchange="handleFileUpload(event)" class="w-full text-xs text-slate-500 file:mr-2 file:py-1.5 file:px-3 file:rounded-xl file:border-0 file:text-xs file:font-bold file:bg-blue-50 file:text-blue-700 hover:file:bg-blue-100">
          </div>

          <!-- Preview รูปภาพ -->
          <div id="menu-img-preview-box" class="hidden mt-2 text-center">
            <p class="text-[10px] text-slate-400 mb-1">ตัวอย่างรูปภาพ:</p>
            <img id="menu-img-preview" src="" alt="Preview" class="h-28 w-full object-cover rounded-xl border mx-auto shadow-sm">
          </div>
        </div>

        <!-- กำหนดสูตรตัดสต็อกวัตถุดิบ -->
        <div class="border-t pt-3">
          <div class="flex justify-between items-center mb-2">
            <label class="text-xs font-bold text-slate-700">📦 สูตรตัดสต็อกวัตถุดิบ (Recipe)</label>
            <button onclick="addRecipeRow()" type="button" class="text-xs text-blue-600 font-bold hover:underline">+ เพิ่มวัตถุดิบ</button>
          </div>
          <div id="recipe-rows-container" class="space-y-2"></div>
          <p class="text-[11px] text-slate-400 mt-1">* วัตถุดิบนี้จะถูกตัดสต็อกอัตโนมัติเมื่อห้องครัวเริ่มทำหรือเสิร์ฟเสร็จ</p>
        </div>

        <button onclick="submitNewMenu()" class="w-full bg-blue-600 hover:bg-blue-700 text-white font-extrabold py-3 rounded-2xl shadow-md transition mt-4">บันทึกเมนูใหม่</button>
      </div>
    </div>
  </div>

  <!-- ================= หน้าจอเข้าสู่ระบบ / สมัครสมาชิก (Auth Screen) ================= -->
  <div id="auth-screen" class="min-h-screen flex items-center justify-center bg-gradient-to-br from-slate-900 via-slate-800 to-blue-950 p-4">
    <div class="bg-white/95 backdrop-blur-md rounded-3xl p-8 max-w-md w-full shadow-2xl border border-white/20 space-y-6">
      <div class="text-center space-y-2">
        <div class="w-16 h-16 bg-blue-600 text-white rounded-2xl flex items-center justify-center text-3xl mx-auto shadow-lg shadow-blue-500/30">
          <i class="fa-solid fa-utensils"></i>
        </div>
        <h2 class="text-2xl font-black text-slate-800 tracking-tight">RESTRO PRO</h2>
        <p class="text-xs text-slate-500">ระบบจัดการร้านอาหารอัจฉริยะ</p>
      </div>

      <!-- แท็บสลับ เข้าสู่ระบบ / สมัครสมาชิก -->
      <div class="flex border-b text-sm font-bold">
        <button id="auth-tab-login" onclick="switchAuthTab('login')" class="flex-1 py-2 text-blue-600 border-b-2 border-blue-600">เข้าสู่ระบบ</button>
        <button id="auth-tab-reg" onclick="switchAuthTab('register')" class="flex-1 py-2 text-slate-400 hover:text-slate-600">สมัครสมาชิก</button>
      </div>

      <!-- ฟอร์มเข้าสู่ระบบ -->
      <div id="form-login" class="space-y-4">
        <div>
          <label class="text-xs font-bold text-slate-600 block mb-1">ชื่อผู้ใช้งาน</label>
          <input type="text" id="login-user" placeholder="Username" class="w-full border border-slate-300 rounded-xl p-3 text-sm focus:ring-2 focus:ring-blue-500 outline-none">
        </div>
        <div>
          <label class="text-xs font-bold text-slate-600 block mb-1">รหัสผ่าน</label>
          <input type="password" id="login-pass" placeholder="••••••••" class="w-full border border-slate-300 rounded-xl p-3 text-sm focus:ring-2 focus:ring-blue-500 outline-none">
        </div>
        <button onclick="handleLogin()" class="w-full bg-blue-600 hover:bg-blue-700 text-white font-bold py-3 rounded-xl text-sm shadow-md shadow-blue-600/30 transition">เข้าสู่ระบบ</button>

        <!-- แถบปุ่ม Quick Login -->
        <div class="pt-4 border-t space-y-2">
          <p class="text-xs text-slate-400 text-center font-medium">⚡ ปุ่มทดสอบสิทธิ์ด่วน:</p>
          <div class="grid grid-cols-3 gap-2">
            <button onclick="quickLogin('admin', 'admin123')" class="bg-purple-50 hover:bg-purple-100 text-purple-700 border border-purple-200 py-1.5 rounded-lg text-xs font-bold">👑 Admin</button>
            <button onclick="quickLogin('staff', 'staff123')" class="bg-blue-50 hover:bg-blue-100 text-blue-700 border border-blue-200 py-1.5 rounded-lg text-xs font-bold">👔 Staff</button>
            <button onclick="customerGuestLogin()" class="bg-emerald-50 hover:bg-emerald-100 text-emerald-700 border border-emerald-200 py-1.5 rounded-lg text-xs font-bold">👤 ลูกค้า</button>
          </div>
        </div>
      </div>

      <!-- ฟอร์มสมัครสมาชิก (ลูกค้าทั่วไปเท่านั้น) -->
      <div id="form-register" class="space-y-4 hidden">
        <div class="bg-blue-50/70 border border-blue-100 rounded-xl p-2.5 text-center text-xs text-blue-700 font-semibold">
          <i class="fa-solid fa-user mr-1"></i> สมัครสมาชิกในฐานะ: ลูกค้าทั่วไป (Customer)
        </div>
        <div>
          <label class="text-xs font-bold text-slate-600 block mb-1">ชื่อ-นามสกุล</label>
          <input type="text" id="reg-name" placeholder="ชื่อของคุณ" class="w-full border border-slate-300 rounded-xl p-2.5 text-sm outline-none">
        </div>
        <div>
          <label class="text-xs font-bold text-slate-600 block mb-1">ชื่อผู้ใช้งาน</label>
          <input type="text" id="reg-user" placeholder="Username" class="w-full border border-slate-300 rounded-xl p-2.5 text-sm outline-none">
        </div>
        <div>
          <label class="text-xs font-bold text-slate-600 block mb-1">รหัสผ่าน</label>
          <input type="password" id="reg-pass" placeholder="••••••••" class="w-full border border-slate-300 rounded-xl p-2.5 text-sm outline-none">
        </div>
        <button onclick="handleRegister()" class="w-full bg-emerald-600 hover:bg-emerald-700 text-white font-bold py-3 rounded-xl text-sm shadow-md transition">ลงทะเบียนลูกค้า</button>
      </div>
    </div>
  </div>

  <!-- ================= หน้าจอแอปพลิเคชันหลัก (Main Application Dashboard) ================= -->
  <div id="main-app" class="flex h-screen overflow-hidden hidden">
    
    <!-- Sidebar นำทาง -->
    <aside class="w-64 bg-slate-900 text-white flex flex-col justify-between p-4 shadow-2xl z-20">
      <div>
        <div class="flex items-center gap-3 p-3 bg-slate-800/80 rounded-2xl border border-slate-700/60 mb-6">
          <div id="user-avatar" class="w-10 h-10 rounded-xl bg-blue-600 flex items-center justify-center font-bold text-white text-base shadow-sm">U</div>
          <div class="flex-1 overflow-hidden">
            <h4 id="user-display-name" class="font-bold text-sm truncate text-white">ผู้ใช้งาน</h4>
            <span id="user-role-badge" class="inline-block text-[10px] font-extrabold uppercase px-2 py-0.5 rounded-full bg-blue-500/20 text-blue-400">ADMIN</span>
          </div>
        </div>

        <nav class="space-y-1 text-sm font-medium">
          <button onclick="switchTab('menu')" id="nav-menu" class="w-full text-left py-2.5 px-3 rounded-xl hover:bg-slate-800 flex items-center gap-3 transition"><i class="fa-solid fa-book-open w-5 text-amber-400"></i> เมนูอาหาร</button>
          <button onclick="switchTab('dash')" id="nav-dash" class="w-full text-left py-2.5 px-3 rounded-xl hover:bg-slate-800 flex items-center gap-3 transition"><i class="fa-solid fa-chart-pie w-5 text-blue-400"></i> Dashboard สรุป</button>
          <button onclick="switchTab('tables')" id="nav-tables" class="w-full text-left py-2.5 px-3 rounded-xl hover:bg-slate-800 flex items-center gap-3 transition"><i class="fa-solid fa-chair w-5 text-emerald-400"></i> แผนผังโต๊ะ & POS</button>
          <button onclick="switchTab('kitchen')" id="nav-kitchen" class="w-full text-left py-2.5 px-3 rounded-xl hover:bg-slate-800 flex items-center gap-3 transition"><i class="fa-solid fa-fire-burner w-5 text-amber-400"></i> จอครัว (KDS)</button>
          <button onclick="switchTab('checkout')" id="nav-checkout" class="w-full text-left py-2.5 px-3 rounded-xl hover:bg-slate-800 flex items-center gap-3 transition"><i class="fa-solid fa-receipt w-5 text-indigo-400"></i> เช็คบิล / ใบเสร็จ</button>
          <button onclick="switchTab('inventory')" id="nav-inventory" class="w-full text-left py-2.5 px-3 rounded-xl hover:bg-slate-800 flex items-center gap-3 transition"><i class="fa-solid fa-boxes-stacked w-5 text-purple-400"></i> สต็อกวัตถุดิบ</button>
          <button onclick="switchTab('queue')" id="nav-queue" class="w-full text-left py-2.5 px-3 rounded-xl hover:bg-slate-800 flex items-center gap-3 transition"><i class="fa-solid fa-users-line w-5 text-pink-400"></i> คิว & จองโต๊ะ</button>
          <button onclick="switchTab('logs')" id="nav-logs" class="w-full text-left py-2.5 px-3 rounded-xl hover:bg-slate-800 flex items-center gap-3 transition"><i class="fa-solid fa-clock-rotate-left w-5 text-rose-400"></i> Audit Logs</button>
          <button onclick="switchTab('qr')" id="nav-qr" class="w-full text-left py-2.5 px-3 rounded-xl hover:bg-slate-800 flex items-center gap-3 transition"><i class="fa-solid fa-mobile-screen w-5 text-teal-400"></i> ลูกค้าสั่งเอง (QR)</button>
        </nav>
      </div>

      <div class="space-y-3 pt-4 border-t border-slate-800">
        <div class="flex items-center justify-between text-xs px-1 text-slate-400">
          <span class="flex items-center gap-2 text-emerald-400 font-semibold">
            <span class="w-2 h-2 rounded-full bg-emerald-400 animate-ping"></span> Live Real-time
          </span>
          <span id="live-clock"></span>
        </div>
        <button onclick="handleLogout()" class="w-full bg-rose-500/10 hover:bg-rose-500/20 text-rose-400 border border-rose-500/20 py-2 rounded-xl text-xs font-bold transition flex items-center justify-center gap-2">
          <i class="fa-solid fa-right-from-bracket"></i> ออกจากระบบ
        </button>
      </div>
    </aside>

    <!-- Main Content Display -->
    <main class="flex-1 overflow-y-auto p-8 custom-scroll">

      <!-- ================= 0. TAB: ดูเมนูอาหารภายในร้าน (พร้อมรูปภาพสวยงาม) ================= -->
      <section id="pane-menu" class="space-y-6 hidden">
        <div class="flex flex-col md:flex-row justify-between items-start md:items-center gap-4">
          <div>
            <h2 class="text-2xl font-black text-slate-800">📋 เมนูอาหารและเครื่องดื่ม</h2>
            <p class="text-xs text-slate-500">รายการเมนูทั้งหมดของทางร้าน พร้อมรูปภาพและสูตรวัตถุดิบ</p>
          </div>
          <div id="menu-staff-controls" class="hidden">
            <button onclick="openAddMenuModal()" class="bg-blue-600 hover:bg-blue-700 text-white text-xs px-4 py-2.5 rounded-xl font-bold shadow-sm transition flex items-center gap-2">
              <i class="fa-solid fa-plus"></i> เพิ่มเมนูใหม่
            </button>
          </div>
        </div>

        <!-- แถบค้นหาและกรองหมวดหมู่ -->
        <div class="bg-white p-4 rounded-2xl border shadow-sm flex flex-col md:flex-row gap-3">
          <div class="flex-1 relative">
            <i class="fa-solid fa-magnifying-glass absolute left-3 top-3 text-slate-400 text-sm"></i>
            <input type="text" id="menu-search-input" oninput="loadCatalogMenus()" placeholder="ค้นหาชื่อเมนูอาหาร..." class="w-full pl-9 pr-3 py-2 border rounded-xl text-sm outline-none">
          </div>
          <div class="w-full md:w-60">
            <select id="menu-cat-filter" onchange="loadCatalogMenus()" class="w-full border rounded-xl py-2 px-3 text-sm bg-white outline-none font-semibold"></select>
          </div>
        </div>

        <!-- รายการการ์ดเมนูอาหารพร้อมรูปภาพ -->
        <div id="menu-catalog-grid" class="grid grid-cols-1 md:grid-cols-3 lg:grid-cols-4 gap-5"></div>
      </section>

      <!-- ================= 1. TAB: Dashboard ================= -->
      <section id="pane-dash" class="space-y-6 hidden">
        <div class="flex justify-between items-center">
          <div>
            <h2 class="text-2xl font-black text-slate-800">📊 ภาพรวมยอดขาย & การดำเนินงาน</h2>
            <p class="text-xs text-slate-500">รายงานข้อมูลและตัวชี้วัดสำคัญของร้านแบบ Real-time</p>
          </div>
          <button onclick="loadDashboard()" class="bg-white border px-3 py-1.5 rounded-xl text-xs font-bold hover:bg-slate-50 shadow-sm"><i class="fa-solid fa-rotate mr-1"></i> อัปเดต</button>
        </div>

        <div class="grid grid-cols-1 md:grid-cols-4 gap-4">
          <div class="bg-white p-5 rounded-2xl border shadow-sm"><p class="text-xs text-slate-400 font-bold uppercase">ยอดขายสุทธิ</p><h3 id="dash-rev" class="text-3xl font-black text-blue-600 mt-2">0.00 ฿</h3></div>
          <div class="bg-white p-5 rounded-2xl border shadow-sm"><p class="text-xs text-slate-400 font-bold uppercase">บิลที่ชำระแล้ว</p><h3 id="dash-bills" class="text-3xl font-black text-emerald-600 mt-2">0 บิล</h3></div>
          <div class="bg-white p-5 rounded-2xl border shadow-sm"><p class="text-xs text-slate-400 font-bold uppercase">โต๊ะที่เปิดอยู่</p><h3 id="dash-tables" class="text-3xl font-black text-amber-600 mt-2">0 / 0</h3></div>
          <div class="bg-white p-5 rounded-2xl border shadow-sm"><p class="text-xs text-slate-400 font-bold uppercase">วัตถุดิบใกล้หมด</p><h3 id="dash-stock" class="text-3xl font-black text-rose-600 mt-2">0 ชนิด</h3></div>
        </div>

        <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
          <div class="bg-white p-6 rounded-2xl border shadow-sm">
            <h3 class="font-extrabold text-base mb-4 text-slate-700 flex items-center gap-2"><i class="fa-solid fa-trophy text-amber-500"></i> 5 อันดับเมนูขายดีประจำวัน</h3>
            <div id="dash-top-sellers" class="space-y-2"></div>
          </div>
          <div class="bg-white p-6 rounded-2xl border shadow-sm">
            <h3 class="font-extrabold text-base mb-4 text-slate-700 flex items-center gap-2"><i class="fa-solid fa-triangle-exclamation text-rose-500"></i> แจ้งเตือนวัตถุดิบถึงจุดต่ำสุด</h3>
            <div id="dash-low-stock-list" class="space-y-2"></div>
          </div>
        </div>
      </section>

      <!-- ================= 2. TAB: แผนผังโต๊ะ & POS ================= -->
      <section id="pane-tables" class="space-y-6 hidden">
        <div class="flex justify-between items-center">
          <div>
            <h2 class="text-2xl font-black text-slate-800">🪑 แผนผังโต๊ะอาหาร & ระบบ POS</h2>
            <p class="text-xs text-slate-500">คลิกที่โต๊ะเพื่อรับออเดอร์ หรือกดปุ่ม QR เพื่อดูโค้ดสำหรับลูกค้า</p>
          </div>
          <div class="flex gap-2">
            <button onclick="openAddTableModal()" class="bg-blue-600 hover:bg-blue-700 text-white text-xs px-3.5 py-2 rounded-xl font-bold shadow-sm transition flex items-center gap-1.5">
              <i class="fa-solid fa-plus"></i> เพิ่มโต๊ะใหม่
            </button>
            <button onclick="openMoveModal()" class="bg-amber-600 hover:bg-amber-700 text-white text-xs px-3 py-2 rounded-xl font-bold shadow-sm transition"><i class="fa-solid fa-arrows-split-up-and-left mr-1"></i> ย้ายโต๊ะ</button>
            <button onclick="openMergeModal()" class="bg-indigo-600 hover:bg-indigo-700 text-white text-xs px-3 py-2 rounded-xl font-bold shadow-sm transition"><i class="fa-solid fa-object-group mr-1"></i> รวมโต๊ะ</button>
          </div>
        </div>

        <div id="tables-grid" class="grid grid-cols-2 md:grid-cols-4 gap-4"></div>

        <div id="table-order-box" class="hidden bg-white p-6 rounded-2xl border shadow-sm">
          <div class="flex justify-between items-center border-b pb-4 mb-4">
            <h3 id="selected-table-title" class="text-lg font-black text-slate-800">จัดการรายการอาหาร - โต๊ะ</h3>
            <button onclick="document.getElementById('table-order-box').classList.add('hidden')" class="text-slate-400 hover:text-slate-600"><i class="fa-solid fa-xmark text-lg"></i></button>
          </div>
          <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div>
              <h4 class="font-bold text-xs text-slate-500 uppercase mb-3">กดสั่งอาหารเข้าโต๊ะ:</h4>
              <div id="pos-menu-list" class="grid grid-cols-2 gap-3 max-h-80 overflow-y-auto pr-1 custom-scroll"></div>
            </div>
            <div class="border-l pl-6">
              <h4 class="font-bold text-xs text-slate-500 uppercase mb-3">รายการอาหารที่สั่งในโต๊ะนี้:</h4>
              <div id="pos-order-items" class="space-y-2 max-h-80 overflow-y-auto pr-1 custom-scroll"></div>
            </div>
          </div>
        </div>
      </section>

      <!-- ================= 3. TAB: จอครัว (KDS) ================= -->
      <section id="pane-kitchen" class="space-y-6 hidden">
        <div class="flex justify-between items-center">
          <div>
            <h2 class="text-2xl font-black text-slate-800">👨‍🍳 Kitchen Display System (KDS)</h2>
            <p class="text-xs text-slate-500">ออเดอร์ตัดสต็อกวัตถุดิบอัตโนมัติทั้งตอนกด "เริ่มทำ" และ "เสิร์ฟแล้ว"</p>
          </div>
          <button onclick="loadKitchenOrders()" class="bg-blue-600 text-white text-xs px-3 py-1.5 rounded-xl font-bold"><i class="fa-solid fa-rotate mr-1"></i> รีเฟรช</button>
        </div>
        <div id="kds-grid" class="grid grid-cols-1 md:grid-cols-3 gap-4"></div>
      </section>

      <!-- ================= 4. TAB: ลูกค้าสั่งเอง (QR Self-Order พร้อมดูรูปภาพ) ================= -->
      <section id="pane-qr" class="space-y-6 hidden">
        <div class="max-w-lg mx-auto bg-white p-6 rounded-3xl border shadow-xl space-y-4">
          <div class="text-center border-b pb-4">
            <span class="bg-blue-100 text-blue-800 text-xs px-3 py-1 rounded-full font-bold">โหมดลูกค้าสั่งอาหารเอง</span>
            <h3 class="text-xl font-black text-slate-800 mt-2">เลือกโต๊ะและปรับแต่งเมนูตามชอบ</h3>
          </div>
          <div class="space-y-3">
            <div>
              <label class="text-xs font-bold text-slate-600 block mb-1">หมายเลขโต๊ะของคุณ</label>
              <select id="qr-table-num" class="w-full border rounded-xl p-2.5 text-sm bg-white font-bold text-blue-600"></select>
            </div>
            <div>
              <label class="text-xs font-bold text-slate-600 block mb-1">เลือกเมนูอาหาร</label>
              <select id="qr-select-menu" onchange="updateQrMenuPreview()" class="w-full border rounded-xl p-2.5 text-sm bg-white"></select>
            </div>

            <!-- กล่องแสดงรูปเมนูที่ลูกค้ากำลังเลือก -->
            <div id="qr-dish-preview" class="border rounded-2xl overflow-hidden bg-slate-50 hidden">
              <img id="qr-dish-img" src="" alt="Menu" class="h-36 w-full object-cover">
              <div class="p-2.5 flex justify-between items-center text-xs">
                <span id="qr-dish-name" class="font-extrabold text-slate-800"></span>
                <span id="qr-dish-price" class="font-black text-blue-600"></span>
              </div>
            </div>

            <div class="grid grid-cols-3 gap-2">
              <div>
                <label class="text-[11px] font-bold text-slate-500">ความเผ็ด</label>
                <select id="qr-opt-spice" class="w-full border p-2 rounded-xl text-xs bg-white">
                  <option>ไม่เผ็ด</option><option selected>เผ็ดกลาง</option><option>เผ็ดมาก</option>
                </select>
              </div>
              <div>
                <label class="text-[11px] font-bold text-slate-500">เพิ่มไข่ (+10฿)</label>
                <select id="qr-opt-egg" class="w-full border p-2 rounded-xl text-xs bg-white">
                  <option>ไม่ใส่</option><option>ไข่ดาว</option><option>ไข่เจียว</option>
                </select>
              </div>
              <div>
                <label class="text-[11px] font-bold text-slate-500">ขนาด (+15฿)</label>
                <select id="qr-opt-size" class="w-full border p-2 rounded-xl text-xs bg-white">
                  <option>ธรรมดา</option><option>พิเศษ</option>
                </select>
              </div>
            </div>
            <button onclick="sendCustomerOrder()" class="w-full bg-blue-600 hover:bg-blue-700 text-white font-extrabold py-3.5 rounded-2xl shadow-lg shadow-blue-500/30 transition">
              <i class="fa-solid fa-paper-plane mr-1"></i> ยืนยันส่งออเดอร์เข้าครัว
            </button>
          </div>
        </div>
      </section>

      <!-- ================= 5. TAB: เช็คบิล / ใบเสร็จ ================= -->
      <section id="pane-checkout" class="space-y-6 hidden">
        <h2 class="text-2xl font-black text-slate-800">💵 คิดเงิน ออกใบเสร็จ & ระบบสะสมแต้ม</h2>
        <div class="grid grid-cols-1 md:grid-cols-2 gap-8">
          <div class="bg-white p-6 rounded-2xl border shadow-sm space-y-4">
            <h3 class="font-extrabold text-base text-slate-700">คำนวณบิลชำระเงิน</h3>
            <div>
              <label class="text-xs font-bold text-slate-600 block mb-1">เลือกโต๊ะที่ต้องการเช็คบิล</label>
              <select id="bill-table-sel" class="w-full border p-2.5 rounded-xl text-sm bg-white font-bold"></select>
            </div>
            <div class="grid grid-cols-2 gap-3">
              <div>
                <label class="text-xs font-bold text-slate-600 block mb-1">ส่วนลดพิเศษ (%)</label>
                <input type="number" id="bill-discount" value="0" min="0" max="100" class="w-full border p-2 rounded-xl text-sm font-semibold">
              </div>
              <div>
                <label class="text-xs font-bold text-slate-600 block mb-1">เบอร์สมาชิก (10฿ = 1 แต้ม)</label>
                <input type="text" id="bill-member" placeholder="08XXXXXXXX" class="w-full border p-2 rounded-xl text-sm">
              </div>
            </div>
            <div class="border-t pt-3 space-y-2">
              <label class="text-xs font-bold text-slate-600 block">รูปแบบการจ่ายเงิน (Split Bill)</label>
              <div class="flex gap-3">
                <select id="bill-split-type" class="flex-1 border p-2 rounded-xl text-xs bg-white">
                  <option value="full">จ่ายเต็มบิลคนเดียว</option>
                  <option value="split_even">หารเท่ากัน (American Share)</option>
                </select>
                <input type="number" id="bill-split-count" value="2" min="2" class="w-24 border p-2 rounded-xl text-xs text-center" placeholder="กี่คน">
              </div>
            </div>
            <button onclick="executeCheckout()" class="w-full bg-emerald-600 hover:bg-emerald-700 text-white font-black py-3 rounded-xl shadow-md transition">
              <i class="fa-solid fa-check-double mr-1"></i> เช็คบิลและพิมพ์ใบเสร็จ
            </button>
          </div>

          <!-- Printable Thermal Receipt Box -->
          <div id="printable-receipt" class="bg-white p-6 rounded-2xl border-2 border-dashed border-slate-300 font-mono text-sm max-w-sm mx-auto shadow-sm">
            <div class="text-center pb-3 border-b border-dashed border-slate-300">
              <h4 class="font-black text-base tracking-wide">RESTRO PRO RECEIPT</h4>
              <p id="rc-id" class="text-[11px] text-slate-400">REC-XXXXXXXX</p>
              <p id="rc-date" class="text-[11px] text-slate-400"></p>
              <p id="rc-table" class="text-xs font-bold mt-1 text-slate-700">โต๊ะ: -</p>
            </div>
            <div id="rc-items" class="py-3 border-b border-dashed border-slate-300 space-y-1 text-xs"></div>
            <div class="py-2 space-y-1 text-xs">
              <div class="flex justify-between"><span>ยอดรวม:</span><span id="rc-sub">0.00 ฿</span></div>
              <div class="flex justify-between"><span>ส่วนลด:</span><span id="rc-disc">0.00 ฿</span></div>
              <div class="flex justify-between"><span>ค่าบริการ (10%):</span><span id="rc-sc">0.00 ฿</span></div>
              <div class="flex justify-between"><span>ภาษี (7%):</span><span id="rc-vat">0.00 ฿</span></div>
              <div class="flex justify-between font-black text-sm text-slate-900 pt-2 border-t"><span>ยอดสุทธิ:</span><span id="rc-net">0.00 ฿</span></div>
              <div id="rc-split-box" class="hidden text-blue-600 font-bold mt-2 pt-1 border-t text-center text-xs"></div>
              <div id="rc-point-box" class="text-emerald-600 text-center font-bold mt-1 text-xs"></div>
            </div>
            <button onclick="window.print()" class="w-full mt-4 bg-slate-900 text-white py-2 rounded-xl text-xs font-sans hover:bg-black font-bold">🖨️ พิมพ์ใบเสร็จ</button>
          </div>
        </div>
      </section>

      <!-- ================= 6. TAB: สต็อกวัตถุดิบ (Inventory) ================= -->
      <section id="pane-inventory" class="space-y-6 hidden">
        <div class="flex justify-between items-center">
          <div>
            <h2 class="text-2xl font-black text-slate-800">📦 คลังวัตถุดิบ (ตัดสต็อกตามสูตร)</h2>
            <p class="text-xs text-slate-500">วัตถุดิบจะถูกหักลบอัตโนมัติเมื่อห้องครัวกดยืนยันเริ่มทำหรือเสิร์ฟเสร็จ</p>
          </div>
          <button onclick="openRestockPrompt()" class="bg-blue-600 text-white text-xs px-4 py-2 rounded-xl font-bold">+ เติมสต็อกวัตถุดิบ</button>
        </div>
        <div class="bg-white rounded-2xl border shadow-sm overflow-hidden">
          <table class="w-full text-left text-sm">
            <thead class="bg-slate-50 text-slate-500 font-bold text-xs uppercase border-b">
              <tr>
                <th class="p-3">รหัส</th>
                <th class="p-3">ชื่อวัตถุดิบ</th>
                <th class="p-3">คงเหลือ</th>
                <th class="p-3">หน่วย</th>
                <th class="p-3">จุดเตือนขั้นต่ำ</th>
                <th class="p-3">สถานะ</th>
              </tr>
            </thead>
            <tbody id="inventory-table-body"></tbody>
          </table>
        </div>
      </section>

      <!-- ================= 7. TAB: คิว & จองโต๊ะ ================= -->
      <section id="pane-queue" class="space-y-6 hidden">
        <h2 class="text-2xl font-black text-slate-800">👥 บัตรคิวหน้าร้าน & จองโต๊ะล่วงหน้า</h2>
        <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
          <div class="bg-white p-6 rounded-2xl border shadow-sm space-y-4">
            <h3 class="font-extrabold text-base text-slate-700">ออกบัตรคิวหน้าร้าน</h3>
            <div class="space-y-3">
              <input type="text" id="q-name" placeholder="ชื่อลูกค้า" class="w-full border p-2.5 rounded-xl text-sm">
              <input type="number" id="q-size" placeholder="จำนวนคน" class="w-full border p-2.5 rounded-xl text-sm">
              <button onclick="createQueueTicket()" class="w-full bg-blue-600 text-white py-2.5 rounded-xl text-xs font-bold">ออกบัตรคิว</button>
            </div>
            <div class="mt-4">
              <h4 class="font-bold text-xs text-slate-500 uppercase mb-2">คิวที่รออยู่:</h4>
              <div id="queue-list" class="space-y-2"></div>
            </div>
          </div>
          <div class="bg-white p-6 rounded-2xl border shadow-sm space-y-4">
            <h3 class="font-extrabold text-base text-slate-700">บันทึกการจองโต๊ะล่วงหน้า</h3>
            <div class="space-y-3">
              <input type="text" id="res-name" placeholder="ชื่อผู้จอง" class="w-full border p-2.5 rounded-xl text-sm">
              <input type="text" id="res-phone" placeholder="เบอร์โทรศัพท์" class="w-full border p-2.5 rounded-xl text-sm">
              <div class="grid grid-cols-2 gap-2">
                <input type="date" id="res-date" class="border p-2.5 rounded-xl text-sm">
                <input type="time" id="res-time" class="border p-2.5 rounded-xl text-sm">
              </div>
              <input type="number" id="res-size" placeholder="จำนวนที่นั่ง" class="w-full border p-2.5 rounded-xl text-sm">
              <button onclick="createReservation()" class="w-full bg-indigo-600 text-white py-2.5 rounded-xl text-xs font-bold">บันทึกการจอง</button>
            </div>
          </div>
        </div>
      </section>

      <!-- ================= 8. TAB: Audit Logs ================= -->
      <section id="pane-logs" class="space-y-6 hidden">
        <h2 class="text-2xl font-black text-slate-800">📝 Audit Logs (ประวัติการเปลี่ยนแปลงข้อมูลสำคัญ)</h2>
        <div class="bg-white rounded-2xl border shadow-sm overflow-hidden">
          <table class="w-full text-left text-xs">
            <thead class="bg-slate-50 text-slate-500 font-bold uppercase border-b">
              <tr>
                <th class="p-3">วัน-เวลา</th>
                <th class="p-3">ผู้ใช้งาน</th>
                <th class="p-3">กิจกรรม</th>
                <th class="p-3">รายละเอียด</th>
              </tr>
            </thead>
            <tbody id="logs-table-body"></tbody>
          </table>
        </div>
      </section>

    </main>
  </div>

  <!-- JavaScript Frontend Logic -->
  <script>
    let currentUser = null;
    let currentTable = null;
    let cachedMenu = [];
    let cachedInventory = [];
    let uploadedImageBase64 = "";

    function showToast(msg, type = 'info') {
      const box = document.getElementById('toast-container');
      const toast = document.createElement('div');
      const colors = {
        success: 'bg-emerald-600 text-white',
        error: 'bg-rose-600 text-white',
        info: 'bg-slate-900 text-white'
      };
      toast.className = `${colors[type] || colors.info} px-4 py-3 rounded-2xl text-xs font-bold shadow-xl flex items-center gap-2 transform transition-all duration-300 translate-y-2 opacity-0 pointer-events-auto`;
      toast.innerHTML = `<i class="fa-solid fa-circle-info"></i> <span>${msg}</span>`;
      box.appendChild(toast);
      setTimeout(() => { toast.classList.remove('translate-y-2', 'opacity-0'); }, 10);
      setTimeout(() => {
        toast.classList.add('opacity-0', 'translate-y-2');
        setTimeout(() => toast.remove(), 300);
      }, 3000);
    }

    function switchAuthTab(type) {
      if(type === 'login') {
        document.getElementById('form-login').classList.remove('hidden');
        document.getElementById('form-register').classList.add('hidden');
        document.getElementById('auth-tab-login').className = 'flex-1 py-2 text-blue-600 border-b-2 border-blue-600';
        document.getElementById('auth-tab-reg').className = 'flex-1 py-2 text-slate-400 hover:text-slate-600';
      } else {
        document.getElementById('form-login').classList.add('hidden');
        document.getElementById('form-register').classList.remove('hidden');
        document.getElementById('auth-tab-reg').className = 'flex-1 py-2 text-blue-600 border-b-2 border-blue-600';
        document.getElementById('auth-tab-login').className = 'flex-1 py-2 text-slate-400 hover:text-slate-600';
      }
    }

    function quickLogin(u, p) {
      document.getElementById('login-user').value = u;
      document.getElementById('login-pass').value = p;
      handleLogin();
    }

    function customerGuestLogin() {
      setupSession({ name: 'ลูกค้าทั่วไป (Guest)', role: 'customer', username: 'guest' });
      showToast('เข้าใช้งานในโหมดลูกค้าเรียบร้อย', 'success');
    }

    function validateAuthInputs(u, p) {
      const userRegex = /^[a-zA-Z0-9]+$/;
      if (!userRegex.test(u)) {
        showToast('ชื่อผู้ใช้ต้องเป็นตัวอักษรภาษาอังกฤษและตัวเลขเท่านั้น (ห้ามใช้ภาษาไทย)', 'error');
        return false;
      }
      if (p.length < 6) {
        showToast('รหัสผ่านต้องมีความยาวอย่างน้อย 6 ตัวอักษร', 'error');
        return false;
      }
      return true;
    }

    async function handleLogin() {
      const u = document.getElementById('login-user').value.trim();
      const p = document.getElementById('login-pass').value.trim();
      if(!u || !p) return showToast('กรุณากรอกข้อมูลให้ครบถ้วน', 'error');
      if(!validateAuthInputs(u, p)) return;

      const res = await fetch('/api/auth/login', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ username: u, password: p })
      });
      const data = await res.json();
      if(res.ok) {
        setupSession(data);
        showToast(`ยินดีต้อนรับคุณ ${data.name}!`, 'success');
      } else {
        showToast(data.detail || 'เข้าสู่ระบบไม่สำเร็จ', 'error');
      }
    }

    async function handleRegister() {
      const name = document.getElementById('reg-name').value.trim();
      const u = document.getElementById('reg-user').value.trim();
      const p = document.getElementById('reg-pass').value.trim();

      if(!name || !u || !p) return showToast('กรุณากรอกข้อมูลให้ครบถ้วน', 'error');
      if(!validateAuthInputs(u, p)) return;

      const res = await fetch('/api/auth/register', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ name: name, username: u, password: p })
      });
      const data = await res.json();
      if(res.ok) {
        showToast('ลงทะเบียนลูกค้าสำเร็จ! กรุณาเข้าสู่ระบบ', 'success');
        switchAuthTab('login');
      } else {
        showToast(data.detail || 'เกิดข้อผิดพลาดในการลงทะเบียน', 'error');
      }
    }

    function setupSession(user) {
      currentUser = user;
      localStorage.setItem('restro_user', JSON.stringify(user));
      document.getElementById('auth-screen').classList.add('hidden');
      document.getElementById('main-app').classList.remove('hidden');

      document.getElementById('user-display-name').innerText = user.name;
      document.getElementById('user-role-badge').innerText = user.role.toUpperCase();
      document.getElementById('user-avatar').innerText = user.name.charAt(0);

      applyRolePermissions(user.role);
    }

    function applyRolePermissions(role) {
      ['dash', 'tables', 'kitchen', 'checkout', 'inventory', 'queue', 'logs', 'qr', 'menu'].forEach(t => {
        const btn = document.getElementById('nav-' + t);
        if(btn) btn.classList.add('hidden');
      });

      const menuControls = document.getElementById('menu-staff-controls');
      if(menuControls) {
        if(role === 'admin' || role === 'staff') {
          menuControls.classList.remove('hidden');
        } else {
          menuControls.classList.add('hidden');
        }
      }

      if(role === 'admin') {
        ['menu', 'dash', 'tables', 'kitchen', 'checkout', 'inventory', 'queue', 'logs', 'qr'].forEach(t => {
          document.getElementById('nav-' + t).classList.remove('hidden');
        });
        switchTab('dash');
      } else if(role === 'staff') {
        ['menu', 'tables', 'kitchen', 'checkout', 'inventory', 'queue'].forEach(t => {
          document.getElementById('nav-' + t).classList.remove('hidden');
        });
        switchTab('menu');
      } else {
        ['menu', 'qr', 'queue'].forEach(t => {
          document.getElementById('nav-' + t).classList.remove('hidden');
        });
        switchTab('menu');
      }
    }

    function handleLogout() {
      localStorage.removeItem('restro_user');
      currentUser = null;
      document.getElementById('main-app').classList.add('hidden');
      document.getElementById('auth-screen').classList.remove('hidden');
      showToast('ออกจากระบบเรียบร้อย', 'info');
    }

    function switchTab(name) {
      document.querySelectorAll('main > section').forEach(s => s.classList.add('hidden'));
      document.querySelectorAll('aside nav button').forEach(b => {
        b.classList.remove('bg-blue-600', 'text-white');
        b.classList.add('text-slate-300');
      });
      const targetPane = document.getElementById('pane-' + name);
      const targetNav = document.getElementById('nav-' + name);
      if(targetPane) targetPane.classList.remove('hidden');
      if(targetNav) {
        targetNav.classList.remove('text-slate-300');
        targetNav.classList.add('bg-blue-600', 'text-white');
      }

      if(name === 'menu') loadCatalogMenus();
      if(name === 'dash') loadDashboard();
      if(name === 'tables') loadTables();
      if(name === 'kitchen') loadKitchenOrders();
      if(name === 'inventory') loadInventory();
      if(name === 'queue') loadQueues();
      if(name === 'logs') loadLogs();
      if(name === 'checkout') loadCheckoutTables();
      if(name === 'qr') loadQrMenu();
    }

    // ================= จัดการรูปภาพในเมนู =================
    function setImgMode(mode) {
      if(mode === 'url') {
        document.getElementById('box-img-url').classList.remove('hidden');
        document.getElementById('box-img-upload').classList.add('hidden');
        document.getElementById('btn-mode-url').className = 'flex-1 py-1.5 rounded-lg font-bold bg-blue-600 text-white transition';
        document.getElementById('btn-mode-upload').className = 'flex-1 py-1.5 rounded-lg font-bold bg-slate-200 text-slate-700 transition';
        uploadedImageBase64 = "";
        previewImage();
      } else {
        document.getElementById('box-img-url').classList.add('hidden');
        document.getElementById('box-img-upload').classList.remove('hidden');
        document.getElementById('btn-mode-upload').className = 'flex-1 py-1.5 rounded-lg font-bold bg-blue-600 text-white transition';
        document.getElementById('btn-mode-url').className = 'flex-1 py-1.5 rounded-lg font-bold bg-slate-200 text-slate-700 transition';
        document.getElementById('new-menu-img-url').value = "";
        previewImage();
      }
    }

    function handleFileUpload(event) {
      const file = event.target.files[0];
      if (file) {
        if (file.size > 3 * 1024 * 1024) {
          showToast('ไฟล์รูปภาพต้องมีขนาดไม่เกิน 3MB', 'error');
          event.target.value = "";
          return;
        }
        const reader = new FileReader();
        reader.onload = function(e) {
          uploadedImageBase64 = e.target.result;
          document.getElementById('menu-img-preview').src = uploadedImageBase64;
          document.getElementById('menu-img-preview-box').classList.remove('hidden');
        };
        reader.readAsDataURL(file);
      }
    }

    function previewImage() {
      const url = document.getElementById('new-menu-img-url').value.trim();
      const preview = document.getElementById('menu-img-preview');
      const box = document.getElementById('menu-img-preview-box');

      if (uploadedImageBase64) {
        preview.src = uploadedImageBase64;
        box.classList.remove('hidden');
      } else if (url) {
        preview.src = url;
        box.classList.remove('hidden');
      } else {
        preview.src = "";
        box.classList.add('hidden');
      }
    }

    // ================= โหลดและแสดงแคตตาล็อกเมนู =================
    async function loadCatalogMenus() {
      const search = document.getElementById('menu-search-input').value.trim();
      const cat = document.getElementById('menu-cat-filter').value;
      
      const res = await fetch(`/api/menu?search=${encodeURIComponent(search)}&category=${encodeURIComponent(cat)}&limit=100`);
      const data = await res.json();
      cachedMenu = data.items;

      const catFilter = document.getElementById('menu-cat-filter');
      const curCat = catFilter.value;
      catFilter.innerHTML = '<option value="">ทุกหมวดหมู่</option>' + data.categories.map(c => `<option value="${c}" ${c === curCat ? 'selected' : ''}>${c}</option>`).join('');

      const invRes = await fetch('/api/inventory');
      cachedInventory = await invRes.json();

      const grid = document.getElementById('menu-catalog-grid');
      const isStaffOrAdmin = currentUser && (currentUser.role === 'admin' || currentUser.role === 'staff');

      grid.innerHTML = data.items.map(m => {
        let recipeTxt = (m.recipe && m.recipe.length > 0) ? m.recipe.map(r => {
          const invItem = cachedInventory.find(i => i.id === r.ingredient_id);
          return `${invItem ? invItem.name : r.ingredient_id} (${r.amount})`;
        }).join(', ') : 'ไม่มีสูตรตัดสต็อก';

        let imgHtml = m.image ? 
          `<img src="${m.image}" alt="${m.name}" class="h-44 w-full object-cover rounded-t-2xl">` : 
          `<div class="h-44 w-full bg-slate-100 flex flex-col items-center justify-center text-slate-300 rounded-t-2xl">
            <i class="fa-solid fa-bowl-food text-4xl mb-1"></i>
            <span class="text-[10px] text-slate-400">ไม่มีรูปภาพ</span>
           </div>`;

        return `
          <div class="bg-white rounded-2xl border shadow-sm flex flex-col justify-between hover:shadow-md transition overflow-hidden">
            <div class="relative">
              ${imgHtml}
              <span class="absolute top-2 right-2 text-[10px] font-extrabold px-2 py-0.5 rounded-full shadow-sm ${m.is_available ? 'bg-emerald-500 text-white' : 'bg-rose-500 text-white'}">
                ${m.is_available ? 'พร้อมขาย' : 'หมด'}
              </span>
              <span class="absolute bottom-2 left-2 text-[10px] font-bold px-2 py-0.5 rounded-md bg-black/60 text-white backdrop-blur-sm">
                ${m.category}
              </span>
            </div>
            <div class="p-4 flex-1 flex flex-col justify-between">
              <div>
                <h4 class="font-black text-base text-slate-800">${m.name}</h4>
                <p class="text-xs text-slate-400 mt-1 line-clamp-1" title="${recipeTxt}"><i class="fa-solid fa-boxes-stacked mr-1"></i>${recipeTxt}</p>
              </div>
              <div class="mt-4 pt-3 border-t">
                <div class="text-blue-600 font-black text-lg mb-3">${m.price.toFixed(2)} ฿</div>
                <div class="flex gap-2">
                  ${isStaffOrAdmin ? `
                    <button onclick="toggleMenu(${m.id})" class="flex-1 bg-slate-100 hover:bg-slate-200 text-slate-700 text-xs py-1.5 rounded-xl font-bold transition">สลับสถานะ</button>
                    <button onclick="deleteMenu(${m.id})" class="text-slate-400 hover:text-rose-600 px-2 py-1.5 rounded-xl text-xs" title="ลบเมนู"><i class="fa-solid fa-trash-can"></i></button>
                  ` : `
                    <button onclick="quickOrderMenu(${m.id})" class="w-full bg-blue-600 hover:bg-blue-700 text-white text-xs py-2 rounded-xl font-bold transition">สั่งเมนูนี้ (QR)</button>
                  `}
                </div>
              </div>
            </div>
          </div>
        `;
      }).join('') || '<p class="text-slate-400 text-xs col-span-4 text-center py-8">ไม่พบรายการเมนู</p>';
    }

    async function toggleMenu(mid) {
      await fetch(`/api/menu/${mid}/toggle`, { method: 'POST' });
      showToast('ปรับสถานะเมนูเรียบร้อย', 'success');
      loadCatalogMenus();
    }

    async function deleteMenu(mid) {
      if(!confirm('คุณแน่ใจหรือไม่ว่าต้องการลบเมนูนี้?')) return;
      const res = await fetch(`/api/menu/${mid}`, { method: 'DELETE' });
      if(res.ok) {
        showToast('ลบเมนูสำเร็จ', 'success');
        loadCatalogMenus();
      }
    }

    function quickOrderMenu(mid) {
      switchTab('qr');
      document.getElementById('qr-select-menu').value = mid;
      updateQrMenuPreview();
    }

    async function openAddMenuModal() {
      const invRes = await fetch('/api/inventory');
      cachedInventory = await invRes.json();
      document.getElementById('recipe-rows-container').innerHTML = '';
      addRecipeRow();
      
      // รีเซ็ตรูปภาพ
      uploadedImageBase64 = "";
      document.getElementById('new-menu-img-url').value = "";
      document.getElementById('new-menu-img-file').value = "";
      document.getElementById('menu-img-preview-box').classList.add('hidden');
      setImgMode('url');

      document.getElementById('add-menu-modal').classList.remove('hidden');
    }

    function addRecipeRow() {
      const c = document.getElementById('recipe-rows-container');
      const row = document.createElement('div');
      row.className = 'flex gap-2 items-center';
      row.innerHTML = `
        <select class="recipe-ing-sel flex-1 border rounded-xl p-2 text-xs bg-white outline-none">
          ${cachedInventory.map(i => `<option value="${i.id}">${i.name} (คงเหลือ: ${i.stock} ${i.unit})</option>`).join('')}
        </select>
        <input type="number" step="0.1" placeholder="ปริมาณที่ใช้" class="recipe-amt-input w-28 border rounded-xl p-2 text-xs outline-none font-bold">
        <button onclick="this.parentElement.remove()" type="button" class="text-slate-400 hover:text-rose-600 p-1"><i class="fa-solid fa-xmark"></i></button>
      `;
      c.appendChild(row);
    }

    async function submitNewMenu() {
      const name = document.getElementById('new-menu-name').value.trim();
      const cat = document.getElementById('new-menu-cat').value.trim();
      const price = parseFloat(document.getElementById('new-menu-price').value);
      const imgUrl = document.getElementById('new-menu-img-url').value.trim();
      const finalImage = uploadedImageBase64 || imgUrl || "";

      if(!name || isNaN(price) || price < 0) {
        return showToast('กรุณากรอกชื่อและราคาให้ถูกต้อง', 'error');
      }

      const recipe = [];
      document.querySelectorAll('#recipe-rows-container > div').forEach(row => {
        const ingId = row.querySelector('.recipe-ing-sel').value;
        const amt = parseFloat(row.querySelector('.recipe-amt-input').value);
        if(ingId && !isNaN(amt) && amt > 0) {
          recipe.push({ ingredient_id: ingId, amount: amt });
        }
      });

      const res = await fetch('/api/menu', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
          name: name,
          category: cat || 'อาหารจานเดียว',
          price: price,
          image: finalImage,
          is_available: true,
          recipe: recipe
        })
      });

      if(res.ok) {
        showToast('เพิ่มเมนูอาหารเรียบร้อยแล้ว!', 'success');
        document.getElementById('add-menu-modal').classList.add('hidden');
        document.getElementById('new-menu-name').value = '';
        document.getElementById('new-menu-price').value = '';
        loadCatalogMenus();
      } else {
        showToast('ไม่สามารถเพิ่มเมนูได้', 'error');
      }
    }

    async function loadDashboard() {
      const res = await fetch('/api/dashboard');
      const d = await res.json();
      document.getElementById('dash-rev').innerText = d.total_revenue.toFixed(2) + ' ฿';
      document.getElementById('dash-bills').innerText = d.total_bills + ' บิล';
      document.getElementById('dash-tables').innerText = `${d.occupied_tables} / ${d.total_tables}`;
      document.getElementById('dash-stock').innerText = d.low_stock_alerts.length + ' ชนิด';

      const topBox = document.getElementById('dash-top-sellers');
      topBox.innerHTML = d.best_sellers.map((b, i) => `
        <div class="flex justify-between items-center text-xs p-2.5 bg-slate-50 rounded-xl">
          <span><b>#${i+1}</b> ${b.name}</span>
          <span class="font-extrabold text-blue-600">${b.qty} จาน</span>
        </div>
      `).join('') || '<p class="text-slate-400 text-xs">ยังไม่มีข้อมูลการขายในวันนี้</p>';

      const lowBox = document.getElementById('dash-low-stock-list');
      lowBox.innerHTML = d.low_stock_alerts.map(l => `
        <div class="flex justify-between items-center text-xs p-2 bg-rose-50 text-rose-700 rounded-xl border border-rose-100">
          <span><i class="fa-solid fa-triangle-exclamation mr-1"></i> ${l.name}</span>
          <b>เหลือ ${l.stock} ${l.unit}</b>
        </div>
      `).join('') || '<p class="text-emerald-600 text-xs">วัตถุดิบทั้งหมดอยู่ในเกณฑ์ปลอดภัย</p>';
    }

    async function loadTables() {
      const res = await fetch('/api/tables');
      const tables = await res.json();
      const grid = document.getElementById('tables-grid');
      grid.innerHTML = tables.map(t => {
        let col = t.status === 'ว่าง' ? 'border-emerald-300 bg-emerald-50/60 text-emerald-800' : (t.status === 'มีลูกค้า' ? 'border-rose-300 bg-rose-50/60 text-rose-800' : 'border-amber-300 bg-amber-50/60 text-amber-800');
        let canDelete = t.status === 'ว่าง' && currentUser && currentUser.role === 'admin';
        return `
          <div class="p-5 rounded-2xl border-2 ${col} shadow-sm relative group">
            <div class="flex justify-between items-center mb-2">
              <h4 class="font-black text-lg">โต๊ะ ${t.table_id}</h4>
              <div class="flex items-center gap-1.5">
                <span class="text-[10px] px-2 py-0.5 rounded-full font-extrabold bg-white shadow-sm">${t.status}</span>
                ${canDelete ? `<button onclick="deleteTable(${t.table_id})" title="ลบโต๊ะนี้" class="text-slate-400 hover:text-rose-600 text-xs p-1"><i class="fa-solid fa-trash-can"></i></button>` : ''}
              </div>
            </div>
            <p class="text-xs opacity-70">ความจุ ${t.capacity} ที่นั่ง</p>
            <div class="mt-4 flex gap-2">
              <button onclick="selectPOS(${t.table_id})" class="flex-1 bg-white hover:bg-slate-100 border text-xs py-1.5 rounded-xl font-bold transition">สั่งอาหาร</button>
              <button onclick="showTableQr(${t.table_id})" title="ดู QR Code โต๊ะนี้" class="bg-slate-800 hover:bg-black text-white px-2.5 py-1.5 rounded-xl text-xs"><i class="fa-solid fa-qrcode"></i></button>
            </div>
          </div>
        `;
      }).join('');
    }

    async function openAddTableModal() {
      const capStr = prompt('กรุณาระบุจำนวนที่นั่งสำหรับโต๊ะใหม่ (เช่น 2, 4, 6):', '4');
      if (capStr !== null) {
        const capacity = parseInt(capStr.trim());
        if (isNaN(capacity) || capacity <= 0) {
          return showToast('กรุณาระบุจำนวนที่นั่งเป็นตัวเลขมากกว่า 0', 'error');
        }

        const res = await fetch('/api/tables', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({ capacity: capacity })
        });
        const data = await res.json();
        if (res.ok) {
          showToast(`เพิ่มโต๊ะหมายเลข ${data.table.table_id} สำเร็จ! (${capacity} ที่นั่ง)`, 'success');
          loadTables();
          loadCheckoutTables();
          loadQrMenu();
        } else {
          showToast(data.detail || 'ไม่สามารถเพิ่มโต๊ะได้', 'error');
        }
      }
    }

    async function deleteTable(tid) {
      if (!confirm(`คุณแน่ใจหรือไม่ว่าต้องการลบ "โต๊ะ ${tid}" ออกจากระบบ?`)) return;

      const res = await fetch(`/api/tables/${tid}`, { method: 'DELETE' });
      const data = await res.json();
      if (res.ok) {
        showToast(data.message, 'success');
        loadTables();
        loadCheckoutTables();
        loadQrMenu();
      } else {
        showToast(data.detail || 'ไม่สามารถลบโต๊ะได้', 'error');
      }
    }

    function showTableQr(tid) {
      document.getElementById('qr-modal-title').innerText = `QR Code - โต๊ะ ${tid}`;
      document.getElementById('qr-modal-img').src = `https://api.qrserver.com/v1/create-qr-code/?size=250x250&data=${encodeURIComponent(window.location.origin + '/?table=' + tid)}`;
      document.getElementById('qr-modal').classList.remove('hidden');
    }

    async function selectPOS(tid) {
      currentTable = tid;
      document.getElementById('table-order-box').classList.remove('hidden');
      document.getElementById('selected-table-title').innerText = `จัดการรายการอาหาร - โต๊ะ ${tid}`;
      
      const mRes = await fetch('/api/menu?limit=50');
      const mData = await mRes.json();
      cachedMenu = mData.items;

      document.getElementById('pos-menu-list').innerHTML = cachedMenu.map(m => `
        <div class="border p-2 rounded-xl text-center bg-slate-50 hover:bg-slate-100 transition flex flex-col justify-between">
          <div class="h-20 w-full mb-1.5 overflow-hidden rounded-lg bg-slate-200">
            ${m.image ? `<img src="${m.image}" alt="${m.name}" class="w-full h-full object-cover">` : `<div class="w-full h-full flex items-center justify-center text-slate-400 text-xs"><i class="fa-solid fa-utensils"></i></div>`}
          </div>
          <div>
            <div class="font-bold text-xs truncate">${m.name}</div>
            <div class="text-blue-600 text-xs font-bold my-1">${m.price} ฿</div>
          </div>
          <button onclick="posAdd(${m.id})" class="bg-blue-600 text-white text-[11px] px-2 py-1 rounded-lg hover:bg-blue-700 w-full font-bold mt-1">+ สั่ง</button>
        </div>
      `).join('');

      loadTableOrders(tid);
    }

    async function loadTableOrders(tid) {
      const res = await fetch(`/api/orders?table_id=${tid}`);
      const orders = await res.json();
      const box = document.getElementById('pos-order-items');
      box.innerHTML = orders.filter(o => o.status !== 'ยกเลิก').map(o => `
        <div class="flex justify-between items-center bg-slate-50 p-2.5 rounded-xl text-xs border">
          <div><b>${o.name}</b> x${o.qty} <span class="text-slate-400">(${o.status})</span></div>
          <span class="font-bold text-slate-800">${(o.unit_price * o.qty).toFixed(2)} ฿</span>
        </div>
      `).join('') || '<p class="text-xs text-slate-400">ยังไม่มีรายการสั่งอาหาร</p>';
    }

    async function posAdd(mid) {
      await fetch('/api/customer/order', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ table_id: currentTable, items: [{ menu_id: mid, qty: 1 }] })
      });
      showToast('เพิ่มรายการเรียบร้อย', 'success');
      loadTables();
      loadTableOrders(currentTable);
    }

    async function loadKitchenOrders() {
      const res = await fetch('/api/orders');
      const orders = await res.json();
      const active = orders.filter(o => o.status === 'รอทำ' || o.status === 'กำลังทำ');
      const grid = document.getElementById('kds-grid');
      grid.innerHTML = active.map(o => `
        <div class="bg-white p-4 rounded-2xl border-t-4 ${o.status === 'รอทำ' ? 'border-amber-500' : 'border-blue-500'} shadow-sm space-y-2">
          <div class="flex justify-between items-center text-xs">
            <span class="font-extrabold text-sm text-slate-800">โต๊ะ ${o.table_id}</span>
            <span class="text-slate-400">${o.created_at.split(' ')[1]}</span>
          </div>
          <h4 class="font-black text-base text-slate-800">${o.name} x ${o.qty}</h4>
          ${o.options ? `<p class="text-xs text-slate-500">ตัวเลือก: ${o.options.spiciness || ''} | ${o.options.egg || ''} | ${o.options.size || ''}</p>` : ''}
          <div class="flex gap-2 pt-2">
            ${o.status === 'รอทำ' ? `<button onclick="kitchenAction(${o.order_id}, 'กำลังทำ')" class="bg-blue-600 hover:bg-blue-700 text-white text-xs px-3 py-2 rounded-xl font-bold flex-1">เริ่มทำ (ตัดสต็อก)</button>` : ''}
            <button onclick="kitchenAction(${o.order_id}, 'เสิร์ฟแล้ว')" class="bg-emerald-600 hover:bg-emerald-700 text-white text-xs px-3 py-2 rounded-xl font-bold flex-1">เสิร์ฟแล้ว (ตัดสต็อก)</button>
          </div>
        </div>
      `).join('') || '<p class="text-slate-400 text-xs">ไม่มีออเดอร์ค้างทำในห้องครัว 🎉</p>';
    }

    async function kitchenAction(oid, st) {
      const res = await fetch(`/api/kitchen/${oid}/status?status=${encodeURIComponent(st)}`, { method: 'POST' });
      if(!res.ok) {
        const err = await res.json();
        showToast(err.detail || 'ไม่สามารถตัดสต็อกได้', 'error');
      } else {
        showToast(`อัปเดตสถานะเป็น "${st}" และตัดสต็อกสำเร็จ`, 'success');
      }
      loadKitchenOrders();
      loadInventory();
      loadDashboard();
    }

    async function loadInventory() {
      const res = await fetch('/api/inventory');
      const inv = await res.json();
      cachedInventory = inv;
      document.getElementById('inventory-table-body').innerHTML = inv.map(i => {
        let isLow = i.stock <= i.min_stock;
        return `
          <tr class="border-b text-xs">
            <td class="p-3 text-slate-400">${i.id}</td>
            <td class="p-3 font-bold text-slate-800">${i.name}</td>
            <td class="p-3 font-extrabold ${isLow ? 'text-rose-600' : 'text-slate-700'}">${i.stock}</td>
            <td class="p-3 text-slate-500">${i.unit}</td>
            <td class="p-3 text-slate-500">${i.min_stock}</td>
            <td class="p-3"><span class="text-[10px] px-2 py-0.5 rounded-full font-bold ${isLow ? 'bg-rose-100 text-rose-700' : 'bg-emerald-100 text-emerald-700'}">${isLow ? 'ใกล้หมด' : 'ปกติ'}</span></td>
          </tr>
        `;
      }).join('');
    }

    async function openRestockPrompt() {
      const id = prompt('ระบุรหัสวัตถุดิบ (เช่น ing_1, ing_2):');
      const qty = parseFloat(prompt('ระบุจำนวนที่ต้องการเพิ่ม:'));
      if(id && qty > 0) {
        await fetch(`/api/inventory/${id}?added_stock=${qty}`, { method: 'PUT' });
        showToast('เติมสต็อกเรียบร้อย', 'success');
        loadInventory();
      }
    }

    async function loadQueues() {
      const res = await fetch('/api/queues');
      const qs = await res.json();
      const canCallQueue = currentUser && (currentUser.role === 'admin' || currentUser.role === 'staff');

      document.getElementById('queue-list').innerHTML = qs.filter(q => q.status === 'รอเรียก').map(q => `
        <div class="flex justify-between items-center p-2.5 bg-slate-50 rounded-xl border text-xs">
          <div><b class="text-blue-600">${q.queue_id}</b> : ${q.name} (${q.party_size} ท่าน)</div>
          ${canCallQueue ? `<button onclick="callQueue('${q.queue_id}')" class="bg-emerald-600 hover:bg-emerald-700 text-white text-[11px] px-2.5 py-1 rounded-lg font-bold">เรียกลูกค้า</button>` : '<span class="text-amber-600 font-bold">รอเรียกคิว</span>'}
        </div>
      `).join('') || '<p class="text-xs text-slate-400">ไม่มีคิวค้างในขณะนี้</p>';
    }

    async function createQueueTicket() {
      const name = document.getElementById('q-name').value.trim();
      const size = parseInt(document.getElementById('q-size').value);
      if(!name || !size || size <= 0) return showToast('กรุณากรอกข้อมูลและจำนวนคนให้ถูกต้อง', 'error');
      
      const res = await fetch('/api/queue/ticket', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ name: name, party_size: size })
      });
      const data = await res.json();
      showToast(`ออกบัตรคิวสำเร็จ! หมายเลขของคุณคือ ${data.queue_id}`, 'success');
      document.getElementById('q-name').value = '';
      document.getElementById('q-size').value = '';
      loadQueues();
    }

    async function callQueue(qid) {
      await fetch(`/api/queue/${qid}/status?status=เรียกแล้ว`, { method: 'POST' });
      showToast(`เรียกลำดับคิว ${qid} แล้ว`, 'info');
      loadQueues();
    }

    async function createReservation() {
      const p = {
        name: document.getElementById('res-name').value.trim(),
        phone: document.getElementById('res-phone').value.trim(),
        date: document.getElementById('res-date').value,
        time: document.getElementById('res-time').value,
        party_size: parseInt(document.getElementById('res-size').value)
      };
      if(!p.name || !p.phone || isNaN(p.party_size)) return showToast('กรุณากรอกข้อมูลให้ครบถ้วน', 'error');
      await fetch('/api/reservations', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(p)
      });
      showToast('บันทึกการจองสำเร็จ!', 'success');
    }

    async function loadLogs() {
      const res = await fetch('/api/logs');
      const d = await res.json();
      document.getElementById('logs-table-body').innerHTML = d.items.map(l => `
        <tr class="border-b text-xs">
          <td class="p-3 text-slate-400">${l.timestamp}</td>
          <td class="p-3 font-bold">${l.user}</td>
          <td class="p-3"><span class="bg-slate-100 px-2 py-0.5 rounded font-bold">${l.action}</span></td>
          <td class="p-3 text-slate-600">${l.details}</td>
        </tr>
      `).join('');
    }

    async function loadCheckoutTables() {
      const res = await fetch('/api/tables');
      const tables = await res.json();
      document.getElementById('bill-table-sel').innerHTML = tables.map(t => `<option value="${t.table_id}">โต๊ะ ${t.table_id} (${t.status})</option>`).join('');
    }

    async function executeCheckout() {
      const tid = parseInt(document.getElementById('bill-table-sel').value);
      const disc = parseFloat(document.getElementById('bill-discount').value) || 0;
      const phone = document.getElementById('bill-member').value.trim();
      const sType = document.getElementById('bill-split-type').value;
      const sCount = parseInt(document.getElementById('bill-split-count').value) || 1;

      const res = await fetch('/api/checkout', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
          table_id: tid,
          discount_percent: disc,
          member_phone: phone,
          split_type: sType,
          split_count: sCount
        })
      });

      const d = await res.json();
      if(!res.ok) return showToast(d.detail || 'ไม่สามารถคิดเงินได้', 'error');

      const r = d.receipt;
      document.getElementById('rc-id').innerText = r.receipt_id;
      document.getElementById('rc-date').innerText = r.timestamp;
      document.getElementById('rc-table').innerText = 'โต๊ะ: ' + r.table_id;
      document.getElementById('rc-items').innerHTML = r.items.map(it => `
        <div class="flex justify-between"><span>${it.name} x${it.qty}</span><span>${(it.unit_price * it.qty).toFixed(2)}</span></div>
      `).join('');
      document.getElementById('rc-sub').innerText = r.subtotal.toFixed(2) + ' ฿';
      document.getElementById('rc-disc').innerText = `-${r.discount_amount.toFixed(2)} ฿ (${r.discount_percent}%)`;
      document.getElementById('rc-sc').innerText = `+${r.service_charge.toFixed(2)} ฿`;
      document.getElementById('rc-vat').innerText = `+${r.vat.toFixed(2)} ฿`;
      document.getElementById('rc-net').innerText = r.net_total.toFixed(2) + ' ฿';

      if(r.split_info) {
        document.getElementById('rc-split-box').classList.remove('hidden');
        document.getElementById('rc-split-box').innerText = `หาร ${r.split_info.split_count} คน = คนละ ${r.split_info.amount_per_person.toFixed(2)} ฿`;
      } else {
        document.getElementById('rc-split-box').classList.add('hidden');
      }

      if(d.earned_points > 0) {
        document.getElementById('rc-point-box').innerText = `⭐ สะสมแต้มสมาชิก: +${d.earned_points} แต้ม`;
      }

      showToast('เช็คบิลและพิมพ์ใบเสร็จสำเร็จ!', 'success');
      loadTables();
    }

    async function sendCustomerOrder() {
      const tid = parseInt(document.getElementById('qr-table-num').value);
      const mid = parseInt(document.getElementById('qr-select-menu').value);
      const spice = document.getElementById('qr-opt-spice').value;
      const egg = document.getElementById('qr-opt-egg').value;
      const size = document.getElementById('qr-opt-size').value;

      await fetch('/api/customer/order', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
          table_id: tid,
          items: [{ menu_id: mid, qty: 1, options: { spiciness: spice, egg: egg, size: size } }]
        })
      });
      showToast('ส่งออเดอร์ถึงห้องครัวเรียบร้อย!', 'success');
    }

    async function loadQrMenu() {
      const res = await fetch('/api/menu?limit=50');
      const d = await res.json();
      cachedMenu = d.items;
      document.getElementById('qr-select-menu').innerHTML = d.items.map(m => `<option value="${m.id}">${m.name} (${m.price} ฿)</option>`).join('');

      const tRes = await fetch('/api/tables');
      const tables = await tRes.json();
      document.getElementById('qr-table-num').innerHTML = tables.map(t => `<option value="${t.table_id}">โต๊ะ ${t.table_id}</option>`).join('');
      
      updateQrMenuPreview();
    }

    function updateQrMenuPreview() {
      const mid = parseInt(document.getElementById('qr-select-menu').value);
      const item = cachedMenu.find(m => m.id === mid);
      const box = document.getElementById('qr-dish-preview');
      if (item && item.image) {
        document.getElementById('qr-dish-img').src = item.image;
        document.getElementById('qr-dish-name').innerText = item.name;
        document.getElementById('qr-dish-price').innerText = item.price.toFixed(2) + ' ฿';
        box.classList.remove('hidden');
      } else {
        box.classList.add('hidden');
      }
    }

    function openMoveModal() {
      const fromT = prompt('ระบุโต๊ะต้นทาง:');
      const toT = prompt('ระบุโต๊ะปลายทาง:');
      if(fromT && toT) {
        fetch('/api/table/move', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({ from_table: parseInt(fromT), to_table: parseInt(toT) })
        }).then(r => r.json()).then(d => {
          showToast(d.message || d.detail, 'info');
          loadTables();
        });
      }
    }

    function openMergeModal() {
      const src = prompt('ระบุเลขโต๊ะที่ต้องการรวม (คั่นด้วยจุลภาค เช่น 2,3):');
      const tgt = prompt('ระบุโต๊ะเป้าหมายหลัก:');
      if(src && tgt) {
        const arr = src.split(',').map(x => parseInt(x.trim()));
        fetch('/api/table/merge', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({ source_tables: arr, target_table: parseInt(tgt) })
        }).then(r => r.json()).then(d => {
          showToast(d.message || d.detail, 'info');
          loadTables();
        });
      }
    }

    // ================= Real-Time Notification SSE =================
    const evt = new EventSource('/api/realtime');
    evt.onmessage = function(e) {
      const ev = JSON.parse(e.data);
      if(ev.type === 'NEW_ORDER') {
        if (currentUser && currentUser.role !== 'customer') {
          showToast(`🔔 โต๊ะ ${ev.data.table_id} มีออเดอร์ใหม่เข้ามา!`, 'info');
          loadKitchenOrders();
        }
      } else if(ev.type === 'STOCK_UPDATE') {
        loadInventory();
        loadDashboard();
      } else if(ev.type === 'QUEUE_UPDATE') {
        loadQueues();
      }
    };

    setInterval(() => {
      document.getElementById('live-clock').innerText = new Date().toLocaleTimeString('th-TH');
    }, 1000);

    const savedUser = localStorage.getItem('restro_user');
    if(savedUser) {
      setupSession(JSON.parse(savedUser));
    }
  </script>
</body>
</html>
"""