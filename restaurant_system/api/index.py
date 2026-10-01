"""
api/index.py - จุดประมวลผลหลักสำหรับ Vercel Serverless
รวม REST APIs, Data Validation (Pydantic), Real-time SSE และ Single-Page Web UI ครบทุกฟังก์ชัน
"""

import json
import asyncio
from datetime import datetime
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, HTTPException, Query, Header
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel, Field

from core.database import load_db, save_db, add_audit_log
from core.auth import verify_password, hash_password, create_access_token, parse_token, check_permission
import core.services as services

app = FastAPI(title="Restaurant Management Vercel", docs_url="/api/docs", openapi_url="/api/openapi.json")

# Broadcast Queue สำหรับ Real-time Notification (SSE)
event_subscribers: List[asyncio.Queue] = []

async def broadcast_event(event_type: str, data: dict):
    message = json.dumps({"type": event_type, "data": data, "time": datetime.now().isoformat()})
    for q in list(event_subscribers):
        try:
            await q.put(message)
        except Exception:
            pass

# ================= 1. Pydantic Schemas (Data Validation) =================

class LoginRequest(BaseModel):
    username: str = Field(..., min_length=3)
    password: str = Field(..., min_length=4)

class RegisterRequest(BaseModel):
    username: str = Field(..., min_length=3)
    password: str = Field(..., min_length=4)
    name: str = Field(..., min_length=2)
    role: str = Field(default="staff")

class MenuRecipeItem(BaseModel):
    ingredient_id: str
    amount: float = Field(..., gt=0)

class MenuCreateRequest(BaseModel):
    name: str = Field(..., min_length=2)
    category: str = Field(default="อาหารจานเดียว")
    price: float = Field(..., ge=0)
    is_available: bool = True
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
    split_type: Optional[str] = "full"  # full หรือ split_even
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

# ================= 2. Authentication & User API =================

@app.post("/api/auth/login")
async def login(req: LoginRequest):
    db = load_db()
    user = next((u for u in db.get("users", []) if u["username"] == req.username), None)
    if not user or not verify_password(req.password, user["password_hash"], user["salt"]):
        raise HTTPException(status_code=401, detail="ชื่อผู้ใช้หรือรหัสผ่านไม่ถูกต้อง")

    token = create_access_token(user["id"], user["role"])
    add_audit_log(user["username"], "LOGIN", "เข้าสู่ระบบสำเร็จ")
    return {"token": token, "role": user["role"], "name": user["name"]}

@app.post("/api/auth/register")
async def register(req: RegisterRequest):
    db = load_db()
    if any(u["username"] == req.username for u in db.get("users", [])):
        raise HTTPException(status_code=400, detail="ชื่อผู้ใช้นี้มีอยู่ในระบบแล้ว")

    h, s = hash_password(req.password)
    new_user = {
        "id": f"u_{len(db.get('users', [])) + 1}",
        "username": req.username,
        "password_hash": h,
        "salt": s,
        "role": req.role if req.role in ("admin", "staff", "customer") else "staff",
        "name": req.name
    }
    db.setdefault("users", []).append(new_user)
    save_db(db)
    add_audit_log("Admin", "REGISTER_USER", f"สร้างผู้ใช้ {req.username} สิทธิ์ {new_user['role']}")
    return {"success": True, "message": "ลงทะเบียนผู้ใช้สำเร็จ"}

# ================= 3. Real-Time Server-Sent Events (SSE) =================

@app.get("/api/realtime")
async def sse_notifications():
    """ระบบแจ้งเตือนแบบ Real-Time ทำงานบน Vercel และเบราว์เซอร์ได้ทันที"""
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

# ================= 4. เมนูอาหาร & หมวดหมู่ (CRUD + Search + Pagination) =================

@app.get("/api/menu")
def get_menus(
    search: str = "",
    category: str = "",
    sort_by: str = "id",
    order: str = "asc",
    page: int = 1,
    limit: int = 10
):
    db = load_db()
    items = db.get("menu", [])
    if category:
        items = [m for m in items if m.get("category") == category]

    result = services.paginate_and_sort(items, search=search, search_field="name", sort_by=sort_by, order=order, page=page, limit=limit)
    result["categories"] = services.get_unique_categories(db.get("menu", []))
    return result

@app.post("/api/menu")
def create_menu(req: MenuCreateRequest):
    db = load_db()
    new_id = max([m["id"] for m in db.get("menu", [])], default=0) + 1
    new_menu = {
        "id": new_id,
        "name": req.name,
        "category": req.category,
        "price": req.price,
        "is_available": req.is_available,
        "recipe": [r.dict() for r in req.recipe]
    }
    db.setdefault("menu", []).append(new_menu)
    add_audit_log("Admin", "CREATE_MENU", f"เพิ่มเมนูใหม่: {req.name} ({req.price} ฿)")
    save_db(db)
    return {"success": True, "menu": new_menu}

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

@app.post("/api/table/status")
def update_table_status(table_id: int = Query(...), status: str = Query(...)):
    db = load_db()
    table = next((t for t in db.get("tables", []) if t["table_id"] == table_id), None)
    if not table:
        raise HTTPException(status_code=404, detail="ไม่พบโต๊ะ")
    if status not in services.TABLE_STATUSES:
        raise HTTPException(status_code=400, detail="สถานะไม่ถูกต้อง")

    table["status"] = status
    save_db(db)
    return {"success": True, "table": table}

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

# ================= 6. สั่งอาหาร & จอครัว (KDS + Auto Stock Cut) =================

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
            "unit_price": menu_item["price"] + extra_price,
            "qty": it.qty,
            "options": it.options.dict() if it.options else {},
            "status": "รอทำ",
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        db.setdefault("orders", []).append(new_order)
        created.append(new_order)

    table["status"] = "มีลูกค้า"
    save_db(db)

    # ส่งข้อความ Real-time เข้าจอครัวทันที
    await broadcast_event("NEW_ORDER", {"table_id": req.table_id, "count": len(created)})
    return {"success": True, "message": "ส่งรายการเข้าครัวแล้ว", "orders": created}

@app.post("/api/kitchen/{order_id}/status")
async def update_kitchen_order(order_id: int, status: str = Query(...)):
    db = load_db()
    order = next((o for o in db.get("orders", []) if o["order_id"] == order_id), None)
    if not order:
        raise HTTPException(status_code=404, detail="ไม่พบรายการออเดอร์")

    # เมื่อเริ่มทำอาหาร ตัดสต็อกวัตถุดิบตามสูตร
    if status == "กำลังทำ" and order["status"] == "รอทำ":
        ok, msg = services.deduct_stock_by_recipe(order["menu_id"], order["qty"], "Kitchen")
        if not ok:
            raise HTTPException(status_code=400, detail=msg)

    order["status"] = status
    save_db(db)
    await broadcast_event("ORDER_STATUS_UPDATE", {"order_id": order_id, "status": status})
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
    add_audit_log("Admin", "ADD_INVENTORY", f"เพิ่มวัตถุดิบ: {req.name} {req.stock} {req.unit}")
    save_db(db)
    return {"success": True, "item": item}

@app.put("/api/inventory/{item_id}")
def update_inventory_stock(item_id: str, added_stock: float = Query(...)):
    db = load_db()
    item = next((i for i in db.get("inventory", []) if i["id"] == item_id), None)
    if not item:
        raise HTTPException(status_code=404, detail="ไม่พบวัตถุดิบ")

    item["stock"] += added_stock
    add_audit_log("Staff", "RESTOCK", f"เติมสต็อกวัตถุดิบ '{item['name']}' เพิ่ม {added_stock} {item['unit']} (คงเหลือ: {item['stock']})")
    save_db(db)
    return {"success": True, "item": item}

# ================= 8. ระบบคิว และจองโต๊ะ =================

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
    add_audit_log("Staff", "RESERVATION", f"จองโต๊ะล่วงหน้า: {req.name} ({req.date} {req.time}) {req.party_size} ท่าน")
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

    # สะสมแต้มสมาชิก
    earned_pts = 0
    if req.member_phone:
        earned_pts = services.process_loyalty_points(req.member_phone, net)

    # คำนวณแยกบิลจ่ายเท่ากัน (American Share)
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

    add_audit_log("Cashier", "CHECKOUT", f"เช็คบิลโต๊ะ {req.table_id} ยอดสุทธิ {net} ฿")
    save_db(db)
    return {"success": True, "receipt": receipt, "earned_points": earned_pts}

# ================= 10. Dashboard, รายงาน และ Audit Logs =================

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

# ================= 11. Single-Page Application (HTML/CSS/JS) =================

@app.get("/", response_class=HTMLResponse)
def index():
    return """<!DOCTYPE html>
<html lang="th">
<head>
  <meta charset="UTF-8">
  <title>Smart Restaurant Pro - Vercel Ready</title>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <script src="https://cdn.tailwindcss.com"></script>
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
  <style>
    @media print {
      body * { visibility: hidden; }
      #printable-receipt, #printable-receipt * { visibility: visible; }
      #printable-receipt { position: absolute; left: 0; top: 0; width: 100%; border: none !important; }
    }
  </style>
</head>
<body class="bg-slate-50 text-slate-800 antialiased">
  <div class="flex h-screen overflow-hidden">
    
    <!-- Sidebar เมนูหลัก -->
    <aside class="w-64 bg-slate-900 text-white flex flex-col justify-between p-4 shadow-xl">
      <div>
        <div class="flex items-center gap-3 px-2 py-4 mb-4 border-b border-slate-800">
          <i class="fa-solid fa-utensils text-2xl text-blue-500"></i>
          <div>
            <h1 class="font-extrabold text-lg tracking-wide text-white">RESTRO PRO</h1>
            <p class="text-xs text-slate-400">ระบบจัดการร้านอาหาร</p>
          </div>
        </div>

        <nav class="space-y-1 text-sm font-medium">
          <button onclick="tab('dash')" id="btn-dash" class="w-full text-left py-2.5 px-3 rounded-lg hover:bg-slate-800 flex items-center gap-3 bg-blue-600 text-white"><i class="fa-solid fa-chart-pie w-5"></i> Dashboard</button>
          <button onclick="tab('tables')" id="btn-tables" class="w-full text-left py-2.5 px-3 rounded-lg hover:bg-slate-800 flex items-center gap-3 text-slate-300"><i class="fa-solid fa-chair w-5"></i> แผนผังโต๊ะ & POS</button>
          <button onclick="tab('kitchen')" id="btn-kitchen" class="w-full text-left py-2.5 px-3 rounded-lg hover:bg-slate-800 flex items-center gap-3 text-slate-300"><i class="fa-solid fa-fire-burner w-5"></i> จอครัว (KDS)</button>
          <button onclick="tab('qr')" id="btn-qr" class="w-full text-left py-2.5 px-3 rounded-lg hover:bg-slate-800 flex items-center gap-3 text-slate-300"><i class="fa-solid fa-qrcode w-5"></i> ลูกค้าสั่งเอง (QR)</button>
          <button onclick="tab('checkout')" id="btn-checkout" class="w-full text-left py-2.5 px-3 rounded-lg hover:bg-slate-800 flex items-center gap-3 text-slate-300"><i class="fa-solid fa-receipt w-5"></i> เช็คบิล / ใบเสร็จ</button>
          <button onclick="tab('inventory')" id="btn-inventory" class="w-full text-left py-2.5 px-3 rounded-lg hover:bg-slate-800 flex items-center gap-3 text-slate-300"><i class="fa-solid fa-boxes-stacked w-5"></i> สต็อกวัตถุดิบ</button>
          <button onclick="tab('queue')" id="btn-queue" class="w-full text-left py-2.5 px-3 rounded-lg hover:bg-slate-800 flex items-center gap-3 text-slate-300"><i class="fa-solid fa-users-line w-5"></i> คิว & จองโต๊ะ</button>
          <button onclick="tab('logs')" id="btn-logs" class="w-full text-left py-2.5 px-3 rounded-lg hover:bg-slate-800 flex items-center gap-3 text-slate-300"><i class="fa-solid fa-clock-rotate-left w-5"></i> Audit Logs</button>
        </nav>
      </div>

      <div class="bg-slate-800/80 p-3 rounded-lg text-xs flex items-center justify-between">
        <span class="flex items-center gap-2 text-emerald-400 font-semibold">
          <span class="w-2 h-2 rounded-full bg-emerald-400 animate-ping"></span> Real-time Live
        </span>
        <span id="clock" class="text-slate-400"></span>
      </div>
    </aside>

    <!-- Main Content Area -->
    <main class="flex-1 overflow-y-auto p-8">

      <!-- 1. Dashboard -->
      <section id="pane-dash" class="space-y-6">
        <h2 class="text-2xl font-bold text-slate-800">📊 ภาพรวมร้านอาหาร (Overview Dashboard)</h2>
        <div class="grid grid-cols-1 md:grid-cols-4 gap-4">
          <div class="bg-white p-5 rounded-xl border border-slate-200 shadow-sm"><p class="text-sm text-slate-500 font-medium">ยอดขายทั้งหมด</p><h3 id="dash-rev" class="text-3xl font-extrabold text-blue-600 mt-2">0.00 ฿</h3></div>
          <div class="bg-white p-5 rounded-xl border border-slate-200 shadow-sm"><p class="text-sm text-slate-500 font-medium">บิลที่ชำระสำเร็จ</p><h3 id="dash-bills" class="text-3xl font-extrabold text-emerald-600 mt-2">0 บิล</h3></div>
          <div class="bg-white p-5 rounded-xl border border-slate-200 shadow-sm"><p class="text-sm text-slate-500 font-medium">โต๊ะที่มีลูกค้า</p><h3 id="dash-tables" class="text-3xl font-extrabold text-amber-600 mt-2">0 / 0</h3></div>
          <div class="bg-white p-5 rounded-xl border border-slate-200 shadow-sm"><p class="text-sm text-slate-500 font-medium">วัตถุดิบใกล้หมด</p><h3 id="dash-stock" class="text-3xl font-extrabold text-rose-600 mt-2">0 รายการ</h3></div>
        </div>

        <div class="grid grid-cols-1 md:grid-cols-2 gap-6 mt-6">
          <div class="bg-white p-6 rounded-xl border border-slate-200 shadow-sm">
            <h3 class="font-bold text-lg mb-4 text-slate-700">🏆 5 อันดับเมนูขายดี</h3>
            <div id="dash-top-sellers" class="space-y-3"></div>
          </div>
          <div class="bg-white p-6 rounded-xl border border-slate-200 shadow-sm">
            <h3 class="font-bold text-lg mb-4 text-slate-700">⚠️ วัตถุดิบที่ต้องเติมด่วน</h3>
            <div id="dash-low-stock-list" class="space-y-2"></div>
          </div>
        </div>
      </section>

      <!-- 2. แผนผังโต๊ะ & POS -->
      <section id="pane-tables" class="hidden space-y-6">
        <div class="flex justify-between items-center">
          <h2 class="text-2xl font-bold">🪑 แผนผังโต๊ะอาหาร & รับออเดอร์</h2>
          <div class="flex gap-2">
            <button onclick="openMoveModal()" class="bg-amber-600 hover:bg-amber-700 text-white text-sm px-4 py-2 rounded-lg font-medium shadow-sm"><i class="fa-solid fa-arrows-split-up-and-left mr-1"></i> ย้ายโต๊ะ</button>
            <button onclick="openMergeModal()" class="bg-indigo-600 hover:bg-indigo-700 text-white text-sm px-4 py-2 rounded-lg font-medium shadow-sm"><i class="fa-solid fa-object-group mr-1"></i> รวมโต๊ะ</button>
          </div>
        </div>
        <div id="tables-grid" class="grid grid-cols-2 md:grid-cols-4 gap-4"></div>

        <!-- กล่องสั่งอาหารสำหรับโต๊ะที่เลือก -->
        <div id="table-order-box" class="hidden bg-white p-6 rounded-xl border border-slate-200 shadow-sm mt-6">
          <div class="flex justify-between items-center border-b pb-4 mb-4">
            <h3 id="selected-table-title" class="text-xl font-bold text-slate-800">จัดการรายการอาหาร - โต๊ะ</h3>
            <button onclick="document.getElementById('table-order-box').classList.add('hidden')" class="text-slate-400 hover:text-slate-600"><i class="fa-solid fa-xmark text-lg"></i></button>
          </div>
          <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div>
              <h4 class="font-semibold text-slate-600 mb-2">เลือกเมนูอาหาร:</h4>
              <div id="pos-menu-list" class="grid grid-cols-2 gap-3 max-h-96 overflow-y-auto p-1"></div>
            </div>
            <div class="border-l pl-6">
              <h4 class="font-semibold text-slate-600 mb-2">รายการที่สั่งในโต๊ะนี้:</h4>
              <div id="pos-order-items" class="space-y-2 max-h-80 overflow-y-auto"></div>
            </div>
          </div>
        </div>
      </section>

      <!-- 3. จอครัว (Kitchen Display System) -->
      <section id="pane-kitchen" class="hidden space-y-6">
        <div class="flex justify-between items-center">
          <div>
            <h2 class="text-2xl font-bold text-slate-800">👨‍🍳 จอครัวแบบเรียลไทม์ (Kitchen Display System)</h2>
            <p class="text-sm text-slate-500">ตัดสต็อกวัตถุดิบอัตโนมัติตามสูตรเมื่อกดยืนยัน "เริ่มทำ"</p>
          </div>
          <button onclick="loadKitchenOrders()" class="bg-blue-600 text-white text-sm px-3 py-1.5 rounded-lg"><i class="fa-solid fa-rotate mr-1"></i> รีเฟรช</button>
        </div>
        <div id="kds-grid" class="grid grid-cols-1 md:grid-cols-3 gap-4"></div>
      </section>

      <!-- 4. ลูกค้าสั่งอาหารเอง (Customer QR Self-Order Mockup) -->
      <section id="pane-qr" class="hidden space-y-6">
        <h2 class="text-2xl font-bold text-slate-800">📱 ลูกค้าสแกน QR Code สั่งอาหารเอง</h2>
        <div class="max-w-md mx-auto bg-white p-6 rounded-2xl border border-slate-200 shadow-md space-y-4">
          <div class="text-center border-b pb-4">
            <span class="bg-blue-100 text-blue-800 text-xs px-2.5 py-1 rounded-full font-bold">โต๊ะหมายเลข 1</span>
            <h3 class="text-lg font-extrabold text-slate-800 mt-2">เมนูอาหาร & ตัวเลือกพิเศษ</h3>
          </div>
          <div class="space-y-3">
            <div>
              <label class="text-sm font-semibold text-slate-600 block mb-1">เลือกเมนูอาหาร</label>
              <select id="qr-select-menu" class="w-full border border-slate-300 p-2.5 rounded-lg text-sm bg-white"></select>
            </div>
            <div class="grid grid-cols-3 gap-2">
              <div>
                <label class="text-xs font-semibold text-slate-500">ระดับความเผ็ด</label>
                <select id="qr-opt-spice" class="w-full border p-2 rounded-lg text-xs bg-white">
                  <option>ไม่เผ็ด</option><option selected>เผ็ดกลาง</option><option>เผ็ดมาก</option>
                </select>
              </div>
              <div>
                <label class="text-xs font-semibold text-slate-500">เพิ่มไข่ (+10฿)</label>
                <select id="qr-opt-egg" class="w-full border p-2 rounded-lg text-xs bg-white">
                  <option>ไม่ใส่</option><option>ไข่ดาว</option><option>ไข่เจียว</option>
                </select>
              </div>
              <div>
                <label class="text-xs font-semibold text-slate-500">ขนาด (+15฿)</label>
                <select id="qr-opt-size" class="w-full border p-2 rounded-lg text-xs bg-white">
                  <option>ธรรมดา</option><option>พิเศษ</option>
                </select>
              </div>
            </div>
            <button onclick="sendCustomerOrder()" class="w-full bg-blue-600 hover:bg-blue-700 text-white font-bold py-3 rounded-xl shadow-md transition">ยืนยันสั่งอาหารเข้าครัว</button>
          </div>
        </div>
      </section>

      <!-- 5. เช็คบิล / ใบเสร็จ & แยกบิล -->
      <section id="pane-checkout" class="hidden space-y-6">
        <h2 class="text-2xl font-bold text-slate-800">💵 เช็คบิล คิดเงิน และพิมพ์ใบเสร็จ</h2>
        <div class="grid grid-cols-1 md:grid-cols-2 gap-8">
          <div class="bg-white p-6 rounded-xl border border-slate-200 shadow-sm space-y-4">
            <h3 class="font-bold text-lg text-slate-700">คำนวณยอดชำระ</h3>
            <div>
              <label class="text-sm font-semibold text-slate-600 block mb-1">เลือกโต๊ะที่ต้องการชำระ</label>
              <select id="bill-table-sel" class="w-full border p-2.5 rounded-lg text-sm bg-white"></select>
            </div>
            <div class="grid grid-cols-2 gap-3">
              <div>
                <label class="text-sm font-semibold text-slate-600 block mb-1">ส่วนลด (%)</label>
                <input type="number" id="bill-discount" value="0" min="0" max="100" class="w-full border p-2 rounded-lg text-sm">
              </div>
              <div>
                <label class="text-sm font-semibold text-slate-600 block mb-1">เบอร์สมาชิก (สะสมแต้ม)</label>
                <input type="text" id="bill-member" placeholder="08XXXXXXXX" class="w-full border p-2 rounded-lg text-sm">
              </div>
            </div>
            <div class="border-t pt-3">
              <label class="text-sm font-semibold text-slate-600 block mb-1">การแยกบิล (Split Bill)</label>
              <div class="flex gap-4 items-center">
                <select id="bill-split-type" class="border p-2 rounded-lg text-sm">
                  <option value="full">จ่ายเต็มบิล</option>
                  <option value="split_even">หารเท่ากัน (American Share)</option>
                </select>
                <input type="number" id="bill-split-count" value="2" min="2" class="border p-2 rounded-lg text-sm w-20" placeholder="กี่คน">
              </div>
            </div>
            <button onclick="executeCheckout()" class="w-full bg-emerald-600 hover:bg-emerald-700 text-white font-bold py-3 rounded-xl shadow-md transition">ชำระเงินและออกใบเสร็จ</button>
          </div>

          <!-- Printable Receipt Display -->
          <div id="printable-receipt" class="bg-white p-6 rounded-xl border-2 border-dashed border-slate-300 font-mono text-sm max-w-sm mx-auto shadow-sm">
            <div class="text-center pb-3 border-b border-dashed border-slate-300">
              <h4 class="font-extrabold text-base">RESTRO PRO RECEIPT</h4>
              <p id="rc-id" class="text-xs text-slate-500">REC-XXXXXXXX</p>
              <p id="rc-date" class="text-xs text-slate-500"></p>
              <p id="rc-table" class="text-xs font-bold mt-1">โต๊ะ: -</p>
            </div>
            <div id="rc-items" class="py-3 border-b border-dashed border-slate-300 space-y-1"></div>
            <div class="py-2 space-y-1 text-xs">
              <div class="flex justify-between"><span>ยอดรวม:</span><span id="rc-sub">0.00 ฿</span></div>
              <div class="flex justify-between"><span>ส่วนลด:</span><span id="rc-disc">0.00 ฿</span></div>
              <div class="flex justify-between"><span>ค่าบริการ (10%):</span><span id="rc-sc">0.00 ฿</span></div>
              <div class="flex justify-between"><span>ภาษี (7%):</span><span id="rc-vat">0.00 ฿</span></div>
              <div class="flex justify-between font-bold text-sm text-slate-800 pt-1 border-t"><span>สุทธิ:</span><span id="rc-net">0.00 ฿</span></div>
              <div id="rc-split-box" class="hidden text-blue-600 font-bold mt-2 pt-1 border-t text-center"></div>
              <div id="rc-point-box" class="text-emerald-600 text-center font-bold mt-1"></div>
            </div>
            <button onclick="window.print()" class="w-full mt-4 bg-slate-800 text-white py-1.5 rounded text-xs font-sans hover:bg-black"><i class="fa-solid fa-print mr-1"></i> พิมพ์ใบเสร็จ</button>
          </div>
        </div>
      </section>

      <!-- 6. สต็อกวัตถุดิบ (Inventory) -->
      <section id="pane-inventory" class="hidden space-y-6">
        <div class="flex justify-between items-center">
          <h2 class="text-2xl font-bold text-slate-800">📦 จัดการสต็อกวัตถุดิบ (สูตรอาหารเชื่อมโยง)</h2>
          <button onclick="openRestockPrompt()" class="bg-blue-600 text-white text-sm px-4 py-2 rounded-lg font-medium">+ เติมสต็อกวัตถุดิบ</button>
        </div>
        <div class="bg-white rounded-xl border border-slate-200 overflow-hidden shadow-sm">
          <table class="w-full text-left text-sm">
            <thead class="bg-slate-100 text-slate-600 font-bold border-b">
              <tr>
                <th class="p-3">รหัส</th>
                <th class="p-3">ชื่อวัตถุดิบ</th>
                <th class="p-3">จำนวนคงเหลือ</th>
                <th class="p-3">หน่วย</th>
                <th class="p-3">จุดเตือนขั้นต่ำ</th>
                <th class="p-3">สถานะ</th>
              </tr>
            </thead>
            <tbody id="inventory-table-body"></tbody>
          </table>
        </div>
      </section>

      <!-- 7. ระบบคิว & จองโต๊ะ -->
      <section id="pane-queue" class="hidden space-y-6">
        <h2 class="text-2xl font-bold text-slate-800">👥 ระบบคิวหน้าร้าน & จองโต๊ะล่วงหน้า</h2>
        <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
          <div class="bg-white p-6 rounded-xl border border-slate-200 shadow-sm">
            <h3 class="font-bold text-lg mb-4 text-slate-700">ออกบัตรคิวหน้าร้าน</h3>
            <div class="space-y-3">
              <input type="text" id="q-name" placeholder="ชื่อลูกค้า" class="w-full border p-2 rounded-lg text-sm">
              <input type="number" id="q-size" placeholder="จำนวนคน" class="w-full border p-2 rounded-lg text-sm">
              <button onclick="createQueueTicket()" class="w-full bg-blue-600 text-white py-2 rounded-lg text-sm font-semibold">ออกบัตรคิว</button>
            </div>
            <div class="mt-6">
              <h4 class="font-semibold text-sm mb-2 text-slate-600">รายการคิวที่รออยู่:</h4>
              <div id="queue-list" class="space-y-2"></div>
            </div>
          </div>
          <div class="bg-white p-6 rounded-xl border border-slate-200 shadow-sm">
            <h3 class="font-bold text-lg mb-4 text-slate-700">จองโต๊ะล่วงหน้า</h3>
            <div class="space-y-3">
              <input type="text" id="res-name" placeholder="ชื่อผู้จอง" class="w-full border p-2 rounded-lg text-sm">
              <input type="text" id="res-phone" placeholder="เบอร์โทรศัพท์" class="w-full border p-2 rounded-lg text-sm">
              <div class="grid grid-cols-2 gap-2">
                <input type="date" id="res-date" class="border p-2 rounded-lg text-sm">
                <input type="time" id="res-time" class="border p-2 rounded-lg text-sm">
              </div>
              <input type="number" id="res-size" placeholder="จำนวนที่นั่ง" class="w-full border p-2 rounded-lg text-sm">
              <button onclick="createReservation()" class="w-full bg-indigo-600 text-white py-2 rounded-lg text-sm font-semibold">บันทึกการจอง</button>
            </div>
          </div>
        </div>
      </section>

      <!-- 8. Audit Logs -->
      <section id="pane-logs" class="hidden space-y-6">
        <h2 class="text-2xl font-bold text-slate-800">📝 ประวัติการแก้ไขข้อมูลสำคัญ (Audit Trail)</h2>
        <div class="bg-white rounded-xl border border-slate-200 overflow-hidden shadow-sm">
          <table class="w-full text-left text-sm">
            <thead class="bg-slate-100 text-slate-600 font-bold border-b">
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

  <script>
    let currentTable = null;
    let cachedMenu = [];

    function tab(name) {
      document.querySelectorAll('main > section').forEach(s => s.classList.add('hidden'));
      document.querySelectorAll('aside nav button').forEach(b => {
        b.classList.remove('bg-blue-600', 'text-white');
        b.classList.add('text-slate-300');
      });
      document.getElementById('pane-' + name).classList.remove('hidden');
      document.getElementById('btn-' + name).classList.add('bg-blue-600', 'text-white');
      
      if(name === 'dash') loadDashboard();
      if(name === 'tables') loadTables();
      if(name === 'kitchen') loadKitchenOrders();
      if(name === 'inventory') loadInventory();
      if(name === 'queue') loadQueues();
      if(name === 'logs') loadLogs();
      if(name === 'checkout') loadCheckoutTables();
    }

    async function loadDashboard() {
      const res = await fetch('/api/dashboard');
      const d = await res.json();
      document.getElementById('dash-rev').innerText = d.total_revenue.toFixed(2) + ' ฿';
      document.getElementById('dash-bills').innerText = d.total_bills + ' บิล';
      document.getElementById('dash-tables').innerText = `${d.occupied_tables} / ${d.total_tables}`;
      document.getElementById('dash-stock').innerText = d.low_stock_alerts.length + ' รายการ';

      const topBox = document.getElementById('dash-top-sellers');
      topBox.innerHTML = d.best_sellers.map((b, i) => `
        <div class="flex justify-between items-center text-sm p-2 bg-slate-50 rounded-lg">
          <span><b>#${i+1}</b> ${b.name}</span>
          <span class="font-bold text-blue-600">${b.qty} จาน</span>
        </div>
      `).join('') || '<p class="text-slate-400 text-sm">ยังไม่มีข้อมูลการขาย</p>';

      const lowBox = document.getElementById('dash-low-stock-list');
      lowBox.innerHTML = d.low_stock_alerts.map(l => `
        <div class="flex justify-between items-center text-xs p-2 bg-rose-50 text-rose-700 rounded-lg border border-rose-100">
          <span><i class="fa-solid fa-triangle-exclamation mr-1"></i> ${l.name}</span>
          <b>เหลือ ${l.stock} ${l.unit}</b>
        </div>
      `).join('') || '<p class="text-emerald-600 text-sm">วัตถุดิบทั้งหมดเพียงพอ</p>';
    }

    async function loadTables() {
      const res = await fetch('/api/tables');
      const tables = await res.json();
      const grid = document.getElementById('tables-grid');
      grid.innerHTML = tables.map(t => {
        let col = t.status === 'ว่าง' ? 'border-emerald-300 bg-emerald-50 text-emerald-800' : (t.status === 'มีลูกค้า' ? 'border-rose-300 bg-rose-50 text-rose-800' : 'border-amber-300 bg-amber-50 text-amber-800');
        return `
          <div onclick="selectPOS(${t.table_id})" class="p-5 rounded-xl border-2 ${col} cursor-pointer hover:shadow-md transition">
            <div class="flex justify-between items-center mb-2">
              <h4 class="font-bold text-lg">โต๊ะ ${t.table_id}</h4>
              <span class="text-xs px-2 py-0.5 rounded-full font-bold bg-white">${t.status}</span>
            </div>
            <p class="text-xs opacity-70">ความจุ ${t.capacity} ที่นั่ง</p>
          </div>
        `;
      }).join('');
    }

    async function selectPOS(tid) {
      currentTable = tid;
      document.getElementById('table-order-box').classList.remove('hidden');
      document.getElementById('selected-table-title').innerText = `จัดการรายการอาหาร - โต๊ะ ${tid}`;
      
      const mRes = await fetch('/api/menu?limit=50');
      const mData = await mRes.json();
      cachedMenu = mData.items;

      document.getElementById('pos-menu-list').innerHTML = cachedMenu.map(m => `
        <div class="border p-2.5 rounded-lg text-center bg-slate-50">
          <div class="font-bold text-xs truncate">${m.name}</div>
          <div class="text-blue-600 text-xs font-semibold my-1">${m.price} ฿</div>
          <button onclick="posAdd(${m.id})" class="bg-blue-600 text-white text-xs px-3 py-1 rounded hover:bg-blue-700 w-full">+ สั่ง</button>
        </div>
      `).join('');

      loadTableOrders(tid);
    }

    async function loadTableOrders(tid) {
      const res = await fetch(`/api/orders?table_id=${tid}`);
      const orders = await res.json();
      const box = document.getElementById('pos-order-items');
      box.innerHTML = orders.filter(o => o.status !== 'ยกเลิก').map(o => `
        <div class="flex justify-between items-center bg-slate-50 p-2 rounded text-xs">
          <div><b>${o.name}</b> x${o.qty} <span class="text-slate-400">(${o.status})</span></div>
          <span class="font-bold text-slate-700">${(o.unit_price * o.qty).toFixed(2)} ฿</span>
        </div>
      `).join('') || '<p class="text-xs text-slate-400">ยังไม่มีรายการสั่ง</p>';
    }

    async function posAdd(mid) {
      await fetch('/api/customer/order', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ table_id: currentTable, items: [{ menu_id: mid, qty: 1 }] })
      });
      loadTables();
      loadTableOrders(currentTable);
    }

    async function loadKitchenOrders() {
      const res = await fetch('/api/orders');
      const orders = await res.json();
      const active = orders.filter(o => o.status === 'รอทำ' || o.status === 'กำลังทำ');
      const grid = document.getElementById('kds-grid');
      grid.innerHTML = active.map(o => `
        <div class="bg-white p-4 rounded-xl border-t-4 ${o.status === 'รอทำ' ? 'border-amber-500' : 'border-blue-500'} shadow-sm space-y-2">
          <div class="flex justify-between items-center text-xs">
            <span class="font-bold text-sm">โต๊ะ ${o.table_id}</span>
            <span class="text-slate-400">${o.created_at.split(' ')[1]}</span>
          </div>
          <h4 class="font-extrabold text-base text-slate-800">${o.name} x ${o.qty}</h4>
          ${o.options ? `<p class="text-xs text-slate-500">ตัวเลือก: ${o.options.spiciness || ''} | ${o.options.egg || ''} | ${o.options.size || ''}</p>` : ''}
          <div class="flex gap-2 pt-2">
            ${o.status === 'รอทำ' ? `<button onclick="kitchenAction(${o.order_id}, 'กำลังทำ')" class="bg-blue-600 text-white text-xs px-3 py-1.5 rounded font-bold flex-1">เริ่มทำ (ตัดสต็อก)</button>` : ''}
            <button onclick="kitchenAction(${o.order_id}, 'เสิร์ฟแล้ว')" class="bg-emerald-600 text-white text-xs px-3 py-1.5 rounded font-bold flex-1">เสิร์ฟแล้ว</button>
          </div>
        </div>
      `).join('') || '<p class="text-slate-400 text-sm">ไม่มีออเดอร์ค้างในห้องครัว</p>';
    }

    async function kitchenAction(oid, st) {
      const res = await fetch(`/api/kitchen/${oid}/status?status=${encodeURIComponent(st)}`, { method: 'POST' });
      if(!res.ok) {
        const err = await res.json();
        alert('เกิดข้อผิดพลาด: ' + (err.detail || 'ไม่สามารถตัดสต็อกได้'));
      }
      loadKitchenOrders();
    }

    async function loadInventory() {
      const res = await fetch('/api/inventory');
      const inv = await res.json();
      document.getElementById('inventory-table-body').innerHTML = inv.map(i => {
        let isLow = i.stock <= i.min_stock;
        return `
          <tr class="border-b">
            <td class="p-3 text-slate-500">${i.id}</td>
            <td class="p-3 font-bold">${i.name}</td>
            <td class="p-3 font-semibold ${isLow ? 'text-rose-600' : 'text-slate-700'}">${i.stock}</td>
            <td class="p-3 text-slate-500">${i.unit}</td>
            <td class="p-3 text-slate-500">${i.min_stock}</td>
            <td class="p-3"><span class="text-xs px-2 py-0.5 rounded font-bold ${isLow ? 'bg-rose-100 text-rose-700' : 'bg-emerald-100 text-emerald-700'}">${isLow ? 'ใกล้หมด' : 'ปกติ'}</span></td>
          </tr>
        `;
      }).join('');
    }

    async function openRestockPrompt() {
      const id = prompt('ระบุรหัสวัตถุดิบที่ต้องการเติม (เช่น ing_1, ing_2):');
      const qty = parseFloat(prompt('ระบุจำนวนที่ต้องการเพิ่ม:'));
      if(id && qty > 0) {
        await fetch(`/api/inventory/${id}?added_stock=${qty}`, { method: 'PUT' });
        loadInventory();
      }
    }

    async function loadQueues() {
      const res = await fetch('/api/queues');
      const qs = await res.json();
      document.getElementById('queue-list').innerHTML = qs.filter(q => q.status === 'รอเรียก').map(q => `
        <div class="flex justify-between items-center p-2 bg-slate-50 rounded border text-sm">
          <div><b class="text-blue-600">${q.queue_id}</b> : ${q.name} (${q.party_size} คน)</div>
          <button onclick="callQueue('${q.queue_id}')" class="bg-emerald-600 text-white text-xs px-2 py-1 rounded">เรียกลูกค้า</button>
        </div>
      `).join('') || '<p class="text-xs text-slate-400">ไม่มีคิวค้าง</p>';
    }

    async function createQueueTicket() {
      const name = document.getElementById('q-name').value;
      const size = parseInt(document.getElementById('q-size').value);
      if(!name || !size) return alert('กรุณากรอกข้อมูลให้ครบ');
      await fetch('/api/queue/ticket', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ name: name, party_size: size })
      });
      loadQueues();
    }

    async function callQueue(qid) {
      await fetch(`/api/queue/${qid}/status?status=เรียกแล้ว`, { method: 'POST' });
      loadQueues();
    }

    async function createReservation() {
      const p = {
        name: document.getElementById('res-name').value,
        phone: document.getElementById('res-phone').value,
        date: document.getElementById('res-date').value,
        time: document.getElementById('res-time').value,
        party_size: parseInt(document.getElementById('res-size').value)
      };
      if(!p.name || !p.phone) return alert('กรุณากรอกข้อมูลให้ครบ');
      await fetch('/api/reservations', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(p)
      });
      alert('บันทึกการจองสำเร็จ!');
    }

    async function loadLogs() {
      const res = await fetch('/api/logs');
      const d = await res.json();
      document.getElementById('logs-table-body').innerHTML = d.items.map(l => `
        <tr class="border-b text-xs">
          <td class="p-3 text-slate-400">${l.timestamp}</td>
          <td class="p-3 font-bold">${l.user}</td>
          <td class="p-3"><span class="bg-slate-100 px-2 py-0.5 rounded">${l.action}</span></td>
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
      const phone = document.getElementById('bill-member').value;
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
      if(!res.ok) return alert(d.detail || 'ไม่สามารถเช็คบิลได้');

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
        document.getElementById('rc-point-box').innerText = `⭐ สะสมแต้มรอบนี้: +${d.earned_points} แต้ม`;
      }

      alert('เช็คบิลและพิมพ์ใบเสร็จเรียบร้อย!');
    }

    async function sendCustomerOrder() {
      const mid = parseInt(document.getElementById('qr-select-menu').value);
      const spice = document.getElementById('qr-opt-spice').value;
      const egg = document.getElementById('qr-opt-egg').value;
      const size = document.getElementById('qr-opt-size').value;

      await fetch('/api/customer/order', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
          table_id: 1,
          items: [{ menu_id: mid, qty: 1, options: { spiciness: spice, egg: egg, size: size } }]
        })
      });
      alert('ส่งออเดอร์ถึงห้องครัวเรียบร้อย!');
    }

    async function loadQrMenu() {
      const res = await fetch('/api/menu?limit=50');
      const d = await res.json();
      document.getElementById('qr-select-menu').innerHTML = d.items.map(m => `<option value="${m.id}">${m.name} (${m.price} ฿)</option>`).join('');
    }

    // Real-Time Server-Sent Events Listener
    const evt = new EventSource('/api/realtime');
    evt.onmessage = function(e) {
      const ev = JSON.parse(e.data);
      if(ev.type === 'NEW_ORDER') {
        loadKitchenOrders();
      }
    };

    function openMoveModal() {
      const fromT = prompt('ระบุหมายเลขโต๊ะต้นทาง:');
      const toT = prompt('ระบุหมายเลขโต๊ะปลายทาง:');
      if(fromT && toT) {
        fetch('/api/table/move', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({ from_table: parseInt(fromT), to_table: parseInt(toT) })
        }).then(r => r.json()).then(d => {
          alert(d.message || d.detail);
          loadTables();
        });
      }
    }

    function openMergeModal() {
      const src = prompt('ระบุเลขโต๊ะที่ต้องการรวม (คั่นด้วยจุลภาค เช่น 2,3):');
      const tgt = prompt('ระบุเลขโต๊ะเป้าหมายหลัก:');
      if(src && tgt) {
        const arr = src.split(',').map(x => parseInt(x.trim()));
        fetch('/api/table/merge', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({ source_tables: arr, target_table: parseInt(tgt) })
        }).then(r => r.json()).then(d => {
          alert(d.message || d.detail);
          loadTables();
        });
      }
    }

    setInterval(() => {
      document.getElementById('clock').innerText = new Date().toLocaleTimeString('th-TH');
    }, 1000);

    // Initial Startup
    loadDashboard();
    loadQrMenu();
  </script>
</body>
</html>
"""