
import os
import json
import asyncio
import re
from datetime import datetime
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, HTTPException, Query, Header, Request
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, Field

from core.database import load_db, save_db, add_audit_log
from core.auth import verify_password, hash_password, create_access_token, parse_token
import core.services as services

app = FastAPI(title="Smart Restaurant Pro Enterprise")

HTML_FILE_PATH = os.path.join(os.path.dirname(__file__), "index.html")

# Error Handlers
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    errors = exc.errors()
    first_err = errors[0] if errors else {}
    field = str(first_err.get("loc", ["ข้อมูล"])[-1])
    return JSONResponse(status_code=400, content={"success": False, "detail": f"ช่อง '{field}' ระบุข้อมูลไม่ถูกต้อง"})

@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    return JSONResponse(status_code=500, content={"success": False, "detail": "เกิดข้อผิดพลาดในการประมวลผล กรุณาลองใหม่อีกครั้ง"})

# Real-Time Event Stream
event_subscribers: List[asyncio.Queue] = []

async def broadcast_event(event_type: str, data: dict):
    message = json.dumps({"type": event_type, "data": data, "time": datetime.now().isoformat()})
    for q in list(event_subscribers):
        try:
            await q.put(message)
        except Exception:
            pass

def require_role(token_str: Optional[str], allowed_roles: List[str]) -> Dict[str, str]:
    if not token_str:
        raise HTTPException(status_code=401, detail="กรุณาเข้าสู่ระบบก่อนทำรายการ")
    user_info = parse_token(token_str)
    if not user_info:
        raise HTTPException(status_code=401, detail="Token ไม่ถูกต้องหรือหมดอายุ")
    if user_info["role"] not in allowed_roles:
        raise HTTPException(status_code=403, detail="คุณไม่มีสิทธิ์ทำรายการนี้")
    return user_info

# Schemas
class LoginRequest(BaseModel):
    username: str
    password: str

class RegisterRequest(BaseModel):
    username: str
    password: str
    name: str

class TableCreateRequest(BaseModel):
    table_id: Optional[int] = None
    capacity: int = Field(default=4, gt=0)

class MenuRecipeItem(BaseModel):
    ingredient_id: str
    amount: float = Field(..., gt=0)

class MenuCreateRequest(BaseModel):
    name: str
    category: str = Field(default="อาหารจานเดียว")
    price: float = Field(..., ge=0)
    is_available: bool = True
    image: Optional[str] = ""
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
    table_id: int = Field(..., gt=0)
    items: List[PlaceOrderItem]

class CheckoutRequest(BaseModel):
    table_id: int = Field(..., gt=0)
    discount_percent: float = Field(default=0.0, ge=0.0, le=100.0)
    member_phone: Optional[str] = None
    payment_method: str = Field(default="cash")
    cash_received: Optional[float] = 0.0

class MoveTableRequest(BaseModel):
    from_table: int = Field(..., gt=0)
    to_table: int = Field(..., gt=0)

class MergeTableRequest(BaseModel):
    source_tables: List[int]
    target_table: int = Field(..., gt=0)

class QueueRequest(BaseModel):
    name: str
    party_size: int = Field(..., gt=0)

class ReservationRequest(BaseModel):
    name: str
    phone: str
    date: str
    time: str
    party_size: int = Field(..., gt=0)

class InventoryItemRequest(BaseModel):
    name: str
    stock: float = Field(..., ge=0)
    unit: str
    min_stock: float = Field(default=10, ge=0)

# ================= APIs =================

@app.post("/api/auth/login")
async def login(req: LoginRequest):
    u_name = req.username.strip().lower()
    if len(req.password) < 6:
        raise HTTPException(status_code=400, detail="รหัสผ่านต้องมีความยาวอย่างน้อย 6 ตัวอักษร")

    db = load_db()
    user = next((u for u in db.get("users", []) if u["username"].lower() == u_name), None)
    if not user or not verify_password(req.password, user["password_hash"], user["salt"]):
        raise HTTPException(status_code=401, detail="ชื่อผู้ใช้หรือรหัสผ่านไม่ถูกต้อง")

    token = create_access_token(user["id"], user["role"])
    add_audit_log(user["name"], "LOGIN", f"เข้าสู่ระบบในฐานะ {user['role']}")
    
    if user["role"] == "customer":
        await broadcast_event("CUSTOMER_ACTIVITY", {"user": user["name"], "action": "LOGIN", "desc": "เข้าสู่ระบบในฐานะลูกค้า"})

    return {"token": token, "role": user["role"], "name": user["name"], "username": user["username"]}

@app.post("/api/auth/register")
async def register(req: RegisterRequest):
    name_clean = req.name.strip()
    u_name = req.username.strip().lower()
    
    if not name_clean:
        raise HTTPException(status_code=400, detail="ชื่อ-นามสกุลต้องไม่เป็นช่องว่าง")
    if not re.match(r"^[a-zA-Z0-9]+$", u_name):
        raise HTTPException(status_code=400, detail="ชื่อผู้ใช้ต้องเป็นภาษาอังกฤษและตัวเลขเท่านั้น")
    if len(req.password) < 6:
        raise HTTPException(status_code=400, detail="รหัสผ่านต้องมีความยาวอย่างน้อย 6 ตัวอักษร")

    db = load_db()
    if any(u["username"].lower() == u_name for u in db.get("users", [])):
        raise HTTPException(status_code=400, detail="ชื่อผู้ใช้นี้มีอยู่ในระบบแล้ว")

    h, s = hash_password(req.password)
    new_user = {
        "id": f"u_{len(db.get('users', [])) + 1}",
        "username": u_name,
        "password_hash": h,
        "salt": s,
        "role": "customer",
        "name": name_clean,
        "registered_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    db.setdefault("users", []).append(new_user)
    save_db(db)
    
    add_audit_log(name_clean, "CUSTOMER_REGISTER", f"ลูกค้าใหม่สมัครสมาชิก: @{u_name}")
    await broadcast_event("CUSTOMER_ACTIVITY", {"user": name_clean, "action": "CUSTOMER_REGISTER", "desc": f"สมัครสมาชิกใหม่ (@{u_name})"})
    return {"success": True, "message": "ลงทะเบียนสมาชิกลูกค้าสำเร็จ"}

@app.get("/api/admin/customers")
def get_admin_customers(x_auth_token: Optional[str] = Header(None)):
    require_role(x_auth_token, ["admin"])
    db = load_db()
    customers = [u for u in db.get("users", []) if u.get("role") == "customer"]
    member_map = {m.get("phone", ""): m.get("points", 0) for m in db.get("members", [])}
    
    customer_list = []
    for c in customers:
        customer_list.append({
            "id": c["id"],
            "username": c["username"],
            "name": c["name"],
            "registered_at": c.get("registered_at", "ก่อนหน้านี้"),
            "points": member_map.get(c.get("username"), 0)
        })

    logs = db.get("audit_logs", [])
    customer_activities = [
        l for l in logs 
        if l.get("action", "").startswith("CUSTOMER_") or l.get("action") in ("REGISTER", "CUSTOMER_REGISTER", "CUSTOMER_ORDER", "CUSTOMER_QUEUE", "CUSTOMER_RESERVE", "CUSTOMER_CHECKOUT")
    ]
    return {"total_customers": len(customer_list), "customers": customer_list, "recent_activities": customer_activities[:40]}

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
async def create_menu(req: MenuCreateRequest, x_auth_token: Optional[str] = Header(None)):
    user = require_role(x_auth_token, ["admin", "staff"])
    name = req.name.strip()
    if not name:
        raise HTTPException(status_code=400, detail="ชื่อเมนูอาหารต้องไม่เป็นช่องว่าง")

    db = load_db()
    new_id = max([m["id"] for m in db.get("menu", [])], default=0) + 1
    new_menu = {
        "id": new_id,
        "name": name,
        "category": req.category.strip() or "อาหารจานเดียว",
        "price": req.price,
        "is_available": req.is_available,
        "image": req.image.strip() if req.image else "",
        "recipe": [r.dict() for r in req.recipe]
    }
    db.setdefault("menu", []).append(new_menu)
    add_audit_log(user.get("role", "Staff"), "CREATE_MENU", f"เพิ่มเมนู: {name} ({req.price} ฿)")
    save_db(db)
    
    await broadcast_event("MENU_UPDATE", {"action": "create", "menu": new_menu})
    return {"success": True, "menu": new_menu}

@app.put("/api/menu/{menu_id}")
async def update_menu(menu_id: int, req: MenuCreateRequest, x_auth_token: Optional[str] = Header(None)):
    user = require_role(x_auth_token, ["admin", "staff"])
    name = req.name.strip()
    if not name:
        raise HTTPException(status_code=400, detail="ชื่อเมนูอาหารต้องไม่เป็นช่องว่าง")

    db = load_db()
    menu = next((m for m in db.get("menu", []) if m["id"] == menu_id), None)
    if not menu:
        raise HTTPException(status_code=404, detail="ไม่พบเมนูอาหารที่ต้องการแก้ไข")

    menu["name"] = name
    menu["category"] = req.category.strip() or "อาหารจานเดียว"
    menu["price"] = req.price
    menu["is_available"] = req.is_available
    if req.image:
        menu["image"] = req.image.strip()
    menu["recipe"] = [r.dict() for r in req.recipe]

    add_audit_log(user.get("role", "Staff"), "EDIT_MENU", f"แก้ไขเมนู: {name} ({req.price} ฿)")
    save_db(db)
    
    await broadcast_event("MENU_UPDATE", {"action": "update", "menu": menu})
    return {"success": True, "menu": menu}

@app.delete("/api/menu/{menu_id}")
async def delete_menu(menu_id: int, x_auth_token: Optional[str] = Header(None)):
    user = require_role(x_auth_token, ["admin", "staff"])
    db = load_db()
    menu = next((m for m in db.get("menu", []) if m["id"] == menu_id), None)
    if not menu:
        raise HTTPException(status_code=404, detail="ไม่พบเมนูอาหาร")

    db["menu"] = [m for m in db["menu"] if m["id"] != menu_id]
    add_audit_log(user.get("role", "Staff"), "DELETE_MENU", f"ลบเมนู: {menu['name']}")
    save_db(db)
    
    await broadcast_event("MENU_UPDATE", {"action": "delete", "menu_id": menu_id})
    return {"success": True, "message": f"ลบเมนู '{menu['name']}' เรียบร้อย"}

@app.post("/api/menu/{menu_id}/toggle")
async def toggle_menu_availability(menu_id: int, x_auth_token: Optional[str] = Header(None)):
    user = require_role(x_auth_token, ["admin", "staff"])
    db = load_db()
    menu = next((m for m in db.get("menu", []) if m["id"] == menu_id), None)
    if not menu:
        raise HTTPException(status_code=404, detail="ไม่พบเมนูอาหาร")

    menu["is_available"] = not menu.get("is_available", True)
    status_str = "พร้อมขาย" if menu["is_available"] else "หมด"
    add_audit_log(user.get("role", "Staff"), "TOGGLE_MENU", f"เปลี่ยนสถานะ '{menu['name']}' เป็น {status_str}")
    save_db(db)
    
    await broadcast_event("MENU_UPDATE", {"action": "toggle", "menu_id": menu_id, "is_available": menu["is_available"]})
    return {"success": True, "is_available": menu["is_available"]}

@app.get("/api/tables")
def get_tables():
    db = load_db()
    return db.get("tables", [])

@app.post("/api/tables")
def create_table(req: TableCreateRequest, x_auth_token: Optional[str] = Header(None)):
    user = require_role(x_auth_token, ["admin", "staff"])
    db = load_db()
    tables = db.setdefault("tables", [])
    new_id = req.table_id if req.table_id else (max([t["table_id"] for t in tables], default=0) + 1)
    if any(t["table_id"] == new_id for t in tables):
        raise HTTPException(status_code=400, detail=f"โต๊ะหมายเลข {new_id} มีอยู่แล้ว")

    new_table = {"table_id": new_id, "status": "ว่าง", "capacity": req.capacity}
    tables.append(new_table)
    add_audit_log(user.get("role", "Staff"), "ADD_TABLE", f"เพิ่มโต๊ะ {new_id} ({req.capacity} ที่นั่ง)")
    save_db(db)
    return {"success": True, "table": new_table}

@app.delete("/api/tables/{table_id}")
def delete_table(table_id: int, x_auth_token: Optional[str] = Header(None)):
    user = require_role(x_auth_token, ["admin"])
    db = load_db()
    tables = db.get("tables", [])
    table = next((t for t in tables if t["table_id"] == table_id), None)
    if not table:
        raise HTTPException(status_code=404, detail="ไม่พบโต๊ะ")
    if table.get("status") != "ว่าง":
        raise HTTPException(status_code=400, detail="ไม่สามารถลบโต๊ะที่มีลูกค้าอยู่ได้")

    db["tables"] = [t for t in tables if t["table_id"] != table_id]
    add_audit_log("Admin", "DELETE_TABLE", f"ลบโต๊ะหมายเลข {table_id}")
    save_db(db)
    return {"success": True, "message": f"ลบโต๊ะหมายเลข {table_id} สำเร็จ"}

@app.post("/api/table/move")
def move_table_route(req: MoveTableRequest, x_auth_token: Optional[str] = Header(None)):
    user = require_role(x_auth_token, ["admin", "staff"])
    ok, msg = services.move_table(req.from_table, req.to_table, user.get("role", "Staff"))
    if not ok:
        raise HTTPException(status_code=400, detail=msg)
    return {"success": True, "message": msg}

@app.post("/api/table/merge")
def merge_table_route(req: MergeTableRequest, x_auth_token: Optional[str] = Header(None)):
    user = require_role(x_auth_token, ["admin", "staff"])
    ok, msg = services.merge_tables(req.source_tables, req.target_table, user.get("role", "Staff"))
    if not ok:
        raise HTTPException(status_code=400, detail=msg)
    return {"success": True, "message": msg}

@app.get("/api/orders")
def get_orders(table_id: Optional[int] = None):
    db = load_db()
    orders = db.get("orders", [])
    if table_id is not None:
        orders = [o for o in orders if o["table_id"] == table_id]
    return orders

@app.post("/api/customer/order")
async def customer_order(req: CustomerOrderRequest, x_auth_token: Optional[str] = Header(None)):
    db = load_db()
    table = next((t for t in db.get("tables", []) if t["table_id"] == req.table_id), None)
    if not table:
        raise HTTPException(status_code=404, detail="ไม่พบหมายเลขโต๊ะ")
    if not req.items:
        raise HTTPException(status_code=400, detail="กรุณาเลือกรายการอาหาร")

    actor_name = f"ลูกค้าโต๊ะ {req.table_id}"
    if x_auth_token:
        u = parse_token(x_auth_token)
        if u:
            matching_user = next((x for x in db.get("users", []) if x["id"] == u["user_id"]), None)
            if matching_user:
                actor_name = f"{matching_user['name']} (โต๊ะ {req.table_id})"

    for it in req.items:
        menu_item = next((m for m in db.get("menu", []) if m["id"] == it.menu_id), None)
        if not menu_item:
            raise HTTPException(status_code=404, detail=f"ไม่พบรหัสเมนู {it.menu_id}")
        if not menu_item.get("is_available", True):
            raise HTTPException(status_code=400, detail=f"ขออภัย เมนู '{menu_item['name']}' หมดชั่วคราว")

    created = []
    order_names = []
    for it in req.items:
        menu_item = next((m for m in db.get("menu", []) if m["id"] == it.menu_id), None)
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
            "unit_price": max(0.0, menu_item["price"] + extra_price),
            "qty": it.qty,
            "options": it.options.dict() if it.options else {},
            "status": "รอทำ",
            "stock_deducted": False,
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        db.setdefault("orders", []).append(new_order)
        created.append(new_order)
        order_names.append(f"{menu_item['name']} x{it.qty}")

    table["status"] = "มีลูกค้า"
    save_db(db)
    
    add_audit_log(actor_name, "CUSTOMER_ORDER", f"สั่งอาหารโต๊ะ {req.table_id}: {', '.join(order_names)}")
    await broadcast_event("NEW_ORDER", {"table_id": req.table_id, "count": len(created)})
    await broadcast_event("CUSTOMER_ACTIVITY", {"user": actor_name, "action": "CUSTOMER_ORDER", "desc": f"สั่งอาหารโต๊ะ {req.table_id} ({len(created)} รายการ)"})
    return {"success": True, "message": "ส่งรายการเข้าครัวแล้ว", "orders": created}

@app.post("/api/kitchen/{order_id}/status")
async def update_kitchen_order(order_id: int, status: str = Query(...), x_auth_token: Optional[str] = Header(None)):
    user = require_role(x_auth_token, ["admin", "staff"])
    if status not in services.KITCHEN_STATUSES:
        raise HTTPException(status_code=400, detail="สถานะไม่ถูกต้อง")

    db = load_db()
    order = next((o for o in db.get("orders", []) if o["order_id"] == order_id), None)
    if not order:
        raise HTTPException(status_code=404, detail="ไม่พบรายการออเดอร์")

    if status in ("กำลังทำ", "เสิร์ฟแล้ว") and not order.get("stock_deducted", False):
        ok, msg = services.deduct_stock_in_db(db, order["menu_id"], order["qty"], user.get("role", "Kitchen"))
        if not ok:
            raise HTTPException(status_code=400, detail=msg)
        order["stock_deducted"] = True

    order["status"] = status
    save_db(db)
    
    await broadcast_event("ORDER_STATUS_UPDATE", {"order_id": order_id, "status": status})
    await broadcast_event("STOCK_UPDATE", {})
    return {"success": True, "status": status}

@app.get("/api/inventory")
def get_inventory():
    db = load_db()
    return db.get("inventory", [])

@app.post("/api/inventory")
def add_new_inventory_item(req: InventoryItemRequest, x_auth_token: Optional[str] = Header(None)):
    user = require_role(x_auth_token, ["admin", "staff"])
    name = req.name.strip()
    if not name:
        raise HTTPException(status_code=400, detail="ชื่อวัตถุดิบต้องไม่เป็นช่องว่าง")

    db = load_db()
    items = db.setdefault("inventory", [])
    if any(i["name"].lower() == name.lower() for i in items):
        raise HTTPException(status_code=400, detail=f"วัตถุดิบ '{name}' มีอยู่ในระบบแล้ว")

    new_id = f"ing_{len(items) + 1}"
    item = {
        "id": new_id,
        "name": name,
        "stock": round(req.stock, 2),
        "unit": req.unit.strip() or "หน่วย",
        "min_stock": round(req.min_stock, 2)
    }
    items.append(item)
    add_audit_log(user.get("role", "Staff"), "ADD_INVENTORY", f"เพิ่มวัตถุดิบใหม่: {name} ({req.stock} {item['unit']})")
    save_db(db)
    return {"success": True, "item": item}

@app.put("/api/inventory/{item_id}")
def update_inventory_stock(item_id: str, added_stock: float = Query(...), x_auth_token: Optional[str] = Header(None)):
    user = require_role(x_auth_token, ["admin", "staff"])
    db = load_db()
    item = next((i for i in db.get("inventory", []) if i["id"] == item_id), None)
    if not item:
        raise HTTPException(status_code=404, detail="ไม่พบวัตถุดิบ")

    if float(item["stock"]) + added_stock < 0:
        raise HTTPException(status_code=400, detail=f"ไม่สามารถเบิกเกินจำนวนที่มีได้ (เหลือ: {item['stock']} {item['unit']})")

    item["stock"] = round(float(item["stock"]) + added_stock, 2)
    action_type = "RESTOCK" if added_stock > 0 else "WITHDRAW_STOCK"
    add_audit_log(user.get("role", "Staff"), action_type, f"ปรับสต็อก {item['name']} ({added_stock} {item['unit']})")
    save_db(db)
    return {"success": True, "item": item}

@app.get("/api/queues")
def get_queues():
    db = load_db()
    return db.get("queues", [])

@app.post("/api/queue/ticket")
async def issue_queue(req: QueueRequest):
    name = req.name.strip()
    if not name:
        raise HTTPException(status_code=400, detail="ชื่อลูกค้าต้องไม่เป็นช่องว่าง")
    db = load_db()
    q_num = f"Q{len(db.get('queues', [])) + 1:02d}"
    ticket = {
        "queue_id": q_num,
        "name": name,
        "party_size": req.party_size,
        "status": "รอเรียก",
        "created_at": datetime.now().strftime("%H:%M:%S")
    }
    db.setdefault("queues", []).append(ticket)
    save_db(db)
    add_audit_log(name, "CUSTOMER_QUEUE", f"รับบัตรคิว {q_num} ({req.party_size} ท่าน)")
    await broadcast_event("QUEUE_UPDATE", ticket)
    await broadcast_event("CUSTOMER_ACTIVITY", {"user": name, "action": "CUSTOMER_QUEUE", "desc": f"รับบัตรคิว {q_num} ({req.party_size} ท่าน)"})
    return ticket

@app.post("/api/queue/{queue_id}/status")
async def update_queue_status(queue_id: str, status: str = Query(...), x_auth_token: Optional[str] = Header(None)):
    user = require_role(x_auth_token, ["admin", "staff"])
    db = load_db()
    q = next((q for q in db.get("queues", []) if q["queue_id"] == queue_id), None)
    if not q:
        raise HTTPException(status_code=404, detail="ไม่พบหมายเลขคิว")
    q["status"] = status
    save_db(db)
    add_audit_log(user.get("role", "Staff"), "CALL_QUEUE", f"ปรับสถานะคิว {queue_id} เป็น {status}")
    await broadcast_event("QUEUE_CALLED", q)
    return {"success": True, "queue": q}

@app.get("/api/reservations")
def get_reservations():
    db = load_db()
    return db.get("reservations", [])

@app.post("/api/reservations")
async def create_reservation(req: ReservationRequest):
    name = req.name.strip()
    phone = req.phone.strip()
    if not name or not phone:
        raise HTTPException(status_code=400, detail="กรุณากรอกชื่อและเบอร์โทรศัพท์")
    db = load_db()
    res_entry = req.dict()
    res_entry["id"] = len(db.get("reservations", [])) + 1
    res_entry["status"] = "จองสำเร็จ"
    db.setdefault("reservations", []).append(res_entry)
    add_audit_log(name, "CUSTOMER_RESERVE", f"จองโต๊ะล่วงหน้า {req.date} {req.time} ({req.party_size} ที่นั่ง)")
    save_db(db)
    await broadcast_event("CUSTOMER_ACTIVITY", {"user": name, "action": "CUSTOMER_RESERVE", "desc": f"จองโต๊ะ {req.date} ({req.party_size} ท่าน)"})
    return {"success": True, "reservation": res_entry}

@app.post("/api/checkout")
async def checkout_order(req: CheckoutRequest, x_auth_token: Optional[str] = Header(None)):
    user = require_role(x_auth_token, ["admin", "staff"])
    db = load_db()
    orders = [o for o in db.get("orders", []) if o["table_id"] == req.table_id and o.get("status") != "ยกเลิก"]
    if not orders:
        raise HTTPException(status_code=400, detail="ไม่มีรายการอาหารค้างชำระสำหรับโต๊ะนี้")

    subtotal, disc_amt, sc, vat, net = services.calculate_bill(orders, req.discount_percent)
    earned_pts = services.process_loyalty_points(req.member_phone, net) if req.member_phone else 0

    change = 0.0
    if req.payment_method == "cash":
        cash_in = max(0.0, float(req.cash_received or 0.0))
        if cash_in < net:
            raise HTTPException(status_code=400, detail=f"จำนวนเงินสดที่รับมา ({cash_in} ฿) น้อยกว่ายอดสุทธิ ({net} ฿)")
        change = round(cash_in - net, 2)

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
        "payment_method": req.payment_method,
        "cash_received": req.cash_received if req.payment_method == "cash" else None,
        "change": change if req.payment_method == "cash" else None,
        "member_phone": req.member_phone,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    db.setdefault("sales", []).append(receipt)
    db["orders"] = [o for o in db.get("orders", []) if o["table_id"] != req.table_id]

    for t in db.get("tables", []):
        if t["table_id"] == req.table_id:
            t["status"] = "ว่าง"

    pay_desc = f"เงินสด รับ: {req.cash_received} ทอน: {change}" if req.payment_method == "cash" else "โอนเงินผ่าน QR สำเร็จ"
    add_audit_log(user.get("role", "Cashier"), "CUSTOMER_CHECKOUT", f"เช็คบิลโต๊ะ {req.table_id} ยอด {net} ฿ ({pay_desc})")
    save_db(db)

    await broadcast_event("DASHBOARD_UPDATE", {"revenue": net})
    await broadcast_event("CUSTOMER_ACTIVITY", {"user": f"โต๊ะ {req.table_id}", "action": "CUSTOMER_CHECKOUT", "desc": f"ชำระเงินเรียบร้อย ยอด {net} ฿ ({pay_desc})"})
    return {"success": True, "receipt": receipt, "earned_points": earned_pts}

@app.get("/api/dashboard")
def get_dashboard(x_auth_token: Optional[str] = Header(None)):
    require_role(x_auth_token, ["admin", "staff"])
    db = load_db()
    
    sales = db.get("sales", [])
    total_sales = sum(float(s.get("net_total", 0.0)) for s in sales)
    occupied_tables = sum(1 for t in db.get("tables", []) if t.get("status") != "ว่าง")
    low_stock = [i for i in db.get("inventory", []) if i.get("stock", 0) <= i.get("min_stock", 10)]

    item_sales_counter = {}
    for s in sales:
        for it in s.get("items", []):
            name = it.get("name", "ไม่ระบุ")
            qty = int(it.get("qty", 1))
            item_sales_counter[name] = item_sales_counter.get(name, 0) + qty

    best_sellers = sorted(item_sales_counter.items(), key=lambda x: x[1], reverse=True)[:5]
    return {
        "total_revenue": round(total_sales, 2),
        "total_bills": len(sales),
        "occupied_tables": occupied_tables,
        "total_tables": len(db.get("tables", [])),
        "low_stock_alerts": low_stock,
        "best_sellers": [{"name": k, "qty": v} for k, v in best_sellers],
        "recent_logs": db.get("audit_logs", [])[:20]
    }

@app.get("/api/logs")
def get_audit_logs(page: int = 1, limit: int = 15, x_auth_token: Optional[str] = Header(None)):
    require_role(x_auth_token, ["admin"])
    db = load_db()
    logs = db.get("audit_logs", [])
    return services.paginate_and_sort(logs, sort_by="", page=page, limit=limit)

# โหลดหน้า HTML แยกต่างหากอย่างปลอดภัย ป้องกันปัญหาหลุดหรือ Error
@app.get("/", response_class=HTMLResponse)
def index():
    if not os.path.exists(HTML_FILE_PATH):
        return "<h1>ไม่พบไฟล์ api/index.html กรุณาสร้างไฟล์ตามคำแนะนำ</h1>"
    with open(HTML_FILE_PATH, "r", encoding="utf-8") as f:
        return f.read()
