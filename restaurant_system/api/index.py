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

app = FastAPI(title="Smart Restaurant Pro Enterprise", docs_url="/api/docs", openapi_url="/api/openapi.json")

# Error Handlers
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    errors = exc.errors()
    first_err = errors[0] if errors else {}
    field = str(first_err.get("loc", ["ข้อมูล"])[-1])
    err_type = first_err.get("type", "")
    if "int" in err_type or "float" in err_type:
        msg = f"ช่อง '{field}' ต้องระบุเป็นตัวเลขเท่านั้น"
    else:
        msg = f"ข้อมูลในช่อง '{field}' ไม่ถูกต้องตามเกณฑ์"
    return JSONResponse(status_code=400, content={"success": False, "detail": msg})

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
    payment_method: str = Field(default="cash") # "cash" หรือ "transfer"
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

# ================= 2. Authentication APIs (แก้ปัญหาตัวพิมพ์เล็กใหญ่ข้ามอุปกรณ์) =================

@app.post("/api/auth/login")
async def login(req: LoginRequest):
    # ปรับเป็นตัวพิมพ์เล็กเสมอ ป้องกันมือถือพิมพ์ตัวใหญ่ตัวแรก
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
    u_name = req.username.strip().lower() # บันทึกเป็นตัวพิมพ์เล็กเสมอ
    
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
    return {"total_customers": len(customer_list), "customers": customer_list, "recent_activities": customer_activities[:30]}

# ================= 3. Real-Time SSE =================

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

# ================= 4. เมนูอาหาร & CRUD (ส่งสัญญาณ Real-time ทุกคำสั่ง) =================

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
    
    # ส่งสัญญาณ Real-time ให้ทุกหน้าจออัปเดตเมนูใหม่ทันที
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
    
    # ส่งสัญญาณ Real-time ให้หน้าจอทุกคนเปลี่ยนข้อมูลทันที
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
    
    # ส่งสัญญาณ Real-time ให้เมนูหายไปจากหน้าจอของลูกค้าและสตาฟทุกคนทันที
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

# ================= 5. จัดการโต๊ะ =================

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

# ================= 6. สั่งอาหาร & จอครัว =================

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

    actor_name = f"ลูกค้า (โต๊ะ {req.table_id})"
    if x_auth_token:
        u = parse_token(x_auth_token)
        if u:
            matching_user = next((x for x in db.get("users", []) if x["id"] == u["user_id"]), None)
            if matching_user:
                actor_name = matching_user["name"]

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

# ================= 7. สต็อกวัตถุดิบ =================

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

# ================= 8. คิว & จองโต๊ะ =================

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

# ================= 9. เช็คบิล: รองรับเงินสด (คำนวณเงินทอน) และโอนเงิน / QR =================

@app.post("/api/checkout")
async def checkout_order(req: CheckoutRequest, x_auth_token: Optional[str] = Header(None)):
    user = require_role(x_auth_token, ["admin", "staff"])
    db = load_db()
    orders = [o for o in db.get("orders", []) if o["table_id"] == req.table_id and o.get("status") != "ยกเลิก"]
    if not orders:
        raise HTTPException(status_code=400, detail="ไม่มีรายการอาหารค้างชำระสำหรับโต๊ะนี้")

    subtotal, disc_amt, sc, vat, net = services.calculate_bill(orders, req.discount_percent)
    earned_pts = services.process_loyalty_points(req.member_phone, net) if req.member_phone else 0

    # คำนวณยอดเงินสดและเงินทอน
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
        "payment_method": req.payment_method, # "cash" หรือ "transfer"
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

    # ส่งสัญญาณ Real-time ให้ Dashboard รีเฟรชทันที
    await broadcast_event("DASHBOARD_UPDATE", {"revenue": net})
    await broadcast_event("CUSTOMER_ACTIVITY", {"user": f"โต๊ะ {req.table_id}", "action": "CUSTOMER_CHECKOUT", "desc": f"ชำระเงินเรียบร้อย ยอด {net} ฿ ({pay_desc})"})
    return {"success": True, "receipt": receipt, "earned_points": earned_pts}

# ================= 10. Dashboard & Audit Logs =================

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
        "recent_logs": db.get("audit_logs", [])[:15]
    }

@app.get("/api/logs")
def get_audit_logs(page: int = 1, limit: int = 15, x_auth_token: Optional[str] = Header(None)):
    require_role(x_auth_token, ["admin"])
    db = load_db()
    logs = db.get("audit_logs", [])
    return services.paginate_and_sort(logs, sort_by="", page=page, limit=limit)

# ================= 11. Single-Page Application (HTML/JS) =================

@app.get("/", response_class=HTMLResponse)
def index():
    return """<!DOCTYPE html>
<html lang="th" class="h-full">
<head>
  <meta charset="UTF-8">
  <title>RESTRO PRO - Modern Restaurant Management Platform</title>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <script src="https://cdn.tailwindcss.com"></script>
  <script>
    tailwind.config = { darkMode: 'class' }
  </script>
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=Prompt:wght@300;400;500;600;700&display=swap" rel="stylesheet">
  <style>
    * { font-family: 'Prompt', 'Plus Jakarta Sans', sans-serif; }
    .font-mono { font-family: ui-monospace, monospace !important; }
    @media print {
      body * { visibility: hidden; }
      #printable-receipt, #printable-receipt * { visibility: visible; }
      #printable-receipt { position: absolute; left: 0; top: 0; width: 100%; border: none !important; }
    }
    .custom-scroll::-webkit-scrollbar { width: 5px; height: 5px; }
    .custom-scroll::-webkit-scrollbar-thumb { background: #cbd5e1; border-radius: 9999px; }
  </style>
</head>
<body class="bg-slate-50 dark:bg-slate-950 text-slate-800 dark:text-slate-100 antialiased h-full flex flex-col overflow-hidden transition-colors duration-200">
  <div id="toast-container" class="fixed top-5 right-5 z-[9999] space-y-2 pointer-events-none"></div>

  <!-- QR Modal -->
  <div id="qr-modal" class="fixed inset-0 bg-slate-900/60 backdrop-blur-sm z-50 hidden flex items-center justify-center p-4">
    <div class="bg-white dark:bg-slate-900 rounded-3xl max-w-sm w-full p-6 text-center space-y-4 shadow-2xl border border-slate-100 dark:border-slate-800">
      <div class="w-12 h-12 rounded-2xl bg-indigo-50 dark:bg-indigo-950/60 text-indigo-600 dark:text-indigo-400 flex items-center justify-center mx-auto text-xl"><i class="fa-solid fa-qrcode"></i></div>
      <h3 id="qr-modal-title" class="font-bold text-xl text-slate-900 dark:text-white">QR Code สั่งอาหาร</h3>
      <div class="bg-slate-50 dark:bg-slate-800 p-5 rounded-2xl flex justify-center border border-slate-100 dark:border-slate-700">
        <img id="qr-modal-img" src="" alt="Table QR" class="w-52 h-52 rounded-xl shadow-sm">
      </div>
      <button onclick="document.getElementById('qr-modal').classList.add('hidden')" class="w-full bg-slate-900 dark:bg-slate-800 hover:bg-black text-white font-semibold py-3 rounded-2xl text-sm transition">ปิดหน้าต่าง</button>
    </div>
  </div>

  <!-- Modal: เพิ่ม / แก้ไขเมนูอาหาร -->
  <div id="add-menu-modal" class="fixed inset-0 bg-slate-900/60 backdrop-blur-sm z-50 hidden flex items-center justify-center p-4">
    <div class="bg-white dark:bg-slate-900 rounded-3xl max-w-lg w-full p-6 shadow-2xl space-y-4 max-h-[92vh] overflow-y-auto custom-scroll border border-slate-100 dark:border-slate-800">
      <div class="flex justify-between items-center border-b border-slate-100 dark:border-slate-800 pb-3">
        <h3 id="menu-modal-heading" class="font-bold text-lg text-slate-900 dark:text-white flex items-center gap-2"><i class="fa-solid fa-utensils text-indigo-600"></i> เพิ่มรายการเมนูใหม่</h3>
        <button onclick="document.getElementById('add-menu-modal').classList.add('hidden')" class="w-8 h-8 rounded-full hover:bg-slate-100 dark:hover:bg-slate-800 flex items-center justify-center text-slate-400"><i class="fa-solid fa-xmark"></i></button>
      </div>
      
      <input type="hidden" id="edit-menu-id" value="">

      <div class="space-y-4">
        <div>
          <label class="text-xs font-semibold text-slate-700 dark:text-slate-300 block mb-1">ชื่อเมนูอาหาร *</label>
          <input type="text" id="new-menu-name" placeholder="เช่น กะเพราหมูสับ, ต้มยำกุ้ง" class="w-full border border-slate-200 dark:border-slate-700 rounded-xl px-3.5 py-2.5 text-sm bg-white dark:bg-slate-800 text-slate-900 dark:text-white outline-none focus:border-indigo-500">
        </div>
        <div class="grid grid-cols-2 gap-3">
          <div>
            <label class="text-xs font-semibold text-slate-700 dark:text-slate-300 block mb-1">หมวดหมู่</label>
            <input type="text" id="new-menu-cat" value="อาหารจานเดียว" class="w-full border border-slate-200 dark:border-slate-700 rounded-xl px-3.5 py-2.5 text-sm bg-white dark:bg-slate-800 text-slate-900 dark:text-white outline-none focus:border-indigo-500">
          </div>
          <div>
            <label class="text-xs font-semibold text-slate-700 dark:text-slate-300 block mb-1">ราคา (บาท) *</label>
            <input type="number" id="new-menu-price" step="0.5" min="0" placeholder="0.00" class="w-full border border-slate-200 dark:border-slate-700 rounded-xl px-3.5 py-2.5 text-sm bg-white dark:bg-slate-800 font-bold text-indigo-600 dark:text-indigo-400 outline-none focus:border-indigo-500">
          </div>
        </div>

        <div class="border border-slate-200 dark:border-slate-800 rounded-2xl p-4 bg-slate-50/60 dark:bg-slate-800/40 space-y-3">
          <label class="text-xs font-semibold text-slate-700 dark:text-slate-300 flex items-center justify-between">
            <span>รูปภาพประกอบเมนู</span>
            <span class="text-[11px] text-slate-400">URL หรืออัปโหลด</span>
          </label>
          <div class="flex gap-2 p-1 bg-slate-200/60 dark:bg-slate-700 rounded-xl text-xs">
            <button type="button" onclick="setImgMode('url')" id="btn-mode-url" class="flex-1 py-1.5 rounded-lg font-semibold bg-white dark:bg-slate-800 text-slate-800 dark:text-white shadow-sm transition">ใส่ลิงก์รูป (URL)</button>
            <button type="button" onclick="setImgMode('upload')" id="btn-mode-upload" class="flex-1 py-1.5 rounded-lg font-semibold text-slate-600 dark:text-slate-300 transition">อัปโหลดไฟล์</button>
          </div>
          <div id="box-img-url">
            <input type="url" id="new-menu-img-url" oninput="previewImage()" placeholder="https://..." class="w-full border border-slate-200 dark:border-slate-700 rounded-xl px-3 py-2 text-xs bg-white dark:bg-slate-800 text-slate-900 dark:text-white outline-none">
          </div>
          <div id="box-img-upload" class="hidden">
            <input type="file" id="new-menu-img-file" accept="image/png, image/jpeg, image/webp" onchange="handleFileUpload(event)" class="w-full text-xs text-slate-500">
          </div>
          <div id="menu-img-preview-box" class="hidden text-center pt-1">
            <img id="menu-img-preview" src="" alt="Preview" class="h-32 w-full object-cover rounded-xl border border-slate-200 dark:border-slate-700 shadow-sm">
          </div>
        </div>

        <div class="border-t border-slate-100 dark:border-slate-800 pt-3">
          <div class="flex justify-between items-center mb-2">
            <label class="text-xs font-semibold text-slate-800 dark:text-slate-200 block">สูตรตัดสต็อก (Recipe)</label>
            <button onclick="addRecipeRow()" type="button" class="text-xs bg-indigo-50 dark:bg-indigo-950/70 text-indigo-600 dark:text-indigo-400 font-semibold px-2.5 py-1 rounded-lg">+ เพิ่มวัตถุดิบในสูตร</button>
          </div>
          <div id="recipe-rows-container" class="space-y-2 mt-2"></div>
        </div>
        <button id="menu-submit-btn" onclick="submitMenuForm()" class="w-full bg-indigo-600 hover:bg-indigo-700 text-white font-semibold py-3 rounded-2xl shadow-md transition">บันทึกเมนูใหม่</button>
      </div>
    </div>
  </div>

  <!-- Modal: เพิ่มวัตถุดิบใหม่เข้าคลัง -->
  <div id="add-inventory-modal" class="fixed inset-0 bg-slate-900/60 backdrop-blur-sm z-50 hidden flex items-center justify-center p-4">
    <div class="bg-white dark:bg-slate-900 rounded-3xl max-w-md w-full p-6 shadow-2xl space-y-4 border border-slate-100 dark:border-slate-800">
      <div class="flex justify-between items-center border-b border-slate-100 dark:border-slate-800 pb-3">
        <h3 class="font-bold text-lg text-slate-900 dark:text-white flex items-center gap-2"><i class="fa-solid fa-box text-indigo-600"></i> เพิ่มวัตถุดิบใหม่เข้าคลังสต็อก</h3>
        <button onclick="document.getElementById('add-inventory-modal').classList.add('hidden')" class="w-8 h-8 rounded-full hover:bg-slate-100 dark:hover:bg-slate-800 flex items-center justify-center text-slate-400"><i class="fa-solid fa-xmark"></i></button>
      </div>
      <div class="space-y-3">
        <div>
          <label class="text-xs font-semibold text-slate-700 dark:text-slate-300 block mb-1">ชื่อวัตถุดิบ *</label>
          <input type="text" id="new-inv-name" placeholder="เช่น พริกจินดา, ปลาร้า, มะนาว" class="w-full border border-slate-200 dark:border-slate-700 rounded-xl px-3.5 py-2.5 text-sm bg-white dark:bg-slate-800 text-slate-900 dark:text-white outline-none focus:border-indigo-500">
        </div>
        <div class="grid grid-cols-2 gap-3">
          <div>
            <label class="text-xs font-semibold text-slate-700 dark:text-slate-300 block mb-1">จำนวนตั้งต้น *</label>
            <input type="number" id="new-inv-stock" step="0.1" min="0" placeholder="0" class="w-full border border-slate-200 dark:border-slate-700 rounded-xl px-3.5 py-2.5 text-sm bg-white dark:bg-slate-800 text-slate-900 dark:text-white font-bold outline-none">
          </div>
          <div>
            <label class="text-xs font-semibold text-slate-700 dark:text-slate-300 block mb-1">หน่วยนับ *</label>
            <input type="text" id="new-inv-unit" placeholder="เช่น กรัม, ช้อน, ฟอง" class="w-full border border-slate-200 dark:border-slate-700 rounded-xl px-3.5 py-2.5 text-sm bg-white dark:bg-slate-800 text-slate-900 dark:text-white outline-none">
          </div>
        </div>
        <div>
          <label class="text-xs font-semibold text-slate-700 dark:text-slate-300 block mb-1">จุดเตือนสต็อกขั้นต่ำ</label>
          <input type="number" id="new-inv-min" step="0.1" min="0" value="10" class="w-full border border-slate-200 dark:border-slate-700 rounded-xl px-3.5 py-2.5 text-sm bg-white dark:bg-slate-800 text-slate-900 dark:text-white outline-none">
        </div>
        <button onclick="submitNewInventory()" class="w-full bg-indigo-600 hover:bg-indigo-700 text-white font-semibold py-3 rounded-2xl shadow-md transition mt-2">บันทึกวัตถุดิบเข้าสต็อก</button>
      </div>
    </div>
  </div>

  <!-- Auth Screen -->
  <div id="auth-screen" class="min-h-screen flex items-center justify-center bg-slate-100 dark:bg-slate-900 p-4 relative overflow-hidden transition-colors duration-200">
    <button onclick="toggleDarkMode()" class="absolute top-5 right-5 z-20 p-3 rounded-2xl bg-white dark:bg-slate-800 hover:bg-slate-50 dark:hover:bg-slate-700 text-slate-700 dark:text-slate-300 border border-slate-200 dark:border-slate-700 shadow-md transition" title="สลับโหมด มืด/สว่าง">
      <i id="auth-theme-icon" class="fa-solid fa-moon text-base"></i>
    </button>

    <div class="bg-white dark:bg-slate-900 rounded-3xl p-8 max-w-md w-full shadow-2xl relative z-10 space-y-6 border border-slate-200/80 dark:border-slate-800">
      <div class="text-center space-y-2">
        <div class="w-14 h-14 bg-indigo-600 text-white rounded-2xl flex items-center justify-center text-2xl mx-auto shadow-lg"><i class="fa-solid fa-utensils"></i></div>
        <h2 class="text-2xl font-extrabold text-slate-900 dark:text-white">RESTRO PRO</h2>
        <p class="text-xs text-slate-500 dark:text-slate-400">ระบบจัดการร้านอาหาร คลังสต็อก และ POS หน้าร้าน</p>
      </div>

      <div class="flex p-1 bg-slate-100 dark:bg-slate-800 rounded-2xl text-xs font-semibold">
        <button id="auth-tab-login" onclick="switchAuthTab('login')" class="flex-1 py-2 rounded-xl bg-white dark:bg-slate-700 text-slate-900 dark:text-white shadow-sm transition">เข้าสู่ระบบ</button>
        <button id="auth-tab-reg" onclick="switchAuthTab('register')" class="flex-1 py-2 rounded-xl text-slate-500 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white transition">สมัครสมาชิก</button>
      </div>

      <div id="form-login" class="space-y-4">
        <div>
          <label class="text-xs font-semibold text-slate-700 dark:text-slate-300 block mb-1">ชื่อผู้ใช้งาน</label>
          <input type="text" id="login-user" placeholder="Username" class="w-full border border-slate-200 dark:border-slate-700 rounded-xl px-3.5 py-2.5 text-sm bg-white dark:bg-slate-800 text-slate-900 dark:text-white outline-none focus:border-indigo-500">
        </div>
        <div>
          <label class="text-xs font-semibold text-slate-700 dark:text-slate-300 block mb-1">รหัสผ่าน</label>
          <input type="password" id="login-pass" placeholder="••••••••" class="w-full border border-slate-200 dark:border-slate-700 rounded-xl px-3.5 py-2.5 text-sm bg-white dark:bg-slate-800 text-slate-900 dark:text-white outline-none focus:border-indigo-500">
        </div>
        <button onclick="handleLogin()" class="w-full bg-indigo-600 hover:bg-indigo-700 text-white font-semibold py-3 rounded-xl text-sm shadow-md transition">เข้าสู่ระบบ</button>
      </div>

      <div id="form-register" class="space-y-4 hidden">
        <div class="bg-indigo-50 dark:bg-indigo-950 text-indigo-700 dark:text-indigo-400 p-2.5 rounded-xl text-xs text-center font-medium">สมัครสมาชิกในฐานะ: ลูกค้าทั่วไป (Customer)</div>
        <div>
          <label class="text-xs font-semibold text-slate-700 dark:text-slate-300 block mb-1">ชื่อ-นามสกุล</label>
          <input type="text" id="reg-name" placeholder="ชื่อจริงของคุณ" class="w-full border border-slate-200 dark:border-slate-700 rounded-xl px-3.5 py-2.5 text-sm bg-white dark:bg-slate-800 text-slate-900 dark:text-white outline-none focus:border-indigo-500">
        </div>
        <div>
          <label class="text-xs font-semibold text-slate-700 dark:text-slate-300 block mb-1">ชื่อผู้ใช้งาน</label>
          <input type="text" id="reg-user" placeholder="Username (ภาษาอังกฤษและตัวเลข)" class="w-full border border-slate-200 dark:border-slate-700 rounded-xl px-3.5 py-2.5 text-sm bg-white dark:bg-slate-800 text-slate-900 dark:text-white outline-none focus:border-indigo-500">
        </div>
        <div>
          <label class="text-xs font-semibold text-slate-700 dark:text-slate-300 block mb-1">รหัสผ่าน</label>
          <input type="password" id="reg-pass" placeholder="อย่างน้อย 6 ตัวอักษร" class="w-full border border-slate-200 dark:border-slate-700 rounded-xl px-3.5 py-2.5 text-sm bg-white dark:bg-slate-800 text-slate-900 dark:text-white outline-none focus:border-indigo-500">
        </div>
        <button onclick="handleRegister()" class="w-full bg-emerald-600 hover:bg-emerald-700 text-white font-semibold py-3 rounded-xl text-sm shadow-md transition">ลงทะเบียนลูกค้า</button>
      </div>
    </div>
  </div>

  <!-- Workspace -->
  <div id="main-app" class="flex-1 flex overflow-hidden hidden">
    <aside class="w-64 bg-slate-900 text-slate-300 flex flex-col justify-between p-4 shadow-2xl z-20 border-r border-slate-800">
      <div>
        <div class="flex items-center gap-3 p-3 bg-slate-800/80 rounded-2xl border border-slate-700/60 mb-6">
          <div id="user-avatar" class="w-10 h-10 rounded-xl bg-indigo-600 flex items-center justify-center font-bold text-white text-base">U</div>
          <div class="flex-1 min-w-0">
            <h4 id="user-display-name" class="font-bold text-sm truncate text-white">ผู้ใช้งาน</h4>
            <span id="user-role-badge" class="inline-block text-[9px] font-extrabold uppercase px-2 py-0.5 rounded-md bg-indigo-500/20 text-indigo-400">ADMIN</span>
          </div>
        </div>
        <nav class="space-y-1 text-sm font-medium">
          <button onclick="switchTab('menu')" id="nav-menu" class="w-full text-left py-2.5 px-3 rounded-xl hover:bg-slate-800 flex items-center gap-3 transition"><i class="fa-solid fa-book-open w-5 text-amber-400"></i> เมนูอาหาร</button>
          <button onclick="switchTab('dash')" id="nav-dash" class="w-full text-left py-2.5 px-3 rounded-xl hover:bg-slate-800 flex items-center gap-3 transition"><i class="fa-solid fa-chart-pie w-5 text-indigo-400"></i> Dashboard สรุป</button>
          <button onclick="switchTab('customers')" id="nav-customers" class="w-full text-left py-2.5 px-3 rounded-xl hover:bg-slate-800 flex items-center gap-3 transition"><i class="fa-solid fa-users-gear w-5 text-pink-400"></i> สมาชิก & กิจกรรม</button>
          <button onclick="switchTab('tables')" id="nav-tables" class="w-full text-left py-2.5 px-3 rounded-xl hover:bg-slate-800 flex items-center gap-3 transition"><i class="fa-solid fa-chair w-5 text-emerald-400"></i> แผนผังโต๊ะ & POS</button>
          <button onclick="switchTab('kitchen')" id="nav-kitchen" class="w-full text-left py-2.5 px-3 rounded-xl hover:bg-slate-800 flex items-center gap-3 transition"><i class="fa-solid fa-fire-burner w-5 text-rose-400"></i> จอครัว (KDS)</button>
          <button onclick="switchTab('checkout')" id="nav-checkout" class="w-full text-left py-2.5 px-3 rounded-xl hover:bg-slate-800 flex items-center gap-3 transition"><i class="fa-solid fa-receipt w-5 text-blue-400"></i> เช็คบิล / ใบเสร็จ</button>
          <button onclick="switchTab('inventory')" id="nav-inventory" class="w-full text-left py-2.5 px-3 rounded-xl hover:bg-slate-800 flex items-center gap-3 transition"><i class="fa-solid fa-boxes-stacked w-5 text-purple-400"></i> สต็อกวัตถุดิบ</button>
          <button onclick="switchTab('queue')" id="nav-queue" class="w-full text-left py-2.5 px-3 rounded-xl hover:bg-slate-800 flex items-center gap-3 transition"><i class="fa-solid fa-users-line w-5 text-teal-400"></i> คิว & จองโต๊ะ</button>
          <button onclick="switchTab('logs')" id="nav-logs" class="w-full text-left py-2.5 px-3 rounded-xl hover:bg-slate-800 flex items-center gap-3 transition"><i class="fa-solid fa-clock-rotate-left w-5 text-slate-400"></i> Audit Logs</button>
          <button onclick="switchTab('qr')" id="nav-qr" class="w-full text-left py-2.5 px-3 rounded-xl hover:bg-slate-800 flex items-center gap-3 transition"><i class="fa-solid fa-mobile-screen w-5 text-sky-400"></i> ลูกค้าสั่งเอง (QR)</button>
        </nav>
      </div>
      <div class="space-y-3 pt-4 border-t border-slate-800">
        <div class="flex items-center justify-between text-xs px-1 text-slate-400">
          <span class="flex items-center gap-2 text-emerald-400 font-semibold"><span class="w-2 h-2 rounded-full bg-emerald-400 animate-ping"></span> Live Real-time</span>
          <button onclick="toggleDarkMode()" class="p-1.5 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-300 transition" title="สลับโหมด มืด/สว่าง">
            <i id="theme-icon" class="fa-solid fa-moon"></i>
          </button>
        </div>
        <button onclick="handleLogout()" class="w-full bg-rose-500/10 hover:bg-rose-500/20 text-rose-400 border border-rose-500/20 py-2.5 rounded-xl text-xs font-semibold transition flex items-center justify-center gap-2"><i class="fa-solid fa-right-from-bracket"></i> ออกจากระบบ</button>
      </div>
    </aside>

    <main class="flex-1 overflow-y-auto p-8 custom-scroll">
      <!-- 0. Menu -->
      <section id="pane-menu" class="space-y-6 hidden">
        <div class="flex flex-col md:flex-row justify-between items-start md:items-center gap-4">
          <div>
            <h2 class="text-2xl font-extrabold text-slate-900 dark:text-white tracking-tight">รายการอาหารและเครื่องดื่ม</h2>
            <p class="text-xs text-slate-500 dark:text-slate-400 mt-0.5">เลือกดูเมนูทั้งหมด พร้อมสถานะและสูตรการตัดสต็อก</p>
          </div>
          <div id="menu-staff-controls" class="hidden">
            <button onclick="openAddMenuModal()" class="bg-indigo-600 hover:bg-indigo-700 text-white text-xs px-4 py-2.5 rounded-xl font-semibold shadow-md transition flex items-center gap-2"><i class="fa-solid fa-plus"></i> เพิ่มเมนูใหม่</button>
          </div>
        </div>
        <div class="bg-white dark:bg-slate-900 p-3.5 rounded-2xl border border-slate-200/70 dark:border-slate-800 shadow-sm flex flex-col md:flex-row gap-3">
          <div class="flex-1 relative">
            <i class="fa-solid fa-magnifying-glass absolute left-3.5 top-3 text-slate-400 text-xs"></i>
            <input type="text" id="menu-search-input" oninput="loadCatalogMenus()" placeholder="ค้นหาเมนูตามชื่อ..." class="w-full pl-9 pr-3 py-2 border border-slate-200 dark:border-slate-700 rounded-xl text-xs bg-white dark:bg-slate-800 text-slate-900 dark:text-white outline-none focus:border-indigo-500 transition">
          </div>
          <div class="w-full md:w-60">
            <select id="menu-cat-filter" onchange="loadCatalogMenus()" class="w-full border border-slate-200 dark:border-slate-700 rounded-xl py-2 px-3 text-xs bg-white dark:bg-slate-800 outline-none font-semibold text-slate-700 dark:text-slate-200 focus:border-indigo-500"></select>
          </div>
        </div>
        <div id="menu-catalog-grid" class="grid grid-cols-1 md:grid-cols-3 lg:grid-cols-4 gap-5"></div>
      </section>

      <!-- 1. Dashboard -->
      <section id="pane-dash" class="space-y-6 hidden">
        <div class="flex justify-between items-center">
          <div>
            <h2 class="text-2xl font-extrabold text-slate-900 dark:text-white tracking-tight">ภาพรวมยอดขาย & สถิติร้าน</h2>
            <p class="text-xs text-slate-500 dark:text-slate-400 mt-0.5">สรุปรายได้ สถานะโต๊ะ และการแจ้งเตือนสต็อกแบบ Real-time</p>
          </div>
          <button onclick="loadDashboard()" class="bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 px-3 py-1.5 rounded-xl text-xs font-semibold text-slate-700 dark:text-slate-200 shadow-sm transition"><i class="fa-solid fa-rotate mr-1 text-slate-400"></i> อัปเดตข้อมูล</button>
        </div>
        <div class="grid grid-cols-1 md:grid-cols-4 gap-4">
          <div class="bg-white dark:bg-slate-900 p-5 rounded-2xl border border-slate-200/70 dark:border-slate-800 shadow-sm flex items-center justify-between">
            <div>
              <p class="text-[11px] text-slate-400 font-semibold uppercase tracking-wider">ยอดขายสุทธิทั้งหมด</p>
              <h3 id="dash-rev" class="text-2xl font-extrabold text-indigo-600 dark:text-indigo-400 mt-1">0.00 ฿</h3>
            </div>
            <div class="w-12 h-12 rounded-2xl bg-indigo-50 dark:bg-indigo-950/60 text-indigo-600 dark:text-indigo-400 flex items-center justify-center text-xl"><i class="fa-solid fa-wallet"></i></div>
          </div>
          <div class="bg-white dark:bg-slate-900 p-5 rounded-2xl border border-slate-200/70 dark:border-slate-800 shadow-sm flex items-center justify-between">
            <div>
              <p class="text-[11px] text-slate-400 font-semibold uppercase tracking-wider">บิลสำเร็จแล้ว</p>
              <h3 id="dash-bills" class="text-2xl font-extrabold text-emerald-600 dark:text-emerald-400 mt-1">0 บิล</h3>
            </div>
            <div class="w-12 h-12 rounded-2xl bg-emerald-50 dark:bg-emerald-950/60 text-emerald-600 dark:text-emerald-400 flex items-center justify-center text-xl"><i class="fa-solid fa-receipt"></i></div>
          </div>
          <div class="bg-white dark:bg-slate-900 p-5 rounded-2xl border border-slate-200/70 dark:border-slate-800 shadow-sm flex items-center justify-between">
            <div>
              <p class="text-[11px] text-slate-400 font-semibold uppercase tracking-wider">โต๊ะที่เปิดอยู่</p>
              <h3 id="dash-tables" class="text-2xl font-extrabold text-amber-500 mt-1">0 / 0</h3>
            </div>
            <div class="w-12 h-12 rounded-2xl bg-amber-50 dark:bg-amber-950/60 text-amber-500 flex items-center justify-center text-xl"><i class="fa-solid fa-chair"></i></div>
          </div>
          <div class="bg-white dark:bg-slate-900 p-5 rounded-2xl border border-slate-200/70 dark:border-slate-800 shadow-sm flex items-center justify-between">
            <div>
              <p class="text-[11px] text-slate-400 font-semibold uppercase tracking-wider">วัตถุดิบใกล้หมด</p>
              <h3 id="dash-stock" class="text-2xl font-extrabold text-rose-500 mt-1">0 รายการ</h3>
            </div>
            <div class="w-12 h-12 rounded-2xl bg-rose-50 dark:bg-rose-950/60 text-rose-500 flex items-center justify-center text-xl"><i class="fa-solid fa-boxes-stacked"></i></div>
          </div>
        </div>
        <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
          <div class="bg-white dark:bg-slate-900 p-6 rounded-2xl border border-slate-200/70 dark:border-slate-800 shadow-sm">
            <h3 class="font-bold text-sm text-slate-800 dark:text-slate-200 mb-4 flex items-center gap-2"><i class="fa-solid fa-trophy text-amber-400"></i> 5 อันดับเมนูขายดี</h3>
            <div id="dash-top-sellers" class="space-y-2"></div>
          </div>
          <div class="bg-white dark:bg-slate-900 p-6 rounded-2xl border border-slate-200/70 dark:border-slate-800 shadow-sm">
            <h3 class="font-bold text-sm text-slate-800 dark:text-slate-200 mb-4 flex items-center gap-2"><i class="fa-solid fa-triangle-exclamation text-rose-500"></i> แจ้งเตือนวัตถุดิบถึงจุดต่ำสุด</h3>
            <div id="dash-low-stock-list" class="space-y-2"></div>
          </div>
        </div>
      </section>

      <!-- 1.5 Admin Customer Activity Tracker -->
      <section id="pane-customers" class="space-y-6 hidden">
        <div class="flex justify-between items-center">
          <div>
            <h2 class="text-2xl font-extrabold text-slate-900 dark:text-white tracking-tight">สถิติสมาชิก & พฤติกรรมลูกค้า (Live Tracker)</h2>
            <p class="text-xs text-slate-500 dark:text-slate-400 mt-0.5">ดูรายชื่อลูกค้าและกิจกรรมในเว็บแบบ Real-time</p>
          </div>
          <button onclick="loadAdminCustomers()" class="bg-indigo-600 text-white text-xs px-3.5 py-1.5 rounded-xl font-semibold shadow-sm transition"><i class="fa-solid fa-rotate mr-1"></i> รีเฟรชข้อมูล</button>
        </div>
        <div class="grid grid-cols-1 md:grid-cols-3 gap-6">
          <div class="bg-white dark:bg-slate-900 p-6 rounded-3xl border border-slate-200/80 dark:border-slate-800 shadow-sm space-y-4">
            <div class="flex items-center justify-between">
              <h3 class="font-bold text-sm text-slate-800 dark:text-slate-200">รายชื่อลูกค้าทั้งหมด</h3>
              <span id="cust-count-badge" class="bg-indigo-50 dark:bg-indigo-950/60 text-indigo-600 dark:text-indigo-400 text-xs px-2.5 py-0.5 rounded-full font-bold">0 คน</span>
            </div>
            <div id="admin-customer-list" class="space-y-2 max-h-96 overflow-y-auto pr-1 custom-scroll"></div>
          </div>
          <div class="md:col-span-2 bg-white dark:bg-slate-900 p-6 rounded-3xl border border-slate-200/80 dark:border-slate-800 shadow-sm space-y-4">
            <h3 class="font-bold text-sm text-slate-800 dark:text-slate-200 flex items-center gap-2">
              <span class="w-2.5 h-2.5 rounded-full bg-emerald-500 animate-ping"></span> กิจกรรมล่าสุดของลูกค้าในเว็บ (Real-time Stream)
            </h3>
            <div id="admin-customer-stream" class="space-y-2.5 max-h-96 overflow-y-auto pr-1 custom-scroll"></div>
          </div>
        </div>
      </section>

      <!-- 2. Tables -->
      <section id="pane-tables" class="space-y-6 hidden">
        <div class="flex justify-between items-center">
          <div>
            <h2 class="text-2xl font-extrabold text-slate-900 dark:text-white tracking-tight">แผนผังโต๊ะอาหาร & POS</h2>
            <p class="text-xs text-slate-500 dark:text-slate-400 mt-0.5">เลือกโต๊ะเพื่อสั่งอาหาร ย้ายโต๊ะ หรือรวมโต๊ะ</p>
          </div>
          <div class="flex gap-2">
            <button onclick="openAddTableModal()" class="bg-indigo-600 hover:bg-indigo-700 text-white text-xs px-3.5 py-2 rounded-xl font-semibold shadow-sm transition flex items-center gap-1.5"><i class="fa-solid fa-plus"></i> เพิ่มโต๊ะใหม่</button>
            <button onclick="openMoveModal()" class="bg-amber-500 hover:bg-amber-600 text-white text-xs px-3 py-2 rounded-xl font-semibold shadow-sm transition"><i class="fa-solid fa-arrows-split-up-and-left mr-1"></i> ย้ายโต๊ะ</button>
            <button onclick="openMergeModal()" class="bg-slate-800 dark:bg-slate-700 hover:bg-black text-white text-xs px-3 py-2 rounded-xl font-semibold shadow-sm transition"><i class="fa-solid fa-object-group mr-1"></i> รวมโต๊ะ</button>
          </div>
        </div>
        <div id="tables-grid" class="grid grid-cols-2 md:grid-cols-4 gap-4"></div>
        <div id="table-order-box" class="hidden bg-white dark:bg-slate-900 p-6 rounded-3xl border border-slate-200 dark:border-slate-800 shadow-xl">
          <div class="flex justify-between items-center border-b border-slate-100 dark:border-slate-800 pb-4 mb-4">
            <h3 id="selected-table-title" class="text-lg font-bold text-slate-900 dark:text-white">จัดการรายการอาหาร - โต๊ะ</h3>
            <button onclick="document.getElementById('table-order-box').classList.add('hidden')" class="w-8 h-8 rounded-full hover:bg-slate-100 dark:hover:bg-slate-800 flex items-center justify-center text-slate-400"><i class="fa-solid fa-xmark"></i></button>
          </div>
          <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div>
              <h4 class="font-semibold text-xs text-slate-500 uppercase tracking-wider mb-3">แตะเลือกเมนูเพื่อสั่ง:</h4>
              <div id="pos-menu-list" class="grid grid-cols-2 gap-3 max-h-80 overflow-y-auto pr-1 custom-scroll"></div>
            </div>
            <div class="border-t md:border-t-0 md:border-l border-slate-100 dark:border-slate-800 pt-4 md:pt-0 md:pl-6 flex flex-col justify-between">
              <div>
                <h4 class="font-semibold text-xs text-slate-500 uppercase tracking-wider mb-3">บิลอาหารของโต๊ะนี้:</h4>
                <div id="pos-order-items" class="space-y-2 max-h-64 overflow-y-auto pr-1 custom-scroll"></div>
              </div>
              <div class="border-t border-slate-100 dark:border-slate-800 pt-3 mt-3 flex justify-between items-center">
                <span class="text-xs text-slate-500 font-medium">รวมทั้งสิ้น:</span>
                <span id="pos-total-price" class="text-lg font-extrabold text-indigo-600 dark:text-indigo-400">0.00 ฿</span>
              </div>
            </div>
          </div>
        </div>
      </section>

      <!-- 3. Kitchen -->
      <section id="pane-kitchen" class="space-y-6 hidden">
        <div class="flex justify-between items-center">
          <div>
            <h2 class="text-2xl font-extrabold text-slate-900 dark:text-white tracking-tight">Kitchen Display System (KDS)</h2>
            <p class="text-xs text-slate-500 dark:text-slate-400 mt-0.5">คิวออเดอร์ตัดสต็อกวัตถุดิบอัตโนมัติเมื่อกดเริ่มปรุงอาหาร</p>
          </div>
          <button onclick="loadKitchenOrders()" class="bg-indigo-600 text-white text-xs px-3 py-1.5 rounded-xl font-semibold shadow-sm transition"><i class="fa-solid fa-rotate mr-1"></i> รีเฟรช</button>
        </div>
        <div id="kds-grid" class="grid grid-cols-1 md:grid-cols-3 gap-4"></div>
      </section>

      <!-- 4. QR Customer -->
      <section id="pane-qr" class="space-y-6 hidden">
        <div class="max-w-lg mx-auto bg-white dark:bg-slate-900 p-7 rounded-3xl border border-slate-200/80 dark:border-slate-800 shadow-xl space-y-4">
          <div class="text-center border-b border-slate-100 dark:border-slate-800 pb-4">
            <span class="bg-indigo-50 dark:bg-indigo-950/60 text-indigo-700 dark:text-indigo-400 text-xs px-3 py-1 rounded-full font-semibold">โหมดลูกค้าสั่งอาหารเอง</span>
            <h3 class="text-xl font-extrabold text-slate-900 dark:text-white mt-2">เลือกโต๊ะและปรับแต่งเมนูตามใจชอบ</h3>
          </div>
          <div class="space-y-3.5">
            <div>
              <label class="text-xs font-semibold text-slate-700 dark:text-slate-300 block mb-1">หมายเลขโต๊ะของคุณ</label>
              <select id="qr-table-num" class="w-full border border-slate-200 dark:border-slate-700 rounded-xl p-2.5 text-sm bg-white dark:bg-slate-800 text-slate-900 dark:text-white font-bold outline-none"></select>
            </div>
            <div>
              <label class="text-xs font-semibold text-slate-700 dark:text-slate-300 block mb-1">เลือกเมนูอาหาร</label>
              <select id="qr-select-menu" onchange="updateQrMenuPreview()" class="w-full border border-slate-200 dark:border-slate-700 rounded-xl p-2.5 text-sm bg-white dark:bg-slate-800 text-slate-900 dark:text-white outline-none"></select>
            </div>
            <div id="qr-dish-preview" class="border border-slate-100 dark:border-slate-800 rounded-2xl overflow-hidden bg-slate-50/70 dark:bg-slate-800/50 hidden shadow-sm">
              <img id="qr-dish-img" src="" alt="Menu" class="h-40 w-full object-cover">
              <div class="p-3 flex justify-between items-center text-xs">
                <span id="qr-dish-name" class="font-bold text-slate-900 dark:text-white"></span>
                <span id="qr-dish-price" class="font-extrabold text-indigo-600 dark:text-indigo-400"></span>
              </div>
            </div>
            <div class="grid grid-cols-3 gap-2 pt-1">
              <div>
                <label class="text-[11px] font-semibold text-slate-600 dark:text-slate-400">ความเผ็ด</label>
                <select id="qr-opt-spice" class="w-full border border-slate-200 dark:border-slate-700 p-2 rounded-xl text-xs bg-white dark:bg-slate-800 text-slate-900 dark:text-white outline-none">
                  <option>ไม่เผ็ด</option><option selected>เผ็ดกลาง</option><option>เผ็ดมาก</option>
                </select>
              </div>
              <div>
                <label class="text-[11px] font-semibold text-slate-600 dark:text-slate-400">เพิ่มไข่ (+10฿)</label>
                <select id="qr-opt-egg" class="w-full border border-slate-200 dark:border-slate-700 p-2 rounded-xl text-xs bg-white dark:bg-slate-800 text-slate-900 dark:text-white outline-none">
                  <option>ไม่ใส่</option><option>ไข่ดาว</option><option>ไข่เจียว</option>
                </select>
              </div>
              <div>
                <label class="text-[11px] font-semibold text-slate-600 dark:text-slate-400">ขนาด (+15฿)</label>
                <select id="qr-opt-size" class="w-full border border-slate-200 dark:border-slate-700 p-2 rounded-xl text-xs bg-white dark:bg-slate-800 text-slate-900 dark:text-white outline-none">
                  <option>ธรรมดา</option><option>พิเศษ</option>
                </select>
              </div>
            </div>
            <button onclick="sendCustomerOrder()" class="w-full bg-indigo-600 hover:bg-indigo-700 text-white font-bold py-3.5 rounded-2xl shadow-lg transition"><i class="fa-solid fa-paper-plane mr-1.5"></i> ยืนยันส่งออเดอร์เข้าครัว</button>
          </div>
        </div>
      </section>

      <!-- 5. Checkout: รองรับเงินสด (คำนวณเงินทอน) และโอนเงิน / สแกน QR -->
      <section id="pane-checkout" class="space-y-6 hidden">
        <h2 class="text-2xl font-extrabold text-slate-900 dark:text-white tracking-tight">คิดเงิน ออกใบเสร็จ & ระบบสะสมแต้ม</h2>
        <div class="grid grid-cols-1 md:grid-cols-2 gap-8">
          <div class="bg-white dark:bg-slate-900 p-6 rounded-3xl border border-slate-200/80 dark:border-slate-800 shadow-sm space-y-4">
            <h3 class="font-bold text-base text-slate-800 dark:text-slate-200">คำนวณบิลชำระเงิน</h3>
            <div>
              <label class="text-xs font-semibold text-slate-700 dark:text-slate-300 block mb-1">เลือกโต๊ะที่ต้องการชำระเงิน</label>
              <select id="bill-table-sel" class="w-full border border-slate-200 dark:border-slate-700 p-2.5 rounded-xl text-sm bg-white dark:bg-slate-800 text-slate-900 dark:text-white font-bold outline-none"></select>
            </div>
            <div class="grid grid-cols-2 gap-3">
              <div>
                <label class="text-xs font-semibold text-slate-700 dark:text-slate-300 block mb-1">ส่วนลดพิเศษ (%)</label>
                <input type="number" id="bill-discount" value="0" min="0" max="100" class="w-full border border-slate-200 dark:border-slate-700 p-2.5 rounded-xl text-sm bg-white dark:bg-slate-800 text-slate-900 dark:text-white font-semibold outline-none">
              </div>
              <div>
                <label class="text-xs font-semibold text-slate-700 dark:text-slate-300 block mb-1">เบอร์สมาชิก (สะสมแต้ม)</label>
                <input type="text" id="bill-member" placeholder="08XXXXXXXX" class="w-full border border-slate-200 dark:border-slate-700 p-2.5 rounded-xl text-sm bg-white dark:bg-slate-800 text-slate-900 dark:text-white outline-none">
              </div>
            </div>

            <!-- ตัวเลือกการชำระเงิน: เงินสด vs โอนเงิน -->
            <div class="border-t border-slate-100 dark:border-slate-800 pt-3 space-y-3">
              <label class="text-xs font-semibold text-slate-700 dark:text-slate-300 block">ช่องทางการชำระเงิน</label>
              <div class="grid grid-cols-2 gap-2">
                <button type="button" onclick="setPayMethod('cash')" id="btn-pay-cash" class="py-2.5 rounded-xl font-bold text-xs bg-indigo-600 text-white shadow-sm flex items-center justify-center gap-1.5 transition">
                  <i class="fa-solid fa-money-bill-wave"></i> เงินสด
                </button>
                <button type="button" onclick="setPayMethod('transfer')" id="btn-pay-transfer" class="py-2.5 rounded-xl font-bold text-xs bg-slate-100 dark:bg-slate-800 text-slate-700 dark:text-slate-300 flex items-center justify-center gap-1.5 transition">
                  <i class="fa-solid fa-qrcode"></i> โอนเงิน / QR Code
                </button>
              </div>

              <!-- ถ้าเลือกเงินสด: มีช่องกรอกรับเงิน และเงินทอน -->
              <div id="cash-input-box" class="space-y-2 p-3 bg-slate-50 dark:bg-slate-800/60 rounded-2xl border border-slate-200/60 dark:border-slate-700">
                <div class="flex justify-between items-center text-xs">
                  <label class="font-semibold text-slate-700 dark:text-slate-300">รับเงินมา (บาท):</label>
                  <input type="number" id="cash-received-input" oninput="calculateChange()" placeholder="0.00" class="w-32 border border-slate-300 dark:border-slate-600 rounded-xl px-2.5 py-1.5 text-right font-bold text-indigo-600 dark:text-indigo-400 bg-white dark:bg-slate-800 outline-none">
                </div>
                <div class="flex justify-between items-center text-xs pt-1 border-t border-slate-200 dark:border-slate-700">
                  <span class="text-slate-500 dark:text-slate-400">เงินทอน:</span>
                  <span id="cash-change-display" class="font-extrabold text-emerald-600 dark:text-emerald-400 text-sm">0.00 ฿</span>
                </div>
              </div>

              <!-- ถ้าเลือกโอนเงิน: แสดงสถานะสแกนจ่าย -->
              <div id="transfer-input-box" class="hidden p-3 bg-indigo-50 dark:bg-indigo-950/60 rounded-2xl border border-indigo-100 dark:border-indigo-900 text-center text-xs text-indigo-700 dark:text-indigo-300">
                <i class="fa-solid fa-mobile-screen-button text-base mb-1 block"></i>
                ลูกค้าสแกน QR Code โอนเงินผ่าน Mobile Banking สำเร็จ
              </div>
            </div>

            <button onclick="executeCheckout()" class="w-full bg-emerald-600 hover:bg-emerald-700 text-white font-bold py-3.5 rounded-2xl shadow-md transition mt-2">
              <i class="fa-solid fa-check-double mr-1.5"></i> ชำระเงินและออกใบเสร็จ
            </button>
          </div>

          <div id="printable-receipt" class="bg-white dark:bg-slate-900 p-6 rounded-3xl border-2 border-dashed border-slate-300 dark:border-slate-700 font-mono text-xs max-w-sm mx-auto shadow-sm">
            <div class="text-center pb-3 border-b border-dashed border-slate-300 dark:border-slate-700">
              <h4 class="font-extrabold text-sm tracking-widest text-slate-900 dark:text-white">RESTRO PRO RECEIPT</h4>
              <p id="rc-id" class="text-[10px] text-slate-400 mt-0.5">REC-XXXXXXXX</p>
              <p id="rc-date" class="text-[10px] text-slate-400"></p>
              <p id="rc-table" class="text-xs font-bold mt-1 text-slate-700 dark:text-slate-300">โต๊ะ: -</p>
            </div>
            <div id="rc-items" class="py-3 border-b border-dashed border-slate-300 dark:border-slate-700 space-y-1 text-xs"></div>
            <div class="py-2.5 space-y-1.5 text-xs">
              <div class="flex justify-between"><span>ยอดรวม:</span><span id="rc-sub">0.00 ฿</span></div>
              <div class="flex justify-between"><span>ส่วนลด:</span><span id="rc-disc">0.00 ฿</span></div>
              <div class="flex justify-between"><span>ค่าบริการ (10%):</span><span id="rc-sc">0.00 ฿</span></div>
              <div class="flex justify-between"><span>ภาษี (7%):</span><span id="rc-vat">0.00 ฿</span></div>
              <div class="flex justify-between font-extrabold text-sm text-slate-900 dark:text-white pt-2 border-t border-slate-200 dark:border-slate-700"><span>ยอดสุทธิ:</span><span id="rc-net">0.00 ฿</span></div>
              
              <div id="rc-payment-method-box" class="text-indigo-600 dark:text-indigo-400 font-bold mt-2 pt-1 border-t border-slate-100 dark:border-slate-800 text-center"></div>
              <div id="rc-point-box" class="text-emerald-600 text-center font-bold mt-1"></div>
            </div>
            <button onclick="window.print()" class="w-full mt-4 bg-slate-900 dark:bg-slate-800 text-white py-2.5 rounded-xl text-xs font-sans hover:bg-black font-semibold transition">🖨️ พิมพ์ใบเสร็จ</button>
          </div>
        </div>
      </section>

      <!-- 6. Inventory -->
      <section id="pane-inventory" class="space-y-6 hidden">
        <div class="flex justify-between items-center">
          <div>
            <h2 class="text-2xl font-extrabold text-slate-900 dark:text-white tracking-tight">คลังวัตถุดิบ & สต็อกตัดอัตโนมัติ</h2>
            <p class="text-xs text-slate-500 dark:text-slate-400 mt-0.5">วัตถุดิบถูกตัดสต็อกตามสูตรทันทีเมื่อห้องครัวเริ่มปรุงอาหาร</p>
          </div>
          <div class="flex gap-2">
            <button onclick="openAddInventoryModal()" class="bg-indigo-600 text-white text-xs px-4 py-2 rounded-xl font-semibold shadow-sm hover:bg-indigo-700 transition flex items-center gap-1.5"><i class="fa-solid fa-plus"></i> เพิ่มวัตถุดิบใหม่</button>
            <button onclick="openRestockPrompt()" class="bg-slate-800 dark:bg-slate-700 text-white text-xs px-3.5 py-2 rounded-xl font-semibold shadow-sm transition"><i class="fa-solid fa-pen-to-square mr-1"></i> ปรับสต็อก</button>
          </div>
        </div>
        <div class="bg-white dark:bg-slate-900 rounded-3xl border border-slate-200/80 dark:border-slate-800 shadow-sm overflow-hidden">
          <table class="w-full text-left text-xs">
            <thead class="bg-slate-50/80 dark:bg-slate-800 text-slate-500 dark:text-slate-400 font-semibold uppercase tracking-wider border-b border-slate-100 dark:border-slate-800">
              <tr>
                <th class="p-3.5">รหัส</th>
                <th class="p-3.5">ชื่อวัตถุดิบ</th>
                <th class="p-3.5">คงเหลือ</th>
                <th class="p-3.5">หน่วย</th>
                <th class="p-3.5">เตือนขั้นต่ำ</th>
                <th class="p-3.5">สถานะ</th>
              </tr>
            </thead>
            <tbody id="inventory-table-body" class="divide-y divide-slate-100 dark:divide-slate-800"></tbody>
          </table>
        </div>
      </section>

      <!-- 7. Queue -->
      <section id="pane-queue" class="space-y-6 hidden">
        <h2 class="text-2xl font-extrabold text-slate-900 dark:text-white tracking-tight">บัตรคิวหน้าร้าน & จองโต๊ะล่วงหน้า</h2>
        <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
          <div class="bg-white dark:bg-slate-900 p-6 rounded-3xl border border-slate-200/80 dark:border-slate-800 shadow-sm space-y-4">
            <h3 class="font-bold text-sm text-slate-800 dark:text-slate-200">ออกบัตรคิวหน้าร้าน</h3>
            <div class="space-y-3">
              <input type="text" id="q-name" placeholder="ชื่อลูกค้า" class="w-full border border-slate-200 dark:border-slate-700 p-2.5 rounded-xl text-xs bg-white dark:bg-slate-800 text-slate-900 dark:text-white outline-none">
              <input type="number" id="q-size" placeholder="จำนวนคน" min="1" class="w-full border border-slate-200 dark:border-slate-700 p-2.5 rounded-xl text-xs bg-white dark:bg-slate-800 text-slate-900 dark:text-white outline-none">
              <button onclick="createQueueTicket()" class="w-full bg-indigo-600 hover:bg-indigo-700 text-white py-2.5 rounded-xl text-xs font-semibold shadow-sm transition">ออกบัตรคิว</button>
            </div>
            <div class="mt-4">
              <h4 class="font-semibold text-xs text-slate-400 uppercase tracking-wider mb-2">คิวที่รออยู่:</h4>
              <div id="queue-list" class="space-y-2"></div>
            </div>
          </div>
          <div class="bg-white dark:bg-slate-900 p-6 rounded-3xl border border-slate-200/80 dark:border-slate-800 shadow-sm space-y-4">
            <h3 class="font-bold text-sm text-slate-800 dark:text-slate-200">บันทึกการจองโต๊ะล่วงหน้า</h3>
            <div class="space-y-3">
              <input type="text" id="res-name" placeholder="ชื่อผู้จอง" class="w-full border border-slate-200 dark:border-slate-700 p-2.5 rounded-xl text-xs bg-white dark:bg-slate-800 text-slate-900 dark:text-white outline-none">
              <input type="text" id="res-phone" placeholder="เบอร์โทรศัพท์" class="w-full border border-slate-200 dark:border-slate-700 p-2.5 rounded-xl text-xs bg-white dark:bg-slate-800 text-slate-900 dark:text-white outline-none">
              <div class="grid grid-cols-2 gap-2">
                <input type="date" id="res-date" class="border border-slate-200 dark:border-slate-700 p-2.5 rounded-xl text-xs bg-white dark:bg-slate-800 text-slate-900 dark:text-white outline-none">
                <input type="time" id="res-time" class="border border-slate-200 dark:border-slate-700 p-2.5 rounded-xl text-xs bg-white dark:bg-slate-800 text-slate-900 dark:text-white outline-none">
              </div>
              <input type="number" id="res-size" placeholder="จำนวนที่นั่ง" min="1" class="w-full border border-slate-200 dark:border-slate-700 p-2.5 rounded-xl text-xs bg-white dark:bg-slate-800 text-slate-900 dark:text-white outline-none">
              <button onclick="createReservation()" class="w-full bg-slate-900 dark:bg-slate-800 hover:bg-black text-white py-2.5 rounded-xl text-xs font-semibold shadow-sm transition">บันทึกการจอง</button>
            </div>
          </div>
        </div>
      </section>

      <!-- 8. Logs -->
      <section id="pane-logs" class="space-y-6 hidden">
        <h2 class="text-2xl font-extrabold text-slate-900 dark:text-white tracking-tight">Audit Logs (ประวัติการแก้ไขข้อมูลสำคัญ)</h2>
        <div class="bg-white dark:bg-slate-900 rounded-3xl border border-slate-200/80 dark:border-slate-800 shadow-sm overflow-hidden">
          <table class="w-full text-left text-xs">
            <thead class="bg-slate-50/80 dark:bg-slate-800 text-slate-500 dark:text-slate-400 font-semibold uppercase tracking-wider border-b border-slate-100 dark:border-slate-800">
              <tr>
                <th class="p-3.5">วัน-เวลา</th>
                <th class="p-3.5">ผู้ใช้งาน</th>
                <th class="p-3.5">กิจกรรม</th>
                <th class="p-3.5">รายละเอียด</th>
              </tr>
            </thead>
            <tbody id="logs-table-body" class="divide-y divide-slate-100 dark:divide-slate-800"></tbody>
          </table>
        </div>
      </section>
    </main>
  </div>

  <script>
    let currentUser = null;
    let currentTable = null;
    let cachedMenu = [];
    let cachedInventory = [];
    let uploadedImageBase64 = "";
    let currentPaymentMethod = "cash";

    function setPayMethod(method) {
      currentPaymentMethod = method;
      const btnCash = document.getElementById('btn-pay-cash');
      const btnTransfer = document.getElementById('btn-pay-transfer');
      const cashBox = document.getElementById('cash-input-box');
      const transferBox = document.getElementById('transfer-input-box');

      if (method === 'cash') {
        btnCash.className = 'py-2.5 rounded-xl font-bold text-xs bg-indigo-600 text-white shadow-sm flex items-center justify-center gap-1.5 transition';
        btnTransfer.className = 'py-2.5 rounded-xl font-bold text-xs bg-slate-100 dark:bg-slate-800 text-slate-700 dark:text-slate-300 flex items-center justify-center gap-1.5 transition';
        cashBox.classList.remove('hidden');
        transferBox.classList.add('hidden');
      } else {
        btnTransfer.className = 'py-2.5 rounded-xl font-bold text-xs bg-indigo-600 text-white shadow-sm flex items-center justify-center gap-1.5 transition';
        btnCash.className = 'py-2.5 rounded-xl font-bold text-xs bg-slate-100 dark:bg-slate-800 text-slate-700 dark:text-slate-300 flex items-center justify-center gap-1.5 transition';
        cashBox.classList.add('hidden');
        transferBox.classList.remove('hidden');
      }
    }

    function calculateChange() {
      const cashIn = parseFloat(document.getElementById('cash-received-input').value) || 0;
      const netText = document.getElementById('rc-net').innerText.replace(' ฿', '');
      const net = parseFloat(netText) || 0;
      const change = Math.max(0, cashIn - net);
      document.getElementById('cash-change-display').innerText = change.toFixed(2) + ' ฿';
    }

    function updateThemeIcons(isDark) {
      const iconClass = isDark ? 'fa-solid fa-sun text-amber-400' : 'fa-solid fa-moon text-slate-400';
      const i1 = document.getElementById('theme-icon');
      const i2 = document.getElementById('auth-theme-icon');
      if (i1) i1.className = iconClass;
      if (i2) i2.className = iconClass;
    }

    function toggleDarkMode() {
      const isDark = document.documentElement.classList.toggle('dark');
      localStorage.setItem('theme', isDark ? 'dark' : 'light');
      updateThemeIcons(isDark);
    }

    if (localStorage.getItem('theme') === 'dark') {
      document.documentElement.classList.add('dark');
    }
    window.addEventListener('DOMContentLoaded', () => {
      updateThemeIcons(document.documentElement.classList.contains('dark'));
    });

    async function apiFetch(url, options = {}) {
      options.headers = options.headers || {};
      if (currentUser && currentUser.token) {
        options.headers['X-Auth-Token'] = currentUser.token;
      }
      const res = await fetch(url, options);
      if (res.status === 401) {
        showToast('เซสชันหมดอายุ กรุณาเข้าสู่ระบบใหม่', 'error');
        handleLogout();
        throw new Error('Unauthorized');
      }
      return res;
    }

    function showToast(msg, type = 'info') {
      const box = document.getElementById('toast-container');
      const toast = document.createElement('div');
      const icons = {
        success: 'fa-circle-check text-emerald-400',
        error: 'fa-circle-xmark text-rose-400',
        info: 'fa-circle-info text-indigo-400'
      };
      toast.className = `bg-slate-900 text-white border border-slate-800 px-4 py-3 rounded-2xl text-xs font-semibold shadow-2xl flex items-center gap-2.5 transform transition-all duration-300 translate-y-2 opacity-0 pointer-events-auto`;
      toast.innerHTML = `<i class="fa-solid ${icons[type] || icons.info} text-sm"></i> <span>${msg}</span>`;
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
        document.getElementById('auth-tab-login').className = 'flex-1 py-2 rounded-xl bg-white dark:bg-slate-700 text-slate-900 dark:text-white shadow-sm transition';
        document.getElementById('auth-tab-reg').className = 'flex-1 py-2 rounded-xl text-slate-500 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white transition';
      } else {
        document.getElementById('form-login').classList.add('hidden');
        document.getElementById('form-register').classList.remove('hidden');
        document.getElementById('auth-tab-reg').className = 'flex-1 py-2 rounded-xl bg-white dark:bg-slate-700 text-slate-900 dark:text-white shadow-sm transition';
        document.getElementById('auth-tab-login').className = 'flex-1 py-2 rounded-xl text-slate-500 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white transition';
      }
    }

    function validateAuthInputs(u, p) {
      const userRegex = /^[a-zA-Z0-9]+$/;
      if (!userRegex.test(u)) {
        showToast('ชื่อผู้ใช้ต้องเป็นภาษาอังกฤษและตัวเลขเท่านั้น (ห้ามใช้ภาษาไทย)', 'error');
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

      try {
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
          showToast(data.detail || 'ชื่อผู้ใช้หรือรหัสผ่านไม่ถูกต้อง', 'error');
        }
      } catch (err) {
        showToast('เกิดข้อผิดพลาดในการเชื่อมต่อ', 'error');
      }
    }

    async function handleRegister() {
      const name = document.getElementById('reg-name').value.trim();
      const u = document.getElementById('reg-user').value.trim();
      const p = document.getElementById('reg-pass').value.trim();

      if(!name || !u || !p) return showToast('กรุณากรอกข้อมูลให้ครบถ้วน', 'error');
      if(!validateAuthInputs(u, p)) return;

      try {
        const res = await fetch('/api/auth/register', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({ name: name, username: u, password: p })
        });
        const data = await res.json();
        if(res.ok) {
          showToast('ลงทะเบียนลูกค้าสำเร็จ! กรุณาเข้าสู่ระบบด้วยชื่อที่สมัคร', 'success');
          switchAuthTab('login');
          document.getElementById('login-user').value = u;
          document.getElementById('login-pass').value = '';
        } else {
          showToast(data.detail || 'เกิดข้อผิดพลาดในการลงทะเบียน', 'error');
        }
      } catch (err) {
        showToast('เกิดข้อผิดพลาดในการเชื่อมต่อ', 'error');
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
      ['dash', 'tables', 'kitchen', 'checkout', 'inventory', 'queue', 'logs', 'qr', 'menu', 'customers'].forEach(t => {
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
        ['menu', 'dash', 'customers', 'tables', 'kitchen', 'checkout', 'inventory', 'queue', 'logs', 'qr'].forEach(t => {
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
      document.getElementById('login-pass').value = '';
      showToast('ออกจากระบบเรียบร้อย', 'info');
    }

    function switchTab(name) {
      document.querySelectorAll('main > section').forEach(s => s.classList.add('hidden'));
      document.querySelectorAll('aside nav button').forEach(b => {
        b.classList.remove('bg-indigo-600', 'text-white', 'shadow-md');
        b.classList.add('text-slate-300');
      });
      const targetPane = document.getElementById('pane-' + name);
      const targetNav = document.getElementById('nav-' + name);
      if(targetPane) targetPane.classList.remove('hidden');
      if(targetNav) {
        targetNav.classList.remove('text-slate-300');
        targetNav.classList.add('bg-indigo-600', 'text-white', 'shadow-md');
      }

      if(name === 'menu') loadCatalogMenus();
      if(name === 'dash') loadDashboard();
      if(name === 'customers') loadAdminCustomers();
      if(name === 'tables') loadTables();
      if(name === 'kitchen') loadKitchenOrders();
      if(name === 'inventory') loadInventory();
      if(name === 'queue') loadQueues();
      if(name === 'logs') loadLogs();
      if(name === 'checkout') loadCheckoutTables();
      if(name === 'qr') loadQrMenu();
    }

    // Image Handlers
    function setImgMode(mode) {
      if(mode === 'url') {
        document.getElementById('box-img-url').classList.remove('hidden');
        document.getElementById('box-img-upload').classList.add('hidden');
        document.getElementById('btn-mode-url').className = 'flex-1 py-1.5 rounded-lg font-semibold bg-white dark:bg-slate-800 text-slate-800 dark:text-white shadow-sm transition';
        document.getElementById('btn-mode-upload').className = 'flex-1 py-1.5 rounded-lg font-semibold text-slate-600 dark:text-slate-300 transition';
        uploadedImageBase64 = "";
        previewImage();
      } else {
        document.getElementById('box-img-url').classList.add('hidden');
        document.getElementById('box-img-upload').classList.remove('hidden');
        document.getElementById('btn-mode-upload').className = 'flex-1 py-1.5 rounded-lg font-semibold bg-white dark:bg-slate-800 text-slate-800 dark:text-white shadow-sm transition';
        document.getElementById('btn-mode-url').className = 'flex-1 py-1.5 rounded-lg font-semibold text-slate-600 dark:text-slate-300 transition';
        document.getElementById('new-menu-img-url').value = "";
        previewImage();
      }
    }

    function handleFileUpload(event) {
      const file = event.target.files[0];
      if (file) {
        if (!file.type.startsWith('image/')) {
          showToast('กรุณาเลือกไฟล์รูปภาพเท่านั้น (PNG, JPG, WEBP)', 'error');
          event.target.value = "";
          return;
        }
        if (file.size > 2 * 1024 * 1024) {
          showToast('ไฟล์รูปภาพต้องมีขนาดไม่เกิน 2MB', 'error');
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
      } else if (url && (url.startsWith('http://') || url.startsWith('https://'))) {
        preview.src = url;
        box.classList.remove('hidden');
      } else {
        preview.src = "";
        box.classList.add('hidden');
      }
    }

    // ================= Menu Logic (เพิ่ม, แก้ไข, ลบ) =================
    async function loadCatalogMenus() {
      const search = document.getElementById('menu-search-input').value.trim();
      const cat = document.getElementById('menu-cat-filter').value;
      const res = await apiFetch(`/api/menu?search=${encodeURIComponent(search)}&category=${encodeURIComponent(cat)}&limit=100`);
      const data = await res.json();
      cachedMenu = data.items;

      const catFilter = document.getElementById('menu-cat-filter');
      const curCat = catFilter.value;
      catFilter.innerHTML = '<option value="">ทุกหมวดหมู่</option>' + data.categories.map(c => `<option value="${c}" ${c === curCat ? 'selected' : ''}>${c}</option>`).join('');

      const invRes = await apiFetch('/api/inventory');
      cachedInventory = await invRes.json();

      const grid = document.getElementById('menu-catalog-grid');
      const isStaffOrAdmin = currentUser && (currentUser.role === 'admin' || currentUser.role === 'staff');

      grid.innerHTML = data.items.map(m => {
        let recipeTxt = (m.recipe && m.recipe.length > 0) ? m.recipe.map(r => {
          const invItem = cachedInventory.find(i => i.id === r.ingredient_id);
          return `${invItem ? invItem.name : r.ingredient_id} (${r.amount})`;
        }).join(', ') : 'ไม่มีสูตรตัดสต็อก';

        let imgHtml = m.image ? 
          `<img src="${m.image}" alt="${m.name}" class="h-44 w-full object-cover">` : 
          `<div class="h-44 w-full bg-slate-100 dark:bg-slate-800 flex flex-col items-center justify-center text-slate-300 dark:text-slate-600"><i class="fa-solid fa-bowl-food text-3xl mb-1"></i><span class="text-[10px] text-slate-400 font-medium">ไม่มีรูปภาพ</span></div>`;

        return `
          <div class="bg-white dark:bg-slate-900 rounded-3xl border border-slate-200/80 dark:border-slate-800 shadow-sm flex flex-col justify-between hover:shadow-md transition overflow-hidden">
            <div class="relative">
              ${imgHtml}
              <span class="absolute top-2.5 right-2.5 text-[10px] font-extrabold px-2.5 py-0.5 rounded-full shadow-sm ${m.is_available ? 'bg-emerald-500 text-white' : 'bg-rose-500 text-white'}">${m.is_available ? 'พร้อมขาย' : 'หมด'}</span>
              <span class="absolute bottom-2.5 left-2.5 text-[10px] font-semibold px-2.5 py-0.5 rounded-lg bg-slate-900/70 text-white">${m.category}</span>
            </div>
            <div class="p-4 flex-1 flex flex-col justify-between">
              <div>
                <h4 class="font-bold text-sm text-slate-900 dark:text-white">${m.name}</h4>
                <p class="text-[11px] text-slate-400 mt-1 line-clamp-1" title="${recipeTxt}"><i class="fa-solid fa-boxes-stacked mr-1"></i>${recipeTxt}</p>
              </div>
              <div class="mt-4 pt-3 border-t border-slate-100 dark:border-slate-800">
                <div class="text-indigo-600 dark:text-indigo-400 font-extrabold text-base mb-2.5">${m.price.toFixed(2)} ฿</div>
                <div class="flex gap-2">
                  ${isStaffOrAdmin ? `
                    <button onclick="toggleMenu(${m.id})" class="flex-1 bg-slate-100 dark:bg-slate-800 hover:bg-slate-200 dark:hover:bg-slate-700 text-slate-700 dark:text-slate-300 text-xs py-2 rounded-xl font-semibold transition">สลับสถานะ</button>
                    <button onclick="openEditMenuModal(${m.id})" class="text-slate-400 hover:text-indigo-600 px-2.5 py-2 rounded-xl text-xs hover:bg-indigo-50 dark:hover:bg-indigo-950 transition" title="แก้ไขเมนู"><i class="fa-solid fa-pen-to-square"></i></button>
                    <button onclick="deleteMenu(${m.id})" class="text-slate-400 hover:text-rose-500 px-2.5 py-2 rounded-xl text-xs hover:bg-rose-50 dark:hover:bg-rose-950 transition" title="ลบเมนู"><i class="fa-solid fa-trash-can"></i></button>
                  ` : `
                    <button onclick="quickOrderMenu(${m.id})" class="w-full bg-indigo-600 hover:bg-indigo-700 text-white text-xs py-2 rounded-xl font-semibold transition shadow-sm">สั่งเมนูนี้</button>
                  `}
                </div>
              </div>
            </div>
          </div>
        `;
      }).join('') || '<p class="text-slate-400 text-xs col-span-4 text-center py-10 font-medium">ไม่พบรายการเมนู</p>';
    }

    async function toggleMenu(mid) {
      const res = await apiFetch(`/api/menu/${mid}/toggle`, { method: 'POST' });
      if (res.ok) {
        showToast('ปรับสถานะเมนูเรียบร้อย', 'success');
        loadCatalogMenus();
      } else {
        const d = await res.json();
        showToast(d.detail || 'ไม่สามารถปรับสถานะได้', 'error');
      }
    }

    async function deleteMenu(mid) {
      if(!confirm('คุณแน่ใจหรือไม่ว่าต้องการลบเมนูนี้ถาวร?')) return;
      const res = await apiFetch(`/api/menu/${mid}`, { method: 'DELETE' });
      const data = await res.json();
      if(res.ok) {
        showToast(data.message || 'ลบเมนูสำเร็จ', 'success');
        loadCatalogMenus();
      } else {
        showToast(data.detail || 'ไม่สามารถลบเมนูได้', 'error');
      }
    }

    function quickOrderMenu(mid) {
      switchTab('qr');
      document.getElementById('qr-select-menu').value = mid;
      updateQrMenuPreview();
    }

    async function openAddMenuModal() {
      const invRes = await apiFetch('/api/inventory');
      cachedInventory = await invRes.json();
      
      document.getElementById('edit-menu-id').value = '';
      document.getElementById('menu-modal-heading').innerHTML = '<i class="fa-solid fa-utensils text-indigo-600"></i> เพิ่มรายการเมนูใหม่';
      document.getElementById('menu-submit-btn').innerText = 'บันทึกเมนูใหม่';
      
      document.getElementById('new-menu-name').value = '';
      document.getElementById('new-menu-cat').value = 'อาหารจานเดียว';
      document.getElementById('new-menu-price').value = '';
      document.getElementById('new-menu-img-url').value = '';
      document.getElementById('new-menu-img-file').value = '';
      uploadedImageBase64 = '';
      document.getElementById('menu-img-preview-box').classList.add('hidden');
      setImgMode('url');

      document.getElementById('recipe-rows-container').innerHTML = '';
      addRecipeRow();
      document.getElementById('add-menu-modal').classList.remove('hidden');
    }

    async function openEditMenuModal(mid) {
      const invRes = await apiFetch('/api/inventory');
      cachedInventory = await invRes.json();

      const m = cachedMenu.find(x => x.id === mid);
      if(!m) return showToast('ไม่พบข้อมูลเมนู', 'error');

      document.getElementById('edit-menu-id').value = m.id;
      document.getElementById('menu-modal-heading').innerHTML = `<i class="fa-solid fa-pen-to-square text-indigo-600"></i> แก้ไขเมนู: ${m.name}`;
      document.getElementById('menu-submit-btn').innerText = 'บันทึกการแก้ไข';

      document.getElementById('new-menu-name').value = m.name;
      document.getElementById('new-menu-cat').value = m.category;
      document.getElementById('new-menu-price').value = m.price;
      
      uploadedImageBase64 = '';
      document.getElementById('new-menu-img-file').value = '';
      setImgMode('url');
      if (m.image) {
        document.getElementById('new-menu-img-url').value = m.image;
        document.getElementById('menu-img-preview').src = m.image;
        document.getElementById('menu-img-preview-box').classList.remove('hidden');
      } else {
        document.getElementById('new-menu-img-url').value = '';
        document.getElementById('menu-img-preview-box').classList.add('hidden');
      }

      const c = document.getElementById('recipe-rows-container');
      c.innerHTML = '';
      if (m.recipe && m.recipe.length > 0) {
        m.recipe.forEach(r => {
          addRecipeRow(r.ingredient_id, r.amount);
        });
      } else {
        addRecipeRow();
      }

      document.getElementById('add-menu-modal').classList.remove('hidden');
    }

    function addRecipeRow(selectedIngId = '', selectedAmt = '') {
      const c = document.getElementById('recipe-rows-container');
      const row = document.createElement('div');
      row.className = 'flex gap-2 items-center';
      row.innerHTML = `
        <select class="recipe-ing-sel flex-1 border border-slate-200 dark:border-slate-700 rounded-xl px-2.5 py-2 text-xs bg-white dark:bg-slate-800 text-slate-900 dark:text-white outline-none">
          ${cachedInventory.map(i => `<option value="${i.id}" ${i.id === selectedIngId ? 'selected' : ''}>${i.name} (คงเหลือ: ${i.stock} ${i.unit})</option>`).join('')}
        </select>
        <input type="number" step="0.1" min="0.1" value="${selectedAmt}" placeholder="ปริมาณ" class="recipe-amt-input w-24 border border-slate-200 dark:border-slate-700 rounded-xl px-2.5 py-2 text-xs bg-white dark:bg-slate-800 text-slate-900 dark:text-white outline-none font-bold">
        <button onclick="this.parentElement.remove()" type="button" class="w-8 h-8 rounded-xl text-slate-400 hover:text-rose-500 hover:bg-rose-50 dark:hover:bg-rose-950/50 flex items-center justify-center transition"><i class="fa-solid fa-xmark"></i></button>
      `;
      c.appendChild(row);
    }

    async function submitMenuForm() {
      const editId = document.getElementById('edit-menu-id').value;
      const name = document.getElementById('new-menu-name').value.trim();
      const cat = document.getElementById('new-menu-cat').value.trim();
      const price = parseFloat(document.getElementById('new-menu-price').value);
      const imgUrl = document.getElementById('new-menu-img-url').value.trim();
      const finalImage = uploadedImageBase64 || imgUrl || "";

      if(!name) return showToast('กรุณาระบุชื่อเมนูอาหาร', 'error');
      if(isNaN(price) || price < 0) return showToast('ราคาสินค้าต้องเป็นตัวเลขที่ไม่ติดลบ', 'error');

      const recipe = [];
      document.querySelectorAll('#recipe-rows-container > div').forEach(row => {
        const ingId = row.querySelector('.recipe-ing-sel').value;
        const amt = parseFloat(row.querySelector('.recipe-amt-input').value);
        if(ingId && !isNaN(amt) && amt > 0) {
          recipe.push({ ingredient_id: ingId, amount: amt });
        }
      });

      const payload = {
        name: name,
        category: cat || 'อาหารจานเดียว',
        price: price,
        image: finalImage,
        is_available: true,
        recipe: recipe
      };

      const url = editId ? `/api/menu/${editId}` : '/api/menu';
      const method = editId ? 'PUT' : 'POST';

      const res = await apiFetch(url, {
        method: method,
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(payload)
      });

      const d = await res.json();
      if(res.ok) {
        showToast(editId ? 'แก้ไขข้อมูลเมนูเรียบร้อยแล้ว!' : 'เพิ่มเมนูอาหารเรียบร้อยแล้ว!', 'success');
        document.getElementById('add-menu-modal').classList.add('hidden');
        loadCatalogMenus();
      } else {
        showToast(d.detail || 'เกิดข้อผิดพลาดในการบันทึกเมนู', 'error');
      }
    }

    // ================= Inventory Logic =================
    function openAddInventoryModal() {
      document.getElementById('new-inv-name').value = '';
      document.getElementById('new-inv-stock').value = '';
      document.getElementById('new-inv-unit').value = '';
      document.getElementById('new-inv-min').value = '10';
      document.getElementById('add-inventory-modal').classList.remove('hidden');
    }

    async function submitNewInventory() {
      const name = document.getElementById('new-inv-name').value.trim();
      const stock = parseFloat(document.getElementById('new-inv-stock').value);
      const unit = document.getElementById('new-inv-unit').value.trim();
      const minStock = parseFloat(document.getElementById('new-inv-min').value) || 10;

      if(!name) return showToast('กรุณาระบุชื่อวัตถุดิบ (เช่น พริก, ปลาร้า)', 'error');
      if(isNaN(stock) || stock < 0) return showToast('กรุณาระบุจำนวนเริ่มต้นที่ไม่ติดลบ', 'error');
      if(!unit) return showToast('กรุณาระบุหน่วยนับ (เช่น กรัม, ฟอง, ช้อน)', 'error');

      const res = await apiFetch('/api/inventory', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ name: name, stock: stock, unit: unit, min_stock: minStock })
      });
      const data = await res.json();
      if(res.ok) {
        showToast(`เพิ่มวัตถุดิบ '${name}' เข้าคลังสต็อกเรียบร้อย!`, 'success');
        document.getElementById('add-inventory-modal').classList.add('hidden');
        loadInventory();
        loadDashboard();
      } else {
        showToast(data.detail || 'ไม่สามารถเพิ่มวัตถุดิบได้', 'error');
      }
    }

    async function loadInventory() {
      const res = await apiFetch('/api/inventory');
      const inv = await res.json();
      cachedInventory = inv;
      document.getElementById('inventory-table-body').innerHTML = inv.map(i => {
        let isLow = i.stock <= i.min_stock;
        return `
          <tr class="hover:bg-slate-50/50 dark:hover:bg-slate-800/40 transition">
            <td class="p-3.5 font-mono text-slate-400">${i.id}</td>
            <td class="p-3.5 font-bold text-slate-900 dark:text-white">${i.name}</td>
            <td class="p-3.5 font-mono font-bold ${isLow ? 'text-rose-600 dark:text-rose-400' : 'text-slate-800 dark:text-slate-200'}">${i.stock}</td>
            <td class="p-3.5 text-slate-500 dark:text-slate-400">${i.unit}</td>
            <td class="p-3.5 font-mono text-slate-400">${i.min_stock}</td>
            <td class="p-3.5"><span class="text-[10px] px-2.5 py-0.5 rounded-full font-bold ${isLow ? 'bg-rose-50 text-rose-600 border border-rose-100' : 'bg-emerald-50 text-emerald-600 border border-emerald-100'}">${isLow ? 'ใกล้หมด' : 'ปกติ'}</span></td>
          </tr>
        `;
      }).join('') || '<tr><td colspan="6" class="p-6 text-center text-slate-400">กำลังโหลดรายการวัตถุดิบ...</td></tr>';
    }

    async function openRestockPrompt() {
      const id = prompt('ระบุรหัสวัตถุดิบที่ต้องการปรับสต็อก (เช่น ing_1, ing_2):');
      if(!id) return;
      const qtyStr = prompt('ระบุจำนวนที่ต้องการเพิ่ม (หรือใส่ค่าลบเพื่อตัดออก):');
      if(!qtyStr) return;
      const qty = parseFloat(qtyStr);
      if(isNaN(qty)) return showToast('กรุณาระบุจำนวนเป็นตัวเลข', 'error');

      const res = await apiFetch(`/api/inventory/${id}?added_stock=${qty}`, { method: 'PUT' });
      const data = await res.json();
      if(res.ok) {
        showToast('ปรับปรุงสต็อกสำเร็จ', 'success');
        loadInventory();
        loadDashboard();
      } else {
        showToast(data.detail || 'ไม่สามารถปรับสต็อกได้', 'error');
      }
    }

    // Admin Customer Tracker Logic
    async function loadAdminCustomers() {
      try {
        const res = await apiFetch('/api/admin/customers');
        const data = await res.json();
        
        document.getElementById('cust-count-badge').innerText = `${data.total_customers} คน`;
        const list = document.getElementById('admin-customer-list');
        list.innerHTML = data.customers.map(c => `
          <div class="p-3 rounded-2xl bg-slate-50 dark:bg-slate-800 border border-slate-100 dark:border-slate-700 flex justify-between items-center text-xs">
            <div>
              <div class="font-bold text-slate-900 dark:text-white">${c.name}</div>
              <div class="text-[11px] text-slate-400">@${c.username} • สมัครเมื่อ: ${c.registered_at.split(' ')[0]}</div>
            </div>
            <span class="bg-indigo-50 dark:bg-indigo-950 text-indigo-600 dark:text-indigo-400 px-2 py-1 rounded-lg font-bold">⭐ ${c.points} แต้ม</span>
          </div>
        `).join('') || '<p class="text-xs text-slate-400 py-4 text-center">ยังไม่มีสมาชิกลูกค้า</p>';

        const stream = document.getElementById('admin-customer-stream');
        stream.innerHTML = data.recent_activities.map(a => `
          <div class="p-3 rounded-2xl bg-slate-50 dark:bg-slate-800 border border-slate-100 dark:border-slate-700 text-xs flex items-center justify-between">
            <div class="flex items-center gap-2">
              <span class="w-2 h-2 rounded-full bg-emerald-500 animate-pulse"></span>
              <div>
                <b class="text-slate-900 dark:text-white">${a.user}</b>: 
                <span class="text-slate-600 dark:text-slate-300">${a.details}</span>
              </div>
            </div>
            <span class="text-[10px] text-slate-400 font-mono">${a.timestamp.split(' ')[1]}</span>
          </div>
        `).join('') || '<p class="text-xs text-slate-400 py-4 text-center">ยังไม่มีประวัติกิจกรรม</p>';
      } catch (err) {}
    }

    // Dashboard Logic
    async function loadDashboard() {
      try {
        const res = await apiFetch('/api/dashboard');
        const d = await res.json();
        document.getElementById('dash-rev').innerText = d.total_revenue.toFixed(2) + ' ฿';
        document.getElementById('dash-bills').innerText = d.total_bills + ' บิล';
        document.getElementById('dash-tables').innerText = `${d.occupied_tables} / ${d.total_tables}`;
        document.getElementById('dash-stock').innerText = d.low_stock_alerts.length + ' รายการ';

        const topBox = document.getElementById('dash-top-sellers');
        topBox.innerHTML = d.best_sellers.map((b, i) => `
          <div class="flex justify-between items-center text-xs p-2.5 bg-slate-50/80 dark:bg-slate-800 rounded-xl border border-slate-100 dark:border-slate-700">
            <span class="font-medium text-slate-700 dark:text-slate-300"><b class="text-indigo-600 dark:text-indigo-400 mr-1.5">#${i+1}</b> ${b.name}</span>
            <span class="font-extrabold text-slate-900 dark:text-white">${b.qty} จาน</span>
          </div>
        `).join('') || '<p class="text-slate-400 text-xs font-medium">ยังไม่มีข้อมูลการขาย</p>';

        const lowBox = document.getElementById('dash-low-stock-list');
        lowBox.innerHTML = d.low_stock_alerts.map(l => `
          <div class="flex justify-between items-center text-xs p-2.5 bg-rose-50 dark:bg-rose-950/40 text-rose-700 dark:text-rose-400 rounded-xl border border-rose-100 dark:border-rose-900">
            <span><i class="fa-solid fa-triangle-exclamation mr-1.5 text-rose-500"></i> ${l.name}</span>
            <b class="font-mono">เหลือ ${l.stock} ${l.unit}</b>
          </div>
        `).join('') || '<p class="text-emerald-600 text-xs font-medium">วัตถุดิบทั้งหมดอยู่ในเกณฑ์ปลอดภัย</p>';
      } catch (err) {}
    }

    async function loadTables() {
      const res = await apiFetch('/api/tables');
      const tables = await res.json();
      const grid = document.getElementById('tables-grid');
      grid.innerHTML = tables.map(t => {
        let col = t.status === 'ว่าง' ? 'border-emerald-200 dark:border-emerald-800 bg-emerald-50/40 dark:bg-emerald-950/20' : (t.status === 'มีลูกค้า' ? 'border-rose-200 dark:border-rose-800 bg-rose-50/40 dark:bg-rose-950/20' : 'border-amber-200 dark:border-amber-800 bg-amber-50/40 dark:bg-amber-950/20');
        let dotCol = t.status === 'ว่าง' ? 'bg-emerald-500' : (t.status === 'มีลูกค้า' ? 'bg-rose-500' : 'bg-amber-500');
        let canDelete = t.status === 'ว่าง' && currentUser && currentUser.role === 'admin';
        return `
          <div class="p-5 rounded-3xl border-2 ${col} shadow-sm relative group flex flex-col justify-between hover:-translate-y-0.5 transition bg-white dark:bg-slate-900">
            <div>
              <div class="flex justify-between items-center mb-2">
                <div class="flex items-center gap-2">
                  <span class="w-2.5 h-2.5 rounded-full ${dotCol}"></span>
                  <h4 class="font-extrabold text-lg text-slate-900 dark:text-white">โต๊ะ ${t.table_id}</h4>
                </div>
                <div class="flex items-center gap-1">
                  <span class="text-[10px] px-2 py-0.5 rounded-full font-bold bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700">${t.status}</span>
                  ${canDelete ? `<button onclick="deleteTable(${t.table_id})" title="ลบโต๊ะนี้" class="text-slate-400 hover:text-rose-600 text-xs p-1"><i class="fa-solid fa-trash-can"></i></button>` : ''}
                </div>
              </div>
              <p class="text-xs text-slate-400 font-medium">ความจุ ${t.capacity} ที่นั่ง</p>
            </div>
            <div class="mt-5 flex gap-2">
              <button onclick="selectPOS(${t.table_id})" class="flex-1 bg-slate-900 dark:bg-slate-800 hover:bg-black text-white text-xs py-2 rounded-xl font-semibold transition">สั่งอาหาร</button>
              <button onclick="showTableQr(${t.table_id})" title="QR สั่งอาหาร" class="w-9 h-9 rounded-xl bg-slate-100 dark:bg-slate-800 hover:bg-slate-200 text-slate-700 dark:text-slate-300 flex items-center justify-center text-xs transition"><i class="fa-solid fa-qrcode"></i></button>
            </div>
          </div>
        `;
      }).join('');
    }

    async function openAddTableModal() {
      const capStr = prompt('กรุณาระบุจำนวนที่นั่งสำหรับโต๊ะใหม่ (เช่น 2, 4, 6):', '4');
      if (capStr !== null) {
        const capacity = parseInt(capStr.trim());
        if (isNaN(capacity) || capacity <= 0) return showToast('กรุณาระบุตัวเลขมากกว่า 0', 'error');

        const res = await apiFetch('/api/tables', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({ capacity: capacity })
        });
        const data = await res.json();
        if (res.ok) {
          showToast(`เพิ่มโต๊ะ ${data.table.table_id} สำเร็จ!`, 'success');
          loadTables();
          loadCheckoutTables();
          loadQrMenu();
        } else {
          showToast(data.detail || 'ไม่สามารถเพิ่มโต๊ะได้', 'error');
        }
      }
    }

    async function deleteTable(tid) {
      if (!confirm(`คุณแน่ใจหรือไม่ว่าต้องการลบ "โต๊ะ ${tid}"?`)) return;
      const res = await apiFetch(`/api/tables/${tid}`, { method: 'DELETE' });
      const data = await res.json();
      if (res.ok) {
        showToast(data.message || 'ลบโต๊ะสำเร็จ', 'success');
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
      const mRes = await apiFetch('/api/menu?limit=50');
      const mData = await mRes.json();
      cachedMenu = mData.items;

      document.getElementById('pos-menu-list').innerHTML = cachedMenu.map(m => `
        <div class="border border-slate-200/80 dark:border-slate-800 p-2.5 rounded-2xl text-center bg-slate-50/50 dark:bg-slate-800/50 hover:bg-white dark:hover:bg-slate-800 transition flex flex-col justify-between">
          <div class="h-20 w-full mb-1.5 overflow-hidden rounded-xl bg-slate-200 dark:bg-slate-700">
            ${m.image ? `<img src="${m.image}" alt="${m.name}" class="w-full h-full object-cover">` : `<div class="w-full h-full flex items-center justify-center text-slate-400 text-xs"><i class="fa-solid fa-utensils"></i></div>`}
          </div>
          <div>
            <div class="font-bold text-xs truncate text-slate-800 dark:text-white">${m.name}</div>
            <div class="text-indigo-600 dark:text-indigo-400 text-xs font-extrabold my-1">${m.price} ฿</div>
          </div>
          <button onclick="posAdd(${m.id})" class="bg-indigo-600 text-white text-[11px] px-2 py-1.5 rounded-xl hover:bg-indigo-700 w-full font-semibold mt-1 transition">+ สั่งอาหาร</button>
        </div>
      `).join('');
      loadTableOrders(tid);
    }

    async function loadTableOrders(tid) {
      const res = await apiFetch(`/api/orders?table_id=${tid}`);
      const orders = await res.json();
      const box = document.getElementById('pos-order-items');
      const activeOrders = orders.filter(o => o.status !== 'ยกเลิก');
      let total = 0;
      activeOrders.forEach(o => { total += (o.unit_price * o.qty); });
      document.getElementById('pos-total-price').innerText = total.toFixed(2) + ' ฿';

      box.innerHTML = activeOrders.map(o => `
        <div class="flex justify-between items-center bg-slate-50 dark:bg-slate-800 p-2.5 rounded-xl text-xs border border-slate-100 dark:border-slate-700">
          <div><b class="text-slate-900 dark:text-white">${o.name}</b> <span class="text-indigo-600 dark:text-indigo-400 font-bold">x${o.qty}</span></div>
          <span class="font-mono font-bold text-slate-800 dark:text-slate-200">${(o.unit_price * o.qty).toFixed(2)} ฿</span>
        </div>
      `).join('') || '<p class="text-xs text-slate-400 py-4 text-center font-medium">ยังไม่มีรายการสั่งอาหาร</p>';
    }

    async function posAdd(mid) {
      const res = await apiFetch('/api/customer/order', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ table_id: currentTable, items: [{ menu_id: mid, qty: 1 }] })
      });
      const data = await res.json();
      if (res.ok) {
        showToast('เพิ่มรายการเรียบร้อย', 'success');
        loadTables();
        loadTableOrders(currentTable);
      } else {
        showToast(data.detail || 'ไม่สามารถสั่งอาหารได้', 'error');
      }
    }

    // KDS Logic
    let kitchenDebounceTimer = null;

    async function loadKitchenOrders() {
      if (kitchenDebounceTimer) clearTimeout(kitchenDebounceTimer);
      kitchenDebounceTimer = setTimeout(async () => {
        try {
          const res = await apiFetch('/api/orders');
          const orders = await res.json();
          const active = orders.filter(o => o.status === 'รอทำ' || o.status === 'กำลังทำ');
          const grid = document.getElementById('kds-grid');
          grid.innerHTML = active.map(o => `
            <div class="bg-white dark:bg-slate-900 p-5 rounded-3xl border-2 ${o.status === 'รอทำ' ? 'border-amber-400' : 'border-indigo-500'} shadow-sm space-y-3 transition-all">
              <div class="flex justify-between items-center text-xs">
                <span class="font-extrabold text-sm px-2.5 py-1 rounded-lg ${o.status === 'รอทำ' ? 'bg-amber-50 dark:bg-amber-950 text-amber-700 dark:text-amber-400' : 'bg-indigo-50 dark:bg-indigo-950 text-indigo-700 dark:text-indigo-400'}">โต๊ะ ${o.table_id}</span>
                <span class="text-slate-400 font-mono"><i class="fa-regular fa-clock mr-1"></i>${o.created_at.split(' ')[1]}</span>
              </div>
              <div>
                <h4 class="font-extrabold text-base text-slate-900 dark:text-white">${o.name} <span class="text-indigo-600 dark:text-indigo-400">x${o.qty}</span></h4>
                ${o.options ? `<p class="text-xs text-slate-500 dark:text-slate-400 mt-1 bg-slate-50 dark:bg-slate-800 p-2 rounded-xl">ตัวเลือก: ${o.options.spiciness || ''} | ${o.options.egg || ''} | ${o.options.size || ''}</p>` : ''}
              </div>
              <div class="flex gap-2 pt-2">
                ${o.status === 'รอทำ' ? `<button onclick="kitchenAction(${o.order_id}, 'กำลังทำ', this)" class="bg-indigo-600 hover:bg-indigo-700 disabled:opacity-50 text-white text-xs px-3 py-2 rounded-xl font-semibold flex-1 transition shadow-sm">เริ่มทำ (ตัดสต็อก)</button>` : ''}
                <button onclick="kitchenAction(${o.order_id}, 'เสิร์ฟแล้ว', this)" class="bg-emerald-600 hover:bg-emerald-700 disabled:opacity-50 text-white text-xs px-3 py-2 rounded-xl font-semibold flex-1 transition shadow-sm">เสิร์ฟแล้ว</button>
              </div>
            </div>
          `).join('') || '<p class="text-slate-400 text-xs col-span-3 text-center py-12 font-medium">ไม่มีออเดอร์ค้างทำในห้องครัว 🎉</p>';
        } catch (err) {}
      }, 50);
    }

    async function kitchenAction(oid, st, btn) {
      if (btn) {
        btn.disabled = true;
        const originalText = btn.innerHTML;
        btn.innerHTML = `<i class="fa-solid fa-spinner animate-spin mr-1"></i>กำลังบันทึก...`;
      }

      try {
        const res = await apiFetch(`/api/kitchen/${oid}/status?status=${encodeURIComponent(st)}`, { method: 'POST' });
        const data = await res.json();
        if(!res.ok) {
          showToast(data.detail || 'ไม่สามารถตัดสต็อกได้', 'error');
          if (btn) { btn.disabled = false; btn.innerHTML = originalText; }
        } else {
          showToast(`อัปเดตสถานะเป็น "${st}" สำเร็จ`, 'success');
        }
      } catch (err) {
        showToast('เกิดข้อผิดพลาดในการเชื่อมต่อ', 'error');
        if (btn) { btn.disabled = false; btn.innerHTML = originalText; }
      } finally {
        loadKitchenOrders();
        loadInventory();
        loadDashboard();
      }
    }

    async function loadQueues() {
      const res = await apiFetch('/api/queues');
      const qs = await res.json();
      const canCallQueue = currentUser && (currentUser.role === 'admin' || currentUser.role === 'staff');

      document.getElementById('queue-list').innerHTML = qs.filter(q => q.status === 'รอเรียก').map(q => `
        <div class="flex justify-between items-center p-3 bg-slate-50 dark:bg-slate-800 rounded-2xl border border-slate-100 dark:border-slate-700 text-xs">
          <div><b class="text-indigo-600 dark:text-indigo-400 font-mono text-sm">${q.queue_id}</b> : <span class="font-semibold text-slate-800 dark:text-slate-200">${q.name}</span> <span class="text-slate-400">(${q.party_size} ท่าน)</span></div>
          ${canCallQueue ? `<button onclick="callQueue('${q.queue_id}')" class="bg-emerald-600 hover:bg-emerald-700 text-white text-[11px] px-3 py-1.5 rounded-xl font-semibold transition">เรียกลูกค้า</button>` : '<span class="text-amber-500 font-semibold">รอเรียกคิว</span>'}
        </div>
      `).join('') || '<p class="text-xs text-slate-400 py-3 text-center font-medium">ไม่มีคิวค้างในขณะนี้</p>';
    }

    async function createQueueTicket() {
      const name = document.getElementById('q-name').value.trim();
      const size = parseInt(document.getElementById('q-size').value);
      if(!name) return showToast('กรุณาระบุชื่อลูกค้า', 'error');
      if(isNaN(size) || size <= 0) return showToast('จำนวนคนต้องมากกว่า 0', 'error');
      
      const res = await apiFetch('/api/queue/ticket', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ name: name, party_size: size })
      });
      const data = await res.json();
      showToast(`ออกบัตรคิวสำเร็จ! หมายเลขคือ ${data.queue_id}`, 'success');
      document.getElementById('q-name').value = '';
      document.getElementById('q-size').value = '';
      loadQueues();
    }

    async function callQueue(qid) {
      const res = await apiFetch(`/api/queue/${qid}/status?status=เรียกแล้ว`, { method: 'POST' });
      if(res.ok) {
        showToast(`เรียกลำดับคิว ${qid} แล้ว`, 'info');
        loadQueues();
      }
    }

    async function createReservation() {
      const p = {
        name: document.getElementById('res-name').value.trim(),
        phone: document.getElementById('res-phone').value.trim(),
        date: document.getElementById('res-date').value,
        time: document.getElementById('res-time').value,
        party_size: parseInt(document.getElementById('res-size').value)
      };
      if(!p.name || !p.phone) return showToast('กรุณากรอกชื่อและเบอร์โทรศัพท์', 'error');
      if(isNaN(p.party_size) || p.party_size <= 0) return showToast('จำนวนที่นั่งต้องมากกว่า 0', 'error');
      
      const res = await apiFetch('/api/reservations', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(p)
      });
      if(res.ok) {
        showToast('บันทึกการจองสำเร็จ!', 'success');
      }
    }

    async function loadLogs() {
      try {
        const res = await apiFetch('/api/logs');
        const d = await res.json();
        document.getElementById('logs-table-body').innerHTML = d.items.map(l => `
          <tr class="hover:bg-slate-50/50 dark:hover:bg-slate-800/40 transition">
            <td class="p-3.5 text-slate-400 font-mono">${l.timestamp}</td>
            <td class="p-3.5 font-bold text-slate-800 dark:text-slate-200">${l.user}</td>
            <td class="p-3.5"><span class="bg-slate-100 dark:bg-slate-800 text-slate-700 dark:text-slate-300 px-2.5 py-0.5 rounded-md font-semibold">${l.action}</span></td>
            <td class="p-3.5 text-slate-600 dark:text-slate-400">${l.details}</td>
          </tr>
        `).join('');
      } catch (err) {}
    }

    async function loadCheckoutTables() {
      const res = await apiFetch('/api/tables');
      const tables = await res.json();
      document.getElementById('bill-table-sel').innerHTML = tables.map(t => `<option value="${t.table_id}">โต๊ะ ${t.table_id} (${t.status})</option>`).join('');
    }

    // ฟังก์ชันเช็คบิล: รองรับเงินสด (พร้อมคำนวณเงินทอน) และโอนเงินผ่าน QR
    async function executeCheckout() {
      const tid = parseInt(document.getElementById('bill-table-sel').value);
      const disc = parseFloat(document.getElementById('bill-discount').value) || 0;
      const phone = document.getElementById('bill-member').value.trim();
      const cashReceived = parseFloat(document.getElementById('cash-received-input').value) || 0;

      if(!tid) return showToast('กรุณาเลือกโต๊ะที่ต้องการเช็คบิล', 'error');

      const payload = {
        table_id: tid,
        discount_percent: disc,
        member_phone: phone,
        payment_method: currentPaymentMethod,
        cash_received: currentPaymentMethod === 'cash' ? cashReceived : 0
      };

      const res = await apiFetch('/api/checkout', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(payload)
      });

      const d = await res.json();
      if(!res.ok) return showToast(d.detail || 'ไม่สามารถคิดเงินได้', 'error');

      const r = d.receipt;
      document.getElementById('rc-id').innerText = r.receipt_id;
      document.getElementById('rc-date').innerText = r.timestamp;
      document.getElementById('rc-table').innerText = 'โต๊ะ: ' + r.table_id;
      document.getElementById('rc-items').innerHTML = r.items.map(it => `
        <div class="flex justify-between"><span>${it.name} x${it.qty}</span><span class="font-mono">${(it.unit_price * it.qty).toFixed(2)}</span></div>
      `).join('');
      document.getElementById('rc-sub').innerText = r.subtotal.toFixed(2) + ' ฿';
      document.getElementById('rc-disc').innerText = `-${r.discount_amount.toFixed(2)} ฿ (${r.discount_percent}%)`;
      document.getElementById('rc-sc').innerText = `+${r.service_charge.toFixed(2)} ฿`;
      document.getElementById('rc-vat').innerText = `+${r.vat.toFixed(2)} ฿`;
      document.getElementById('rc-net').innerText = r.net_total.toFixed(2) + ' ฿';

      const payBox = document.getElementById('rc-payment-method-box');
      if (r.payment_method === 'cash') {
        payBox.innerHTML = `ชำระด้วยเงินสด<br>รับเงิน: ${r.cash_received.toFixed(2)} ฿ | เงินทอน: ${r.change.toFixed(2)} ฿`;
      } else {
        payBox.innerHTML = `ชำระด้วยการโอนเงิน / สแกน QR สำเร็จ`;
      }

      if(d.earned_points > 0) {
        document.getElementById('rc-point-box').innerText = `⭐ ได้รับแต้มสะสม: +${d.earned_points} แต้ม`;
      }

      showToast('เช็คบิลสำเร็จ! ยอดขายถูกบันทึกแล้ว', 'success');
      loadTables();
      loadCheckoutTables();
      await loadDashboard();
    }

    async function sendCustomerOrder() {
      const tid = parseInt(document.getElementById('qr-table-num').value);
      const mid = parseInt(document.getElementById('qr-select-menu').value);
      const spice = document.getElementById('qr-opt-spice').value;
      const egg = document.getElementById('qr-opt-egg').value;
      const size = document.getElementById('qr-opt-size').value;

      if(!tid) return showToast('กรุณาระบุหมายเลขโต๊ะ', 'error');
      if(!mid) return showToast('กรุณาเลือกเมนูอาหาร', 'error');

      const res = await apiFetch('/api/customer/order', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
          table_id: tid,
          items: [{ menu_id: mid, qty: 1, options: { spiciness: spice, egg: egg, size: size } }]
        })
      });
      const d = await res.json();
      if(res.ok) {
        showToast('ส่งออเดอร์ถึงห้องครัวเรียบร้อย!', 'success');
      } else {
        showToast(d.detail || 'ไม่สามารถสั่งอาหารได้', 'error');
      }
    }

    async function loadQrMenu() {
      const res = await apiFetch('/api/menu?limit=50');
      const d = await res.json();
      cachedMenu = d.items;
      document.getElementById('qr-select-menu').innerHTML = d.items.map(m => `<option value="${m.id}">${m.name} (${m.price} ฿)</option>`).join('');

      const tRes = await apiFetch('/api/tables');
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
        apiFetch('/api/table/move', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({ from_table: parseInt(fromT), to_table: parseInt(toT) })
        }).then(r => r.json()).then(d => {
          showToast(d.message || d.detail, d.success ? 'success' : 'error');
          loadTables();
        });
      }
    }

    function openMergeModal() {
      const src = prompt('ระบุเลขโต๊ะที่ต้องการรวม (คั่นด้วยจุลภาค เช่น 2,3):');
      const tgt = prompt('ระบุโต๊ะเป้าหมายหลัก:');
      if(src && tgt) {
        const arr = src.split(',').map(x => parseInt(x.trim())).filter(x => !isNaN(x));
        apiFetch('/api/table/merge', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({ source_tables: arr, target_table: parseInt(tgt) })
        }).then(r => r.json()).then(d => {
          showToast(d.message || d.detail, d.success ? 'success' : 'error');
          loadTables();
        });
      }
    }

    // Real-Time SSE Listener (รับสัญญาณอัปเดตเมนูสดข้ามอุปกรณ์)
    const evt = new EventSource('/api/realtime');
    evt.onmessage = function(e) {
      const ev = JSON.parse(e.data);
      if(ev.type === 'NEW_ORDER') {
        if (currentUser && currentUser.role !== 'customer') {
          showToast(`🔔 โต๊ะ ${ev.data.table_id} มีออเดอร์ใหม่เข้ามา!`, 'info');
          loadKitchenOrders();
        }
      } else if(ev.type === 'MENU_UPDATE') {
        // เมื่อแอดมินลบ/เพิ่ม/แก้ไขเมนู ทุกหน้าจอจะอัปเดตตามทันที
        loadCatalogMenus();
        loadQrMenu();
        if(currentTable) selectPOS(currentTable);
      } else if(ev.type === 'STOCK_UPDATE') {
        loadInventory();
        loadDashboard();
      } else if(ev.type === 'DASHBOARD_UPDATE') {
        loadDashboard();
        loadTables();
      } else if(ev.type === 'QUEUE_UPDATE') {
        loadQueues();
      } else if(ev.type === 'CUSTOMER_ACTIVITY') {
        if (currentUser && currentUser.role === 'admin') {
          loadAdminCustomers();
        }
      }
    };

    const savedUser = localStorage.getItem('restro_user');
    if(savedUser) {
      setupSession(JSON.parse(savedUser));
    }
  </script>
</body>
</html>
"""