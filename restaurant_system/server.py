"""
server.py - เว็บเซิร์ฟเวอร์แบบเบ็ดเสร็จ (Built-in HTTP Server) รองรับ REST API และ Web UI
"""

from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import os
import urllib.parse
from datetime import datetime
import storage
import services


class RestaurantHandler(BaseHTTPRequestHandler):

    def log_message(self, format, *args):
        # ปิดการพิมพ์ Log รกหน้าจอเทอร์มินัล
        return

    def _send_json(self, data: dict, status: int = 200):
        try:
            body = json.dumps(data, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except Exception:
            pass

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path

        try:
            # เสิร์ฟรูปภาพใน uploads/
            if path.startswith("/uploads/"):
                filepath = path.lstrip("/")
                if os.path.exists(filepath):
                    with open(filepath, "rb") as f:
                        content = f.read()
                    self.send_response(200)
                    self.send_header("Content-Type", "image/jpeg")
                    self.end_headers()
                    self.wfile.write(content)
                    return
                else:
                    self.send_error(404)
                    return

            # API Data Endpoints
            if path == "/api/data":
                db = storage.load_database()
                db["categories"] = services.get_unique_categories(db.get("menu", []))
                self._send_json(db)
                return

            if path == "/api/report":
                params = urllib.parse.parse_qs(parsed.query)
                target_date = params.get("date", [""])[0]
                report = services.generate_sales_report(target_date)
                self._send_json(report)
                return

            # เสิร์ฟหน้าเว็บ Single Page Application (HTML/CSS/JS)
            if path in ("/", "/index.html"):
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                self.wfile.write(self.get_html_page().encode("utf-8"))
                return

            self.send_error(404, "Page Not Found")
        except Exception:
            self.send_error(500, "Internal Server Error")

    def do_POST(self):
        try:
            length = int(self.headers.get("Content-Length", 0))
            body_data = self.rfile.read(length).decode("utf-8")
            payload = json.loads(body_data) if body_data else {}
            path = self.path

            db = storage.load_database()

            # 1. จัดการเมนู: เพิ่ม/แก้ไข/ปรับสถานะหมด
            if path == "/api/menu/save":
                menu_id = payload.get("id")
                img_path = payload.get("image_path", "")

                # รองรับการอัปโหลดรูปภาพแบบ Base64
                if payload.get("image_base64"):
                    img_path = storage.save_uploaded_image(
                        payload.get("image_filename", "menu.jpg"),
                        payload.get("image_base64")
                    )

                if menu_id:  # อัปเดตรายการเดิม
                    for m in db["menu"]:
                        if m["id"] == int(menu_id):
                            m["name"] = str(payload.get("name", m["name"]))
                            m["category"] = str(payload.get("category", m["category"]))
                            m["price"] = float(payload.get("price", m["price"]))
                            m["is_available"] = bool(payload.get("is_available", True))
                            if img_path:
                                m["image_path"] = img_path
                            break
                else:  # เพิ่มเมนูใหม่
                    new_id = max([m["id"] for m in db["menu"]], default=0) + 1
                    db["menu"].append({
                        "id": new_id,
                        "name": str(payload.get("name", "")),
                        "category": str(payload.get("category", "ทั่วไป")),
                        "price": float(payload.get("price", 0.0)),
                        "is_available": bool(payload.get("is_available", True)),
                        "image_path": img_path
                    })
                storage.save_database(db)
                self._send_json({"success": True})
                return

            # สลับสถานะ หมด / พร้อมขาย
            if path == "/api/menu/toggle":
                mid = int(payload.get("id", 0))
                for m in db["menu"]:
                    if m["id"] == mid:
                        m["is_available"] = not m["is_available"]
                        break
                storage.save_database(db)
                self._send_json({"success": True})
                return

            # 2. จัดการโต๊ะ (เปลี่ยนสถานะ ว่าง/มีลูกค้า/รอเช็คบิล)
            if path == "/api/table/status":
                tid = int(payload.get("table_id", 0))
                new_status = payload.get("status")
                if new_status in services.TABLE_STATUSES:
                    for t in db["tables"]:
                        if t["table_id"] == tid:
                            t["status"] = new_status
                            break
                    storage.save_database(db)
                    self._send_json({"success": True})
                else:
                    self._send_json({"success": False, "message": "สถานะไม่ถูกต้อง"}, 400)
                return

            # 3. รับออเดอร์ เพิ่ม/ลด/ยกเลิก
            if path == "/api/order/action":
                tid = int(payload.get("table_id", 0))
                mid = int(payload.get("menu_id", 0))
                change = int(payload.get("change", 1))
                res = services.add_or_update_order(tid, mid, change)
                self._send_json(res)
                return

            # 4. หน้าจอครัว: อัปเดตสถานะของอาหาร
            if path == "/api/kitchen/update":
                oid = int(payload.get("order_id", 0))
                st = payload.get("status")
                if st in services.KITCHEN_STATUSES:
                    for o in db["orders"]:
                        if o["order_id"] == oid:
                            o["status"] = st
                            break
                    storage.save_database(db)
                    self._send_json({"success": True})
                else:
                    self._send_json({"success": False}, 400)
                return

            # 5. เช็คบิล คิดเงิน พร้อมพิมพ์ใบเสร็จ
            if path == "/api/checkout":
                tid = int(payload.get("table_id", 0))
                discount = float(payload.get("discount", 0.0))

                table_orders = [
                    o for o in db["orders"]
                    if o["table_id"] == tid and o["status"] != "ยกเลิก"
                ]

                if not table_orders:
                    self._send_json({"success": False, "message": "ไม่มีรายการอาหารที่ต้องชำระ"}, 400)
                    return

                subtotal, disc_amt, sc, vat, net = services.calculate_bill(table_orders, discount)

                receipt = {
                    "receipt_id": f"REC-{datetime.now().strftime('%Y%m%d%H%M%S')}",
                    "table_id": tid,
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "items": table_orders,
                    "subtotal": subtotal,
                    "discount_percent": discount,
                    "discount_amount": disc_amt,
                    "service_charge": sc,
                    "vat": vat,
                    "net_total": net
                }

                # บันทึกลงยอดขาย
                db["sales"].append(receipt)

                # ล้างรายการอาหารของโต๊ะนี้ และรีเซ็ตสถานะโต๊ะเป็น 'ว่าง'
                db["orders"] = [o for o in db["orders"] if o["table_id"] != tid]
                for t in db["tables"]:
                    if t["table_id"] == tid:
                        t["status"] = "ว่าง"

                storage.save_database(db)
                self._send_json({"success": True, "receipt": receipt})
                return

            self.send_error(404)
        except Exception:
            self._send_json({"success": False, "message": "เกิดข้อผิดพลาดในการประมวลผลข้อมูล"}, 500)

    def get_html_page(self) -> str:
        """HTML, CSS และ JavaScript สำหรับส่วนหน้าบ้าน (UI Dashboard)"""
        return """<!DOCTYPE html>
<html lang="th">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>ระบบจัดการร้านอาหาร (Restaurant Management)</title>
<style>
  :root {
    --primary: #2563eb; --primary-hover: #1d4ed8;
    --success: #16a34a; --warning: #d97706; --danger: #dc2626;
    --bg: #f8fafc; --card: #ffffff; --text: #1e293b; --border: #e2e8f0;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; font-family: 'Segoe UI', Tahoma, sans-serif; }
  body { background: var(--bg); color: var(--text); padding-bottom: 50px; }
  header { background: #0f172a; color: white; padding: 1rem 2rem; display: flex; justify-content: space-between; align-items: center; }
  nav { display: flex; gap: 0.5rem; background: #1e293b; padding: 0.5rem 2rem; overflow-x: auto; }
  nav button { background: transparent; border: none; color: #94a3b8; font-size: 0.95rem; font-weight: 600; padding: 0.6rem 1.2rem; cursor: pointer; border-radius: 6px; transition: 0.2s; }
  nav button.active, nav button:hover { background: var(--primary); color: white; }
  .container { max-width: 1200px; margin: 2rem auto; padding: 0 1rem; }
  .tab-pane { display: none; }
  .tab-pane.active { display: block; }
  .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(240px, 1fr)); gap: 1.5rem; }
  .card { background: var(--card); border: 1px solid var(--border); border-radius: 10px; padding: 1.2rem; box-shadow: 0 2px 4px rgba(0,0,0,0.03); }
  .badge { display: inline-block; padding: 0.25rem 0.6rem; border-radius: 9999px; font-size: 0.8rem; font-weight: bold; }
  .bg-free { background: #dcfce7; color: #15803d; }
  .bg-occupied { background: #fee2e2; color: #b91c1c; }
  .bg-billing { background: #fef3c7; color: #b45309; }
  .btn { display: inline-block; padding: 0.5rem 1rem; border: none; border-radius: 6px; font-weight: 600; cursor: pointer; transition: 0.2s; text-align: center; }
  .btn-primary { background: var(--primary); color: white; }
  .btn-primary:hover { background: var(--primary-hover); }
  .btn-danger { background: var(--danger); color: white; }
  .btn-warning { background: var(--warning); color: white; }
  .btn-success { background: var(--success); color: white; }
  .btn-sm { padding: 0.25rem 0.5rem; font-size: 0.85rem; }
  table { width: 100%; border-collapse: collapse; margin-top: 1rem; }
  th, td { border: 1px solid var(--border); padding: 0.75rem; text-align: left; }
  th { background: #f1f5f9; }
  .form-group { margin-bottom: 1rem; }
  label { display: block; margin-bottom: 0.3rem; font-weight: 600; font-size: 0.9rem; }
  input, select { width: 100%; padding: 0.6rem; border: 1px solid var(--border); border-radius: 6px; }
  .receipt-box { background: white; border: 2px dashed #94a3b8; padding: 1.5rem; max-width: 420px; margin: 1.5rem auto; border-radius: 8px; font-family: monospace; }
  @media print {
    body * { visibility: hidden; }
    #printable-receipt, #printable-receipt * { visibility: visible; }
    #printable-receipt { position: absolute; left: 0; top: 0; width: 100%; }
  }
</style>
</head>
<body>

<header>
  <h2>🍽️ Restaurant Management System</h2>
  <span id="current-time"></span>
</header>

<nav>
  <button class="active" onclick="switchTab('tables')">🪑 แผนผังโต๊ะ & สั่งอาหาร</button>
  <button onclick="switchTab('kitchen')">👨‍🍳 หน้าจอครัว (KDS)</button>
  <button onclick="switchTab('menu')">📋 จัดการเมนู</button>
  <button onclick="switchTab('checkout')">💵 เช็คบิล / ใบเสร็จ</button>
  <button onclick="switchTab('report')">📊 รายงานยอดขาย</button>
</nav>

<div class="container">

  <!-- แท็บ 1: โต๊ะอาหาร และการสั่งอาหาร -->
  <div id="tab-tables" class="tab-pane active">
    <h3>สถานะโต๊ะอาหาร</h3>
    <div class="grid" id="tables-grid" style="margin-top: 1rem;"></div>

    <div id="order-section" class="card" style="margin-top: 2rem; display: none;">
      <h3 id="order-table-title">สั่งอาหาร - โต๊ะ</h3>
      <div style="display: flex; gap: 1rem; margin-top: 1rem;">
        <div style="flex: 1;">
          <h4>เมนูที่สามารถสั่งได้</h4>
          <div id="available-menu-list" class="grid" style="margin-top: 0.5rem;"></div>
        </div>
        <div style="flex: 1; border-left: 1px solid var(--border); padding-left: 1.5rem;">
          <h4>รายการที่สั่งแล้วของโต๊ะนี้</h4>
          <div id="table-orders-list"></div>
          <button class="btn btn-warning" style="margin-top: 1rem;" onclick="setTableWaitBilling()">🔔 ขอเช็คบิล (เปลี่ยนสถานะเป็นรอเช็คบิล)</button>
        </div>
      </div>
    </div>
  </div>

  <!-- แท็บ 2: Kitchen Display System (KDS) -->
  <div id="tab-kitchen" class="tab-pane">
    <h3>👨‍🍳 หน้าจอครัว (Kitchen Display System)</h3>
    <p style="color: #64748b; font-size: 0.9rem;">แสดงลำดับออเดอร์ตามเวลาที่สั่ง จัดการทำอาหารและเสิร์ฟ</p>
    <div class="grid" id="kitchen-orders-grid" style="margin-top: 1.5rem;"></div>
  </div>

  <!-- แท็บ 3: จัดการเมนูอาหาร -->
  <div id="tab-menu" class="tab-pane">
    <div style="display: flex; justify-content: space-between; align-items: center;">
      <h3>จัดการรายการอาหารและเครื่องดื่ม</h3>
      <button class="btn btn-primary" onclick="showAddMenuForm()">+ เพิ่มเมนูใหม่</button>
    </div>

    <!-- ฟอร์มเพิ่ม/แก้ไขเมนู -->
    <div id="menu-form-card" class="card" style="margin-top: 1rem; display: none;">
      <h4 id="menu-form-title">เพิ่มเมนูใหม่</h4>
      <input type="hidden" id="menu-id">
      <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; margin-top: 0.5rem;">
        <div class="form-group">
          <label>ชื่อเมนู:</label>
          <input type="text" id="menu-name" required placeholder="เช่น ผัดกะเพราไข่ดาว">
        </div>
        <div class="form-group">
          <label>หมวดหมู่:</label>
          <input type="text" id="menu-category" placeholder="เช่น อาหารจานเดียว, เครื่องดื่ม">
        </div>
        <div class="form-group">
          <label>ราคา (บาท):</label>
          <input type="number" id="menu-price" step="0.5" min="0" placeholder="0.00">
        </div>
        <div class="form-group">
          <label>อัปโหลดรูปภาพ:</label>
          <input type="file" id="menu-file" accept="image/*">
        </div>
      </div>
      <div style="display: flex; gap: 0.5rem; margin-top: 0.5rem;">
        <button class="btn btn-success" onclick="saveMenu()">บันทึกข้อมูล</button>
        <button class="btn btn-danger" onclick="document.getElementById('menu-form-card').style.display='none'">ยกเลิก</button>
      </div>
    </div>

    <table id="menu-table">
      <thead>
        <tr>
          <th>ภาพ</th>
          <th>ชื่อเมนู</th>
          <th>หมวดหมู่</th>
          <th>ราคา</th>
          <th>สถานะ</th>
          <th>จัดการ</th>
        </tr>
      </thead>
      <tbody id="menu-list-body"></tbody>
    </table>
  </div>

  <!-- แท็บ 4: เช็คบิล คิดภาษี ส่วนลด และพิมพ์ใบเสร็จ -->
  <div id="tab-checkout" class="tab-pane">
    <h3>💵 เช็คบิลและพิมพ์ใบเสร็จ</h3>
    <div class="card" style="max-width: 500px; margin-top: 1rem;">
      <div class="form-group">
        <label>เลือกโต๊ะที่ต้องการชำระเงิน:</label>
        <select id="checkout-table-select" onchange="renderCheckoutPreview()"></select>
      </div>
      <div class="form-group">
        <label>ส่วนลด (%):</label>
        <input type="number" id="checkout-discount" value="0" min="0" max="100" onchange="renderCheckoutPreview()">
      </div>
      <button class="btn btn-success" style="width: 100%; font-size: 1.1rem;" onclick="processCheckout()">ชำระเงินและออกใบเสร็จ</button>
    </div>

    <!-- ส่วนแสดงใบเสร็จ -->
    <div id="printable-receipt" class="receipt-box" style="display: none;">
      <div style="text-align: center; border-bottom: 1px dashed black; padding-bottom: 0.5rem;">
        <h3>RESTAURANT RECEIPT</h3>
        <p id="rec-id">REC-000000</p>
        <p id="rec-date"></p>
        <p id="rec-table">โต๊ะ: </p>
      </div>
      <table style="width: 100%; border: none; margin: 0.5rem 0;" id="rec-items"></table>
      <div style="border-top: 1px dashed black; padding-top: 0.5rem; line-height: 1.6;">
        <div style="display: flex; justify-content: space-between;"><span>ยอดรวม:</span><span id="rec-sub">0 ฿</span></div>
        <div style="display: flex; justify-content: space-between;"><span>ส่วนลด:</span><span id="rec-disc">0 ฿</span></div>
        <div style="display: flex; justify-content: space-between;"><span>ค่าบริการ (10%):</span><span id="rec-sc">0 ฿</span></div>
        <div style="display: flex; justify-content: space-between;"><span>ภาษีมูลค่าเพิ่ม (7%):</span><span id="rec-vat">0 ฿</span></div>
        <div style="display: flex; justify-content: space-between; font-weight: bold; font-size: 1.1rem; margin-top: 0.3rem;">
          <span>สุทธิ:</span><span id="rec-net">0 ฿</span>
        </div>
      </div>
      <div style="text-align: center; margin-top: 1rem;">
        <button class="btn btn-primary btn-sm" onclick="window.print()">🖨️ พิมพ์ใบเสร็จ</button>
      </div>
    </div>
  </div>

  <!-- แท็บ 5: รายงานยอดขาย -->
  <div id="tab-report" class="tab-pane">
    <div style="display: flex; justify-content: space-between; align-items: center;">
      <h3>📊 รายงานสรุปยอดขายรายวัน</h3>
      <div>
        <label style="display: inline;">เลือกวันที่: </label>
        <input type="date" id="report-date" style="width: auto;" onchange="loadReport()">
      </div>
    </div>
    
    <div class="grid" style="margin-top: 1.5rem;">
      <div class="card" style="border-left: 5px solid var(--primary);">
        <h4>ยอดขายรวมทั้งหมด</h4>
        <h2 id="report-total-rev" style="color: var(--primary); margin-top: 0.5rem;">0.00 ฿</h2>
      </div>
      <div class="card" style="border-left: 5px solid var(--success);">
        <h4>จำนวนใบเสร็จที่ชำระ</h4>
        <h2 id="report-total-bills" style="color: var(--success); margin-top: 0.5rem;">0 ใบ</h2>
      </div>
    </div>

    <div class="card" style="margin-top: 1.5rem;">
      <h4>🏆 5 อันดับเมนูขายดีประจำวัน</h4>
      <table style="margin-top: 0.5rem;">
        <thead>
          <tr>
            <th>อันดับ</th>
            <th>ชื่อเมนู</th>
            <th>จำนวนที่ขายได้ (จาน)</th>
          </tr>
        </thead>
        <tbody id="report-best-sellers"></tbody>
      </table>
    </div>
  </div>

</div>

<script>
  let AppData = { menu: [], tables: [], orders: [] };
  let currentActiveTable = null;

  function switchTab(tabName) {
    document.querySelectorAll('.tab-pane').forEach(el => el.classList.remove('active'));
    document.querySelectorAll('nav button').forEach(el => el.classList.remove('active'));
    document.getElementById(`tab-${tabName}`).classList.add('active');
    event.target.classList.add('active');
    fetchData();
    if(tabName === 'report') loadReport();
  }

  async function fetchData() {
    try {
      const res = await fetch('/api/data');
      AppData = await res.json();
      renderTables();
      renderMenu();
      renderKitchen();
      renderCheckoutOptions();
      if(currentActiveTable) openOrderTable(currentActiveTable);
    } catch(err) {
      console.error(err);
    }
  }

  // --- จัดการโต๊ะ & ออเดอร์ ---
  function renderTables() {
    const grid = document.getElementById('tables-grid');
    grid.innerHTML = '';
    AppData.tables.forEach(t => {
      let badgeClass = t.status === 'ว่าง' ? 'bg-free' : (t.status === 'มีลูกค้า' ? 'bg-occupied' : 'bg-billing');
      grid.innerHTML += `
        <div class="card" style="cursor: pointer;" onclick="openOrderTable(${t.table_id})">
          <div style="display: flex; justify-content: space-between; align-items: center;">
            <h4>โต๊ะ ${t.table_id}</h4>
            <span class="badge ${badgeClass}">${t.status}</span>
          </div>
          <p style="margin-top: 0.5rem; font-size: 0.9rem; color: #64748b;">กดเพื่อสั่งอาหารหรือดูรายการ</p>
        </div>
      `;
    });
  }

  function openOrderTable(tid) {
    currentActiveTable = tid;
    document.getElementById('order-section').style.display = 'block';
    document.getElementById('order-table-title').innerText = `สั่งอาหาร - โต๊ะ ${tid}`;

    // เมนูที่สั่งได้
    const mList = document.getElementById('available-menu-list');
    mList.innerHTML = '';
    AppData.menu.forEach(m => {
      let isOut = !m.is_available;
      mList.innerHTML += `
        <div class="card" style="padding: 0.75rem; text-align: center; opacity: ${isOut ? '0.5' : '1'};">
          <div style="font-weight: bold;">${m.name}</div>
          <div style="color: var(--primary);">${m.price.toFixed(2)} ฿</div>
          ${isOut ? '<span class="badge bg-occupied">หมด</span>' : 
            `<button class="btn btn-primary btn-sm" style="margin-top: 0.3rem;" onclick="orderItem(${tid}, ${m.id}, 1)">+ สั่ง</button>`}
        </div>
      `;
    });

    // รายการที่สั่งของโต๊ะ
    const oList = document.getElementById('table-orders-list');
    const tableOrders = AppData.orders.filter(o => o.table_id === tid && o.status !== 'ยกเลิก');
    if(tableOrders.length === 0) {
      oList.innerHTML = '<p style="color: #64748b;">ยังไม่มีรายการสั่งอาหาร</p>';
    } else {
      let html = '<table><tr><th>รายการ</th><th>จำนวน</th><th>สถานะ</th><th>ปรับ</th></tr>';
      tableOrders.forEach(o => {
        html += `
          <tr>
            <td>${o.name}</td>
            <td>${o.qty}</td>
            <td><span class="badge bg-billing">${o.status}</span></td>
            <td>
              <button class="btn btn-sm btn-primary" onclick="orderItem(${tid}, ${o.menu_id}, 1)">+</button>
              <button class="btn btn-sm btn-danger" onclick="orderItem(${tid}, ${o.menu_id}, -1)">-</button>
            </td>
          </tr>
        `;
      });
      html += '</table>';
      oList.innerHTML = html;
    }
  }

  async function orderItem(tid, mid, change) {
    await fetch('/api/order/action', {
      method: 'POST',
      body: JSON.stringify({ table_id: tid, menu_id: mid, change: change })
    });
    fetchData();
  }

  async function setTableWaitBilling() {
    if(!currentActiveTable) return;
    await fetch('/api/table/status', {
      method: 'POST',
      body: JSON.stringify({ table_id: currentActiveTable, status: 'รอเช็คบิล' })
    });
    fetchData();
  }

  // --- KITCHEN DISPLAY SYSTEM ---
  function renderKitchen() {
    const kGrid = document.getElementById('kitchen-orders-grid');
    kGrid.innerHTML = '';
    const activeOrders = AppData.orders.filter(o => o.status === 'รอทำ' || o.status === 'กำลังทำ');
    if(activeOrders.length === 0) {
      kGrid.innerHTML = '<p style="color: #64748b;">ไม่มีออเดอร์ค้างทำในครัวในขณะนี้ 🎉</p>';
      return;
    }
    activeOrders.forEach(o => {
      kGrid.innerHTML += `
        <div class="card" style="border-top: 4px solid ${o.status === 'รอทำ' ? 'var(--warning)' : 'var(--primary)'};">
          <div style="display: flex; justify-content: space-between;">
            <span style="font-weight: bold; font-size: 1.1rem;">โต๊ะ ${o.table_id}</span>
            <small style="color: #64748b;">${o.created_at.split(' ')[1]}</small>
          </div>
          <div style="font-size: 1.2rem; margin: 0.5rem 0; font-weight: bold;">${o.name} x ${o.qty}</div>
          <div style="margin-bottom: 0.5rem;">สถานะ: <b>${o.status}</b></div>
          <div style="display: flex; gap: 0.5rem;">
            ${o.status === 'รอทำ' ? 
              `<button class="btn btn-sm btn-primary" onclick="updateKitchenStatus(${o.order_id}, 'กำลังทำ')">เริ่มทำ</button>` : ''}
            <button class="btn btn-sm btn-success" onclick="updateKitchenStatus(${o.order_id}, 'เสิร์ฟแล้ว')">เสิร์ฟแล้ว</button>
            <button class="btn btn-sm btn-danger" onclick="updateKitchenStatus(${o.order_id}, 'ยกเลิก')">ยกเลิก</button>
          </div>
        </div>
      `;
    });
  }

  async function updateKitchenStatus(oid, status) {
    await fetch('/api/kitchen/update', {
      method: 'POST',
      body: JSON.stringify({ order_id: oid, status: status })
    });
    fetchData();
  }

  // --- จัดการเมนู ---
  function renderMenu() {
    const tbody = document.getElementById('menu-list-body');
    tbody.innerHTML = '';
    AppData.menu.forEach(m => {
      tbody.innerHTML += `
        <tr>
          <td>${m.image_path ? `<img src="${m.image_path}" style="width: 45px; height: 45px; object-fit: cover; border-radius: 4px;">` : 'ไม่มีภาพ'}</td>
          <td><b>${m.name}</b></td>
          <td>${m.category}</td>
          <td>${m.price.toFixed(2)} ฿</td>
          <td>
            <span class="badge ${m.is_available ? 'bg-free' : 'bg-occupied'}">${m.is_available ? 'พร้อมขาย' : 'หมด'}</span>
          </td>
          <td>
            <button class="btn btn-sm btn-warning" onclick="toggleMenuStatus(${m.id})">สลับสถานะ</button>
          </td>
        </tr>
      `;
    });
  }

  function showAddMenuForm() {
    document.getElementById('menu-id').value = '';
    document.getElementById('menu-name').value = '';
    document.getElementById('menu-category').value = '';
    document.getElementById('menu-price').value = '';
    document.getElementById('menu-file').value = '';
    document.getElementById('menu-form-card').style.display = 'block';
  }

  async function toggleMenuStatus(id) {
    await fetch('/api/menu/toggle', { method: 'POST', body: JSON.stringify({ id: id }) });
    fetchData();
  }

  async function saveMenu() {
    const name = document.getElementById('menu-name').value.trim();
    const category = document.getElementById('menu-category').value.trim() || 'ทั่วไป';
    const price = parseFloat(document.getElementById('menu-price').value);
    const fileInput = document.getElementById('menu-file');

    if(!name || isNaN(price)) {
      alert('กรุณากรอกชื่อและราคาให้ถูกต้อง');
      return;
    }

    let base64 = "";
    let filename = "";
    if(fileInput.files.length > 0) {
      const file = fileInput.files[0];
      filename = file.name;
      base64 = await new Promise((resolve) => {
        const reader = new FileReader();
        reader.onload = () => resolve(reader.result);
        reader.readAsDataURL(file);
      });
    }

    await fetch('/api/menu/save', {
      method: 'POST',
      body: JSON.stringify({
        name: name,
        category: category,
        price: price,
        is_available: true,
        image_filename: filename,
        image_base64: base64
      })
    });

    document.getElementById('menu-form-card').style.display = 'none';
    fetchData();
  }

  // --- เช็คบิล / ใบเสร็จ ---
  function renderCheckoutOptions() {
    const sel = document.getElementById('checkout-table-select');
    sel.innerHTML = '<option value="">-- เลือกโต๊ะ --</option>';
    AppData.tables.forEach(t => {
      sel.innerHTML += `<option value="${t.table_id}">โต๊ะ ${t.table_id} (${t.status})</option>`;
    });
  }

  async function processCheckout() {
    const tid = parseInt(document.getElementById('checkout-table-select').value);
    const discount = parseFloat(document.getElementById('checkout-discount').value) || 0;
    if(!tid) { alert('กรุณาเลือกโต๊ะก่อน'); return; }

    const res = await fetch('/api/checkout', {
      method: 'POST',
      body: JSON.stringify({ table_id: tid, discount: discount })
    });
    const result = await res.json();
    if(result.success) {
      showReceipt(result.receipt);
      fetchData();
    } else {
      alert(result.message || 'ไม่สามารถชำระเงินได้');
    }
  }

  function showReceipt(r) {
    document.getElementById('rec-id').innerText = r.receipt_id;
    document.getElementById('rec-date').innerText = r.timestamp;
    document.getElementById('rec-table').innerText = `โต๊ะ: ${r.table_id}`;
    
    let tableHtml = '';
    r.items.forEach(it => {
      tableHtml += `<tr><td>${it.name} x${it.qty}</td><td style="text-align: right;">${(it.price * it.qty).toFixed(2)}</td></tr>`;
    });
    document.getElementById('rec-items').innerHTML = tableHtml;
    document.getElementById('rec-sub').innerText = r.subtotal.toFixed(2) + ' ฿';
    document.getElementById('rec-disc').innerText = `-${r.discount_amount.toFixed(2)} ฿ (${r.discount_percent}%)`;
    document.getElementById('rec-sc').innerText = `+${r.service_charge.toFixed(2)} ฿`;
    document.getElementById('rec-vat').innerText = `+${r.vat.toFixed(2)} ฿`;
    document.getElementById('rec-net').innerText = r.net_total.toFixed(2) + ' ฿';
    document.getElementById('printable-receipt').style.display = 'block';
  }

  // --- รายงานยอดขาย ---
  async function loadReport() {
    const dInput = document.getElementById('report-date');
    if(!dInput.value) {
      dInput.value = new Date().toISOString().split('T')[0];
    }
    const res = await fetch(`/api/report?date=${dInput.value}`);
    const rep = await res.json();
    document.getElementById('report-total-rev').innerText = rep.total_revenue.toFixed(2) + ' ฿';
    document.getElementById('report-total-bills').innerText = rep.total_bills + ' ใบ';

    const tbody = document.getElementById('report-best-sellers');
    tbody.innerHTML = '';
    if(rep.best_sellers.length === 0) {
      tbody.innerHTML = '<tr><td colspan="3" style="text-align: center; color: #64748b;">ไม่มีข้อมูลการขายในวันนี้</td></tr>';
    } else {
      rep.best_sellers.forEach((item, idx) => {
        tbody.innerHTML += `<tr><td>#${idx+1}</td><td>${item.name}</td><td>${item.qty} จาน</td></tr>`;
      });
    }
  }

  setInterval(() => {
    document.getElementById('current-time').innerText = new Date().toLocaleTimeString('th-TH');
  }, 1000);

  // เริ่มต้นโหลดข้อมูล
  fetchData();
</script>
</body>
</html>
"""