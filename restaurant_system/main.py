"""
main.py - จุดเริ่มต้นโปรแกรม (Main Entry Point)
มีระบบจัดการ Menu Loop (While), Try-Except และออกจากโปรแกรมได้อย่างปลอดภัย
"""

import importlib.util
import sys
import threading
from http.server import HTTPServer
from pathlib import Path

import storage

PROJECT_ROOT = Path(__file__).resolve().parent

try:
    import services
except ModuleNotFoundError:
    services_path = PROJECT_ROOT / "services.py"
    if not services_path.exists():
        raise
    spec = importlib.util.spec_from_file_location("services", services_path)
    if spec is None or spec.loader is None:
        raise ModuleNotFoundError(f"Cannot load services module: {services_path}")
    services = importlib.util.module_from_spec(spec)
    sys.modules["services"] = services
    spec.loader.exec_module(services)

from server import RestaurantHandler

PORT = 8000
httpd_server = None
server_thread = None


def start_web_server():
    """ฟังก์ชันที่ 8: เริ่มรันเว็บเซิร์ฟเวอร์แบบเบื้องหลัง (Background Thread)"""
    global httpd_server
    try:
        httpd_server = HTTPServer(("0.0.0.0", PORT), RestaurantHandler)
        print(f"\n[SERVER] เริ่มต้นเซิร์ฟเวอร์สำเร็จที่: http://localhost:{PORT}")
        print(f"[SERVER] สามารถเปิดผ่านเบราว์เซอร์ได้ทันที\n")
        httpd_server.serve_forever()
    except OSError as e:
        print(f"[ERROR] ไม่สามารถเปิด Port {PORT} ได้: {e}")
    except Exception:
        print("[ERROR] เกิดข้อผิดพลาดในการรันเซิร์ฟเวอร์")


def display_console_menu():
    """แสดงรายการเมนูการทำงานทาง Terminal"""
    print("\n" + "=" * 45)
    print("      ระบบจัดการร้านอาหาร (RESTAURANT CMS)")
    print("=" * 45)
    print("  [1] เริ่มการทำงาน Web Application (Port 8000)")
    print("  [2] แสดงรายงานยอดขายวันนี้ทางหน้าจอ")
    print("  [3] รีเซ็ตฐานข้อมูลเป็นค่าเริ่มต้น")
    print("  [0] ออกจากโปรแกรมอย่างปลอดภัย")
    print("=" * 45)


def main():
    """ฟังก์ชันหลัก: มี While loop และ Try-Except ป้องกันการแครชทุกจุด"""
    global server_thread, httpd_server

    # โหลดฐานข้อมูลเบื้องต้น
    storage.load_database()

    is_running: bool = True
    server_started: bool = False

    while is_running:
        try:
            display_console_menu()
            choice: str = input("กรุณาเลือกคำสั่ง (0-3): ").strip()

            if choice == "1":
                if not server_started:
                    server_thread = threading.Thread(target=start_web_server, daemon=True)
                    server_thread.start()
                    server_started = True
                else:
                    print(f"\n[INFO] เซิร์ฟเวอร์กำลังทำงานอยู่ที่ http://localhost:{PORT}")

            elif choice == "2":
                report = services.generate_sales_report()
                print("\n" + "-" * 35)
                print(f"รายงานยอดขายประจำวันที่: {report['date']}")
                print(f"ยอดขายสุทธิ: {report['total_revenue']} บาท")
                print(f"จำนวนบิลที่เช็คแล้ว: {report['total_bills']} บิล")
                print("เมนูขายดี 5 อันดับแรก:")
                for idx, item in enumerate(report["best_sellers"], 1):
                    print(f"  {idx}. {item['name']} - จำนวน {item['qty']} จาน")
                print("-" * 35)

            elif choice == "3":
                confirm = input("ยืนยันการคืนค่าเริ่มต้นข้อมูลทั้งหมดหรือไม่? (y/N): ").strip().lower()
                if confirm == "y":
                    default_data = storage.get_default_data()
                    storage.save_database(default_data)
                    print("[SUCCESS] คืนค่าข้อมูลเริ่มต้นเรียบร้อยแล้ว")
                else:
                    print("[INFO] ยกเลิกการคืนค่าข้อมูล")

            elif choice == "0":
                print("\nกำลังปิดระบบและบันทึกข้อมูล...")
                if httpd_server:
                    httpd_server.shutdown()
                    httpd_server.server_close()
                is_running = False
                print("ออกจากโปรแกรมเรียบร้อยแล้ว ขอบคุณที่ใช้งานครับ.")
                sys.exit(0)

            else:
                print("[WARNING] คำสั่งไม่ถูกต้อง กรุณาระบุหมายเลข 0 - 3")

        except KeyboardInterrupt:
            # ดักจับ Ctrl + C ออกจากโปรแกรมได้โดยไม่แสดง Traceback
            print("\n\n[INFO] ตรวจพบการยกเลิกโดยผู้ใช้ กำลังปิดระบบ...")
            if httpd_server:
                httpd_server.shutdown()
            is_running = False
            sys.exit(0)
        except Exception:
            # ดักจับข้อผิดพลาดทั่วไป ป้องกันหน้าจอดำ/traceback หลุด
            print("\n[ERROR] เกิดข้อผิดพลาดบางประการ โปรดลองใหม่อีกครั้ง")


if __name__ == "__main__":
    main()