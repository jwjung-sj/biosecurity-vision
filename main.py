# main_dev.py

from ultralytics import YOLO
import subprocess
import torch
import time
import sys
import os
import pymysql
import argparse
import configparser
import numpy as np
import threading
from datetime import datetime, date
from pathlib import Path
from queue import Queue, Empty

# 사용자 정의 라이브러리
from lib.utils import format_timestamp
from lib.video_processor import process_video
from lib.service_manager import DriveManager
from lib.warning_client_manager import RPIClient, WebhookClient

OUR_MODEL = 'lib/model/251120_s_best.pt'
# OUR_MODEL = 'lib/model/250912_s_best.pt'

# =========================================================
# 1. 설정 로드 및 Factory 로직
# =========================================================
def load_farm_config(config_path, farm_name):
    if not os.path.exists(config_path):
        print(f"❌ 설정 파일을 찾을 수 없습니다: {config_path}")
        sys.exit(1)

    cfg = configparser.ConfigParser()
    cfg.read(config_path, encoding='utf-8')
    
    if farm_name not in cfg:
        print(f"❌ 설정 파일에 [{farm_name}] 섹션이 없습니다.")
        sys.exit(1)
    
    farm_config = dict(cfg[farm_name])
    
    # [DEFAULT] 섹션의 템플릿 상속 확인
    if 'webhook_template' not in farm_config and 'webhook_template' in cfg['DEFAULT']:
        farm_config['webhook_template'] = cfg['DEFAULT']['webhook_template']

    def safe_get(key, default, type_func):
        val = farm_config.get(key)
        if not val:  # 값이 없거나 빈 문자열('')인 경우
            return default
        try:
            return type_func(val)
        except (ValueError, TypeError):
            print(f"⚠️ [{key}] 설정값 오류('{val}'). 기본값 {default}을 사용합니다.")
            return default

    # 형변환
    farm_config['farm_code'] = safe_get('farm_code', int(farm_config.get('cd', 0)), int)
        
    if farm_config['farm_code'] == 0:
        print("❌ 'farm_code' 설정 누락. DB 기록 불가.")
        sys.exit(1)

    farm_config['pig_reenter_thresh'] = safe_get('pig_reenter_thresh', 0.35, float)
    farm_config['worker_conf'] = safe_get('worker_conf', 0.6, float)
    farm_config['motion_threshold'] = safe_get('motion_threshold', 300, int)
    
    return farm_config

def create_warning_client(farm_config):
    w_type = farm_config.get('warning_type', 'none').lower()
    farm_code = farm_config.get('farm_code')
    
    # [CASE 1] RPI (Socket)
    if w_type == 'rpi':
        host = farm_config.get('host')
        port = farm_config.get('port')
        if host and port:
            print(f"🔌 [{farm_code}] 경고 장치: RPI Socket ({host}:{port})")
            return RPIClient(host, int(port))
        else:
            print("❌ RPI 설정 오류: host/port 누락")
            return None

    # [CASE 2] Webhook (HTTP)
    elif w_type == 'webhook':

        # 1순위: 전체 URL이 설정되어 있는지 확인
        url = farm_config.get('webhook_full_url')
        
        # 2순위: 템플릿 사용 (기존 로직)
        if not url and 'webhook_template' in farm_config:
            template = farm_config.get('webhook_template')
            try:
                url = template.format(**farm_config)
            except KeyError:
                pass

        # URL이 없거나 비어있으면 None 반환
        if not url:
            print(f"⚠️ [{farm_code}] Webhook 설정이 비어있습니다. 경광등 기능 없이 모니터링만 수행합니다.")
            return None
        
        print(f"🔗 [{farm_code}] 경고 장치: Webhook ({url})")
        return WebhookClient(url)

    print(f"🚫 [{farm_code}] 경고 장치 미사용")
    return None

    # elif w_type == 'webhook' or w_type == 'interface':
    #     template = farm_config.get('webhook_template')
    #     if not template:
    #         # 템플릿 없으면 직접 URL 사용 시도
    #         if 'webhook_full_url' in farm_config:
    #             url = farm_config['webhook_full_url']
    #         else:
    #             print("❌ Webhook 템플릿 없음.")
    #             return None
    #     else:
    #         try:
    #             # 템플릿 변수 치환 ({host}, {token} 등)
    #             url = template.format(**farm_config)
    #         except KeyError as e:
    #             print(f"❌ Webhook URL 생성 실패: 필수 값 {e} 누락")
    #             return None
        
    #     print(f"🔗 [{farm_code}] 경고 장치: Webhook ({url})")
    #     return WebhookClient(url)

    # print(f"🚫 [{farm_code}] 경고 장치 미사용")
    # return None

# =========================================================
# 2. DailyCountManager
# =========================================================
class DailyCountManager:
    def __init__(self, db_config, farm_cd):
        self.db_config = db_config
        self.farm_cd = farm_cd
        self.count = 0
        self.last_save_date = date.today()
        self.lock = threading.Lock()
        self._stop_event = threading.Event()

    def load_initial_count(self):
        if not self.farm_cd: return
        today_ymd = self.last_save_date.strftime('%y%m%d')
        query = "SELECT shipment_headno FROM dc_piglet_shipment_day_aggr WHERE farm_div_cd = %s AND shipment_ymd = %s"
        try:
            conn = pymysql.connect(**self.db_config)
            with conn.cursor() as cursor:
                cursor.execute(query, (self.farm_cd, today_ymd))
                res = cursor.fetchone()
                with self.lock:
                    self.count = res['shipment_headno'] if res else 0
                print(f"📈 오늘 출하량 로드: {self.count}두")
        except Exception as e:
            print(f"❌ DB 초기 로드 오류: {e}")
        finally:
            if 'conn' in locals() and conn.open: conn.close()

    def save_or_update_count(self, target_date, count):
        if not self.farm_cd: return
        target_ymd = target_date.strftime('%y%m%d')
        query = """
            INSERT INTO dc_piglet_shipment_day_aggr (farm_div_cd, shipment_ymd, shipment_headno, reg_dttm)
            VALUES (%s, %s, %s, NOW())
            ON DUPLICATE KEY UPDATE shipment_headno = VALUES(shipment_headno), reg_dttm = NOW()
        """
        try:
            conn = pymysql.connect(**self.db_config)
            with conn.cursor() as cursor:
                cursor.execute(query, (self.farm_cd, target_ymd, count))
            conn.commit()
        except Exception as e:
            print(f"❌ DB 저장 오류: {e}")
        finally:
            if 'conn' in locals() and conn.open: conn.close()

    def increment(self):
        with self.lock: self.count += 1; return self.count
    def decrement(self):
        with self.lock: self.count -= 1; return self.count
    def get_current_count(self):
        with self.lock: return self.count

    def run_periodic_check(self):
        while not self._stop_event.is_set():
            try:
                today = date.today()
                if today != self.last_save_date:
                    yesterday_cnt = self.get_current_count()
                    if yesterday_cnt > 0: self.save_or_update_count(self.last_save_date, yesterday_cnt)
                    with self.lock:
                        self.count = 0; self.last_save_date = today
                else:
                    curr = self.get_current_count()
                    if curr > 0: self.save_or_update_count(today, curr)
                self._stop_event.wait(60)
            except Exception: self._stop_event.wait(60)

    def stop(self): self._stop_event.set()

# =========================================================
# 3. 메인 로직
# =========================================================
def log_connection_status(db_config, farm_cd, status):
    if not farm_cd: return
    try:
        conn = pymysql.connect(**db_config)
        with conn.cursor() as cur:
            cur.execute("INSERT INTO dc_camera_connect_hist (farm_div_cd, event_dttm, connect_yn, reg_dttm) VALUES (%s, NOW(), %s, NOW())", (farm_cd, status))
        conn.commit()
    except: pass
    finally:
        if 'conn' in locals() and conn.open: conn.close()

def ffmpeg_frame_reader(proc_stdout, queue, size, stop_ev):
    while not stop_ev.is_set():
        try:
            raw = proc_stdout.read(size)
            if len(raw) == size: queue.put(raw)
            else: break
        except: break

def main_rtsp(rtsp_url, gdrive, db_config, warning_client, farm_cd, conn_status, shutdown, count_mgr, farm_config):
    width, height = 640, 384
    frame_size = width * height * 3
    fps = 15.0
    model = YOLO(OUR_MODEL)
    
    print(f"🚀 RTSP 시작 (Farm: {farm_cd})")

    while True:
        if shutdown['manual_quit']: break
        process, reader = None, None
        stop_ev = threading.Event()
        queue = Queue(maxsize=30)
        first_raw = None

        try:
            process = subprocess.Popen([
                "ffmpeg", "-rtsp_transport", "tcp", "-i", rtsp_url, "-vf", f"scale={width}:{height}",
                "-f", "rawvideo", "-pix_fmt", "bgr24", "-vcodec", "rawvideo", "-loglevel", "warning", "-"
            ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=10**8)
            
            time.sleep(2)
            if process.poll() is not None: raise RuntimeError("FFmpeg Start Fail")

            reader = threading.Thread(target=ffmpeg_frame_reader, args=(process.stdout, queue, frame_size, stop_ev), daemon=True)
            reader.start()

            try:
                first_raw = queue.get(timeout=5)
                if len(first_raw) != frame_size: raise RuntimeError("Invalid Frame")
            except Empty: raise RuntimeError("No Frame")

            print("✅ 스트림 연결 성공")
            if not conn_status['is_connected']:
                log_connection_status(db_config, farm_cd, 'Y')
                conn_status['is_connected'] = True

            def get_frame():
                nonlocal first_raw
                if first_raw: d = first_raw; first_raw = None; return np.frombuffer(d, dtype=np.uint8).reshape((height, width, 3)).copy()
                try: return np.frombuffer(queue.get(timeout=3), dtype=np.uint8).reshape((height, width, 3)).copy()
                except: return None

            # [중요] farm_config 전달
            process_video(get_frame, model, gdrive, db_config, warning_client, shutdown, count_mgr, 
                          farm_config=farm_config, fps=fps, width=width, height=height)

        except RuntimeError as e:
            if conn_status['is_connected']:
                log_connection_status(db_config, farm_cd, 'N')
                conn_status['is_connected'] = False
            print(f"🔄 재연결 대기: {e}")
            time.sleep(5)
        except Exception as e:
            print(f"❌ 오류: {e}")
            break
        finally:
            if stop_ev: stop_ev.set()
            if process: process.terminate()

def main_video(path, gdrive, db_config, warning_client, shutdown, count_mgr, farm_config, rec_path=None):
    from cv2 import VideoCapture
    cap = VideoCapture(path)
    if not cap.isOpened(): return
    fps = cap.get(5)
    model = YOLO(OUR_MODEL)
    if torch.cuda.is_available(): model.to('cuda')
    
    def get_frame():
        ret, f = cap.read()
        return f if ret else None

    process_video(get_frame, model, gdrive, db_config, warning_client, shutdown, count_mgr, 
                  farm_config=farm_config, fps=fps, width=640, height=384, record_output_path=rec_path)
    cap.release()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--rtsp", help="RTSP URL")
    group.add_argument("--video", help="Video path")
    parser.add_argument("--farm-name", required=True)
    parser.add_argument("--record", action="store_true")
    args = parser.parse_args()

    # 1. Config 로드
    CONFIG_PATH = './lib/farm_config.ini'
    DB_PATH = './lib/db_info_config.ini'
    
    farm_config = load_farm_config(CONFIG_PATH, args.farm_name)
    farm_idx = farm_config['farm_code']

    # 2. DB Config
    db_p = configparser.ConfigParser()
    db_p.read(DB_PATH)
    dbs = db_p['database']
    DB_CONFIG = {
        'host': dbs.get('host'), 'user': dbs.get('user'), 'password': dbs.get('password'),
        'db': dbs.get('db_name'), 'port': dbs.getint('port'), 'charset': 'utf8mb4',
        'cursorclass': pymysql.cursors.DictCursor
    }

    # 3. Client & Drive & Counter
    warning_client = create_warning_client(farm_config)
    if warning_client and hasattr(warning_client, 'connect'): warning_client.connect()
    
    drive_manager = DriveManager()
    count_manager = DailyCountManager(DB_CONFIG, farm_idx)
    count_manager.load_initial_count()
    t = threading.Thread(target=count_manager.run_periodic_check, daemon=True)
    t.start()

    shutdown = {'manual_quit': False}
    conn = {'is_connected': False}

    try:
        if args.rtsp:
            main_rtsp(args.rtsp, drive_manager, DB_CONFIG, warning_client, farm_idx, conn, shutdown, count_manager, farm_config)
        elif args.video:
            rec_path = None
            if args.record:
                Path("./recorded_videos").mkdir(exist_ok=True)
                rec_path = f"./recorded_videos/{Path(args.video).stem}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.mp4"
            main_video(args.video, drive_manager, DB_CONFIG, warning_client, shutdown, count_manager, farm_config, rec_path)
    except KeyboardInterrupt: pass
    finally:
        if args.rtsp and conn['is_connected']: log_connection_status(DB_CONFIG, farm_idx, 'N')
        if warning_client: warning_client.close()
        count_manager.stop()
        print("연결 종료.")