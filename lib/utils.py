'''
    release date: 2025-06-09
'''
import cv2
import numpy as np
import os
import threading
from datetime import datetime, timedelta
import pymysql

def find_or_create_folder(gdrive, parent_folder_id, folder_name):
    """Google Drive에서 폴더를 찾거나 생성하며, 예외 발생 시 None을 반환합니다."""
    try:
        query = f"title='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
        if parent_folder_id:
            query += f" and '{parent_folder_id}' in parents"
        else:
            query += " and 'root' in parents"

        list_params = {
            'q': query,
            'supportsAllDrives': True,
            'includeItemsFromAllDrives': True
        }
        file_list = gdrive.ListFile(list_params).GetList()
        # file_list = gdrive.ListFile({'q': query}).GetList()

        if file_list:
            return file_list[0]['id']
        else:
            file_metadata = {
                'title': folder_name,
                'mimeType': 'application/vnd.google-apps.folder'
            }
            if parent_folder_id:
                file_metadata['parents'] = [{'id': parent_folder_id}]

            folder = gdrive.CreateFile(metadata=file_metadata)
            folder.Upload({'supportsAllDrives': True})
            # folder.Upload()
            print(f"폴더 '{folder_name}'이(가) 생성되었습니다. (ID: {folder['id']})")
            return folder['id']
    except Exception as e:
        print(f"폴더 처리 중 오류 발생: {e}")
        return None

def upload_video_to_drive(gdrive, file_path, parent_folder_id=None):
    if not os.path.exists(file_path):
        print(f"오류: 파일 '{file_path}'을 찾을 수 없습니다.")
        return None

    now = datetime.now()
    date_folder_name = now.strftime("%y%m%d")

    target_folder_id = find_or_create_folder(gdrive, parent_folder_id, date_folder_name)

    if not target_folder_id:
        print("업로드할 폴더를 준비하지 못해 파일 업로드를 중단합니다.")
        return None
    
    title = os.path.basename(file_path)
    file_metadata = {'title': title, 'parents': [{'id': target_folder_id}]}

    try:
        file = gdrive.CreateFile(metadata=file_metadata)
        file.SetContentFile(file_path)
        print(f"파일 '{title}' 업로드 중...")
        # file.Upload()
        file.Upload({'supportsAllDrives': True})
        print(f"파일 '{title}'이 '{date_folder_name}' 폴더에 업로드되었습니다. (ID: {file['id']})")


        permission_body = {'type': 'anyone', 'role': 'reader'}
        gdrive.auth.service.permissions().insert(
            fileId=file['id'],
            body=permission_body,
            supportsAllDrives=True
        ).execute()
        
        updated_file_info = gdrive.auth.service.files().get(
            fileId=file['id'],
            fields='alternateLink', # 필요한 필드만 요청 (공유 링크)
            supportsAllDrives=True
        ).execute()
        share_url = updated_file_info.get('alternateLink')
        
        # file.InsertPermission({
        #     'type': 'anyone',  # '링크가 있는 모든 사용자'
        #     'role': 'reader'   # '뷰어' (읽기) 권한
        # })
                
        # file.FetchMetadata(fetch_all=True)  # 전체 메타데이터를 다시 가져옵니다.
        # share_url = file['alternateLink']
        return share_url
    except Exception as e:
        print(f"파일 업로드 중 오류 발생: {e}")
        return None


#   ---   위반 내역 DB 기록  ---
def insert_violation_to_db(db_conn, event_dttm, div_cd, start_dttm, end_dttm, file_nm, link_addr):
    """데이터베이스에 위반 기록을 삽입하고, 실패 시 에러 로그를 파일로 저장합니다."""
    if not db_conn or not db_conn.open: # 연결이 없거나 닫힌 경우 확인
        print("❌ DB 연결이 유효하지 않아 저장을 건너뜁니다.")
        # 로그 파일 생성 로직을 여기에 추가할 수도 있습니다. (예: 연결 실패 로그)
        # 이 경우, db_conn이 None일 수 있으므로 rollback() 호출 시 주의
        error_log_dir = "db_error_logs"
        os.makedirs(error_log_dir, exist_ok=True)
        timestamp_err = datetime.now().strftime("%Y%m%d_%H%M%S_%f") # 마이크로초 추가하여 파일명 고유성 강화
        error_log_filename = os.path.join(error_log_dir, f"db_conn_error_{timestamp_err}.txt")
        with open(error_log_filename, "w", encoding="utf-8") as f:
            f.write(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}\n")
            f.write("Error Message: DB Connection is not valid or closed.\n")
            f.write(f"Attempted to insert: {file_nm}\n")
        print(f"📄 DB 연결 오류 로그가 '{error_log_filename}' 파일에 저장되었습니다.")
        return False

    sql = """
        INSERT INTO dc_biosec_violation_hist
        (event_dttm, detection_target_div_cd, record_start_dttm, record_end_dttm,
         snapshot_file_nm, snapshot_drive_link_addr, reg_dttm)
        VALUES (%s, %s, %s, %s, %s, %s, NOW())
    """
    values = (event_dttm, div_cd, start_dttm, end_dttm, file_nm, link_addr)
    cursor = None
    try:
        cursor = db_conn.cursor()
        cursor.execute(sql, values)
        db_conn.commit()
        print(f"💾 DB 저장 성공: {file_nm}")
        return True
    except pymysql.Error as e:
        # pymysql.Error 발생 시 상세 로깅
        print(f"❌ DB 저장 실패 (pymysql.Error) - Raw: {e}")
        print(f"❌ DB 저장 실패 (pymysql.Error) - Repr: {repr(e)}")
        print(f"❌ DB 저장 실패 (pymysql.Error) - Args: {e.args}")
        print(f"❌ DB 저장 실패 (pymysql.Error) - Type: {type(e).__name__}")

        if db_conn and db_conn.open: # 연결이 유효하면 롤백 시도
            try:
                db_conn.rollback()
                print("↪️ DB 롤백 시도됨.")
            except Exception as rb_err:
                print(f"⚠️ DB 롤백 중 오류 발생: {rb_err}")
        else:
            print("⚠️ DB 연결이 유효하지 않아 롤백을 건너뜁니다.")


        error_log_dir = "db_error_logs"
        os.makedirs(error_log_dir, exist_ok=True)
        timestamp_err = datetime.now().strftime("%Y%m%d_%H%M%S_%f") # 마이크로초 추가
        error_log_filename = os.path.join(error_log_dir, f"db_insert_error_{timestamp_err}.txt")
        
        with open(error_log_filename, "w", encoding="utf-8") as f:
            f.write(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}\n")
            f.write(f"Error Type: {type(e).__name__}\n")
            f.write(f"Error Raw: {str(e)}\n")
            f.write(f"Error Repr: {repr(e)}\n")
            f.write(f"Error Args: {str(e.args)}\n\n")
            f.write("SQL Query:\n")
            f.write(f"{sql}\n\n")
            f.write("Values:\n")
            f.write(f"{str(values)}\n")
        print(f"📄 DB 저장 에러 상세 로그가 '{error_log_filename}' 파일에 저장되었습니다.")
        return False
    except Exception as ex:
        # pymysql.Error 외의 다른 예외 발생 시 (예: cursor 생성 실패 등)
        print(f"❌ DB 작업 중 예기치 않은 오류 발생 - Raw: {ex}")
        print(f"❌ DB 작업 중 예기치 않은 오류 발생 - Repr: {repr(ex)}")
        print(f"❌ DB 작업 중 예기치 않은 오류 발생 - Args: {ex.args}")
        print(f"❌ DB 작업 중 예기치 않은 오류 발생 - Type: {type(ex).__name__}")

        if db_conn and db_conn.open:
            try:
                db_conn.rollback()
                print("↪️ DB 롤백 시도됨 (일반 오류).")
            except Exception as rb_err:
                print(f"⚠️ DB 롤백 중 오류 발생 (일반 오류): {rb_err}")
        else:
            print("⚠️ DB 연결이 유효하지 않아 롤백을 건너뜁니다 (일반 오류).")
        
        # 여기에 대한 에러 로그 파일 생성도 고려할 수 있습니다.
        return False
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception as cur_close_err:
                print(f"⚠️ 커서 닫기 중 오류: {cur_close_err}")

def upload_and_cleanup(gdrive, file_path, db_config, parent_folder_id, start_time, event_counter):
    """
    파일을 Google Drive에 업로드하고, DB에 기록한 후 로컬 파일을 정리합니다.
    DB 연결은 이 함수 내에서 스레드별로 생성 및 관리됩니다.
    """
    db_conn_thread = None
    try:
        # 스레드별 DB 연결 생성
        print(f"🧵 [{threading.get_ident()}] DB 연결 시도 중...")
        db_conn_thread = pymysql.connect(**db_config)
        print(f"🧵 [{threading.get_ident()}] DB 연결 성공.")

        share_url = upload_video_to_drive(gdrive, file_path, parent_folder_id)
        if share_url:
            filename = os.path.basename(file_path)
            event_dt = datetime.fromtimestamp(start_time)
            # 시간 계산 시 시간대(timezone) 고려가 필요할 수 있습니다.
            # 기본적으로 로컬 시간대를 사용합니다.
            start_dt = event_dt - timedelta(seconds=3) 
            end_dt = event_dt + timedelta(seconds=3)

            event_dttm_str = event_dt.strftime('%Y-%m-%d %H:%M:%S')
            record_start_str = start_dt.strftime('%Y-%m-%d %H:%M:%S')
            record_end_str = end_dt.strftime('%Y-%m-%d %H:%M:%S')

            people = event_counter.get("worker", 0)
            pig = event_counter.get("pig", 0)

            if people > 0 and pig > 0:
                div_cd = 0 # 복합
            elif people > 0:
                div_cd = 1 # 사람
            elif pig > 0:
                div_cd = 2 # 돼지
            else:
                div_cd = 9 # 알 수 없음

            db_success = insert_violation_to_db(
                db_conn_thread, event_dttm_str, div_cd, record_start_str,
                record_end_str, filename, share_url
            )

            if db_success:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    print(f"🗑️ 업로드 후 로컬 클립 삭제 완료: {file_path}")
                else:
                    print(f"⚠️ 로컬 클립 파일이 이미 삭제되었거나 찾을 수 없음: {file_path}")
            else:
                print(f"⚠️ DB 저장 실패로 로컬 파일 유지: {file_path}")
        else:
            print(f"파일 업로드 실패 (또는 정보 부족)로 인해 DB 저장 및 로컬 삭제를 건너뜀: {file_path}")

    except pymysql.Error as db_e: # DB 연결 생성 실패 등 pymysql 관련 오류
        print(f"❌ 업로드/정리 중 DB 관련 오류(pymysql.Error) 발생 - 스레드 [{threading.get_ident()}]")
        print(f"  - Raw: {db_e}")
        print(f"  - Repr: {repr(db_e)}")
        print(f"  - Args: {db_e.args}")
        print(f"  - Type: {type(db_e).__name__}")
        # 여기에 대한 에러 로그 파일 생성을 고려할 수 있습니다.
    except Exception as e:
        # 기타 모든 예외 (파일 업로드, os.remove 등 포함)
        print(f"❌ 업로드 또는 삭제 중 일반 예외 발생 - 스레드 [{threading.get_ident()}]")
        print(f"  - Raw: {e}")
        print(f"  - Repr: {repr(e)}")
        print(f"  - Args: {e.args}")
        print(f"  - Type: {type(e).__name__}")
        # 여기에 대한 에러 로그 파일 생성을 고려할 수 있습니다.
    finally:
        if db_conn_thread and db_conn_thread.open:
            try:
                db_conn_thread.close()
                print(f"🧵 [{threading.get_ident()}] DB 연결 닫힘.")
            except Exception as close_err:
                print(f"⚠️ [{threading.get_ident()}] DB 연결 닫기 중 오류: {close_err}")

def save_infos(frames, start_time, event_counter, gdrive, db_config):                # , parent_folder_id=None
    if not frames:
        return
    # filename = datetime.fromtimestamp(start_time).strftime("%Y%m%d_%H%M%S") + ".mp4"
    filename = format_violation_filename(start_time, event_counter)
    temp_dir = "temp_clips"
    os.makedirs(temp_dir, exist_ok=True)
    out_path = os.path.join(temp_dir, filename)

    height, width = frames[0].shape[:2]
    out = cv2.VideoWriter(out_path, cv2.VideoWriter_fourcc(*"mp4v"), 15.0, (width, height))
    for f in frames:
        out.write(f)
    out.release()

    print(f"🎞️ 저장된 클립: {out_path} | 작업자: {event_counter['worker']} 명, 돼지: {event_counter['pig']} 마리")
    
    if gdrive:
        parent_folder_id = "0AE8IjXvFrukSUk9PVA"              # folder ID 개인 계정:     1ymI94ojlsHxDIi3OHFA13VYTVWNVImVK
        upload_thread = threading.Thread(
            target=upload_and_cleanup,
            args=(gdrive, out_path, db_config, parent_folder_id, start_time, event_counter),
            daemon=True
        )
        upload_thread.start()
    else:
        print(f"[FAIL] gdrive 객체가 유효하지 않아 Google Drive 업로드 및 DB 저장을 건너뜁니다: {filename}")

def format_timestamp(ts):
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")

def is_above_line(point, line):
    (x1, y1), (x2, y2) = line
    x, y = point
    return (x2 - x1)*(y - y1) - (y2 - y1)*(x - x1) < 0

def motion_detected_background(prev_gray, curr_gray, bg_subtractor, threshold):
    if prev_gray is None:
        return False
    fg_mask = bg_subtractor.apply(curr_gray)
    motion_amount = np.count_nonzero(fg_mask)
    return motion_amount > threshold

def draw_line(frame, line):
    cv2.line(frame, line[0], line[1], (0, 255, 255), 2)

# def draw_detection_box(frame, box, label, track_id, is_alert):
#     x1, y1, x2, y2 = box
#     color = (0, 0, 255) if is_alert else (255, 0, 0)
#     cv2.rectangle(frame, (x1, y1), (x2, y2), color, 2)
#     cv2.putText(frame, f"{label}-{track_id}", (x1, y1 - 10),
#                 cv2.FONT_HERSHEY_SIMPLEX, 0.5, color, 2)
def draw_detection_box(frame, box, label, track_id, crossed=False):
    x1, y1, x2, y2 = map(int, box)
    if crossed:
        overlay = frame.copy()
        cv2.rectangle(overlay, (x1, y1), (x2, y2), (0, 0, 255), -1)
        frame[:] = cv2.addWeighted(overlay, 0.4, frame, 0.6, 0)
    else:
        color = (229, 209, 92) if label == 'worker' else (0, 255, 0)
        cv2.rectangle(frame, (x1, y1), (x2, y2), color, 2)

    cv2.putText(frame, label, (x1, y1 - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (255, 255, 255), 1)
    cv2.putText(frame, f"ID: {track_id}", (x1, y2 + 15), cv2.FONT_HERSHEY_SIMPLEX, 0.4, (255, 255, 0), 1)
    
def format_violation_filename(timestamp, event_counter):
    dt = datetime.fromtimestamp(timestamp)
    time_str = dt.strftime("%y%m%d_%H%M%S")
    # violation_code = "0" * event_counter["people"] + "1" * event_counter["pig"]
    people_count = event_counter.get("worker", 0)
    pig_count = event_counter.get("pig", 0)
    violation_code = f"Worker{people_count}Pig{pig_count}" 
    return f"{time_str}_{violation_code}.mp4"



