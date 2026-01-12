import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from lib.service_manager import get_database_service
from datetime import datetime, timedelta
import pymysql
import schedule
import time
import configparser
import os
from typing import List, Dict, Tuple, Any


GMAIL_CONFIG_PATH = './lib/gmail_config.ini'
DB_CONFIG_PATH = './lib/db_info_config.ini'

def load_configurations(gmail_path: str, db_path: str) -> Tuple[Dict, str]:
    """ 지정된 경로의 ini 파일들을 로드. """
    try:
        config = configparser.ConfigParser()
        config.read([gmail_path, db_path], encoding='utf-8')

        smtp_settings = dict(config.items('smtp'))
        smtp_settings['port'] = int(smtp_settings['port'])

        aes_key = config.get('database', 'aes_key')
        
        print("✅ ini 파일 로드 완료.")
        return smtp_settings, aes_key
    except Exception as e:
        print(f"❌ ini 파일 로드 중 오류 발생: {e}")
        return None, None

def load_ini_config(file_path):
    """지정된 INI 설정 파일을 로드합니다."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"설정 파일 '{file_path}'을(를) 찾을 수 없습니다.")
    config = configparser.ConfigParser()
    config.read(file_path, encoding='utf-8')
    return config

def get_last_week_date_range():
    """지난주 시작/종료 날짜를 'yymmdd' 형식으로 반환합니다."""
    today = datetime.now()
    start_of_current_week = today - timedelta(days=today.weekday())
    start_of_last_week = start_of_current_week - timedelta(days=7)
    end_of_last_week = start_of_last_week + timedelta(days=6)
    return start_of_last_week.strftime('%y%m%d'), end_of_last_week.strftime('%y%m%d')

def get_weekly_violations_from_db(db_conn):
    """지난주 위반 기록을 DB에서 가져옵니다."""
    if not db_conn or not db_conn.open: 
        print("❌ DB 연결이 없습니다.")
        return [], 0, 0

    cursor = None
    try:
        today = datetime.now()
        start_of_current_week = today - timedelta(days=today.weekday())
        start_of_last_week = start_of_current_week - timedelta(days=7)
        start_date = start_of_last_week.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = start_of_current_week.replace(hour=0, minute=0, second=0, microsecond=0)

        sql = """
            SELECT event_dttm, snapshot_file_nm, snapshot_drive_link_addr, detection_target_div_cd
            FROM dc_biosec_violation_hist
            WHERE event_dttm >= %s AND event_dttm < %s
            ORDER BY event_dttm ASC
        """
        cursor = db_conn.cursor() # DictCursor로 설정했으므로 딕셔너리로 반환
        cursor.execute(sql, (start_date, end_date))
        results = cursor.fetchall()

        people_count = sum(1 for row in results if row['detection_target_div_cd'] in ["0", "1"])
        pig_count = sum(1 for row in results if row['detection_target_div_cd'] in ["0", "2"])
        return results, people_count, pig_count
    except pymysql.Error as e:
        print(f"❌ DB 조회 중 오류 발생: {e}")
        return [], 0, 0
    finally:
        if cursor:
            cursor.close()

def get_email_recipients(db_conn, aes_key):
    """DB에서 복호화된 이메일 수신자 목록을 조회합니다."""
    if not db_conn or not db_conn.open:
        print("❌ DB 연결이 없습니다.")
        return []

    recipients_list = []
    cursor = None
    try:
        sql = """
            SELECT CAST(AES_DECRYPT(UNHEX(T1.user_email_addr), %s) AS CHAR) AS decrypted_email
            FROM dw_biosec_user_mas T1
            INNER JOIN dw_biosec_receive_info T2 ON T1.seq = T2.user_seq
            WHERE T2.receive_yn = %s AND T2.alarm_method_div_cd = %s;
        """
        cursor = db_conn.cursor() # DictCursor 사용 시 `row['decrypted_email']`로 접근
        cursor.execute(sql, (aes_key, 'y', 1))
        results = cursor.fetchall()
        # DictCursor를 사용했으므로 row['decrypted_email']로 접근
        recipients_list = [row['decrypted_email'] for row in results if row and row['decrypted_email']]
        print(f"📬 조회된 수신자 목록: {recipients_list}")
        return recipients_list
    except pymysql.Error as e:
        print(f"❌ 수신자 조회(복호화) 중 DB 오류 발생: {e}")
        return []
    except Exception as e:
        print(f"❌ 복호화 또는 처리 중 오류 발생: {e}")
        return []
    finally:
        if cursor:
            cursor.close()

def generate_weekly_summary(
    violation_data, people_count, pig_count,
    first_date, last_date,
    smtp_config, recipients_list, email_subject
):
    """DB 데이터를 기반으로 주간 요약 이메일을 생성하고 발송합니다."""
    msg = MIMEMultipart('alternative')
    msg['From'] = smtp_config['sender_email']
    msg['To'] = ', '.join(recipients_list)
    msg['Subject'] = email_subject

    text_body = ""
    html_body = "<html><head><style>table, td, th {border: 1px solid black; border-collapse: collapse; padding: 5px;} th {background-color: #f2f2f2; text-align: center;} td {text-align: center;}</style></head><body>"
    html_body += f"<h2>주간 위반 감지 요약 ({first_date} ~ {last_date})</h2>"

    total_count = len(violation_data)

    if violation_data:
        summary_text = f"ㆍ총 위반 건수: {total_count} 건\n" \
                       f"\t - 작업자 관련: {people_count} 건\n" \
                       f"\t - 돼지 관련: {pig_count} 건\n" \
                       f"ㆍ 상세 내역 (아래)\n\n"
        summary_html = f"<p>ㆍ총 위반 건수: {total_count} 건<br>" \
                       f"&nbsp;&nbsp;&nbsp;- 작업자 관련: {people_count} 건<br>" \
                       f"&nbsp;&nbsp;&nbsp;- 돼지 관련: {pig_count} 건<br>" \
                       f"ㆍ 상세 내역 (아래)</p>"
        text_body += summary_text
        html_body += summary_html

        html_body += """
            <table style="width:100%;">
                <tr>
                    <th>발생 일시</th>
                    <th>탐지 유형</th>
                    <th>스냅샷 파일명</th>
                    <th>영상 확인 (링크)</th>
                </tr>
        """
        text_body += "발생 일시 | 탐지 유형 | 파일명 | 링크\n"
        text_body += "---|---|---|---\n"

        for record in violation_data:
            event_time = record['event_dttm'].strftime('%Y-%m-%d %H:%M:%S')
            file_name = record['snapshot_file_nm']
            link = record['snapshot_drive_link_addr']
            div_cd = record['detection_target_div_cd']

            if div_cd == '0':
                type_str = "작업자+돼지"
            elif div_cd == '1':
                type_str = "작업자"
            elif div_cd == '2':
                type_str = "돼지"
            else:
                type_str = "알 수 없음"

            html_body += f"""
                <tr>
                    <td>{event_time}</td>
                    <td>{type_str}</td>
                    <td>{file_name}</td>
                    <td><a href='{link}' target='_blank'>영상 보기</a></td>
                </tr>
            """
            text_body += f"{event_time} | {type_str} | {file_name} | {link}\n"

        html_body += "</table>"

    else:
        text_body += f"지난주({first_date} ~ {last_date}) 동안 감지된 위반 사항이 없습니다."
        html_body += f"<p>지난주({first_date} ~ {last_date}) 동안 감지된 위반 사항이 없습니다.</p>"

    html_body += "</body></html>"
    msg.attach(MIMEText(text_body, 'plain', 'utf-8'))
    msg.attach(MIMEText(html_body, 'html', 'utf-8'))

    try:
        with smtplib.SMTP_SSL(smtp_config['host'], smtp_config['port']) as server:
            server.login(smtp_config['sender_email'], smtp_config['sender_password'])
            server.sendmail(smtp_config['sender_email'], recipients_list, msg.as_string())
        print("✅ 이메일 발송 성공!")
    except Exception as e:
        print(f"❌ 이메일 발송 실패: {e}")

def run_weekly_report_job(smtp_config: Dict, aes_key: str, db_config_path: str):
    """주간 보고서 생성 및 발송 메인 함수."""
    try:
        print(f"[{datetime.now()}] 주간 요약 이메일 작업을 시작합니다...")
        db_conn_task = get_database_service(config_file_path=db_config_path)
        if db_conn_task is None:
            print("❌ 작업용 DB 연결 실패. 이메일 작업을 건너뜁니다.")
            return
        print(f"[{datetime.now()}] 작업용 DB 연결 성공.")
        
        recipients = get_email_recipients(db_conn_task, aes_key)
        if not recipients:
            print("❌ 이메일을 보낼 수신자가 없어 작업을 종료합니다.")
            return
        violation_data, people_count, pig_count = get_weekly_violations_from_db(db_conn_task)
        first_date, last_date = get_last_week_date_range()
        
        week_num = datetime.now().isocalendar()[1]
        subject = f"[{datetime.now().strftime('%Y-%m-%d')}] {week_num}주차 위반 감지 요약 보고서"

        generate_weekly_summary(
            violation_data, people_count, pig_count,
            first_date, last_date,
            smtp_config, recipients, subject
        )
    except Exception as e:
        print(f"❌ 주간보고서 작업 중 오류 발생: {e}")
    finally:
        if db_conn_task and db_conn_task.open:
            db_conn_task.close()
            print(f"[{datetime.now()}] 작업용 DB 연결 해제.")
    print(f"[{datetime.now()}] ✅ 주간 요약 이메일 작업 완료.")


def setup_and_run_scheduler(smtp_config: Dict, aes_key: str, db_config_path: str):
    """ 스케줄러를 설정하고 실행합니다. """
    schedule.every().monday.at("09:00").do(
        run_weekly_report_job,
        smtp_config=smtp_config,
        aes_key=aes_key,
        db_config_path=db_config_path
    )

    print("✅ 스케줄러 설정 완료. 매주 월요일 09:00에 보고서가 발송됩니다.")

    # # Test
    # schedule.every().tuesday.at("09:00").do(
    #     run_weekly_report_job,
    #     smtp_config=smtp_settings,
    #     aes_key=db_aes_key
    # )
    # schedule.every().wednesday.at("09:00").do(
    #     run_weekly_report_job,
    #     smtp_config=smtp_settings,
    #     aes_key=db_aes_key
    # )
    # schedule.every().thursday.at("09:00").do(
    #     run_weekly_report_job,
    #     smtp_config=smtp_settings,
    #     aes_key=db_aes_key
    # )
    # schedule.every().friday.at("09:00").do(
    #     run_weekly_report_job,
    #     smtp_config=smtp_settings,
    #     aes_key=db_aes_key
    # )

    # 스케줄러 시작 후 첫 작업 즉시 실행 (Test용)
    # run_weekly_report_job(smtp_config, aes_key, db_config_path)

    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    try:
        smtp_settings, db_aes_key = load_configurations(GMAIL_CONFIG_PATH, DB_CONFIG_PATH)
        if smtp_settings and db_aes_key:
            setup_and_run_scheduler(smtp_settings, db_aes_key, DB_CONFIG_PATH)
    except Exception as e:
        print(f"❌ 프로그램 실행 중 심각한 오류 발생: {e}")