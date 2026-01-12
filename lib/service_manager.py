from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import configparser
import pymysql
import os, time

def get_database_service(config_file_path: str):
    """
    INI 파일로부터 데이터베이스 설정을 읽어와 MySQL 연결 객체를 반환합니다.
    연결 실패 시 None을 반환하고 오류 메시지를 출력합니다.
    """
    config = configparser.ConfigParser()

    # 1. 설정 파일 로드 시도
    if not os.path.exists(config_file_path):
        print(f"오류: 설정 파일 '{config_file_path}'을(를) 찾을 수 없습니다.")
        print("config.ini 파일이 올바른 경로에 있는지 확인하고, 필수 DB 설정이 포함되어 있는지 확인하세요.")
        return None

    try:
        config.read(config_file_path)

        if 'database' not in config:
            print(f"오류: '{config_file_path}' 파일에 '[database]' 섹션이 없습니다.")
            return None

        DB_HOST = config['database']['host']
        DB_USER = config['database']['user']
        DB_PASSWORD = config['database']['password']
        DB_NAME = config['database']['db_name']
        DB_PORT = int(config['database'].get('port', 3306)) 

    except KeyError as e:
        print(f"오류: '{config_file_path}' 파일의 [database] 섹션에 필수 설정 '{e}'이(가) 누락되었습니다.")
        return None
    except ValueError as e:
        print(f"오류: '{config_file_path}' 파일의 'port' 설정이 숫자가 아닙니다: {e}")
        return None
    except Exception as e:
        print(f"설정 파일 읽기 중 알 수 없는 오류 발생: {e}")
        return None

    try:
        conn = pymysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            db=DB_NAME,
            port=DB_PORT,
            charset='utf8mb4', # 전체 유니코드 지원을 위해 권장
            cursorclass=pymysql.cursors.DictCursor # 선택 사항: 결과를 딕셔너리 형태로 반환
        )
        print("MySQL 데이터베이스 연결 성공.")
        return conn
    
    except pymysql.Error as e:
        print(f"MySQL 데이터베이스 연결 오류: {e}")
        if e.args[0] == 2003:                           # MySQL 서버에 연결할 수 없음
            print("데이터베이스 서버가 실행 중인지, 호스트/포트 설정이 올바른지 확인하세요.")
        elif e.args[0] == 1045:                         # 사용자 접근 거부
            print("데이터베이스 사용자 이름과 비밀번호를 확인하세요.")
        elif e.args[0] == 1049:                         # 알 수 없는 데이터베이스
            print(f"데이터베이스 '{DB_NAME}'가 존재하지 않습니다. 데이터베이스 이름을 확인하세요.")
        elif e.args[0] == 2002:                         # 로컬 연결 오류
            print("로컬 MySQL 소켓 연결 오류입니다. MySQL 서비스가 실행 중인지, 소켓 경로가 올바른지 확인하세요.")
        return None

class DriveManager:
    def __init__(self, creds_file="admincreds.json", client_config="admin_cred_config.json"):
        """DriveManager 초기화 및 최초 인증 시도"""
        self.creds_file = creds_file
        self.client_config = client_config
        self.gauth = GoogleAuth()
        self.last_refresh_time = 0
        self.refresh_interval = 2700                # 45분 (3600초 = 1시간)

        if not self._authenticate_and_load():
            print("초기 Google Drive 인증 실패!")
            self.gauth = None # 인증 실패 표시----------------------

    def _authenticate_and_load(self):
        """Google Drive 인증을 로드하거나 새로 수행하고, 토큰을 저장합니다."""
        try:
            # 1. 기존 인증 정보 로드 시도
            if os.path.exists(self.creds_file):
                self.gauth.LoadCredentialsFile(self.creds_file)
            else:
                self.gauth.credentials = None

            # 2. 인증 정보가 없으면 새로 인증
            if self.gauth.credentials is None:
                print("🔑 새 Google Drive 인증을 시작합니다...")
                if not os.path.exists(self.client_config):
                    print(f"❌ 오류: 클라이언트 설정 파일 '{self.client_config}'을(를) 찾을 수 없습니다.")
                    return False
                self.gauth.LoadClientConfigFile(self.client_config)
                self.gauth.LocalWebserverAuth() # 웹 브라우저를 통한 인증
                print("✅ 새 인증 성공.")

            # 3. 토큰이 만료되었으면 갱신
            elif self.gauth.access_token_expired:
                print("🔄 Access Token이 만료되어 갱신을 시도합니다...")
                self.gauth.Refresh()
                print("✅ Access Token 갱신 성공.")

            # 4. 인증 정보 저장 및 갱신 시간 기록
            self.gauth.SaveCredentialsFile(self.creds_file)
            self.last_refresh_time = time.time()
            print("✅ Google Drive 인증 준비 완료.")
            return True

        except Exception as e:
            print(f"❌ Google Drive 인증/로드 중 오류 발생: {e}")
            # 만료된 인증서 파일 문제일 수 있으므로 삭제 후 재시도 제안
            if "invalid_grant" in str(e) and os.path.exists(self.creds_file):
                print(f"⚠️ 'invalid_grant' 오류 발생. '{self.creds_file}' 파일을 삭제하고 다시 시도해 보세요.")
            return False

    def get_drive(self):
        """
        GoogleDrive 객체를 반환합니다. 필요 시 토큰을 갱신합니다.
        """
        if not self.gauth or not self.gauth.credentials:
            print("⚠️ 인증 정보가 없습니다. 재인증을 시도합니다...")
            if not self._authenticate_and_load():
                return None # 재인증 실패 시 None 반환

        current_time = time.time()
        # 토큰 만료 시간이 임박했거나, 마지막 갱신 후 일정 시간이 지났으면 갱신 시도
        if self.gauth.access_token_expired or (current_time - self.last_refresh_time > self.refresh_interval):
            print("주기적 또는 만료 임박으로 토큰 갱신을 시도합니다...")
            try:
                self.gauth.Refresh()
                self.gauth.SaveCredentialsFile(self.creds_file)
                self.last_refresh_time = time.time() # 갱신 시간 업데이트
                print("✅ 토큰 갱신 성공.")
            except Exception as e:
                print(f"❌ 토큰 갱신 중 오류 발생: {e}. 이전 연결을 반환합니다.")
                # 갱신 실패 시 일단 기존 gauth로 시도해볼 수 있음

        return GoogleDrive(self.gauth)