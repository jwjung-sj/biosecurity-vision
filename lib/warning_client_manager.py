'''
    release date: 2025-06-09
    release date: 2025-08-04
        - RPI Client, Web Wook Client 분리
'''

import socket
import requests


class RPIClient:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.socket = None
        self.is_connected = False

    def connect(self):
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.settimeout(5) # 연결 시도 시간 초과 설정 (5초)
            self.socket.connect((self.host, self.port))
            self.is_connected = True
            print(f"✅ 경고 서버에 성공적으로 연결되었습니다 ({self.host}:{self.port}).")
            return True
        except socket.error as e:
            print(f"❌ 경고 서버 연결 실패 ({self.host}:{self.port}): {e}")
            self.socket = None
            self.is_connected = False
            return False

    def send_signal(self, message):
        if not self.is_connected or self.socket is None:
            print(" 소켓이 연결되어 있지 않습니다. 재연결 시도 중...")
            if not self.connect():                                      # 연결이 끊어졌거나 실패한 경우 재연결 시도
                print(" 재연결 실패. 신호를 보낼 수 없습니다.")
                return False
        
        try:
            self.socket.sendall(message.encode('utf-8'))
            print(f"💡 신호 '{message}'를 서버로 전송했습니다.")
            # 서버로부터 응답을 받을 필요가 있다면 아래 코드 추가
            # response = self.socket.recv(1024)
            # print(f" 서버로부터 응답 수신: {response.decode('utf-8')}")
            return True
        except socket.error as e:
            print(f"❌ 신호 전송 중 오류 발생: {e}")
            self.is_connected = False # 오류 발생 시 연결 상태 변경
            if self.socket:
                self.socket.close()
            self.socket = None
            return False
        except Exception as e:
            print(f"❌ 신호 전송 중 예기치 않은 오류 발생: {e}")
            self.is_connected = False
            if self.socket:
                self.socket.close()
            self.socket = None
            return False

    def close(self):
        if self.socket:
            try:
                self.socket.close()
                print(" 서버와의 연결을 종료했습니다.")
            except socket.error as e:
                print(f"❌ 소켓 종료 중 오류 발생: {e}")
            finally:
                self.socket = None
                self.is_connected = False


class WebhookClient:
    """웹훅 요청을 보내는 클라이언트 클래스"""
    def __init__(self, webhook_url):
        """
        Args:
            webhook_url (str): 요청을 보낼 전체 웹훅 URL
        """
        self.webhook_url = webhook_url
        self.is_connected = False # 소켓 클라이언트와의 호환성을 위한 플래그

    def connect(self):
        """URL 유효성만 확인하여 연결 상태를 설정합니다."""
        if self.webhook_url:
            self.is_connected = True
            print(f"✅ Webhook 설정이 유효합니다. ({self.webhook_url})")
            return True
        else:
            self.is_connected = False
            print("❌ Webhook URL이 설정되지 않아 연결할 수 없습니다.")
            return False

    def send_signal(self, message):
        """
        웹훅 URL로 GET 요청을 전송합니다.
        
        Args:
            message (str): 인터페이스 호환용 인수 (실제 요청에는 사용되지 않음)
        
        Returns:
            bool: 성공 여부
        """
        if not self.is_connected:
            print("❌ Webhook이 연결(설정)되지 않았습니다. 신호를 보낼 수 없습니다.")
            return False
        
        print(f" Webhook 신호 전송 시도: {self.webhook_url}")
        try:
            response = requests.get(self.webhook_url, verify=False, timeout=5)
            response.raise_for_status() 
            print(f"✅ Webhook 전송 성공. 응답 코드: {response.status_code}")
            return True
        except requests.exceptions.RequestException as e:
            print(f"❌ Webhook 전송 실패: {e}")
            return False
        except Exception as e:
            print(f"❌ Webhook 전송 중 예기치 않은 오류 발생: {e}")
            return False

    def close(self):
        """소켓 클라이언트와의 호환성을 위한 메서드."""
        pass
