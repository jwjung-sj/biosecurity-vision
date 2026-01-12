# video_processor_dev.py

import cv2
import time
from collections import defaultdict, deque
from lib.utils import (
    format_timestamp, motion_detected_background, draw_line,
    draw_detection_box, save_infos, is_above_line
)

# 기본 상수 (Config 누락 시 Fallback용)
DEFAULT_PIG_THRESH = 0.35
DEFAULT_CY_THRESH = 0.20
VIOLATION_FRAME_COUNT = 3
OBJECT_TIMEOUT_SECONDS = 10

class Line:
    def __init__(self, points):
        self.points = points
        (x1, y1), (x2, y2) = points
        
        # Height(상하) 기준 기울기 (y = mx + b)
        if x2 == x1: self.m_y = float('inf')
        else: self.m_y = (y2 - y1) / (x2 - x1)
        self.b_y = y1 - (self.m_y * x1) if self.m_y != float('inf') else 0

        # Width(좌우) 기준 기울기 (x = my + b)
        if y2 == y1: self.m_x = float('inf')
        else: self.m_x = (x2 - x1) / (y2 - y1)
        self.b_x = x1 - (self.m_x * y1) if self.m_x != float('inf') else 0

    def y_at(self, x): # 세로 이동 판별용
        if self.m_y == float('inf'): return (self.points[0][1] + self.points[1][1]) // 2
        return int(self.m_y * x + self.b_y)

    def x_at(self, y): # 가로 이동 판별용
        if self.m_x == float('inf'): return (self.points[0][0] + self.points[1][0]) // 2
        return int(self.m_x * y + self.b_x)

class Pig:
    def __init__(self, track_id, config):
        self.id = track_id
        self.config = config
        self.orientation = config.get('orientation', 'height') # 'height' or 'width'
        
        self.state = "none"
        self.state_history = ["none"]
        self.reenter_count = 0
        self.pos_max = 0    # (y_max 혹은 x_max)
        self.c_pos_max = 0  # (cy_max 혹은 cx_max)
        self.last_seen = time.time()
        self.has_crossed_down = False # (down 혹은 right)

        self.reenter_thresh = self.config.get('pig_reenter_thresh', DEFAULT_PIG_THRESH)
        self.cy_thresh = DEFAULT_CY_THRESH

    def _change_state(self, new_state):
        if self.state != new_state:
            self.state = new_state
            self.state_history.append(new_state)

    def is_expired(self, current_time):
        return current_time - self.last_seen > OBJECT_TIMEOUT_SECONDS

    def update(self, box, line_info, timestamp):
        self.last_seen = timestamp
        x1, y1, x2, y2 = box
        h = y2 - y1
        cx, cy = (x1 + x2) // 2, (y1 + y2) // 2
        if h == 0: return

        # [핵심] 방향에 따른 좌표 매핑
        # p1: 시작점(위/좌), p2: 끝점(아래/우), val: 중심축값(y/x), len: 진행길이(h/w)
        if self.orientation == 'width':
            p1, p2 = x1, x2
            c_pos = cx
            line_val = line_info.x_at(cy)
            total_len = x2 - x1
        else: # height (default)
            p1, p2 = y1, y2
            c_pos = cy
            line_val = line_info.y_at(cx)
            total_len = y2 - y1

        # 로직 수행 (변수명만 추상화됨)
        if self.state == "none":
            if p2 < line_val: # Line보다 작음 (위/좌측) -> Clean
                self._change_state("on_line")
                self.has_crossed_down = False
            elif p1 > line_val: # Line보다 큼 (아래/우측) -> Dirty
                self._change_state("under_line")
                self.has_crossed_down = True
        
        elif self.state == "on_line":
            if p1 < line_val and p1 + total_len * (1 - self.reenter_thresh) >= line_val:
                self._change_state("crossing")
                self.pos_max = p2
                self.c_pos_max = c_pos
                self.reenter_count = 0
        
        elif self.state == "under_line":
            check_val = p1 + int(total_len * self.reenter_thresh)
            # is_above_line을 위해 좌표 복원
            check_pt = (check_val, cy) if self.orientation == 'width' else (cx, check_val)

            if is_above_line(check_pt, line_info.points):
                self.reenter_count += 1
            else:
                self.reenter_count = 0
            
            if self.reenter_count >= VIOLATION_FRAME_COUNT:
                self._change_state("re-enter-from-under")

        elif self.state == "crossing":
            if p2 > self.pos_max:
                self.pos_max = p2
                self.c_pos_max = c_pos
                self.reenter_count = 0
            else:
                p2_moved_up = p2 < self.pos_max - (total_len * self.reenter_thresh)
                c_moved_up = c_pos < self.c_pos_max - (total_len * self.cy_thresh)
                if p2_moved_up and c_moved_up:
                    self.reenter_count += 1
                else:
                    self.reenter_count = 0

            if self.reenter_count >= VIOLATION_FRAME_COUNT:
                self._change_state("re-enter-from-crossing")
            elif p1 > line_val:
                self._change_state("under_line")

def trigger_violation(track_id, label, timestamp, reentered_ids, event_counter, save_active, clip_start, warning_client=None, history=None):
    print(f"{format_timestamp(timestamp)} [ALERT] ID {track_id} violated! ({label})")
    reentered_ids.add(track_id)
    event_counter[label] += 1
    
    if not save_active[0]:
        save_active[0] = True
        clip_start[0] = timestamp

    if label == "worker" and warning_client:
        print(f"🚨 사람 위반 (ID: {track_id}), 신호 전송...")
        # Client 객체 타입(RPI/Webhook)에 상관없이 send_signal 호출
        warning_client.send_signal("LIGHT_ON")

def process_video(read_frame_func, model, drive_mgr, db_cfg, warning_client, shutdown, count_mgr, 
                  farm_config, fps=15.0, width=640, height=384, record_output_path=None):
    
    detecting = False
    prev_detecting = False
    idle_start_time = None
    prev_small_gray = None

    # 설정 로드
    motion_thresh = farm_config.get('motion_threshold', 300)
    worker_conf = farm_config.get('worker_conf', 0.6)
    orientation = farm_config.get('orientation', 'height')
    if not orientation: orientation = 'height'

    # Line 파싱
    line_str = farm_config.get('line_coords', '')
    line_points = []
    
    try:
        # 빈 문자열이면 에러 발생 유도
        if not line_str: raise ValueError("Empty coordinates")
        coords = list(map(int, line_str.split(',')))
        if len(coords) != 4: raise ValueError("Invalid format")
        line_points = [(coords[0], coords[1]), (coords[2], coords[3])]
    except (ValueError, IndexError):
        # 좌표가 없으면 화면 중앙선 생성 (프로그램 종료 방지)
        print(f"⚠️ [{farm_config.get('farm_code')}] 라인 좌표 미설정/오류. 기본 중앙선으로 대체합니다.")
        if orientation == 'width':
            line_points = [(width // 2, 0), (width // 2, height)] # 세로선
        else:
            line_points = [(0, height // 2), (width, height // 2)] # 가로선
            
    LINE = Line(line_points)

    bg_sub = cv2.createBackgroundSubtractorMOG2(history=300, varThreshold=16, detectShadows=False)
    pigs = {}
    object_flags = defaultdict(dict)
    track_history = defaultdict(list)
    reentered_ids = set()
    frame_count = 0
    yolo_cache = None
    violation_buffer = deque(maxlen=int(fps * 6))
    save_active = [False]
    clip_start = [0]
    event_counter = {"worker": 0, "pig": 0}

    recorder = None
    if record_output_path:
        recorder = cv2.VideoWriter(record_output_path, cv2.VideoWriter_fourcc(*'mp4v'), fps, (width, height))

    while True:
        frame = read_frame_func()
        if frame is None: break
        
        timestamp = time.time()
        frame = cv2.resize(frame, (width, height))
        gray = cv2.GaussianBlur(cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY), (5, 5), 0)
        small_gray = cv2.resize(gray, (width//2, height//2))
        
        motion = motion_detected_background(prev_small_gray, small_gray, bg_sub, motion_thresh)
        prev_small_gray = small_gray.copy()

        if motion:
            detecting = True; idle_start_time = None
        else:
            if detecting and idle_start_time is None: idle_start_time = time.time()
            elif detecting and (time.time() - idle_start_time > 3): detecting = False
        
        if detecting != prev_detecting:
            print(f"[{format_timestamp(timestamp)}] {'움직임 감지' if detecting else '대기'}")
            prev_detecting = detecting

        active_ids = set()
        if detecting:
            if frame_count % 1 == 0 or yolo_cache is None:
                yolo_cache = model.track(frame, persist=True, verbose=False)[0]
            results = yolo_cache
            frame_count += 1

            for box in results.boxes:
                if box.id is None: continue
                track_id = int(box.id.item())
                cls = int(box.cls.item())
                label = model.names[cls]
                
                xyxy = box.xyxy[0].cpu().numpy()
                x1, y1, x2, y2 = map(int, xyxy)
                cx, cy = (x1 + x2)//2, (y1 + y2)//2

                if label == "pig":
                    if track_id not in pigs: pigs[track_id] = Pig(track_id, farm_config)
                    pig = pigs[track_id]
                    pig.update((x1, y1, x2, y2), LINE, timestamp)

                    # 완전 넘어감 판별 (Orientation 고려)
                    if orientation == 'width':
                        is_fully_above = x2 < LINE.x_at(cy) # Left side (Clean)
                        is_fully_below = x1 > LINE.x_at(cy) # Right side (Dirty)
                    else:
                        is_fully_above = y2 < LINE.y_at(cx) # Upper side
                        is_fully_below = y1 > LINE.y_at(cx) # Lower side

                    if pig.state == "under_line" and not pig.has_crossed_down:
                        if "on_line" in pig.state_history or "crossing" in pig.state_history:
                            count_mgr.increment(); pig.has_crossed_down = True
                    elif pig.state == "re-enter-handled" and is_fully_below:
                        if not pig.has_crossed_down:
                            if "on_line" in pig.state_history or "crossing" in pig.state_history:
                                count_mgr.increment(); pig.has_crossed_down = True
                        pig._change_state("under_line")
                    elif pig.state in ["re-enter-from-under", "re-enter-handled"] and is_fully_above:
                        if pig.has_crossed_down:
                            count_mgr.decrement(); pig.has_crossed_down = False
                        pig._change_state("on_line")

                    if pig.state.startswith("re-enter") and track_id not in reentered_ids:              
                        trigger_violation(track_id, "pig", timestamp, reentered_ids, event_counter, save_active, clip_start, history=pig.state_history)
                        pig._change_state("re-enter-handled")

                elif label == "worker" and box.conf.item() > worker_conf:
                    object_flags[track_id]['last_seen'] = timestamp
                    
                    if "initial_pos" not in object_flags[track_id]:
                        object_flags[track_id]["initial_pos"] = "above" if is_above_line((cx, cy), LINE.points) else "below"
                    
                    if object_flags[track_id]["initial_pos"] == "below":
                        # Worker Check Point 생성 (방향 고려)
                        if orientation == 'width':
                            check_pt = (int(x1 + 0.35 * (x2 - x1)), cy)
                        else:
                            check_pt = (cx, int(y1 + 0.35 * (y2 - y1)))
                            
                        if is_above_line(check_pt, LINE.points) and track_id not in reentered_ids:
                            trigger_violation(track_id, "worker", timestamp, reentered_ids, event_counter, save_active, clip_start, warning_client)

                draw_detection_box(frame, (x1, y1, x2, y2), label, track_id, track_id in reentered_ids)
                active_ids.add(track_id)
                track_history[track_id].append((cx, cy))
                if len(track_history[track_id]) > 10: track_history[track_id].pop(0)

        # 저장/삭제 로직 (동일)
        if save_active[0] and (timestamp - clip_start[0] >= 3):
            gdrive = drive_mgr.get_drive()
            if gdrive: save_infos(list(violation_buffer), clip_start[0], event_counter, gdrive, db_cfg)
            save_active[0] = False; event_counter = {"worker": 0, "pig": 0}

        for k in list(track_history.keys()):
            if k not in active_ids: track_history.pop(k, None)
        for k in list(pigs.keys()):
            if pigs[k].is_expired(timestamp): pigs.pop(k, None); reentered_ids.discard(k)
        for k in list(object_flags.keys()):
            if timestamp - object_flags[k].get('last_seen', timestamp) > OBJECT_TIMEOUT_SECONDS: object_flags.pop(k, None); reentered_ids.discard(k)

        # 그리기
        for tid in track_history:
            t = track_history[tid]
            if len(t) >= 2:
                for i in range(1, len(t)): cv2.line(frame, t[i-1], t[i], (255,255,255), 1)
        draw_line(frame, LINE.points)
        
        cv2.putText(frame, f"Count: {count_mgr.get_current_count()}", (10, 50), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (255,255,255), 2)
        violation_buffer.append(frame.copy())
        cv2.imshow("Detection", frame)
        if recorder: recorder.write(frame)

        if cv2.waitKey(1) & 0xFF == ord('q'):
            shutdown['manual_quit'] = True
            break

    cv2.destroyAllWindows()
    if recorder: recorder.release()