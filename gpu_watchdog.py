# GPU Watchdog - GPU 상태 모니터링 및 자동 복구 시스템
#
# 이 스크립트는 FARM 및 LAB 그룹의 서버들에 대해 GPU 상태를 모니터링하고,
# 문제가 발견되면 자동으로 재부팅을 수행하여 복구를 시도합니다.

import json
import subprocess
import time
import requests
import os
from datetime import datetime, timedelta, timezone

# ======================== 설정 ========================
# 상태 정보를 저장할 JSON 파일 경로
STATE_FILE = "./state.json"

# GPU 체크 주기 (초 단위, 기본 5분)
CHECK_INTERVAL = 300

# GPU 문제 발견 후 재부팅까지의 대기 시간
REBOOT_DELAY = timedelta(minutes=2)

# Ansible 설정 파일 경로
ANSIBLE_CONFIG = "/home/svmanager/ansible/ansible.cfg"
ANSIBLE_INVENTORY = "/home/svmanager/ansible/inventory.ini"

# Ansible 실행을 위한 환경 변수 설정
# 스크립트 시작 시 한 번만 설정하여 모든 Ansible 명령에 적용
env = os.environ.copy()
env["ANSIBLE_CONFIG"] = ANSIBLE_CONFIG

# Slack webhook URLs (환경 변수에서 가져옴)
# 각 그룹별로 다른 Slack 채널에 알림을 보낼 수 있도록 구분
SLACK_WEBHOOKS = {
    "FARM": os.getenv("SLACK_WEBHOOK_FARM"),
    "LAB": os.getenv("SLACK_WEBHOOK_LAB")
}
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")

# ======================== 유틸리티 함수 ========================

def now():
    """
    현재 시각을 한국 시간대(KST, UTC+9)로 반환

    Returns:
        datetime: KST 시간대의 현재 시각
    """
    KST = timezone(timedelta(hours=9))
    return datetime.now(KST)

def load_state():
    """
    이전에 저장된 상태 정보를 파일에서 불러옴

    상태 파일에는 각 호스트별로 다음 정보가 저장됨:
    - last_gpu_ok: 마지막 GPU 체크 결과
    - reboot_scheduled_at: 재부팅 예약 시각
    - reboot_done: 재부팅 완료 여부
    - reboot_fail_count: 재부팅 실패 횟수
    - reboot_failed_notified: 재부팅 실패 알림 전송 여부
    - persistent_failure_notified: 재부팅 후에도 실패 시 알림 전송 여부
    - last_checked: 마지막 체크 시각

    Returns:
        dict: {"FARM": {호스트명: 상태}, "LAB": {호스트명: 상태}} 형태의 딕셔너리
    """
    try:
        with open(STATE_FILE) as f:
            state = json.load(f)
            total_hosts = sum(len(hosts) for hosts in state.values())
            print(f"[STATE] Loaded state file with {total_hosts} hosts")
            return state
    except FileNotFoundError:
        print(f"[STATE] State file not found, starting with empty state")
        # 파일이 없으면 빈 상태로 시작
        return {"FARM": {}, "LAB": {}}

def save_state(state):
    """
    현재 상태를 파일에 저장

    Args:
        state: {"FARM": {호스트명: 상태}, "LAB": {호스트명: 상태}} 형태의 딕셔너리
    """
    with open(STATE_FILE, "w") as f:
        # default=str을 사용하여 datetime 객체를 문자열로 변환
        json.dump(state, f, indent=2, default=str)
    total_hosts = sum(len(hosts) for hosts in state.values())
    print(f"[STATE] Saved state file with {total_hosts} hosts")

def run(cmd):
    """
    Ansible 명령어 실행을 위한 헬퍼 함수

    환경 변수에 ANSIBLE_CONFIG를 설정하여 Ansible이 올바른 설정 파일을 사용하도록 함

    Args:
        cmd: 실행할 쉘 명령어

    Returns:
        CompletedProcess: subprocess 실행 결과
    """
    global env 
    return subprocess.run(cmd, shell=True, capture_output=True, text=True, env=env)

# ======================== GPU 체크 함수 ========================

def check_gpu(group):
    """
    지정된 그룹(FARM 또는 LAB)의 모든 호스트에 대해 GPU 사용 가능 여부를 체크

    Ansible을 통해 각 호스트에서 다음을 확인:
    1. nvidia-smi 명령어가 존재하는지
    2. nvidia-smi -L 명령어가 정상적으로 실행되는지 (GPU 목록 조회)

    두 조건을 모두 만족하면 GPU 사용 가능(true), 하나라도 실패하면 불가능(false)

    Args:
        group: "FARM" 또는 "LAB" 그룹명

    Returns:
        dict: {호스트명: bool} 형태의 딕셔너리
              True = GPU 사용 가능, False = GPU 사용 불가능
              응답이 없는 호스트는 결과에 포함되지 않음 (재부팅 중일 가능성)
    """
    print(f"[INFO] Checking GPU status for group: {group}")

    # Ansible shell 모듈을 사용하여 nvidia-smi 체크
    # command -v: 명령어 존재 여부 확인
    # nvidia-smi -L: GPU 목록 조회 (정상 작동 확인)
    cmd = (
        f"ansible {group} -m shell -a "
        "'command -v nvidia-smi >/dev/null 2>&1 "
        "&& nvidia-smi -L >/dev/null 2>&1 && echo true || echo false'"
    )
    res = run(cmd)

    result = {}
    lines = res.stdout.splitlines()

    # Ansible 출력 파싱
    # Ansible 출력 형식:
    # hostname | CHANGED | rc=0 >>
    # true (또는 false)
    i = 0
    while i < len(lines):
        line = lines[i]
        # 호스트 응답 라인 찾기 ("|" 구분자와 CHANGED/SUCCESS 상태 포함)
        if "|" in line and ("CHANGED" in line or "SUCCESS" in line):
            # 호스트명 추출 ("|" 앞부분)
            host = line.split("|")[0].strip()
            # 다음 줄에 실제 명령 실행 결과(true/false)가 있음
            i += 1
            if i < len(lines):
                output = lines[i].strip()
                if output == "true":
                    result[host] = True
                    print(f"[GPU OK] {host}: GPU available")
                elif output == "false":
                    result[host] = False
                    print(f"[GPU FAIL] {host}: GPU unavailable")
        i += 1

    # 결과가 없으면 경고 (그룹에 호스트가 없거나 모두 응답 없음)
    if not result:
        print(f"[WARNING] No hosts found in group {group}")

    return result

# ======================== 재부팅 및 알림 함수 ========================

def reboot_host(host, delay_minutes=2):
    """
    호스트에 재부팅을 예약

    Ansible을 통해 원격 호스트에서 shutdown 명령을 실행
    -b 옵션: become (sudo 권한 사용)
    shutdown -r +N: N분 후 재부팅

    Args:
        host: 재부팅할 호스트명
        delay_minutes: 몇 분 후에 재부팅할지 (기본 2분)
                       지연 시간을 두는 이유:
                       - 실행 중인 작업이 안전하게 종료될 수 있도록
                       - 로그 및 알림이 정상적으로 전송될 수 있도록
    """
    print(f"[REBOOT] Scheduling reboot for {host} in {delay_minutes} minutes")
    # shutdown -r +N: N분 후 재부팅
    cmd = f"ansible {host} -b -m shell -a 'shutdown -r +{delay_minutes}'"
    run(cmd)
    print(f"[REBOOT] Reboot scheduled for {host} (in {delay_minutes} minutes)")
    pass  # 테스트용으로 주석 처리

def send_slack(msg, group):
    """
    Slack으로 메시지 전송

    각 그룹(FARM/LAB)별로 설정된 Slack webhook URL로 메시지를 전송
    환경 변수에 webhook URL이 설정되어 있어야 함.

    전송되는 메시지 종류:
    - ⚠️ GPU 사용 불가 및 재부팅 예정 알림
    - ✅ 재부팅 후 GPU 정상 복구 알림
    - ❌ 재부팅 실패 또는 재부팅 후에도 문제 지속 알림

    Args:
        msg: 전송할 메시지 내용
        group: "FARM" 또는 "LAB" 그룹명
    """
    webhook_url = SLACK_WEBHOOKS.get(group)
    if not webhook_url:
        print(f"[SLACK ERROR] Unknown group: {group}")
        return

    try:
        # Slack webhook 페이로드 구성
        payload = {"text": f"[{group}] {msg}"}
        # 5초 타임아웃으로 POST 요청
        response = requests.post(webhook_url, json=payload, timeout=5)
        if response.status_code == 200:
            print(f"[SLACK OK] {msg}")
        else:
            print(f"[SLACK ERROR] Status {response.status_code}: {msg}")
    except Exception as e:
        # 네트워크 오류 등의 예외 처리
        print(f"[SLACK ERROR] {e}: {msg}")

# ======================== 메인 처리 로직 ========================

def process_group(group, state):
    """
    특정 그룹(FARM 또는 LAB)의 모든 호스트에 대해 GPU 상태를 체크하고 처리

    처리 흐름:
    1. check_gpu()로 그룹의 모든 호스트 GPU 상태 확인
    2. 각 호스트의 이전 상태와 현재 상태를 비교
    3. 상태에 따라 적절한 조치 수행:
       - GPU 문제 발견 → 재부팅 예약
       - 재부팅 예약 후 응답 없음 → 재부팅 실패 처리
       - 재부팅 후 GPU 정상 → 복구 완료 처리
       - 재부팅 후에도 GPU 문제 지속 → 영구적 실패 알림

    Args:
        group: "FARM" 또는 "LAB" 그룹명
        state: 전체 상태 딕셔너리 {"FARM": {...}, "LAB": {...}}
              상태는 함수 내에서 직접 수정됨 (mutable)
    """
    print(f"\n[PROCESS] Processing group: {group}")

    # 1. 현재 GPU 상태 체크
    gpu_status = check_gpu(group)
    now_ts = now()

    # 2. 그룹별 상태 초기화 (첫 실행 시)
    if group not in state:
        state[group] = {}
    group_state = state[group]

    # 3. 처리할 모든 호스트 목록 생성
    # - state에 있는 호스트: 이전에 문제가 있었던 호스트 (추적 중)
    # - gpu_status에 있는 호스트: 현재 응답한 호스트
    # 합집합(|)을 사용하여 두 경우를 모두 포함
    all_hosts = set(group_state.keys()) | set(gpu_status.keys())

    # 4. 각 호스트별 처리
    for host in all_hosts:
        # 호스트의 이전 상태 가져오기
        host_state = group_state.get(host, {})
        # 현재 GPU 상태 (None = 응답 없음, True = 정상, False = 문제 있음)
        gpu_ok = gpu_status.get(host, None)

        print(f"\n[HOST] Processing {host} (GPU: {'OK' if gpu_ok else 'FAIL' if gpu_ok is False else 'NO RESPONSE'})")

        # ===== 케이스 1: 재부팅 예약이 있는 경우 =====
        # 이전에 GPU 문제가 발견되어 재부팅을 예약한 상태
        if host_state.get("reboot_scheduled_at"):
            # 케이스 1-1: 응답 없음 → 재부팅 실패 (호스트가 다운되었거나 네트워크 문제)
            if gpu_ok is None:
                print(f"[REBOOT FAILED] {host} did not respond")
                reboot_fail_count = host_state.get("reboot_fail_count", 0) + 1

                # 두 번째 실패부터 알림 발송 (일시적 네트워크 문제일 수 있으므로)
                if reboot_fail_count >= 2 and not host_state.get("reboot_failed_notified"):
                    send_slack(f"❌ {host}: 재부팅 실패 (응답 없음, {reboot_fail_count}회)", group)
                    group_state[host]["reboot_failed_notified"] = True

                # 상태 업데이트: 재부팅 실패 기록
                group_state[host]["reboot_fail_count"] = reboot_fail_count
                group_state[host]["last_gpu_ok"] = False
                group_state[host]["reboot_done"] = False
                group_state[host]["last_checked"] = now_ts.isoformat()

            # 케이스 1-2: 응답 있음 → 재부팅 성공 (호스트가 다시 살아남)
            else:
                print(f"[REBOOTED] {host} rebooted successfully")
                # 재부팅 완료 플래그 설정
                group_state[host]["reboot_done"] = True
                group_state[host]["reboot_fail_count"] = 0
                group_state[host]["reboot_failed_notified"] = False
                group_state[host]["last_checked"] = now_ts.isoformat()

        # ===== 케이스 2: GPU 사용 불가능한 경우 =====
        if not gpu_ok and gpu_ok is not None:
            # 케이스 2-1: 재부팅 완료했는데도 여전히 GPU 불가 → 영구적 실패
            if host_state.get("reboot_done"):
                print(f"[PERSISTENT] {host} still has GPU issues after reboot")
                # 영구적 실패 알림 (한 번만 전송)
                if not host_state.get("persistent_failure_notified"):
                    send_slack(f"❌ {host}: 재부팅 이후에도 GPU 비정상", group)
                    group_state[host]["persistent_failure_notified"] = True
                group_state[host]["last_checked"] = now_ts.isoformat()

            # 케이스 2-2: 처음 발견한 GPU 문제 → 재부팅 예약
            elif not host_state.get("reboot_scheduled_at"):
                delay_minutes = int(REBOOT_DELAY.total_seconds() / 60)
                scheduled = now_ts + REBOOT_DELAY
                print(f"[SCHEDULE] {host} GPU failed, scheduling reboot in {delay_minutes} minutes")

                # 재부팅 명령 실행
                reboot_host(host, delay_minutes=delay_minutes)

                # 상태 업데이트: 재부팅 예약 기록
                group_state[host] = {
                    "last_gpu_ok": False,
                    "reboot_scheduled_at": scheduled.isoformat(),
                    "reboot_done": False,
                    "last_checked": now_ts.isoformat()
                }
                # 알림 전송: GPU 문제 및 재부팅 예정
                send_slack(f"⚠️ {host}: GPU 사용 불가, {delay_minutes}분 후 재부팅 예정", group)

        # ===== 케이스 3: GPU 사용 가능한 경우 =====
        else:
            # 케이스 3-1: 재부팅 후 GPU 정상 복구
            if host_state.get("reboot_done"):
                print(f"[RECOVERY] {host} recovered after reboot")
                # 복구 완료 알림
                send_slack(f"✅ {host}: 재부팅 후 GPU 정상", group)
            # 케이스 3-2: 정상 상태 유지 (문제 없음)
            else:
                print(f"[HEALTHY] {host} is healthy")

            # 상태 초기화: 모든 플래그를 정상 상태로 리셋
            group_state[host] = {
                "last_gpu_ok": True,
                "reboot_scheduled_at": None,
                "reboot_done": False,
                "reboot_fail_count": 0,
                "reboot_failed_notified": False,
                "persistent_failure_notified": False,
                "last_checked": now_ts.isoformat()
            }

# ======================== 메인 함수 ========================

def main():
    """
    GPU Watchdog 메인 함수

    실행 흐름:
    1. 이전 상태 로드 (state.json 파일에서)
    2. FARM 그룹 처리
    3. LAB 그룹 처리
    4. 변경된 상태 저장

    """
    print("=" * 80)
    print(f"[START] GPU Watchdog started at {now().isoformat()}")
    print("=" * 80)

    # 1. 이전 실행의 상태 정보 로드
    state = load_state()

    # 2. FARM과 LAB 그룹 모두 처리
    for group in ["FARM", "LAB"]:
        process_group(group, state)

    # 3. 업데이트된 상태 저장
    save_state(state)

    print("\n" + "=" * 80)
    print(f"[END] GPU Watchdog completed at {now().isoformat()}")
    print("=" * 80)

# 스크립트 직접 실행 시 main() 함수 호출
if __name__ == "__main__":
    main()