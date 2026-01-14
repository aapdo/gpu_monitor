import json
import subprocess
import time
import requests
import os
from datetime import datetime, timedelta, timezone

STATE_FILE = "./state.json"
CHECK_INTERVAL = 300          # 5분
REBOOT_DELAY = timedelta(minutes=2)
ANSIBLE_CONFIG = "/home/svmanager/ansible/ansible.cfg"
ANSIBLE_INVENTORY = "/home/svmanager/ansible/inventory.ini"

# Slack webhook URLs (from environment variables)
SLACK_WEBHOOKS = {
    "FARM": os.getenv("SLACK_WEBHOOK_FARM"),
    "LAB": os.getenv("SLACK_WEBHOOK_LAB")
}
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")

def now():
    """한국 시간대(UTC+9) 반환"""
    KST = timezone(timedelta(hours=9))
    return datetime.now(KST)

def load_state():
    try:
        with open(STATE_FILE) as f:
            state = json.load(f)
            total_hosts = sum(len(hosts) for hosts in state.values())
            print(f"[STATE] Loaded state file with {total_hosts} hosts")
            return state
    except FileNotFoundError:
        print(f"[STATE] State file not found, starting with empty state")
        return {"FARM": {}, "LAB": {}}

def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2, default=str)
    total_hosts = sum(len(hosts) for hosts in state.values())
    print(f"[STATE] Saved state file with {total_hosts} hosts")

def run(cmd):
    env = os.environ.copy()
    env["ANSIBLE_CONFIG"] = ANSIBLE_CONFIG
    return subprocess.run(cmd, shell=True, capture_output=True, text=True, env=env)

def check_gpu(group):
    """
    지정된 그룹(FARM 또는 LAB)의 GPU 사용 가능 여부를 체크

    Args:
        group: "FARM" 또는 "LAB"

    Returns:
        dict: {hostname: bool} 형태의 GPU 상태
    """
    print(f"[INFO] Checking GPU status for group: {group}")
    cmd = (
        f"ansible {group} -m shell -a "
        "'command -v nvidia-smi >/dev/null 2>&1 "
        "&& nvidia-smi -L >/dev/null 2>&1 && echo true || echo false'"
    )
    res = run(cmd)

    result = {}
    lines = res.stdout.splitlines()

    i = 0
    while i < len(lines):
        line = lines[i]
        # "host | CHANGED | rc=0 >>" 형태의 줄 찾기
        if "|" in line and ("CHANGED" in line or "SUCCESS" in line):
            host = line.split("|")[0].strip()
            # 다음 줄에 실제 결과(true/false)가 있음
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

    if not result:
        print(f"[WARNING] No hosts found in group {group}")

    return result

def reboot_host(host, delay_minutes=2):
    """
    호스트를 재부팅 예약

    Args:
        host: 호스트명
        delay_minutes: 몇 분 후에 재부팅할지 (기본 2분)
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

    Args:
        msg: 전송할 메시지
        group: "FARM" 또는 "LAB"
    """
    webhook_url = SLACK_WEBHOOKS.get(group)
    if not webhook_url:
        print(f"[SLACK ERROR] Unknown group: {group}")
        return

    try:
        payload = {"text": f"[{group}] {msg}"}
        response = requests.post(webhook_url, json=payload, timeout=5)
        if response.status_code == 200:
            print(f"[SLACK OK] {msg}")
        else:
            print(f"[SLACK ERROR] Status {response.status_code}: {msg}")
    except Exception as e:
        print(f"[SLACK ERROR] {e}: {msg}")

def process_group(group, state):
    """
    특정 그룹(FARM 또는 LAB)의 GPU 상태를 체크하고 처리

    Args:
        group: "FARM" 또는 "LAB"
        state: 전체 상태 딕셔너리 {"FARM": {...}, "LAB": {...}}
    """
    print(f"\n[PROCESS] Processing group: {group}")
    gpu_status = check_gpu(group)
    now_ts = now()

    # 그룹별 상태 초기화
    if group not in state:
        state[group] = {}
    group_state = state[group]

    # state에 있는 호스트 + gpu_status에 있는 호스트 모두 처리
    all_hosts = set(group_state.keys()) | set(gpu_status.keys())

    for host in all_hosts:
        host_state = group_state.get(host, {})
        gpu_ok = gpu_status.get(host, None)  # None이면 응답 없음 (재부팅 중)

        print(f"\n[HOST] Processing {host} (GPU: {'OK' if gpu_ok else 'FAIL' if gpu_ok is False else 'NO RESPONSE'})")

        # 재부팅 예약이 있는 경우 - 가장 먼저 체크
        if host_state.get("reboot_scheduled_at"):
            # 응답 없음 -> 재부팅 안됨
            if gpu_ok is None:
                print(f"[REBOOT FAILED] {host} did not respond")
                reboot_fail_count = host_state.get("reboot_fail_count", 0) + 1

                # 두 번째 실패부터 알림 발송
                if reboot_fail_count >= 2 and not host_state.get("reboot_failed_notified"):
                    send_slack(f"❌ {host}: 재부팅 실패 (응답 없음, {reboot_fail_count}회)", group)
                    group_state[host]["reboot_failed_notified"] = True

                group_state[host]["reboot_fail_count"] = reboot_fail_count
                group_state[host]["last_gpu_ok"] = False
                group_state[host]["reboot_done"] = False
                group_state[host]["last_checked"] = now_ts.isoformat()
            # 응답 있음 -> 재부팅 성공
            else:
                print(f"[REBOOTED] {host} rebooted successfully")
                group_state[host]["reboot_done"] = True
                group_state[host]["reboot_fail_count"] = 0
                group_state[host]["reboot_failed_notified"] = False
                group_state[host]["last_checked"] = now_ts.isoformat()

        # 1. GPU 사용 불가능한 경우
        if not gpu_ok and gpu_ok is not None:
            # reboot_done인데 여전히 GPU 불가
            if host_state.get("reboot_done"):
                print(f"[PERSISTENT] {host} still has GPU issues after reboot")
                if not host_state.get("persistent_failure_notified"):
                    send_slack(f"❌ {host}: 재부팅 이후에도 GPU 비정상", group)
                    group_state[host]["persistent_failure_notified"] = True
                group_state[host]["last_checked"] = now_ts.isoformat()
            # 재부팅 예약도 없고, reboot 된 적도 없는 경우 == 처음 발견 -> 재부팅 예약
            elif not host_state.get("reboot_scheduled_at"):
                delay_minutes = int(REBOOT_DELAY.total_seconds() / 60)
                scheduled = now_ts + REBOOT_DELAY
                print(f"[SCHEDULE] {host} GPU failed, scheduling reboot in {delay_minutes} minutes")

                # 재부팅 명령 실행
                reboot_host(host, delay_minutes=delay_minutes)

                group_state[host] = {
                    "last_gpu_ok": False,
                    "reboot_scheduled_at": scheduled.isoformat(),
                    "reboot_done": False,
                    "last_checked": now_ts.isoformat()
                }
                send_slack(f"⚠️ {host}: GPU 사용 불가, {delay_minutes}분 후 재부팅 예정", group)
        # 2. GPU 사용 가능한 경우
        else:
            # 재부팅 후 GPU 사용이 가능한 경우
            if host_state.get("reboot_done"):
                print(f"[RECOVERY] {host} recovered after reboot")
                send_slack(f"✅ {host}: 재부팅 후 GPU 정상", group)
            else:
                print(f"[HEALTHY] {host} is healthy")

            group_state[host] = {
                "last_gpu_ok": True,
                "reboot_scheduled_at": None,
                "reboot_done": False,
                "reboot_fail_count": 0,
                "reboot_failed_notified": False,
                "persistent_failure_notified": False,
                "last_checked": now_ts.isoformat()
            }

def main():
    print("=" * 80)
    print(f"[START] GPU Watchdog started at {now().isoformat()}")
    print("=" * 80)

    state = load_state()

    # FARM과 LAB 그룹 모두 처리
    for group in ["FARM", "LAB"]:
        process_group(group, state)

    save_state(state)

    print("\n" + "=" * 80)
    print(f"[END] GPU Watchdog completed at {now().isoformat()}")
    print("=" * 80)

if __name__ == "__main__":
    main()