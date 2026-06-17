import sqlite3
import glob
import os
import sys
import unicodedata
import requests
import time
import io
import re
import concurrent.futures
import traceback
import json

from PySide6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout, QGroupBox, 
    QCheckBox, QRadioButton, QPushButton, QTextEdit, QMessageBox
)
from PySide6.QtCore import QObject, Signal, QThread, QTimer
from PySide6.QtGui import QTextCursor

# --- Auto-Update Logic ---
__version__ = "1.7.0"
# NOTE: 이 URL들은 GitHub 저장소의 메인 브랜치에 있는 원본 파일을 가리킵니다.
VERSION_CHECK_URL = "https://raw.githubusercontent.com/deabbo/MTGA_KR_patcher/main/version.json"
SCRIPT_UPDATE_URL = "https://raw.githubusercontent.com/deabbo/MTGA_KR_patcher/main/app/mtga_KR_patcher.py"

# ★ [중요] 빌드된 최신 .exe 파일을 직접 다운로드할 수 있는 직링크 URL을 입력해주세요.
# 예시: 깃허브 Releases 탭의 최신 에셋 다운로드 주소
EXE_UPDATE_URL = "https://github.com/deabbo/MTGA_KR_patcher/releases/latest/download/mtga_KR_patcher.exe"


def check_for_updates():
    """Checks for a new version of the script/executable and prompts the user to update."""
    try:
        print("Checking for updates...")
        response = requests.get(VERSION_CHECK_URL, timeout=5)
        response.raise_for_status()
        latest_version = response.json()["version"]

        if latest_version > __version__:
            msg_box = QMessageBox()
            msg_box.setIcon(QMessageBox.Information)
            msg_box.setWindowTitle("새 버전 알림")
            msg_box.setText(f"새로운 버전 ({latest_version})이 있습니다. 업데이트하시겠습니까?\n(현재 버전: {__version__})")
            msg_box.setStandardButtons(QMessageBox.Yes | QMessageBox.No)
            msg_box.setDefaultButton(QMessageBox.Yes)
            
            ret = msg_box.exec()

            if ret == QMessageBox.Yes:
                import subprocess
                is_frozen = getattr(sys, 'frozen', False)
                
                if is_frozen:
                    # =========================================================================
                    # 1. 실행 파일(.exe)로 동작 중일 때의 자동 업데이트 로직
                    # =========================================================================
                    exe_path = os.path.abspath(sys.executable)
                    exe_dir = os.path.dirname(exe_path)
                    new_exe_path = os.path.join(exe_dir, "mtga_KR_patcher_new.exe")
                    
                    print(f"Downloading executable update from {EXE_UPDATE_URL}...")
                    exe_response = requests.get(EXE_UPDATE_URL, timeout=30)
                    exe_response.raise_for_status()
                    
                    # 새 실행 파일을 임시 파일 이름으로 저장
                    with open(new_exe_path, 'wb') as f:
                        f.write(exe_response.content)
                    
                    # 업데이트를 수행할 임시 배치 파일(.bat) 생성
                    # - timeout /t 2 : 현재 프로그램이 정상 종료되어 파일 락(Lock)이 해제될 때까지 2초 대기
                    # - del /f /q    : 구버전 실행 파일 강제 삭제
                    # - move /y      : 다운로드된 새 실행 파일을 원본 이름으로 변경
                    # - start ""     : 새 프로그램 실행
                    # - del "%~f0"   : 배치 파일 자신을 스스로 삭제
                    bat_path = os.path.join(exe_dir, "update.bat")
                    bat_content = f"""@echo off
timeout /t 2 /nobreak > nul
if exist "{exe_path}" del /f /q "{exe_path}"
if exist "{new_exe_path}" move /y "{new_exe_path}" "{exe_path}"
start "" "{exe_path}"
del "%~f0"
"""
                    # 한국어 윈도우 환경 및 한글 경로 인식을 위해 cp949 코덱으로 저장
                    with open(bat_path, "w", encoding="cp949") as f:
                        f.write(bat_content)
                    
                    QMessageBox.information(None, "업데이트 준비 완료", "프로그램이 종료된 후 최신 버전으로 자동 교체 및 재실행됩니다.")
                    
                    # 검은색 cmd 창이 표시되지 않도록 백그라운드로 배치 파일 실행
                    CREATE_NO_WINDOW = 0x08000000
                    subprocess.Popen([bat_path], shell=True, creationflags=CREATE_NO_WINDOW)
                    
                    # 현재 프로그램 즉시 안전 종료 (파일 락 해제 목적)
                    sys.exit(0)
                    
                else:
                    # =========================================================================
                    # 2. .py 소스코드 스크립트 상태로 동작 중일 때의 기존 업데이트 로직
                    # =========================================================================
                    print(f"Downloading update from {SCRIPT_UPDATE_URL}...")
                    script_response = requests.get(SCRIPT_UPDATE_URL, timeout=15)
                    script_response.raise_for_status()
                    new_script_content = script_response.content.decode('utf-8')

                    current_script_path = os.path.abspath(__file__)
                    with open(current_script_path, 'w', encoding='utf-8') as f:
                        f.write(new_script_content)

                    QMessageBox.information(None, "업데이트 완료", "업데이트가 완료되었습니다. 프로그램을 다시 시작해주세요.")
                    sys.exit(0)
                    
    except requests.exceptions.RequestException as e:
        print(f"업데이트 확인 중 오류 발생 (네트워크 문제): {e}")
    except Exception as e:
        print(f"업데이트 처리 중 예기치 않은 오류 발생: {e}")
# --- End Auto-Update Logic ---


application_path = None # 패치 시작 시 동적으로 경로 설정

def find_and_set_mtga_path(log_callback):
    global application_path
    log_callback("MTG 아레나 설치 경로를 찾는 중...")
    try:
        log_file_path = os.path.join(os.getenv('APPDATA'), '..', 'LocalLow', 'Wizards Of The Coast', 'MTGA', 'Player.log')
        log_file_path = os.path.normpath(log_file_path)
        if os.path.exists(log_file_path):
            with open(log_file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            match = re.search(r"Mono path\[0\] = '(.+?)/MTGA_Data/Managed'", content)
            if match:
                mtga_base_path = os.path.normpath(match.group(1))
                raw_path = os.path.join(mtga_base_path, "MTGA_Data", "Downloads", "Raw")
                if os.path.isdir(raw_path):
                    log_callback(f"성공: 자동으로 경로를 찾았습니다: {raw_path}")
                    application_path = raw_path
                    return True
    except Exception as e:
        log_callback(f"오류: Player.log 분석 중 예외 발생: {e}")
    log_callback("정보: 자동으로 경로를 찾지 못했습니다. 프로그램 위치를 기준으로 다시 시도합니다.")
    if getattr(sys, 'frozen', False):
        base_path = os.path.dirname(sys.executable)
    else:
        base_path = os.path.dirname(os.path.abspath(__file__))
    paths_to_check = [base_path, os.path.dirname(base_path), os.path.dirname(os.path.dirname(base_path))]
    for path in paths_to_check:
        abs_path = os.path.abspath(path)
        if glob.glob(os.path.join(abs_path, 'Raw_CardDatabase_*.mtga')):
            log_callback(f"정보: 프로그램 근처 폴더에서 게임 데이터를 찾았습니다: {abs_path}")
            application_path = abs_path
            return True
    log_callback("실패: MTG 아레나 데이터 경로를 찾을 수 없습니다.")
    log_callback("팁: 이 패쳐 폴더를 MTG 아레나 설치 폴더의 'MTGA_Data/Downloads/Raw' 폴더 안에 놓고 실행해보세요.")
    return False

def get_target_card_data(log_callback):
    db_file_pattern = os.path.join(application_path, 'Raw_CardDatabase_*.mtga')
    db_files = glob.glob(db_file_pattern)
    if not db_files:
        return []
    db_path = db_files[0]
    card_data_list = []
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        query_cards = """SELECT ArtId, GrpId, ExpansionCode, InterchangeableTitleId, titleId FROM Cards 
                       WHERE (InterchangeableTitleId IS NOT NULL AND InterchangeableTitleId != 0)
                          OR (ExpansionCode glob 'OM[0-9]*') OR ExpansionCode = 'OMB'"""
        cursor.execute(query_cards)
        initial_results = cursor.fetchall()
        query_loc = "SELECT Loc FROM Localizations_enUS WHERE LocId = ? ORDER BY Formatted ASC LIMIT 1"
        
        for row in initial_results:
            interchange_id = row['InterchangeableTitleId']
            title_id = row['titleId']
            
            # Get name associated with titleId
            title_name_result = None
            if title_id:
                cursor.execute(query_loc, (title_id,))
                title_name_result = cursor.fetchone()

            # Get name associated with InterchangeableTitleId
            interchange_name_result = None
            if interchange_id and interchange_id != 0:
                cursor.execute(query_loc, (interchange_id,))
                interchange_name_result = cursor.fetchone()

            # We need at least one name to proceed.
            # The 'CardName' field will hold the name that the OLD logic would have found, for compatibility.
            # But we will also store both names in new fields.
            primary_loc_id = interchange_id if interchange_id and interchange_id != 0 else title_id
            primary_name = None
            if primary_loc_id == interchange_id:
                primary_name = interchange_name_result['Loc'] if interchange_name_result else (title_name_result['Loc'] if title_name_result else None)
            else: # primary_loc_id was title_id
                primary_name = title_name_result['Loc'] if title_name_result else (interchange_name_result['Loc'] if interchange_name_result else None)

            if primary_name:
                card_data_list.append({
                    'ArtId': row['ArtId'], 
                    'GrpId': row['GrpId'], 
                    'ExpansionCode': row['ExpansionCode'], 
                    'CardName': primary_name, # For compatibility with existing code that uses this.
                    'titleId': row['titleId'], 
                    'InterchangeableTitleId': row['InterchangeableTitleId'],
                    'titleName': title_name_result['Loc'] if title_name_result else None,
                    'interchangeName': interchange_name_result['Loc'] if interchange_name_result else None
                })

    except sqlite3.Error as e:
        log_callback(f"데이터베이스 오류 발생: {e}")
    finally:
        if conn: conn.close()
    return card_data_list

def normalize_name(name):
    return re.sub(r'[^a-z0-9]', '', name.lower()) if name else ""


def has_final_consonant(char):
    if '가' <= char <= '힣': return (ord(char) - ord('가')) % 28 > 0
    return True

def run_english_name_patch(log_callback):
    log_callback("=== 카드 이름 영문화 패치 시작 ===")
    db_file_pattern = os.path.join(application_path, 'Raw_CardDatabase_*.mtga')
    db_files = glob.glob(db_file_pattern)
    if not db_files:
        log_callback("  - 카드 데이터베이스 파일을 찾지 못해 건너뜁니다.")
        return
    db_path = db_files[0]
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # 1. Get all LocIds that are purely card titles from the Cards table's titleId column, excluding tokens.
        # Also exclude IDs that are used for abilities with Category = 3 as requested.
        log_callback("  - 카드 테이블에서 순수 카드 제목 ID를 수집하는 중 (토큰 제외)...")
        query = """
            SELECT DISTINCT titleId FROM Cards 
            WHERE titleId IS NOT NULL AND isToken = 0
            AND titleId NOT IN (SELECT TextId FROM Abilities WHERE Category = 3 AND TextId IS NOT NULL)
        """
        cursor.execute(query)
        card_title_ids = {str(row[0]) for row in cursor.fetchall()}
        
        if not card_title_ids:
            log_callback("  - 카드 테이블에서 타이틀 ID를 찾을 수 없습니다.")
            return
        log_callback(f"  - {len(card_title_ids)}개의 고유한 카드 타이틀 ID를 찾았습니다.")

        # 2. Get English localizations for only the collected card title IDs.
        log_callback("  - 영어 로컬라이제이션 데이터를 읽는 중...")
        placeholders = ', '.join('?' * len(card_title_ids))
        cursor.execute(f"SELECT LocId, Loc FROM Localizations_enUS WHERE LocId IN ({placeholders})", list(card_title_ids))
        
        updates = []
        for loc_id, loc_text in cursor.fetchall():
            updates.append((loc_text, str(loc_id)))
        
        if not updates:
            log_callback("  - 업데이트할 카드 이름을 찾지 못했습니다.")
            return

        # 3. Execute the update on the Korean localization table, with special handling for "A-" cards.
        log_callback(f"  - {len(updates)}개의 카드 이름을 영어로 덮어씁니다...")
        
        a_minus_sprite_tag = '<sprite="SpriteSheet_MiscIcons" name="arena_a">'
        total_updated_rows = 0
        a_minus_cards_count = 0

        for loc_text, loc_id in updates:
            # First, update all formatted versions for this LocId with the English name.
            cursor.execute("UPDATE Localizations_koKR SET Loc = ? WHERE LocId = ?", (loc_text, loc_id))
            total_updated_rows += cursor.rowcount

            # If the name starts with "A-", replace the prefix with a sprite for Formatted = 1.
            if loc_text.startswith("A-"):
                a_minus_cards_count += 1
                
                # Replace only the "A-" prefix with the sprite tag.
                new_loc_text = a_minus_sprite_tag + loc_text[2:]
                
                # This is a specific override for a single formatted version.
                cursor.execute(
                    "UPDATE Localizations_koKR SET Loc = ? WHERE LocId = ? AND Formatted = 1",
                    (new_loc_text, loc_id)
                )

        conn.commit()
        
        log_callback(f"  - 총 {total_updated_rows}개 항목이 업데이트되었습니다.")
        if a_minus_cards_count > 0:
            log_callback(f"  - 그 중 {a_minus_cards_count}개의 'A-' 카드를 특별 아이콘으로 처리했습니다.")
        log_callback("=== 카드 이름 영문화 패치 완료 ===")

    except sqlite3.Error as e:
        log_callback(f"  - 카드 이름 영어로 변경 중 데이터베이스 오류 발생: {e}")
    finally:
        if conn: conn.close()

   
def fetch_json_from_url(url, log_callback):
    try: response = requests.get(url); response.raise_for_status(); return response.json()
    except requests.exceptions.RequestException as e: log_callback(f"웹에서 데이터를 가져오는 중 오류 발생: {e}"); return None

def formatting_for_2(text): return unicodedata.normalize('NFD', text)

def patch_card_text(log_callback, data_URL):
    """사용자 정의 JSON 파일을 읽어 DB에 패치를 적용합니다."""
    log_callback(f"=== 카드 텍스트 변경시작 ===")

    card_file_pattern = os.path.join(application_path, 'Raw_CardDatabase_*.mtga')
    card_files = glob.glob(card_file_pattern)
    if not card_files:
        log_callback(f"카드 텍스트 변경실패")
    else:
        log_callback("카드 번역 데이터 불러오는중...")
        card_values_to_update = fetch_json_from_url(data_URL, log_callback)

        for card_file in card_files:
            # log_callback(f"카드 데이터베이스 처리 중: {card_file}")
            try:
                card_conn = sqlite3.connect(card_file)
                card_cursor = card_conn.cursor()
                
                card_cursor.execute("SELECT LocId, Loc, Formatted FROM Localizations_koKR WHERE Loc LIKE '%&lt;%' OR Loc LIKE '%&gt;%'" )
                rows_to_update = card_cursor.fetchall()
                for loc_id, loc_text, formatted in rows_to_update:
                    new_loc_text = loc_text.replace('&lt;', '<').replace('&gt;', '>')
                    if formatted == 2: new_loc_text = formatting_for_2(new_loc_text)
                    else: new_loc_text = unicodedata.normalize('NFC', new_loc_text)
                    if new_loc_text != loc_text:
                        card_cursor.execute("UPDATE Localizations_koKR SET Loc = ? WHERE LocId = ? AND Formatted = ?", (new_loc_text, loc_id, formatted))

                if card_values_to_update:
                    for card in card_values_to_update:
                        search_value = card['LocId']
                        value_for_0 = card['Formatted_0']
                        value_for_1 = card['Formatted_1']
                        value_for_2 = formatting_for_2(card['Formatted_0'])
                        
                        card_cursor.execute("SELECT Formatted FROM Localizations_koKR WHERE LocId = ?", (search_value,))
                        rows = card_cursor.fetchall()

                        if not rows:
                            continue
                        
                        for row in rows:
                            formatted_value = row[0]
                            new_value = None
                            if formatted_value == 0: new_value = value_for_0
                            elif formatted_value == 1: new_value = value_for_1 if value_for_1 else value_for_0
                            elif formatted_value == 2: new_value = value_for_2
                            
                            if new_value:
                                card_cursor.execute("UPDATE Localizations_koKR SET Loc = ? WHERE LocId = ? AND Formatted = ?", (new_value, search_value, formatted_value))

                card_conn.commit()
                log_callback("  - 카드 데이터베이스 수정 완료.")

            except sqlite3.Error as e:
                log_callback(f"    - 에러 발생: {e}")
            finally:
                if card_conn: card_conn.close()
        


def run_localization_patch(log_callback):
    """메인 한글패치 로직"""
    log_callback("=== 한글 오역 패치 시작 ===")
    
    # 1. UI 오역 수정
    client_file_pattern = os.path.join(application_path, 'Raw_ClientLocalization_*.mtga')
    client_files = glob.glob(client_file_pattern)
    
    if not client_files:
        log_callback(f"클라이언트 파일을 찾지 못했습니다. UI 관련 패치를 건너뜁니다.")
    else:
        log_callback("UI 번역 데이터 불러오는중...")
        client_json_url = "https://docs.google.com/uc?export=download&id=1oOqAmmoyJ9FJZsrWccMoLjMchAatWtou&confirm=t"
        client_values_to_update = fetch_json_from_url(client_json_url, log_callback)
        
        if client_values_to_update:
            for client_file in client_files:
                # log_callback(f"  - 파일 처리 중: {client_file}")
                try:
                    client_conn = sqlite3.connect(client_file)
                    client_cursor = client_conn.cursor()
                    for client in client_values_to_update:
                        client_cursor.execute("UPDATE Loc SET koKR = ? WHERE key = ?", (client['KoKR'], client['Key']))
                    client_conn.commit()
                except sqlite3.Error as e:
                    log_callback(f"    - 에러 발생: {e}")
                finally:
                    if 'client_conn' in locals() and client_conn: client_conn.close()
            log_callback("UI 번역 업데이트 완료.")
        else:
            log_callback("UI 번역 데이터를 가져오지 못했습니다.")

    # 2. 카드 오역 수정
    patch_card_text(log_callback, "https://docs.google.com/uc?export=download&id=1pSF_YCV0NPuy240Rtt0bzOmr1GyE5HMd&confirm=t")
    
    # patch_seek_keyword(log_callback) 더이상 사용하지 않음 
    patch_sneak_keyword(log_callback)
    patch_vanishing_keyword(log_callback)
    log_callback("=== 한글 오역 패치 완료 ===")

def patch_vanishing_keyword(log_callback):
    log_callback("  - '소실' -> '사라짐' 키워드 패치 시작...")
    db_files = glob.glob(os.path.join(application_path, 'Raw_CardDatabase_*.mtga'))
    if not db_files:
        log_callback("    - 카드 데이터베이스를 찾을 수 없어 건너뜁니다.")
        return

    db_path = db_files[0]
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # 1. AbilityWord = 394 인 항목의 Id와 TextId 조회
        try:
            cursor.execute("SELECT Id, TextId FROM Abilities WHERE AbilityWord = 53")
            ability_rows = cursor.fetchall()
        except sqlite3.OperationalError:
            log_callback("    - 'Abilities' 테이블에 'AbilityWord' 컬럼이 없거나 쿼리 오류.")
            return
        

        if not ability_rows:
            log_callback("    - '소실' 관련 능력 텍스트 ID를 찾지 못했습니다.")
            return

        target_loc_ids = set()
        redirect_check_ability_ids = set()

        # 2. TextId 수집 및 #NoTranslationNeeded 확인
        text_ids_to_check = {str(row[1]) for row in ability_rows if row[1]}
        
        loc_lookup = {}
        if text_ids_to_check:
            placeholders = ', '.join('?' * len(text_ids_to_check))
            cursor.execute(f"SELECT LocId, Loc FROM Localizations_koKR WHERE LocId IN ({placeholders})", list(text_ids_to_check))
            
            rows = cursor.fetchall()
            for row in rows:
                loc_id = str(row[0])
                loc_text = row[1]
                if loc_id not in loc_lookup or loc_text == "#NoTranslationNeeded":
                    loc_lookup[loc_id] = loc_text

        # 3. 분류
        for ab_id, text_id in ability_rows:
            if not text_id: continue
            
            val = loc_lookup.get(str(text_id))
            if val == "#NoTranslationNeeded":
                redirect_check_ability_ids.add(ab_id)
            elif val:
                target_loc_ids.add(str(text_id))


        # 4. 리다이렉트 처리 (Cards 테이블 스캔)
        if redirect_check_ability_ids:
            cursor.execute("SELECT AbilityIds FROM Cards WHERE AbilityIds IS NOT NULL AND AbilityIds LIKE '%:%'")
            
            card_rows = cursor.fetchall()
            found_redirects = 0
            
            for (ab_ids_str,) in card_rows:
                if not ab_ids_str: continue
                for segment in ab_ids_str.split(','):
                    segment = segment.strip()
                    if ':' in segment:
                        parts = segment.split(':', 1)
                        if len(parts) == 2:
                            try:
                                a_id = int(parts[0].strip())
                                b_id = parts[1].strip()
                                
                                if a_id in redirect_check_ability_ids:
                                    target_loc_ids.add(str(b_id))
                                    found_redirects += 1
                            except ValueError:
                                pass

        if not target_loc_ids:
            log_callback("    - 수정할 텍스트 ID를 찾지 못했습니다.")
            return

        # 5. 최종 수정 실행
        placeholders = ', '.join('?' * len(target_loc_ids))
        query = f"SELECT LocId, Loc, Formatted FROM Localizations_koKR WHERE LocId IN ({placeholders})"
        cursor.execute(query, list(target_loc_ids))

        updates = []
        
        target_nfc = "소실"
        replace_nfc = "사라짐"
        target_nfd = unicodedata.normalize('NFD', target_nfc)
        replace_nfd = unicodedata.normalize('NFD', replace_nfc)

        for loc_id, loc_text, formatted in cursor.fetchall():
            new_text = loc_text
            
            if formatted == 2:
                if target_nfd in loc_text:
                    new_text = loc_text.replace(target_nfd, replace_nfd)
            else:
                if target_nfc in loc_text:
                    new_text = loc_text.replace(target_nfc, replace_nfc)

            if new_text != loc_text:
                updates.append((new_text, loc_id, formatted))

        if updates:
            cursor.executemany("UPDATE Localizations_koKR SET Loc = ? WHERE LocId = ? AND Formatted = ?", updates)
            conn.commit()
            log_callback(f"  - 총 {len(updates)}개 항목에서 '소실'을 '사라짐'으로 변경했습니다.")
        else:
            log_callback("  - 변경할 '소실' 텍스트가 없거나 이미 패치되었습니다.")

    except sqlite3.Error as e:
        log_callback(f"    - '소실' 패치 중 데이터베이스 오류 발생: {e}")
    finally:
        if conn: conn.close()

def patch_sneak_keyword(log_callback):
    log_callback("  - '기습' -> '암습' 키워드 패치 시작...")
    db_files = glob.glob(os.path.join(application_path, 'Raw_CardDatabase_*.mtga'))
    if not db_files:
        log_callback("    - 카드 데이터베이스를 찾을 수 없어 건너뜁니다.")
        return

    db_path = db_files[0]
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # 1. 394 관련 Ability 조회 (Id와 TextId)
        cursor.execute("SELECT Id, TextId FROM Abilities WHERE Id = 394 OR BaseId = 394 OR ReferencedAbilityIds = 394")
        ability_rows = cursor.fetchall()
        
        
        if not ability_rows:
            log_callback("    - '기습' 관련 능력을 찾지 못했습니다.")
            return

        target_loc_ids = set()
        redirect_check_ability_ids = set() # #NoTranslationNeeded 인 AbilityId들

        # 2. TextId 수집
        text_ids_to_check = {str(row[1]) for row in ability_rows if row[1]}
        
        loc_lookup = {}
        if text_ids_to_check:
            placeholders = ', '.join('?' * len(text_ids_to_check))
            # Formatted 조건 없이 모든 관련 텍스트 조회 (디버깅 및 누락 방지)
            cursor.execute(f"SELECT LocId, Loc FROM Localizations_koKR WHERE LocId IN ({placeholders})", list(text_ids_to_check))
            
            rows = cursor.fetchall()

            for row in rows:
                loc_id = str(row[0])
                loc_text = row[1]
                # 이미 저장된 값이 없거나, 현재 값이 #NoTranslationNeeded라면 덮어씀 (우선순위 부여)
                if loc_id not in loc_lookup or loc_text == "#NoTranslationNeeded":
                    loc_lookup[loc_id] = loc_text

        # 3. 분류: 일반 수정 vs 리다이렉트 필요
        for ab_id, text_id in ability_rows:
            if not text_id: continue
            
            val = loc_lookup.get(str(text_id))
            if val == "#NoTranslationNeeded":
                # 3-1. #NoTranslationNeeded 인 경우 -> 리다이렉트 목록(AbilityId)에 추가
                redirect_check_ability_ids.add(ab_id)
            elif val:
                # 3-2. 일반 텍스트인 경우 -> 수정 대상(TextId)에 바로 추가
                target_loc_ids.add(str(text_id))
            else:
                # DB에 없는 경우 로그 남김
                # log_callback(f"    [DEBUG] TextId {text_id} not found in koKR DB.")
                pass


        # 4. 리다이렉트 처리 (Cards 테이블 스캔)
        if redirect_check_ability_ids:
            cursor.execute("SELECT AbilityIds FROM Cards WHERE AbilityIds IS NOT NULL AND AbilityIds LIKE '%:%'")
            
            card_rows = cursor.fetchall()
            found_redirects = 0
            
            for (ab_ids_str,) in card_rows:
                if not ab_ids_str: continue
                # AbilityIds 포맷 예: "123, 394:99999, 456"
                for segment in ab_ids_str.split(','):
                    segment = segment.strip()
                    if ':' in segment:
                        parts = segment.split(':', 1)
                        if len(parts) == 2:
                            try:
                                a_id = int(parts[0].strip()) # AbilityId
                                b_id = parts[1].strip()      # New Target LocId
                                
                                # A 자리에 있는 값이 리다이렉트 대상 AbilityId라면 B 값을 타겟으로 추가
                                if a_id in redirect_check_ability_ids:
                                    target_loc_ids.add(str(b_id))
                                    found_redirects += 1
                            except ValueError:
                                pass

        if not target_loc_ids:
            log_callback("    - 수정할 텍스트 ID를 찾지 못했습니다.")
            return

        # 5. 최종 수정 실행 (Formatted 0, 1, 2 모두 처리)
        placeholders = ', '.join('?' * len(target_loc_ids))
        query = f"SELECT LocId, Loc, Formatted FROM Localizations_koKR WHERE LocId IN ({placeholders})"
        cursor.execute(query, list(target_loc_ids))

        updates = []
        
        # 검색어 (NFC)
        target_nfc = "기습"
        replace_nfc = "잠행"
        # 검색어 (NFD) - Formatted=2 용
        target_nfd = unicodedata.normalize('NFD', target_nfc)
        replace_nfd = unicodedata.normalize('NFD', replace_nfc)

        for loc_id, loc_text, formatted in cursor.fetchall():
            new_text = loc_text
            
            if formatted == 2:
                # Formatted=2 (NFD) 처리
                if target_nfd in loc_text:
                    new_text = loc_text.replace(target_nfd, replace_nfd)
            else:
                # Formatted=0, 1 (NFC) 처리
                if target_nfc in loc_text:
                    new_text = loc_text.replace(target_nfc, replace_nfc)

            if new_text != loc_text:
                updates.append((new_text, loc_id, formatted))

        if updates:
            cursor.executemany("UPDATE Localizations_koKR SET Loc = ? WHERE LocId = ? AND Formatted = ?", updates)
            conn.commit()
            log_callback(f"  - 총 {len(updates)}개 항목에서 '기습(Sneak)'을 '잠행'으로 변경했습니다.")
        else:
            log_callback("  - 변경할 '기습(Sneak)' 텍스트가 없거나 이미 패치되었습니다.")

    except sqlite3.Error as e:
        log_callback(f"    - '기습(Sneak)' 패치 중 데이터베이스 오류 발생: {e}")
    finally:
        if conn: conn.close()

def run_patch_removal(log_callback, script_base_path):
    log_callback("=== 패치 제거 시작 ===")
    
    # 1. Remove patched image asset bundles
    patch_log_path = os.path.join(script_base_path, 'patch_log.json')
    if os.path.exists(patch_log_path):
        log_callback("  - patch_log.json을 기반으로 이미지 에셋 번들 제거 중...")
        try:
            with open(patch_log_path, 'r', encoding='utf-8') as f:
                patch_log = json.load(f)
            
            patched_items = patch_log.get('patched_images', {})
            if patched_items:
                asset_bundle_path = os.path.join(application_path, "..", "AssetBundle")
                removed_count = 0
                for key, value in patched_items.items():
                    found_files = []
                    if key.startswith("sleeve_"):
                        if isinstance(value, dict) and value.get('bucket_id'):
                            asset_pattern = os.path.join(asset_bundle_path, f"*{value.get('bucket_id')}*")
                            found_files = glob.glob(asset_pattern)
                    else: # Old format for cards
                        art_id = key
                        asset_pattern = os.path.join(asset_bundle_path, f"{str(art_id).zfill(6)}_CardArt_*")
                        found_files = glob.glob(asset_pattern)

                    for file_path in found_files:
                        try:
                            os.remove(file_path)
                            log_callback(f"    - 삭제됨: {os.path.basename(file_path)}")
                            removed_count += 1
                        except OSError as e:
                            log_callback(f"    - 오류: {os.path.basename(file_path)} 삭제 실패: {e}")
                log_callback(f"  - 총 {removed_count}개의 이미지 에셋 번들 파일을 삭제했습니다.")
            else:
                log_callback("  - 제거할 이미지 에셋 번들 로그가 없습니다.")

            # Remove the log file itself
            try:
                os.remove(patch_log_path)
                log_callback("  - patch_log.json 파일을 삭제했습니다.")
            except OSError as e:
                log_callback(f"  - 오류: patch_log.json 삭제 실패: {e}")

        except (json.JSONDecodeError, IOError) as e:
            log_callback(f"  - 오류: patch_log.json 읽기 실패: {e}")
    else:
        log_callback("  - patch_log.json을 찾을 수 없어 이미지 에셋 번들 제거를 건너뜁니다.")

    # 2. Remove database files
    log_callback("  - 데이터베이스 파일 제거 중...")
    files_to_remove = []
    
    # Database files in 'Raw'
    db_pattern = os.path.join(application_path, 'Raw_CardDatabase_*.mtga')
    client_pattern = os.path.join(application_path, 'Raw_ClientLocalization_*.mtga')
    files_to_remove.extend(glob.glob(db_pattern))
    files_to_remove.extend(glob.glob(client_pattern))

    if files_to_remove:
        removed_file_count = 0
        for file_path in files_to_remove:
            try:
                os.remove(file_path)
                log_callback(f"    - 삭제됨: {os.path.basename(file_path)}")
                removed_file_count += 1
            except OSError as e:
                log_callback(f"    - 오류: {os.path.basename(file_path)} 삭제 실패: {e}")
        log_callback(f"  - 총 {removed_file_count}개의 데이터베이스 파일을 삭제했습니다.")
    else:
        log_callback("  - 제거할 데이터베이스 파일을 찾지 못했습니다.")
    
    log_callback("패치 제거가 완료되었습니다. 게임을 재시작하여 파일을 복구하세요.")


class PatchWorker(QObject):
    log = Signal(str, bool)
    finished = Signal()

    def __init__(self, patch_options):
        super().__init__()
        self.patch_options = patch_options

    def run(self):
        def log_callback(message, update_last_line=False):
            self.log.emit(message, update_last_line)
        
        try:
            if getattr(sys, 'frozen', False):
                script_base_path = os.path.dirname(sys.executable)
            else:
                script_base_path = os.path.dirname(os.path.abspath(__file__))

            if not find_and_set_mtga_path(log_callback):
                log_callback("*** 필수 파일 경로를 찾지 못해 패치를 중단합니다. ***")
                self.finished.emit()
                return

            if self.patch_options['remove_patch']:
                run_patch_removal(log_callback, script_base_path)
                log_callback("*** 패치 제거가 완료되었습니다. ***")
                self.finished.emit()
                return

            if self.patch_options['mistranslation']:
                run_localization_patch(log_callback)
            
            # if self.patch_options['images']:
            #     run_image_change(log_callback, self.patch_options['name_option'], script_base_path)

            if self.patch_options['english_names_only']:
                run_english_name_patch(log_callback)
            
            log_callback("*** 모든 작업이 완료되었습니다. ***")

        except Exception as e:
            log_callback(f"*** 패치 중 오류 발생: {e} ***")
            log_callback(traceback.format_exc())
        finally:
            self.finished.emit()

class PatcherWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("MTG 아레나 한글 통합패치")
        self.resize(700, 550)
        self._last_log_was_update = False

        # --- Widgets ---
        self.mistranslation_check = QCheckBox("한글 오역 패치")
        self.mistranslation_check.setChecked(True)

        self.english_names_only_check = QCheckBox("카드이름만 영어로(고인물용)")
        self.remove_patch_check = QCheckBox("패치 제거")

        self.run_button = QPushButton("패치 시작")
        self.log_box = QTextEdit()
        self.log_box.setReadOnly(True)

        # --- Layouts ---
        main_layout = QVBoxLayout(self)

        options_group = QGroupBox("패치 종류")
        options_layout = QHBoxLayout()
        options_layout.addWidget(self.mistranslation_check)
        # options_layout.addWidget(self.image_check)
        options_layout.addWidget(self.english_names_only_check)
        options_layout.addWidget(self.remove_patch_check)
        options_group.setLayout(options_layout)

        log_group = QGroupBox("로그")
        log_layout = QVBoxLayout()
        log_layout.addWidget(self.log_box)
        log_group.setLayout(log_layout)

        main_layout.addWidget(options_group)
        # main_layout.addWidget(name_group)
        main_layout.addWidget(self.run_button)
        main_layout.addWidget(log_group)

        # --- Connections ---
        self.mistranslation_check.stateChanged.connect(self.update_ui_state)
        # self.image_check.stateChanged.connect(self.update_ui_state)
        self.english_names_only_check.stateChanged.connect(self.update_ui_state)
        self.remove_patch_check.stateChanged.connect(self.update_ui_state)
        # self.image_check.stateChanged.connect(self.show_image_warning)
        self.run_button.clicked.connect(self.start_patch)

        # --- Initial State ---
        self.update_ui_state()

    def update_ui_state(self):
        is_remove = self.remove_patch_check.isChecked()

        if is_remove:
            # '패치 제거'가 선택되면 다른 모든 옵션을 비활성화
            self.mistranslation_check.setChecked(False)
            # self.image_check.setChecked(False)
            self.english_names_only_check.setChecked(False)

            self.mistranslation_check.setEnabled(False)
            # self.image_check.setEnabled(False)
            self.english_names_only_check.setEnabled(False)
            # self.name_group.setEnabled(False)
            
            self.run_button.setEnabled(True)
            self.run_button.setText("패치 제거 시작")
            return

        # '패치 제거'가 선택되지 않은 경우, 다른 옵션들을 다시 활성화
        self.mistranslation_check.setEnabled(True)
        # self.image_check.setEnabled(True)
        self.english_names_only_check.setEnabled(True)

        is_mistranslation = self.mistranslation_check.isChecked()
        is_images = False # self.image_check.isChecked()
        is_english_only = self.english_names_only_check.isChecked()


        can_run = is_mistranslation or is_images or is_english_only
        self.run_button.setEnabled(can_run)

        texts = []
        if is_images:
            texts.append("실물카드")
        if is_mistranslation:
            texts.append("오역")
        if is_english_only:
            texts.append("영문 이름")

        if texts:
            self.run_button.setText(f"{ ' & '.join(texts)} 패치 시작")
        else:
            self.run_button.setText("옵션을 선택하세요")

    def start_patch(self):
        self.run_button.setText("패치 진행 중...")
        self.run_button.setEnabled(False)
        self.log_box.clear()

        patch_options = {
            'mistranslation': self.mistranslation_check.isChecked(),
            'images': False, # self.image_check.isChecked(),
            'english_names_only': self.english_names_only_check.isChecked(),
            'remove_patch': self.remove_patch_check.isChecked(),
            'name_option': 'art_only'
        }
        # if self.real_english_radio.isChecked():
        #     patch_options['name_option'] = 'real_english'
        # elif self.unofficial_korean_radio.isChecked():
        #     patch_options['name_option'] = 'unofficial_korean'

        # --- Threading Setup ---
        self.thread = QThread()
        self.worker = PatchWorker(patch_options)
        self.worker.moveToThread(self.thread)

        self.thread.started.connect(self.worker.run)
        self.worker.finished.connect(self.thread.quit)
        self.worker.finished.connect(self.worker.deleteLater)
        self.thread.finished.connect(self.thread.deleteLater)
        self.worker.log.connect(self.append_log)
        self.thread.finished.connect(self.on_patch_finished)

        self.thread.start()

    def append_log(self, message, update_last_line=False):
        if update_last_line and self._last_log_was_update:
            cursor = self.log_box.textCursor()
            cursor.movePosition(QTextCursor.End)
            cursor.movePosition(QTextCursor.StartOfBlock, QTextCursor.MoveAnchor)
            cursor.movePosition(QTextCursor.EndOfBlock, QTextCursor.KeepAnchor)
            cursor.insertText(message)
        else:
            self.log_box.append(message)
        
        self._last_log_was_update = update_last_line

    def on_patch_finished(self):
        self.run_button.setText("완료!")
        self.append_log("3초 후 자동으로 종료됩니다...")
        QTimer.singleShot(3000, self.close)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    
    # Run the update check before starting the main window
    check_for_updates()

    window = PatcherWindow()
    window.show()
    sys.exit(app.exec())
