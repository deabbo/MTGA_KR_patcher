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
__version__ = "1.3"
# NOTE: These URLs point to the raw files in the main branch of the GitHub repository.
VERSION_CHECK_URL = "https://raw.githubusercontent.com/deabbo/MTGA_KR_patcher/main/version.json"
SCRIPT_UPDATE_URL = "https://raw.githubusercontent.com/deabbo/MTGA_KR_patcher/main/app/mtga_KR_patcher.py"

def check_for_updates():
    """Checks for a new version of the script and prompts the user to update."""
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

# Pillow와 UnityPy는 외부 라이브러리이므로, 실행 전 설치가 필요합니다.
try:
    import UnityPy
    from PIL import Image
except ImportError:
    print("필수 라이브러리가 설치되지 않았습니다. 'pip install UnityPy Pillow' 명령어로 설치해주세요.")
    os.system("pause")
    sys.exit()

# --- 기존 백엔드 로직 (수정 없음) ---

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

def setup_unitypy():
    UnityPy.config.FALLBACK_UNITY_VERSION = '2022.3.42f1'
    UnityPy.set_assetbundle_decrypt_key('3141592653589793')

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

def fetch_all_sets_data(expansion_codes, log_callback):
    all_sets_data = {}
    base_url = "https://api.scryfall.com/cards/search"
    headers = {'User-Agent': 'MTGA-Art-Replacer/0.8', 'Accept': 'application/json'}
    for code in expansion_codes:
        if code.upper() == 'OM1':
            search_codes = ['spm', 'tspm']
        elif code.upper() == 'OMB':
            search_codes = ['mar', 'tspm']
        else:
            search_codes = [code.lower()]
        set_cards_by_name = {}
        for search_code in search_codes:
            params = {'q': f"set:{search_code}", 'include_variations': 'true'}
            try:
                response = requests.get(base_url, headers=headers, params=params)
                response.raise_for_status()
                json_data = response.json()
            except requests.exceptions.RequestException as e:
                log_callback(f"  - '{search_code}' 검색 중 오류 발생: {e}")
                continue
            while True:
                for card_info in json_data.get('data', []):
                    if "card_faces" in card_info:
                        for face in card_info["card_faces"]:
                            if face.get("name") and "image_uris" in face:
                                set_cards_by_name[normalize_name(face.get("name"))] = face
                    elif card_info.get('name'):
                        set_cards_by_name[normalize_name(card_info.get('name'))] = card_info
                if json_data.get('has_more'):
                    try:
                        time.sleep(0.1)
                        response = requests.get(json_data['next_page'], headers=headers)
                        response.raise_for_status()
                        json_data = response.json()
                    except requests.exceptions.RequestException as e:
                        log_callback(f"  - 다음 페이지 요청 중 오류: {e}")
                        break
                else:
                    break
        all_sets_data[code] = set_cards_by_name
    return all_sets_data

def download_images(art_id_to_url, log_callback):
    log_callback("--- 모든 이미지 다운로드 시작 ---")
    art_id_to_image_data = {}
    total = len(art_id_to_url)
    def _download(art_id, url):
        try:
            response = requests.get(url)
            response.raise_for_status()
            return art_id, response.content
        except requests.exceptions.RequestException:
            return art_id, None
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        future_to_art_id = {executor.submit(_download, art_id, url): art_id for art_id, url in art_id_to_url.items()}
        for i, future in enumerate(concurrent.futures.as_completed(future_to_art_id)):
            art_id, content = future.result()
            if content:
                art_id_to_image_data[art_id] = content
            else:
                log_callback(f"  - 오류: ArtId {art_id} 이미지 다운로드 실패.")
            log_callback(f"이미지 다운로드: ({i + 1}/{total})", update_last_line=True)
    log_callback("이미지 다운로드 완료.")
    return art_id_to_image_data

def replace_card_art(art_id, image_content, log_callback):
    asset_bundle_pattern = os.path.join(application_path, "..", "AssetBundle", f"{str(art_id).zfill(6)}_CardArt_*")
    found_files = glob.glob(asset_bundle_pattern)
    if not found_files:
        log_callback(f"  - 정보: ArtId {art_id}에 해당하는 에셋 번들을 찾을 수 없습니다. 건너뜁니다.")
        return False

    if len(found_files) > 1:
        # log_callback(f"  - 경고: ArtId {art_id}에 대해 여러 에셋 번들이 발견되었습니다. 첫 번째 파일({os.path.basename(found_files[0])})을 사용합니다.")
        pass

    asset_path = found_files[0]
    try:
        downloaded_image = Image.open(io.BytesIO(image_content))
        env = UnityPy.load(asset_path)
        
        padded_art_id = str(art_id).zfill(6)
        targets = [obj.read() for obj in env.objects if obj.type.name == "Texture2D" and obj.read().m_Name == f"{padded_art_id}_AIF"]
        
        if not targets:
            texture_names = [obj.read().m_Name for obj in env.objects if obj.type.name == "Texture2D"]
            log_callback(f"  - 정보: 에셋 번들 {os.path.basename(asset_path)}에서 '{padded_art_id}_AIF' Texture2D를 찾을 수 없습니다. 사용 가능한 텍스쳐: {texture_names}. 건너뜁니다.")
            return False

        for target_obj in targets:
            target_obj.image = downloaded_image
            target_obj.save()
            
        with open(asset_path, "wb") as f:
            f.write(env.file.save())
        
        return True
            
    except Exception as e:
        log_callback(f"  - 오류: ArtId {art_id} ({os.path.basename(asset_path)}) 작업 중 에러 발생: {e}")
        return False

def update_card_names(log_callback, name_option):
    if name_option == "art_only": return
    elif name_option == "real_english":
        log_callback("  - '실물 영어로 변경' ")
        db_file_pattern = os.path.join(application_path, 'Raw_CardDatabase_*.mtga')
        db_files = glob.glob(db_file_pattern)
        if not db_files: return
        db_path = db_files[0]
        conn = None
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT LocId, Loc FROM Localizations_enUS")
            en_loc_map = {str(row[0]): row[1] for row in cursor.fetchall()}
            cursor.execute("SELECT titleId, InterchangeableTitleId FROM Cards WHERE InterchangeableTitleId IS NOT NULL AND InterchangeableTitleId != 0")
            cards_to_update = cursor.fetchall()
            translation_data_for_abilities, name_updates = [], []
            for title_id, interchangeable_id in cards_to_update:
                new_name, old_name = en_loc_map.get(str(interchangeable_id)), en_loc_map.get(str(title_id))
                if new_name and old_name and new_name != old_name:
                    name_updates.append((new_name, title_id))
                    translation_data_for_abilities.append({'titleId': title_id, 'origin_en_name': old_name, 'new_en_name': new_name})
            if name_updates:
                cursor.executemany("UPDATE Localizations_koKR SET Loc = ? WHERE LocId = ?", name_updates)
                cursor.executemany("UPDATE Localizations_enUS SET Loc = ? WHERE LocId = ?", name_updates)
                conn.commit()
            log_callback(f"  - 총 {len(name_updates)}개 카드의 이름을 영문으로 업데이트했습니다.")
            if translation_data_for_abilities:
                update_ability_text(log_callback, translation_data_for_abilities, only_english=True)
        except sqlite3.Error as e:
            log_callback(f"  - 카드 이름 변경 중 데이터베이스 오류 발생: {e}")
        finally:
            if conn: conn.close()
    elif name_option == "unofficial_korean":
        log_callback("  - '비공식 한국어로 변경'을 시작합니다...")
        json_url = "https://drive.usercontent.google.com/u/0/uc?id=1sEroW-b2FxC6rsH6-gTsRsj_5iADgcWm&confirm=t"
        translation_data = fetch_json_from_url(json_url, log_callback)
        if not translation_data: return
        db_files = glob.glob(os.path.join(application_path, 'Raw_CardDatabase_*.mtga'))
        if not db_files: return
        db_path = db_files[0]
        conn = None
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT LocId, Formatted FROM Localizations_koKR")
            formatted_map = {str(loc_id): [] for loc_id, _ in cursor.fetchall()}
            for loc_id, formatted in formatted_map.items():
                cursor.execute("SELECT Formatted FROM Localizations_koKR WHERE LocId = ?", (loc_id,))
                formatted_map[loc_id] = [row[0] for row in cursor.fetchall()]
            update_count = 0
            for card in translation_data:
                title_id, interchangeable_id = card['titleId'], card['InterchangeableTitleId']
                new_en, new_ko = card['new_en_name'], card['new_ko_name']
                origin_en, origin_ko = card['origin_en_name'], card['origin_ko_name']
                cursor.execute("UPDATE Localizations_enUS SET Loc = ? WHERE LocId = ?", (new_en, title_id)); update_count += cursor.rowcount
                if str(title_id) in formatted_map:
                    for fmt in formatted_map[str(title_id)]:
                        ko_loc = new_ko['nfd'] if fmt == 2 else new_ko['nfc']
                        cursor.execute("UPDATE Localizations_koKR SET Loc = ? WHERE LocId = ? AND Formatted = ?", (ko_loc, title_id, fmt)); update_count += cursor.rowcount
                cursor.execute("UPDATE Localizations_enUS SET Loc = ? WHERE LocId = ?", (origin_en, interchangeable_id)); update_count += cursor.rowcount
                if str(interchangeable_id) in formatted_map:
                    for fmt in formatted_map[str(interchangeable_id)]:
                        ko_loc = origin_ko['nfd'] if fmt == 2 else origin_ko['nfc']
                        cursor.execute("UPDATE Localizations_koKR SET Loc = ? WHERE LocId = ? AND Formatted = ?", (ko_loc, interchangeable_id, fmt)); update_count += cursor.rowcount
            conn.commit()
            log_callback(f"  - 총 {update_count}개의 이름 항목을 업데이트했습니다.")
            update_ability_text(log_callback, translation_data)
        except Exception as e:
            log_callback(f"  - 카드 이름 변경 중 오류 발생: {e}")
        finally:
            if conn: conn.close()

def has_final_consonant(char):
    if '가' <= char <= '힣': return (ord(char) - ord('가')) % 28 > 0
    return True

def update_ability_text(log_callback, translation_data, only_english=False):
    db_files = glob.glob(os.path.join(application_path, 'Raw_CardDatabase_*.mtga'))
    if not db_files: return
    db_path = db_files[0]
    conn = None
    all_updates_en, all_updates_ko = [], []
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        creature_type_loc_ids = set()
        try:
            cursor.execute("SELECT LocId FROM Enums WHERE Type = 'SubType'")
            for row in cursor.fetchall():
                creature_type_loc_ids.add(str(row[0]))
        except sqlite3.OperationalError:
            log_callback("  - 경고: 'Enums' 테이블에서 생물 유형 목록을 가져올 수 없습니다. 일부 카드 능력 텍스트가 의도와 다르게 변경될 수 있습니다.")

        creature_types_en = set()
        creature_types_ko_nfc = set()
        creature_types_ko_nfd = set()

        if creature_type_loc_ids:
            placeholders_ct = ', '.join('?' * len(creature_type_loc_ids))
            cursor.execute(f"SELECT Loc FROM Localizations_enUS WHERE LocId IN ({placeholders_ct})", list(creature_type_loc_ids))
            for row in cursor.fetchall(): 
                creature_types_en.add(row[0])
            
            if not only_english:
                cursor.execute(f"SELECT Loc, Formatted FROM Localizations_koKR WHERE LocId IN ({placeholders_ct})", list(creature_type_loc_ids))
                for loc, formatted in cursor.fetchall():
                    if formatted == 2:
                        creature_types_ko_nfd.add(loc)
                    else: # Formatted 0 or 1 are both NFC
                        creature_types_ko_nfc.add(loc)

        for card in translation_data:
            card_title_id = card.get('titleId')
            if not card_title_id: continue
            replacements = {'en': [], 'ko_nfc': [], 'ko_nfd': []}
            origin_en, new_en = card.get('origin_en_name', ''), card.get('new_en_name', '')
            if origin_en and new_en:
                replacements['en'].append((origin_en, new_en))
                if ',' in origin_en: replacements['en'].append((origin_en.split(',')[0].strip(), new_en.split(',')[0].strip()))
                elif ' ' in origin_en: replacements['en'].append((origin_en.split(' ')[0].strip(), new_en))
            if not only_english:
                origin_ko_nfc, new_ko_nfc = card.get('origin_ko_name', {}).get('nfc', ''), card.get('new_ko_name', {}).get('nfc', '')
                if origin_ko_nfc and new_ko_nfc:
                    replacements['ko_nfc'].append((origin_ko_nfc, new_ko_nfc))
                    if ',' in origin_ko_nfc: short_origin, short_new = origin_ko_nfc.split(',')[-1].strip(), new_ko_nfc.split(',')[-1].strip() if ',' in new_ko_nfc else new_ko_nfc; replacements['ko_nfc'].append((short_origin, short_new))
                    elif ' ' in origin_ko_nfc: short_origin, short_new = origin_ko_nfc.split(' ')[-1].strip(), new_ko_nfc; replacements['ko_nfc'].append((short_origin, short_new))
                origin_ko_nfd, new_ko_nfd = card.get('origin_ko_name', {}).get('nfd', ''), card.get('new_ko_name', {}).get('nfd', '')
                if origin_ko_nfd and new_ko_nfd:
                    replacements['ko_nfd'].append((origin_ko_nfd, new_ko_nfd))
                    if ',' in origin_ko_nfd: short_origin, short_new = origin_ko_nfd.split(',')[-1].strip(), new_ko_nfd.split(',')[-1].strip() if ',' in new_ko_nfd else new_ko_nfd; replacements['ko_nfd'].append((short_origin, short_new))
                    elif ' ' in origin_ko_nfd: short_origin, short_new = origin_ko_nfd.split(' ')[-1].strip(), new_ko_nfd; replacements['ko_nfd'].append((short_origin, short_new))
            for key in replacements: replacements[key] = sorted(list(set(replacements[key])), key=lambda x: len(x[0]), reverse=True)
            cursor.execute("SELECT AbilityIds, HiddenAbilityIds FROM Cards WHERE titleId = ?", (card_title_id,)); row = cursor.fetchone()
            if not row: continue
            ability_ids_str, hidden_ability_ids_str = row
            direct_loc_ids, ability_ids_to_lookup = set(), set()
            for id_part in ((ability_ids_str or '') + ',' + (hidden_ability_ids_str or '')).split(','):
                id_part = id_part.strip()
                if not id_part: continue
                if ':' in id_part: direct_loc_ids.add(id_part.split(':', 1)[1].strip())
                else: ability_ids_to_lookup.add(id_part)
            if ability_ids_to_lookup:
                placeholders = ', '.join('?' * len(ability_ids_to_lookup))
                cursor.execute(f"SELECT DISTINCT TextId FROM Abilities WHERE Id IN ({placeholders}) AND TextId != 0", list(ability_ids_to_lookup))
                for r in cursor.fetchall(): direct_loc_ids.add(str(r[0]))
            text_ids = [str(loc_id) for loc_id in direct_loc_ids if loc_id]
            if not text_ids: continue
            placeholders = ', '.join('?' * len(text_ids))

            cursor.execute(f"SELECT LocId, Loc FROM Localizations_enUS WHERE LocId IN ({placeholders})", text_ids)
            for loc_id, loc_text in cursor.fetchall():
                protected_map, work_text = {}, loc_text
                sorted_creature_types = sorted(list(creature_types_en), key=len, reverse=True)
                for i, creature_type in enumerate(sorted_creature_types):
                    if creature_type in work_text:
                        placeholder = f"__PROTECTED_{i}__"
                        work_text = work_text.replace(creature_type, placeholder)
                        protected_map[placeholder] = creature_type
                
                for old, new in replacements['en']:
                    work_text = work_text.replace(old, new)

                for placeholder, original_word in protected_map.items():
                    work_text = work_text.replace(placeholder, original_word)

                if work_text != loc_text: all_updates_en.append((work_text, loc_id))

            if not only_english:
                cursor.execute(f"SELECT LocId, Loc, Formatted FROM Localizations_koKR WHERE LocId IN ({placeholders})", text_ids)
                for loc_id, loc_text, formatted in cursor.fetchall():
                    creature_types_ko = creature_types_ko_nfd if formatted == 2 else creature_types_ko_nfc
                    sorted_creature_types = sorted(list(creature_types_ko), key=len, reverse=True)
                    
                    protected_map, work_text = {}, loc_text
                    for i, creature_type in enumerate(sorted_creature_types):
                        if creature_type in work_text:
                            placeholder = f"__PROTECTED_{i}__"
                            work_text = work_text.replace(creature_type, placeholder)
                            protected_map[placeholder] = creature_type

                    replace_map = replacements['ko_nfd'] if formatted == 2 else replacements['ko_nfc']
                    for old, new in replace_map:
                        start_index = 0
                        while start_index < len(work_text):
                            index = work_text.find(old, start_index)
                            if index == -1: break
                            work_text = work_text[:index] + new + work_text[index + len(old):]
                            josa_index = index + len(new)
                            if josa_index < len(work_text):
                                next_char, josa_pair = work_text[josa_index], None
                                if next_char in ['은', '는']: josa_pair = ('은', '는')
                                elif next_char in ['이', '가']: josa_pair = ('이', '가')
                                elif next_char in ['을', '를']: josa_pair = ('을', '를')
                                elif next_char in ['과', '와']: josa_pair = ('과', '와')
                                if josa_pair:
                                    correct_josa = josa_pair[0] if has_final_consonant(new[-1]) else josa_pair[1]
                                    if next_char != correct_josa: work_text = work_text[:josa_index] + correct_josa + work_text[josa_index + 1:]
                            start_index = index + len(new)

                    for placeholder, original_word in protected_map.items():
                        work_text = work_text.replace(placeholder, original_word)

                    if work_text != loc_text: all_updates_ko.append((work_text, loc_id, formatted))
        if all_updates_en: cursor.executemany("UPDATE Localizations_enUS SET Loc = ? WHERE LocId = ?", all_updates_en)
        if not only_english and all_updates_ko: cursor.executemany("UPDATE Localizations_koKR SET Loc = ? WHERE LocId = ? AND Formatted = ?", all_updates_ko)
        conn.commit()
    except Exception as e:
        log_callback(f"  - 능력 텍스트 변경 중 오류 발생: {e}")
    finally:
        if conn: conn.close()


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

        # 1. Get all LocIds that are actual card titles from the Cards table.
        log_callback("  - 카드 테이블에서 모든 타이틀 ID를 수집하는 중...")
        cursor.execute("SELECT DISTINCT titleId FROM Cards WHERE titleId IS NOT NULL")
        card_title_ids = {str(row[0]) for row in cursor.fetchall()}
        
        # Also consider InterchangeableTitleId as they are also card titles
        cursor.execute("SELECT DISTINCT InterchangeableTitleId FROM Cards WHERE InterchangeableTitleId IS NOT NULL AND InterchangeableTitleId != 0")
        for row in cursor.fetchall():
            card_title_ids.add(str(row[0]))

        if not card_title_ids:
            log_callback("  - 카드 테이블에서 타이틀 ID를 찾을 수 없습니다.")
            return
        log_callback(f"  - {len(card_title_ids)}개의 고유한 카드 타이틀 ID를 찾았습니다.")

        # 2. Get all English localizations.
        log_callback("  - 영어 로컬라이제이션 데이터를 읽는 중...")
        cursor.execute("SELECT LocId, Loc FROM Localizations_enUS")
        all_en_locs = cursor.fetchall()

        # 3. Filter the English localizations to only include card titles.
        updates = []
        for loc_id, loc_text in all_en_locs:
            if str(loc_id) in card_title_ids:
                updates.append((loc_text, str(loc_id)))
        
        if not updates:
            log_callback("  - 업데이트할 카드 이름을 찾지 못했습니다.")
            return

        # 4. Execute the update.
        log_callback(f"  - {len(updates)}개의 카드 이름을 영어로 덮어씁니다...")
        cursor.executemany("UPDATE Localizations_koKR SET Loc = ? WHERE LocId = ?", updates)
        conn.commit()
        
        log_callback(f"  - 총 {cursor.rowcount}개의 항목이 업데이트되었습니다.")
        log_callback("=== 카드 이름 영문화 패치 완료 ===")

    except sqlite3.Error as e:
        log_callback(f"  - 카드 이름 영어로 변경 중 데이터베이스 오류 발생: {e}")
    finally:
        if conn: conn.close()


def replace_sleeve_art(sleeve_bucket_id, image_content, log_callback):
    asset_bundle_dir = os.path.join(application_path, "..", "AssetBundle")
    asset_bundle_pattern = os.path.join(asset_bundle_dir, f"{sleeve_bucket_id}*")
    found_files = glob.glob(asset_bundle_pattern)
    
    if not found_files:
        log_callback(f"  - 정보: 슬리브 에셋 '{sleeve_bucket_id}'을(를) 찾을 수 없습니다. 건너킵니다.")
        return False

    if len(found_files) > 1:
        log_callback(f"  - 경고: 슬리브 에셋 '{sleeve_bucket_id}'에 대해 여러 에셋 번들이 발견되었습니다. 첫 번째 파일({os.path.basename(found_files[0])})을 사용합니다.")
    
    asset_path = found_files[0]

    try:
        downloaded_image = Image.open(io.BytesIO(image_content))
        env = UnityPy.load(asset_path)
        
        # Find the specific Texture2D object whose name starts with "CardBack_"
        card_back_textures = [
            obj.read() for obj in env.objects 
            if obj.type.name == "Texture2D" and obj.read().m_Name.startswith("CardBack_")
        ]
        
        if not card_back_textures:
            # Fallback for logging if the primary target is not found
            all_texture_names = [obj.read().m_Name for obj in env.objects if obj.type.name == "Texture2D"]
            log_callback(f"  - 정보: 에셋 번들 {os.path.basename(asset_path)}에서 'CardBack_'으로 시작하는 Texture2D를 찾을 수 없습니다. 사용 가능한 텍스쳐: {all_texture_names}. 건너뜁니다.")
            return False

        # Replace all found textures that match the criteria
        for target_obj in card_back_textures:
            target_obj.image = downloaded_image
            target_obj.save()
            
        with open(asset_path, "wb") as f:
            f.write(env.file.save())
        
        return True
            
    except Exception as e:
        log_callback(f"  - 오류: 슬리브 에셋 {os.path.basename(asset_path)} 작업 중 에러 발생: {e}\n{traceback.format_exc()}")
        return False

def update_sleeve_names(log_callback, sleeve_data, translation_data):
    client_files = glob.glob(os.path.join(application_path, 'Raw_ClientLocalization_*.mtga'))
    if not client_files:
        log_callback("  - 클라이언트 파일을 찾을 수 없어 슬리브 이름 변경을 건너뜁니다.")
        return

    translation_map = {normalize_name(card['origin_en_name']): card for card in translation_data}
    
    for client_file in client_files:
        conn = None
        try:
            conn = sqlite3.connect(client_file)
            cursor = conn.cursor()
            
            updates = []
            for art_id, data in sleeve_data.items():
                key = data['key']
                cursor.execute("SELECT enUS FROM Loc WHERE key = ?", (key,))
                result = cursor.fetchone()
                if not result or not result[0]:
                    # log_callback(f"  - 정보: DB에서 슬리브 키 '{key}'의 영어 이름을 찾지 못했습니다.")
                    continue
                
                original_en_sleeve_name = result[0]
                base_name_en = original_en_sleeve_name.replace(" Sleeve", "").strip()
                normalized_base_name = normalize_name(base_name_en)

                if normalized_base_name in translation_map:
                    match = translation_map[normalized_base_name]
                    new_en_name = match.get('new_en_name')
                    new_ko_name_nfc = match.get('new_ko_name', {}).get('nfc')

                    if new_en_name and new_ko_name_nfc:
                        final_new_en = f"{new_en_name} Sleeve"
                        final_new_ko = f"{new_ko_name_nfc} 슬리브"
                        updates.append((final_new_en, final_new_ko, key))
                # else:
                    # log_callback(f"  - 정보: 번역 데이터에서 '{base_name_en}'에 대한 항목을 찾지 못했습니다.")

            if updates:
                cursor.executemany("UPDATE Loc SET enUS = ?, koKR = ? WHERE key = ?", updates)
                conn.commit()
                log_callback(f"  - 총 {len(updates)}개의 슬리브 이름을 업데이트했습니다.")

        except sqlite3.Error as e:
            log_callback(f"  - 슬리브 이름 변경 중 DB 오류 발생: {e}")
        finally:
            if conn: conn.close()

def run_image_change(log_callback, name_option, script_base_path):
    log_callback("=== 실물 카드 패치 시작 ===")
    patch_spiderman_expansion_name(log_callback)
    patch_soul_stone_card(log_callback)
    
    setup_unitypy()

    target_cards = get_target_card_data(log_callback)
    if not target_cards: 
        log_callback("이미지 교체 대상 카드가 없습니다.")
        return

    patch_log_path = os.path.join(script_base_path, 'patch_log.json')
    patch_log = {}
    if os.path.exists(patch_log_path):
        try:
            with open(patch_log_path, 'r', encoding='utf-8') as f:
                patch_log = json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            log_callback(f"  - 경고: patch_log.json 파일을 읽는 중 오류 발생: {e}. 로그를 새로 시작합니다.")
            patch_log = {}
    
    # --- Define sleeve data map ---
    sleeve_data_map = {
        "461218": {"bucket_id": "Textures_Bucket_Card.Sleeve_1096", "key": "MainNav/DeckBuilder/Sleeves/CardBack_OM1_461218", "exp_code": "OM1"},
        "461290": {"bucket_id": "Textures_Bucket_Card.Sleeve_1099", "key": "MainNav/DeckBuilder/Sleeves/CardBack_OM1_461290", "exp_code": "OM1"},
        "461308": {"bucket_id": "Textures_Bucket_Card.Sleeve_1101", "key": "MainNav/DeckBuilder/Sleeves/CardBack_OM1_461308", "exp_code": "OM1"},
        "461311": {"bucket_id": "Textures_Bucket_Card.Sleeve_1102", "key": "MainNav/DeckBuilder/Sleeves/CardBack_OM1_461311", "exp_code": "OM1"},
        "461328": {"bucket_id": "Textures_Bucket_Card.Sleeve_1103", "key": "MainNav/DeckBuilder/Sleeves/CardBack_OM1_461328", "exp_code": "OM1"},
        "461338": {"bucket_id": "Textures_Bucket_Card.Sleeve_1105", "key": "MainNav/DeckBuilder/Sleeves/CardBack_OM1_461338", "exp_code": "OM1"},
    }

    # --- Collect all art_ids for both cards and sleeves ---
    all_art_ids_to_fetch_url = {} # art_id -> {"name": card_name, "exp_code": exp_code, "is_sleeve": bool}
    all_expansion_codes = set()

    # For regular cards
    for card in target_cards:
        art_id, exp_code = str(card['ArtId']), card['ExpansionCode']
        title_name = card.get('titleName')
        interchange_name = card.get('interchangeName')
        card_name = interchange_name or title_name
        if card_name:
            all_art_ids_to_fetch_url[art_id] = {"name": card_name, "exp_code": exp_code, "is_sleeve": False}
            all_expansion_codes.add(exp_code)

    # For sleeves
    sleeve_art_id_to_name = {}
    for card in target_cards: # Re-use target_cards to get sleeve names
        art_id = str(card['ArtId'])
        if art_id in sleeve_data_map:
            card_name = card.get('interchangeName') or card.get('titleName')
            if card_name:
                sleeve_art_id_to_name[art_id] = card_name
                all_art_ids_to_fetch_url[art_id] = {"name": card_name, "exp_code": sleeve_data_map[art_id]['exp_code'], "is_sleeve": True}
                all_expansion_codes.add(sleeve_data_map[art_id]['exp_code'])

    # --- Fetch Scryfall data once for all ---
    scryfall_data = fetch_all_sets_data(all_expansion_codes, log_callback)

    # --- Determine URLs for all art_ids that need patching ---
    art_ids_to_download = {} # art_id -> url
    for art_id, card_info_dict in all_art_ids_to_fetch_url.items():
        card_name = card_info_dict["name"]
        exp_code = card_info_dict["exp_code"]
        is_sleeve = card_info_dict["is_sleeve"]

        normalized_name = normalize_name(card_name)
        card_info = scryfall_data.get(exp_code, {}).get(normalized_name)
        if card_info and 'image_uris' in card_info and 'art_crop' in card_info['image_uris']:
            url = card_info['image_uris']['art_crop']
            
            log_key = f"sleeve_{art_id}" if is_sleeve else str(art_id)
            log_entry = patch_log.get('patched_images', {}).get(log_key)
            
            is_patched_with_same_url = False
            if log_entry:
                if is_sleeve and isinstance(log_entry, dict) and log_entry.get('url') == url:
                    is_patched_with_same_url = True
                elif not is_sleeve and log_entry == url: # Old format for cards
                    is_patched_with_same_url = True

            if not is_patched_with_same_url:
                art_ids_to_download[art_id] = url
        else:
            log_callback(f"  - 정보: Scryfall에서 ArtId {art_id} ({card_name})의 이미지를 찾지 못했습니다.")

    # --- Download all images once ---
    downloaded_images = {}
    if art_ids_to_download:
        downloaded_images = download_images(art_ids_to_download, log_callback)
    else:
        log_callback("새롭게 다운로드할 이미지가 없거나 이미 처리되었습니다.")

    # --- Patch sleeves ---
    log_callback("=== 카드 슬리브 교체 시작 ===")
    if downloaded_images:
        patched_sleeve_count = 0
        total_sleeves_to_patch = len([art_id for art_id in sleeve_data_map if art_id in downloaded_images])
        current_sleeve_patch_count = 0
        for art_id, data in sleeve_data_map.items():
            if art_id in downloaded_images:
                current_sleeve_patch_count += 1
                log_callback(f"  슬리브 에셋 적용: ({current_sleeve_patch_count}/{total_sleeves_to_patch})", update_last_line=True)
                
                bucket_id = data['bucket_id']
                image_data = downloaded_images[art_id]
                success = replace_sleeve_art(bucket_id, image_data, log_callback)
                if success:
                    patched_sleeve_count += 1
                    sleeve_log_key = f"sleeve_{art_id}"
                    if 'patched_images' not in patch_log: patch_log['patched_images'] = {}
                    patch_log['patched_images'][sleeve_log_key] = {"url": art_ids_to_download[art_id], "bucket_id": bucket_id}
        log_callback(f"  슬리브 에셋 적용 완료. ({patched_sleeve_count}개 적용됨)")
    else:
        log_callback("새롭게 교체할 슬리브 이미지가 없거나 이미 처리되었습니다.")

    # --- Patch regular cards ---
    log_callback("=== 일러스트 교체 시작 ===")
    if downloaded_images:
        patched_card_count = 0
        total_cards_to_patch = len([art_id for art_id in all_art_ids_to_fetch_url if not all_art_ids_to_fetch_url[art_id]["is_sleeve"] and art_id in downloaded_images])
        current_card_patch_count = 0
        for art_id, image_data in downloaded_images.items():
            if all_art_ids_to_fetch_url.get(art_id, {}).get("is_sleeve"): # Skip sleeves
                continue

            current_card_patch_count += 1
            log_callback(f"  에셋 적용: ({current_card_patch_count}/{total_cards_to_patch})", update_last_line=True)
            success = replace_card_art(int(art_id), image_data, log_callback)
            if success:
                patched_card_count += 1
                if 'patched_images' not in patch_log: patch_log['patched_images'] = {}
                patch_log['patched_images'][str(art_id)] = art_ids_to_download[art_id]
        
        log_callback(f"에셋 적용 완료. ({patched_card_count}개 적용됨)")
        log_callback("=== 커스텀 일러스트 패치 완료 ===")
            
    else:
        log_callback("새롭게 교체할 카드 이미지가 없거나 이미 처리되었습니다.")

    # Save the patch log, which now contains both card and sleeve info
    try:
        with open(patch_log_path, 'w', encoding='utf-8') as f:
            json.dump(patch_log, f, indent=2, ensure_ascii=False)
    except IOError as e:
        log_callback(f"  - 경고: patch_log.json 파일에 쓰는 중 오류 발생: {e}")

    # --- Update names for both cards and sleeves ---
    # Call update_sleeve_names here, as it needs sleeve_data_map and translation_data
    if name_option == "unofficial_korean":
        log_callback("  - 슬리브 이름 비공식 한국어로 변경 시작...")
        json_url = "https://drive.usercontent.google.com/u/0/uc?id=1sEroW-b2FxC6rsH6-gTsRsj_5iADgcWm&confirm=t"
        translation_data = fetch_json_from_url(json_url, log_callback)
        if translation_data:
            update_sleeve_names(log_callback, sleeve_data_map, translation_data)
        else:
            log_callback("  - 비공식 한국어 이름 데이터를 가져오지 못해 슬리브 이름 변경을 건너뜁니다.")
    
    update_card_names(log_callback, name_option) # This already handles regular cards

def fetch_json_from_url(url, log_callback):
    try: response = requests.get(url); response.raise_for_status(); return response.json()
    except requests.exceptions.RequestException as e: log_callback(f"웹에서 데이터를 가져오는 중 오류 발생: {e}"); return None

def formatting_for_2(text): return unicodedata.normalize('NFD', text)

def patch_spiderman_expansion_name(log_callback):
    client_files = glob.glob(os.path.join(application_path, 'Raw_ClientLocalization_*.mtga'))
    if not client_files:
        log_callback("  - 클라이언트 파일을 찾을 수 없어 확장팩 이름 변경을 건너뜁니다.")
        return

    # Specific replacements (full string match)
    specific_replacements = [
        {
            "old_en": "Omenpath Bonus Sheet", "new_en": "Spider-Man Bonus Sheet",
            "old_ko": "오멘패스 보너스 시트", "new_ko": "스파이더맨 보너스 시트"
        },
        {
            "old_en": "Begin your journey through the Omenpaths with these three packs!",
            "new_en": "Begin your journey with Spider-Man with these three packs!",
            "old_ko": "3개의 팩을 가지고 오멘패스를 가로지르는 여행을 시작하십시오!",
            "new_ko": "3개의 팩을 가지고 스파이더맨과 함께 여행을 시작하십시오!"
        },
        {
            "old_en": "Begin your journey though the Omenpaths with these three packs!", # Typo version
            "new_en": "Begin your journey with Spider-Man with these three packs!",
            "old_ko": "3개의 팩을 가지고 오멘패스를 가로지르는 여행을 시작하십시오!",
            "new_ko": "3개의 팩을 가지고 스파이더맨과 함께 여행을 시작하십시오!"
        }
    ]

    # General replacements (substring match)
    old_en_general = "Through the Omenpaths"
    new_en_general = "Marvel's Spider-Man"
    old_ko_general_1 = "오멘패스를 가로지르다" # This one needs josa correction
    old_ko_general_2 = "Through the Omenpaths" # This one is in koKR column, just replace
    new_ko_general = "마블 스파이더맨"

    for client_file in client_files:
        conn = None
        try:
            conn = sqlite3.connect(client_file)
            cursor = conn.cursor()

            try:
                cursor.execute("SELECT key, enUS, koKR FROM Loc")
                rows = cursor.fetchall()
            except sqlite3.OperationalError as e:
                log_callback(f"  - DB 오류: 'Loc' 테이블 또는 'enUS'/'koKR' 컬럼을 찾을 수 없습니다. ({e})")
                log_callback("  - 스파이더맨 확장팩 이름 변경을 건너뜁니다.")
                continue

            en_updates = []
            ko_updates = []

            for key, en_text, ko_text in rows:
                if not en_text and not ko_text: continue
                original_en, original_ko = en_text, ko_text
                
                if en_text:
                    is_specific_match_en = False
                    for r in specific_replacements:
                        if en_text == r["old_en"]:
                            en_text = r["new_en"]
                            is_specific_match_en = True
                            break
                    if not is_specific_match_en:
                        en_text = en_text.replace(old_en_general, new_en_general)

                if ko_text:
                    is_specific_match_ko = False
                    for r in specific_replacements:
                        if ko_text == r["old_ko"]:
                            ko_text = r["new_ko"]
                            is_specific_match_ko = True
                            break
                    
                    if not is_specific_match_ko:
                        ko_text = ko_text.replace(old_ko_general_2, new_ko_general)
                        if old_ko_general_1 in ko_text:
                            start_index = 0
                            while start_index < len(ko_text):
                                index = ko_text.find(old_ko_general_1, start_index)
                                if index == -1: break
                                ko_text = ko_text[:index] + new_ko_general + ko_text[index + len(old_ko_general_1):]
                                josa_index = index + len(new_ko_general)
                                if josa_index < len(ko_text):
                                    next_char = ko_text[josa_index]
                                    josa_pair = None
                                    if next_char in ['은', '는']: josa_pair = ('은', '는')
                                    elif next_char in ['이', '가']: josa_pair = ('이', '가')
                                    elif next_char in ['을', '를']: josa_pair = ('을', '를')
                                    elif next_char in ['과', '와']: josa_pair = ('과', '와')
                                    if josa_pair:
                                        correct_josa = josa_pair[0] if has_final_consonant(new_ko_general[-1]) else josa_pair[1]
                                        if next_char != correct_josa: ko_text = ko_text[:josa_index] + correct_josa + ko_text[josa_index + 1:]
                                start_index = index + len(new_ko_general)

                if en_text != original_en: en_updates.append((en_text, key))
                if ko_text != original_ko: ko_updates.append((ko_text, key))

            if en_updates: cursor.executemany("UPDATE Loc SET enUS = ? WHERE key = ?", en_updates)
            if ko_updates: cursor.executemany("UPDATE Loc SET koKR = ? WHERE key = ?", ko_updates)
            if en_updates or ko_updates:
                conn.commit()
                # log_callback(f"  - {os.path.basename(client_file)} 파일에 변경사항을 저장했습니다.")

        except sqlite3.Error as e:
            log_callback(f"  - 확장팩 이름 변경 중 DB 오류 발생: {e}")
        finally:
            if conn: conn.close()

def patch_soul_stone_card(log_callback):
    # log_callback("  - '영혼석' 관련 카드(1072918) 텍스트를 수정합니다 (단순 치환 방식)...")
    
    card_db_files = glob.glob(os.path.join(application_path, 'Raw_CardDatabase_*.mtga'))
    if not card_db_files:
        log_callback("    - 카드 데이터베이스 파일을 찾지 못해 건너뜁니다.")
        return

    for card_db_path in card_db_files:
        conn = None
        try:
            conn = sqlite3.connect(card_db_path)
            cursor = conn.cursor()
            
            # --- English Localization ---
            # As per user request, use a simple REPLACE since the keyword is unique.
            cursor.execute("UPDATE Localizations_enUS SET Loc = REPLACE(Loc, 'Origin', '∞') WHERE LocId = 1072918")
            en_changes = cursor.rowcount

            # --- Korean Localization ---
            cursor.execute("UPDATE Localizations_koKR SET Loc = REPLACE(Loc, '기원', '∞') WHERE LocId = 1072918")
            ko_changes = cursor.rowcount
            
            conn.commit()
            
            if en_changes == 0 and ko_changes == 0:
                log_callback("    - 카드 데이터베이스에서 LocId 1072918에 해당하는 수정 대상을 찾지 못했습니다.")

        except sqlite3.Error as e:
            log_callback(f"    - 카드 DB 수정 중 오류 발생: {e}")
        finally:
            if conn: conn.close()

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
    card_file_pattern = os.path.join(application_path, 'Raw_CardDatabase_*.mtga')
    card_files = glob.glob(card_file_pattern)
    if not card_files:
        log_callback(f"카드 데이터베이스 파일을 찾지 못했습니다. 카드 관련 패치를 건너뜁니다.")
    else:
        log_callback("카드 번역 데이터 불러오는중...")
        card_json_url = "https://docs.google.com/uc?export=download&id=1pSF_YCV0NPuy240Rtt0bzOmr1GyE5HMd&confirm=t"
        card_values_to_update = fetch_json_from_url(card_json_url, log_callback)

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
                if 'card_conn' in locals() and card_conn: card_conn.close()
    
    patch_seek_keyword(log_callback)
    log_callback("=== 한글 오역 패치 완료 ===")

def patch_seek_keyword(log_callback):
    log_callback("  - 'SEEK' 키워드 자동 번역 패치 시작...")
    db_files = glob.glob(os.path.join(application_path, 'Raw_CardDatabase_*.mtga'))
    if not db_files:
        log_callback("    - 카드 데이터베이스를 찾을 수 없어 건너뜁니다.")
        return

    db_path = db_files[0]
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Step 1: Get all LocIds for abilities of cards in 'Y' expansions.
        cursor.execute("SELECT AbilityIds, HiddenAbilityIds FROM Cards WHERE ExpansionCode LIKE 'Y%'" )
        rows = cursor.fetchall()
        
        all_ability_ids = set()
        direct_loc_ids = set()

        for ability_ids_str, hidden_ability_ids_str in rows:
            for id_part in ((ability_ids_str or '') + ',' + (hidden_ability_ids_str or '')).split(','):
                id_part = id_part.strip()
                if not id_part: continue
                if ':' in id_part:
                    direct_loc_ids.add(id_part.split(':', 1)[1].strip())
                else:
                    all_ability_ids.add(id_part)
        
        if all_ability_ids:
            placeholders = ', '.join('?' * len(all_ability_ids))
            cursor.execute(f"SELECT DISTINCT TextId FROM Abilities WHERE Id IN ({placeholders}) AND TextId != 0", list(all_ability_ids))
            for r in cursor.fetchall():
                direct_loc_ids.add(str(r[0]))

        if not direct_loc_ids:
            log_callback("    - 'Y' 확장팩에서 처리할 능력 텍스트를 찾지 못했습니다.")
            return

        # Step 2: Get English texts for all candidate LocIds to determine which ones to process
        placeholders = ', '.join('?' * len(direct_loc_ids))
        cursor.execute(f"SELECT LocId, Loc FROM Localizations_enUS WHERE LocId IN ({placeholders}) AND (Loc LIKE '%SEEK%' OR Loc LIKE '%Discover%')", list(direct_loc_ids))
        
        en_texts_to_process = {str(loc_id): loc_text for loc_id, loc_text in cursor.fetchall()}
        
        # Filter for LocIds that actually contain SEEK
        loc_ids_with_seek = {loc_id for loc_id, en_text in en_texts_to_process.items() if "SEEK" in en_text}

        if not loc_ids_with_seek:
            log_callback("    - 'SEEK' 오역이 처리되었거나 없습니다.")
            return

        # Step 3: For the LocIds that need changes, fetch ALL their Korean localizations.
        placeholders_ko = ', '.join('?' * len(loc_ids_with_seek))
        cursor.execute(f"SELECT LocId, Loc, Formatted FROM Localizations_koKR WHERE LocId IN ({placeholders_ko})", list(loc_ids_with_seek))
        
        ko_texts_to_process = cursor.fetchall()
        updates_ko = []
        
        for loc_id, ko_text, formatted in ko_texts_to_process:
            loc_id_str = str(loc_id)
            en_text = en_texts_to_process.get(loc_id_str)
            if not en_text: continue

            # --- Idempotent Logic Starts Here ---
            
            # 1. Determine target state from English text
            target_discover_count = en_text.count("Discover")
            target_seek_count = en_text.count("SEEK")

            if target_seek_count == 0:
                continue

            # 2. Determine current state from Korean text
            current_discover_count = ko_text.count('발견')
            current_seek_count = ko_text.count('탐색')

            # 3. Idempotency Check: If the text is already in the correct state, skip.
            if current_discover_count == target_discover_count and current_seek_count == target_seek_count:
                continue

            # 4. Revert to a predictable "untouched" state for processing
            ko_reverted = ko_text.replace('탐색', '발견')

            new_ko_text = ko_reverted

            # 5. Apply the transformation logic from the reverted state
            if target_discover_count > 0 and target_seek_count > 0:
                discover_pos = en_text.find("Discover")
                seek_pos = en_text.find("SEEK")
                
                expected_발견_count = target_discover_count + target_seek_count
                if ko_reverted.count('발견') < expected_발견_count:
                    continue

                if discover_pos < seek_pos:
                    # Order: Discover, then SEEK.
                    # Replace the Nth '발견' that corresponds to SEEK.
                    nth_occurrence = target_discover_count
                    
                    matches = list(re.finditer('발견', ko_reverted))
                    if len(matches) > nth_occurrence:
                        match_to_replace = matches[nth_occurrence]
                        start_pos = match_to_replace.start()
                        new_ko_text = ko_reverted[:start_pos] + '탐색' + ko_reverted[start_pos + len('발견'):]
                    else:
                         continue
                else: # seek_pos < discover_pos
                    # Order: SEEK, then Discover. Replace the first N '발견's with '탐색'.
                    new_ko_text = ko_reverted.replace('발견', '탐색', target_seek_count)
            elif target_seek_count > 0:
                new_ko_text = ko_reverted.replace('발견', '탐색')
            
            if new_ko_text != ko_text:
                final_text = formatting_for_2(new_ko_text) if formatted == 2 else new_ko_text
                updates_ko.append((final_text, loc_id, formatted))
        
        if updates_ko:
            cursor.executemany("UPDATE Localizations_koKR SET Loc = ? WHERE LocId = ? AND Formatted = ?", updates_ko)
            conn.commit()

        log_callback(f"  - 총 {len(updates_ko)}개 레코드에서 'SEEK'를 '탐색'으로 변경했습니다.")

    except sqlite3.Error as e:
        log_callback(f"    - 'SEEK' 패치 중 데이터베이스 오류 발생: {e}")
    finally:
        if conn: conn.close()

# --- GUI Application using PySide6 ---

def run_patch_removal(log_callback, script_base_path):
    log_callback("=== 패치 제거 시작 ===")
    
    # 1. Remove patched asset bundles
    patch_log_path = os.path.join(script_base_path, 'patch_log.json')
    if os.path.exists(patch_log_path):
        log_callback("  - patch_log.json을 기반으로 에셋 번들 제거 중...")
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
                        if isinstance(value, dict): # New format for sleeves
                            bucket_id = value.get('bucket_id')
                            if bucket_id:
                                asset_pattern = os.path.join(asset_bundle_path, f"*{bucket_id}*")
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
                log_callback(f"  - 총 {removed_count}개의 에셋 번들 파일을 삭제했습니다.")
            else:
                log_callback("  - 제거할 에셋 번들 로그가 없습니다.")

            # Remove the log file itself
            try:
                os.remove(patch_log_path)
                log_callback("  - patch_log.json 파일을 삭제했습니다.")
            except OSError as e:
                log_callback(f"  - 오류: patch_log.json 삭제 실패: {e}")

        except (json.JSONDecodeError, IOError) as e:
            log_callback(f"  - 오류: patch_log.json 읽기 실패: {e}")
    else:
        log_callback("  - patch_log.json을 찾을 수 없어 에셋 번들 제거를 건너뜁니다.")

    # 2. Remove database files
    log_callback("  - 데이터베이스 파일 제거 중...")
    files_to_remove = []
    db_pattern = os.path.join(application_path, 'Raw_CardDatabase_*.mtga')
    client_pattern = os.path.join(application_path, 'Raw_ClientLocalization_*.mtga')
    files_to_remove.extend(glob.glob(db_pattern))
    files_to_remove.extend(glob.glob(client_pattern))

    if files_to_remove:
        removed_db_count = 0
        for file_path in files_to_remove:
            try:
                os.remove(file_path)
                log_callback(f"    - 삭제됨: {os.path.basename(file_path)}")
                removed_db_count += 1
            except OSError as e:
                log_callback(f"    - 오류: {os.path.basename(file_path)} 삭제 실패: {e}")
        log_callback(f"  - 총 {removed_db_count}개의 데이터베이스 파일을 삭제했습니다.")
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
            
            if self.patch_options['images']:
                run_image_change(log_callback, self.patch_options['name_option'], script_base_path)

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

        self.image_check = QCheckBox("실물 카드로 변경 (SPM 등)")
        self.english_names_only_check = QCheckBox("카드이름만 영어로(고인물용)")
        self.remove_patch_check = QCheckBox("패치 제거")

        self.art_only_radio = QRadioButton("일러스트만 변경")
        self.art_only_radio.setChecked(True)
        self.real_english_radio = QRadioButton("실물 영어로 변경")
        self.unofficial_korean_radio = QRadioButton("비공식 한국어로 변경")

        self.run_button = QPushButton("패치 시작")
        self.log_box = QTextEdit()
        self.log_box.setReadOnly(True)

        # --- Layouts ---
        main_layout = QVBoxLayout(self)

        options_group = QGroupBox("패치 종류")
        options_layout = QHBoxLayout()
        options_layout.addWidget(self.mistranslation_check)
        options_layout.addWidget(self.image_check)
        options_layout.addWidget(self.english_names_only_check)
        options_layout.addWidget(self.remove_patch_check)
        options_group.setLayout(options_layout)

        name_group = QGroupBox("카드 이름 변경")
        name_layout = QVBoxLayout()
        name_layout.addWidget(self.art_only_radio)
        name_layout.addWidget(self.real_english_radio)
        name_layout.addWidget(self.unofficial_korean_radio)
        self.name_group = name_group # To enable/disable
        name_group.setLayout(name_layout)

        log_group = QGroupBox("로그")
        log_layout = QVBoxLayout()
        log_layout.addWidget(self.log_box)
        log_group.setLayout(log_layout)

        main_layout.addWidget(options_group)
        main_layout.addWidget(name_group)
        main_layout.addWidget(self.run_button)
        main_layout.addWidget(log_group)

        # --- Connections ---
        self.mistranslation_check.stateChanged.connect(self.update_ui_state)
        self.image_check.stateChanged.connect(self.update_ui_state)
        self.english_names_only_check.stateChanged.connect(self.update_ui_state)
        self.remove_patch_check.stateChanged.connect(self.update_ui_state)
        self.image_check.stateChanged.connect(self.show_image_warning)
        self.run_button.clicked.connect(self.start_patch)

        # --- Initial State ---
        self.update_ui_state()

    def update_ui_state(self):
        is_remove = self.remove_patch_check.isChecked()

        if is_remove:
            # '패치 제거'가 선택되면 다른 모든 옵션을 비활성화
            self.mistranslation_check.setChecked(False)
            self.image_check.setChecked(False)
            self.english_names_only_check.setChecked(False)

            self.mistranslation_check.setEnabled(False)
            self.image_check.setEnabled(False)
            self.english_names_only_check.setEnabled(False)
            self.name_group.setEnabled(False)
            
            self.run_button.setEnabled(True)
            self.run_button.setText("패치 제거 시작")
            return

        # '패치 제거'가 선택되지 않은 경우, 다른 옵션들을 다시 활성화
        self.mistranslation_check.setEnabled(True)
        self.image_check.setEnabled(True)
        self.english_names_only_check.setEnabled(True)

        is_mistranslation = self.mistranslation_check.isChecked()
        is_images = self.image_check.isChecked()
        is_english_only = self.english_names_only_check.isChecked()

        # The name_group (radio buttons) should only be enabled if '실물 카드로 변경' (is_images) is checked.
        # The '카드이름만 영어로' (is_english_only) option does not affect the enabled state of this group.
        self.name_group.setEnabled(is_images)

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

    def show_image_warning(self, state):
        if state:
            QMessageBox.warning(self, "경고", "이 기능을 사용하는 경우 Wizards of the Coast LLC에 의해 제재받을 수 있습니다.")

    def start_patch(self):
        self.run_button.setText("패치 진행 중...")
        self.run_button.setEnabled(False)
        self.log_box.clear()

        patch_options = {
            'mistranslation': self.mistranslation_check.isChecked(),
            'images': self.image_check.isChecked(),
            'english_names_only': self.english_names_only_check.isChecked(),
            'remove_patch': self.remove_patch_check.isChecked(),
            'name_option': 'art_only'
        }
        if self.real_english_radio.isChecked():
            patch_options['name_option'] = 'real_english'
        elif self.unofficial_korean_radio.isChecked():
            patch_options['name_option'] = 'unofficial_korean'

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
