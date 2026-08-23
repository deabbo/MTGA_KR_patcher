[app]

# (필수) 앱 이름 및 패키지 식별자
title = MTGA_KR_Patcher_android
package.name = mtgakorpatcher
package.domain = org.deabbo

# (필수) 소스코드 위치 및 포함할 파일 확장자
source.dir = .
source.include_exts = py,png,jpg,kv,atlas,json

# 앱 버전
version = 1.0.0

# (중요) 앱 구동에 필요한 라이브러리 목록
# 파이썬3, Kivy, SAF 호출용 PyJnius, 네트워크 패치용 Requests 포함
requirements = hostpython3==3.11.8,python3==3.11.8,kivy,pyjnius,requests,certifi,idna,urllib3,charset_normalizer

# 앱 화면 방향 (portrait: 세로, landscape: 가로, all: 모두)
orientation = portrait

# (중요) 안드로이드 필수 권한 설정
# 인터넷(업데이트/JSON 수신용) 및 저장소 접근 권한
android.permissions = INTERNET,READ_EXTERNAL_STORAGE,WRITE_EXTERNAL_STORAGE

# (중요) 안드로이드 타깃 SDK 버전 설정
android.api = 33
android.minapi = 24
android.sdk_build_tools_version = 33.0.2
android.ndk = 25b

# 빌드 시 안드로이드 SDK 라이선스 자동 동의
android.accept_sdk_license = True

# 백그라운드 서비스 미사용
# p4a.branch = master

[buildozer]
# 빌드 로그 출력 수준 (2: 상세 로그)
log_level = 2
warn_on_root = 1