package com.deabbo.mtgakrpatcher

import java.io.File

/**
 * 이 코드는 UserServiceImpl 안에서, 즉 Shizuku가 부여한 shell 권한으로
 * 실행되는 프로세스 안에서만 정상 동작합니다. 우리 앱(MainActivity)의
 * 일반 프로세스에서 이 코드를 호출하면 권한이 없어 파일이 안 보입니다.
 */
object MtgaFileLocator {

    private const val REMOTE_RAW_DIR =
        "/storage/emulated/0/Android/data/com.wizards.mtga/files/Downloads/Raw"

    data class DbPaths(
        val cardDbFile: File?,
        val clientLocDbFile: File?
    )

    fun locate(log: (String) -> Unit): DbPaths {
        log("MTG 아레나 설치 경로를 확인하는 중...")
        val dir = File(REMOTE_RAW_DIR)

        if (!dir.isDirectory) {
            log("실패: $REMOTE_RAW_DIR 를 찾을 수 없습니다. MTGA 앱을 한 번 이상 실행했는지 확인하세요.")
            return DbPaths(null, null)
        }

        val cardDb = dir.listFiles { f ->
            f.isFile && f.name.startsWith("Raw_CardDatabase_") && f.name.endsWith(".mtga")
        }?.firstOrNull()

        val clientDb = dir.listFiles { f ->
            f.isFile && f.name.startsWith("Raw_ClientLocalization_") && f.name.endsWith(".mtga")
        }?.firstOrNull()

        if (cardDb != null) log("성공: 카드 데이터베이스를 찾았습니다: ${cardDb.name}")
        else log("경고: 카드 데이터베이스 파일을 찾지 못했습니다.")

        if (clientDb != null) log("성공: 클라이언트 로컬라이제이션 파일을 찾았습니다: ${clientDb.name}")
        else log("경고: 클라이언트 로컬라이제이션 파일을 찾지 못했습니다.")

        return DbPaths(cardDb, clientDb)
    }
}
