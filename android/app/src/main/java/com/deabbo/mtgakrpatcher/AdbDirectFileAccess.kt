package com.deabbo.mtgakrpatcher

import java.io.File

/**
 * AdbSessionManager(libadb-android 기반 직접 연결)를 PrivilegedFileAccess로 감쌉니다.
 *
 * [알아두실 점] readFile/writeFile이 파일 전체를 메모리에 ByteArray로 올립니다.
 * 카드 데이터베이스가 지금 수준(수 MB)에서는 문제없지만, 나중에 훨씬 커지면
 * 스트리밍 방식으로 바꿔야 할 수 있습니다. 지금은 단순하게 갑니다.
 */
class AdbDirectFileAccess(private val session: AdbSessionManager) : PrivilegedFileAccess {

    private val rawDir = "/storage/emulated/0/Android/data/com.wizards.mtga/files/Downloads/Raw"

    override fun locateDbPaths(): Pair<String?, String?> {
        val cardDb = session.findFirst(rawDir, "Raw_CardDatabase_*.mtga")
        val clientDb = session.findFirst(rawDir, "Raw_ClientLocalization_*.mtga")
        return cardDb to clientDb
    }

    override fun pullToLocal(remotePath: String, localFile: File) {
        localFile.writeBytes(session.readFile(remotePath))
    }

    override fun pushFromLocal(localFile: File, remotePath: String) {
        session.writeFile(remotePath, localFile.readBytes())
    }

    override fun deleteIfExists(remotePath: String): Boolean = session.deleteIfExists(remotePath)
}
