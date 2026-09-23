package com.deabbo.mtgakrpatcher

import java.io.File

/**
 * "특권 권한으로 파일을 찾고/가져오고/밀어넣고/지우는" 공통 인터페이스.
 * Shizuku 경로(ShizukuFileAccess)와 앱 내장 직접 연결 경로(AdbDirectFileAccess)
 * 둘 다 이걸 구현하고, MainActivity/LocalizationPatcher는 어느 쪽인지 몰라도 됩니다.
 */
interface PrivilegedFileAccess {
    /** (카드DB 경로, 클라이언트DB 경로). 못 찾으면 각각 null. */
    fun locateDbPaths(): Pair<String?, String?>

    /** 원격 파일을 로컬 파일로 통째로 가져옵니다. */
    fun pullToLocal(remotePath: String, localFile: File)

    /** 로컬 파일 내용을 원격 파일에 그대로 씁니다. */
    fun pushFromLocal(localFile: File, remotePath: String)

    /** 원격 파일을 삭제합니다. 있었고 지웠으면 true. */
    fun deleteIfExists(remotePath: String): Boolean
}
