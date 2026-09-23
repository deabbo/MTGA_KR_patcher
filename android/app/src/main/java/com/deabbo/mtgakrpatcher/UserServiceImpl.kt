package com.deabbo.mtgakrpatcher

import android.os.ParcelFileDescriptor
import java.io.File

/**
 * SQLiteDatabase를 열지 않습니다(이유는 이전 커밋 메시지 참고: UserService
 * 프로세스는 정상 앱 프로세스가 아니라서 ContentResolver 접근이 막힙니다).
 * 파일 찾기 / 읽기·쓰기용 fd 제공 / 삭제만 담당합니다.
 *
 * PC 버전과 동일하게 백업(.bak)은 만들지 않습니다. "패치 제거"는 파일을
 * 삭제해서 게임이 재다운로드하도록 유도하는 방식입니다.
 */
class UserServiceImpl : IUserService.Stub() {

    override fun destroy() {
        System.exit(0)
    }

    override fun locateDbPaths(): Array<String> {
        val paths = MtgaFileLocator.locate { }
        return arrayOf(
            paths.cardDbFile?.absolutePath ?: "",
            paths.clientLocDbFile?.absolutePath ?: ""
        )
    }

    override fun openForRead(remotePath: String): ParcelFileDescriptor {
        return ParcelFileDescriptor.open(File(remotePath), ParcelFileDescriptor.MODE_READ_ONLY)
    }

    override fun openForWrite(remotePath: String): ParcelFileDescriptor {
        val file = File(remotePath)
        return ParcelFileDescriptor.open(
            file,
            ParcelFileDescriptor.MODE_READ_WRITE or ParcelFileDescriptor.MODE_TRUNCATE
        )
    }

    override fun deleteIfExists(remotePath: String): Boolean {
        val file = File(remotePath)
        if (!file.exists()) return false
        return file.delete()
    }
}
