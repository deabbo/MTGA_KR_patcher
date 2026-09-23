package com.deabbo.mtgakrpatcher

import android.os.ParcelFileDescriptor
import java.io.File

/** 기존 Shizuku UserService(IUserService AIDL)를 PrivilegedFileAccess로 감쌉니다. */
class ShizukuFileAccess(private val service: IUserService) : PrivilegedFileAccess {

    override fun locateDbPaths(): Pair<String?, String?> {
        val paths = service.locateDbPaths()
        val cardPath = paths.getOrNull(0)?.takeIf { it.isNotBlank() }
        val clientPath = paths.getOrNull(1)?.takeIf { it.isNotBlank() }
        return cardPath to clientPath
    }

    override fun pullToLocal(remotePath: String, localFile: File) {
        val pfd: ParcelFileDescriptor = service.openForRead(remotePath)
        ParcelFileDescriptor.AutoCloseInputStream(pfd).use { input ->
            localFile.outputStream().use { output -> input.copyTo(output) }
        }
    }

    override fun pushFromLocal(localFile: File, remotePath: String) {
        val pfd: ParcelFileDescriptor = service.openForWrite(remotePath)
        ParcelFileDescriptor.AutoCloseOutputStream(pfd).use { output ->
            localFile.inputStream().use { input -> input.copyTo(output) }
        }
    }

    override fun deleteIfExists(remotePath: String): Boolean = service.deleteIfExists(remotePath)
}
