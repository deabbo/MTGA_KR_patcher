package com.deabbo.mtgakrpatcher

import android.content.Context
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

/**
 * libadb-android(MuntashirAkon/libadb-android)로 무선 디버깅 페어링과 ADB shell
 * 세션을 우리 앱 안에서 직접 관리합니다. Shizuku 앱 설치가 필요 없습니다.
 *
 * AdbConnectionManager는 같은 패키지 안의 우리 자체 구현(AbsAdbConnectionManager
 * 상속)을 씁니다 — import 필요 없음.
 *
 * [이번에 고친 것 - "Stream closed"가 큰 파일뿐 아니라 아주 작은 명령(find)에서도 남]
 * 큰 파일 전송이 한 번 실패하고 나면, 그 밑의 ADB 연결 자체가 사실상 죽어버리는데
 * isConnected()는 그걸 감지하지 못해 계속 "연결됨"으로 보고합니다. 그 뒤로는 어떤
 * 명령을 보내도(파일 찾기처럼 아주 작은 것도) 죽은 연결 위에서 즉시 실패합니다.
 *
 * 그래서 이제 셸 명령이 하나라도 실패하면, 그냥 재시도하는 게 아니라 "연결을 끊고
 * 다시 붙인 뒤" 재시도합니다. 페어링 신뢰(trust)는 기기 쪽에 이미 저장돼 있어서,
 * 재연결(connect)만 다시 하면 새 페어링 없이 복구됩니다. 큰 파일 읽기는 여기에
 * 더해 "이어받기"(지난 수정)도 계속 유지합니다 — 재연결 후에도 어차피 마지막
 * 청크 경계에서 또 끊길 수 있어서, 둘 다 필요합니다.
 */
class AdbSessionManager(private val context: Context) {

    companion object {
        private const val READ_ATTEMPT_TIMEOUT_MS = 20_000L
        private const val MAX_READ_ATTEMPTS = 6
    }

    private val manager by lazy { AdbConnectionManager.getInstance(context) }
    private val ioExecutor = Executors.newSingleThreadExecutor()

    @Volatile private var lastHost: String? = null
    @Volatile private var lastPort: Int = -1

    /** 개발자 옵션 > 무선 디버깅 > "페어링 코드로 기기 페어링" 화면에 표시되는
     *  IP:포트와 6자리 코드. 최초 1회만 필요합니다. */
    fun pair(pairHost: String, pairPort: Int, pairingCode: String): Boolean {
        return manager.pair(pairHost, pairPort, pairingCode)
    }

    /** 페어링 후, 메인 "무선 디버깅" 화면에 표시되는 IP주소와 포트로 연결합니다.
     *  페어링 포트와 연결 포트는 서로 다른 값이니 화면에 보이는 그대로 넣어야 합니다. */
    fun connect(connectHost: String, connectPort: Int): Boolean {
        val result = manager.connect(connectHost, connectPort)
        if (result) {
            // 나중에 연결이 죽었을 때 같은 주소로 재연결하기 위해 기억해둡니다.
            lastHost = connectHost
            lastPort = connectPort
        }
        return result
    }

    fun isConnected(): Boolean = manager.isConnected

    fun disconnect() {
        manager.disconnect()
    }

    /** 연결이 죽어있는 것 같을 때, 마지막으로 성공했던 주소로 다시 연결을 시도합니다.
     *  페어링은 기기 쪽에 이미 저장돼 있어서 새 페어링 없이 재연결만으로 복구됩니다. */
    private fun tryReconnect(): Boolean {
        val host = lastHost ?: return false
        if (lastPort <= 0) return false
        return try {
            try { manager.disconnect() } catch (e: Exception) { /* 이미 끊겨 있어도 무시 */ }
            manager.connect(host, lastPort)
        } catch (e: Exception) {
            false
        }
    }

    /** 셸 명령을 실행하고 표준출력을 문자열로 돌려줍니다.
     *  실패하면 재연결을 한 번 시도한 뒤 딱 한 번 더 재시도합니다. */
    fun shell(command: String): String {
        return try {
            shellOnce(command)
        } catch (e: Exception) {
            if (tryReconnect()) {
                shellOnce(command)
            } else {
                throw e
            }
        }
    }

    private fun shellOnce(command: String): String {
        val stream = manager.openStream("shell:$command")
        try {
            return stream.openInputStream().bufferedReader().use { it.readText() }
        } finally {
            stream.close()
        }
    }

    /**
     * 원격 파일을 통째로 바이트로 읽습니다 (바이너리 sqlite 파일용).
     * 도중에 끊기면 이미 받은 지점부터 이어받고(tail -c +N), 반복 실패하면
     * 연결 자체를 재연결한 뒤 계속 이어받습니다.
     */
    fun readFile(remotePath: String): ByteArray {
        val expectedSize = remoteFileSize(remotePath)
        val output = ByteArrayOutputStream()
        var offset = 0L
        var lastError: Throwable? = null

        repeat(MAX_READ_ATTEMPTS) { attempt ->
            try {
                readChunkWithTimeout(remotePath, offset, output).let { newOffset -> offset = newOffset }
                if (expectedSize == null || offset == expectedSize) {
                    return output.toByteArray()
                }
                lastError = IOException("크기 불일치 (기대 $expectedSize, 현재 $offset)")
            } catch (e: Exception) {
                lastError = e
                // 연결 자체가 죽었을 수 있으니 재연결을 시도합니다. 실패해도 다음 반복에서 또 시도됩니다.
                tryReconnect()
            }
            Thread.sleep(250)
        }
        throw IOException(
            "파일을 완전히 받지 못했습니다 (기대 크기 $expectedSize, 받은 크기 $offset, " +
            "마지막 오류: ${lastError?.message ?: lastError?.javaClass?.simpleName}). " +
            "네트워크 상태를 확인하고 다시 시도해주세요.",
            lastError
        )
    }

    /** offset부터 이어서 읽어 output에 쓰고, 새 offset을 돌려줍니다. 타임아웃이 걸리면 취소하고 예외를 던집니다. */
    private fun readChunkWithTimeout(remotePath: String, startOffset: Long, output: ByteArrayOutputStream): Long {
        val future = ioExecutor.submit<Long> {
            val cmd = if (startOffset == 0L) "cat '$remotePath'" else "tail -c +${startOffset + 1} '$remotePath'"
            val stream = manager.openStream("shell:$cmd")
            var received = startOffset
            try {
                val input = stream.openInputStream()
                val buffer = ByteArray(16 * 1024)
                while (true) {
                    val n = input.read(buffer)
                    if (n == -1) break
                    output.write(buffer, 0, n)
                    received += n
                }
            } finally {
                stream.close()
            }
            received
        }
        return try {
            future.get(READ_ATTEMPT_TIMEOUT_MS, TimeUnit.MILLISECONDS)
        } catch (e: TimeoutException) {
            future.cancel(true)
            throw IOException("응답 시간 초과 (기존에 받은 ${output.size()}바이트는 유지됩니다)", e)
        } catch (e: java.util.concurrent.ExecutionException) {
            throw (e.cause as? Exception) ?: e
        }
    }

    /** 로컬 바이트를 원격 파일에 그대로 씁니다.
     *  크기가 안 맞거나 예외가 나면 재연결 후 재시도합니다. */
    fun writeFile(remotePath: String, data: ByteArray) {
        var lastError: Exception? = null
        var lastRemoteSize: Long? = null

        repeat(3) {
            try {
                val stream = manager.openStream("shell:cat > '$remotePath'")
                try {
                    stream.openOutputStream().use { out ->
                        out.write(data)
                        out.flush()
                    }
                } finally {
                    stream.close()
                }
                val remoteSize = remoteFileSize(remotePath)
                lastRemoteSize = remoteSize
                if (remoteSize == data.size.toLong()) return
            } catch (e: Exception) {
                lastError = e
                tryReconnect()
            }
            Thread.sleep(300)
        }
        throw IOException(
            "파일을 완전히 쓰지 못했습니다 (보낸 크기 ${data.size}, 확인된 크기 $lastRemoteSize, " +
            "마지막 오류: ${lastError?.message}). 네트워크 상태를 확인하고 다시 시도해주세요.",
            lastError
        )
    }

    private fun remoteFileSize(remotePath: String): Long? {
        return shell("stat -c%s '$remotePath' 2>/dev/null").trim().toLongOrNull()
    }

    fun deleteIfExists(remotePath: String): Boolean {
        val result = shell("[ -f '$remotePath' ] && rm -f '$remotePath' && echo deleted || echo missing")
        return result.trim() == "deleted"
    }

    fun findFirst(dir: String, pattern: String): String? {
        val result = shell("find '$dir' -maxdepth 1 -name '$pattern' 2>/dev/null | head -n 1")
        return result.trim().ifEmpty { null }
    }
}
