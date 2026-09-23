package com.deabbo.mtgakrpatcher

import android.content.Context
import android.content.Intent
import android.util.Log
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

/**
 * "코드 입력 -> mDNS로 기기 찾기 -> 페어링 -> 연결"의 전체 흐름을 한 곳에 모았습니다.
 * PairingActivity 화면과 알림(RemoteInput) 둘 다 이 클래스를 통해 동작합니다.
 *
 * [이번에 고친 것]
 *  - 모든 실패 지점에서 Log.e()로 Logcat에 실제 예외를 남깁니다 (전에는 화면 텍스트로만 보여서
 *    Logcat만 봐서는 원인을 알 수 없었습니다).
 *  - pair()/connect() 호출에 명시적 타임아웃(기본 12초)을 걸어서, 막히면 "시간 초과"로 빠르게
 *    끝나도록 했습니다. 전에는 타임아웃이 없어 소켓이 막히면 아주 오래(관찰상 29초 이상) 멈춰있었습니다.
 *  - mDNS로 찾은 주소가 127.0.0.1(루프백)이면 걸러냅니다 — 일부 기기에서 mDNS가 루프백 주소를
 *    잘못 돌려주는 경우가 있어, 이 경우 조용히 연결이 안 되는 문제가 있었습니다.
 */
class QuickPairHelper(context: Context) {

    companion object {
        private const val TAG = "QuickPairHelper"
        private const val OP_TIMEOUT_MS = 12_000L

        /** 어느 경로(화면 버튼 / 알림)로 성공했든, 이 방송을 보내 성공을 알립니다.
         *  PairingActivity가 열려 있으면 이걸 받아서 자동으로 닫힙니다. */
        const val ACTION_CONNECTED = "com.deabbo.mtgakrpatcher.ACTION_ADB_CONNECTED"
    }

    private val appContext = context.applicationContext
    private val sessionManager = AdbSessionManager(appContext)
    private val discovery = AdbServiceDiscovery(appContext)
    private val executor = Executors.newSingleThreadExecutor()

    interface Listener {
        fun onProgress(message: String)
        fun onSuccess()
        fun onFailure(message: String)
    }

    /** 코드 하나로 탐색 -> 페어링 -> 탐색 -> 연결까지 전부 시도합니다. 백그라운드 스레드에서 호출하세요. */
    fun runQuickPair(code: String, listener: Listener) {
        if (code.length != 6) {
            listener.onFailure("페어링 코드는 6자리 숫자입니다.")
            return
        }

        listener.onProgress("네트워크에서 기기를 찾는 중...")
        val pairService = discoverBlocking(isPairing = true)
        if (pairService == null) {
            listener.onFailure("기기를 찾지 못했습니다. 무선 디버깅이 켜져 있는지 확인하세요.")
            return
        }

        pairAndConnect(pairService, code, listener)
    }

    /**
     * 이미 mDNS로 찾아둔 페어링 서비스가 있을 때(알림의 "검색 중" 단계를 이미 거쳤을 때)
     * 코드만 받아서 페어링 -> 탐색 -> 연결까지 이어갑니다.
     */
    fun pairAndConnect(pairService: AdbServiceDiscovery.DiscoveredService, code: String, listener: Listener) {
        if (code.length != 6) {
            listener.onFailure("페어링 코드는 6자리 숫자입니다.")
            return
        }

        listener.onProgress("페어링 시도 중...")
        val paired = try {
            runWithTimeout("pair") { sessionManager.pair(pairService.host, pairService.port, code) }
        } catch (e: TimeoutException) {
            Log.e(TAG, "pair() 시간 초과", e)
            listener.onFailure("페어링 시간 초과. 코드를 다시 확인하거나 다시 시도하세요.")
            return
        } catch (e: java.net.ConnectException) {
            Log.e(TAG, "pair() 연결 거부", e)
            listener.onFailure("연결이 거부되었습니다. 페어링 코드는 화면을 새로 열 때마다 바뀝니다 — 설정에서 코드를 새로 받아 다시 시도하세요.")
            return
        } catch (e: Exception) {
            Log.e(TAG, "pair() 실패", e)
            listener.onFailure("페어링 오류: ${describeError(e)}")
            return
        }

        if (!paired) {
            Log.w(TAG, "pair()가 false를 반환했습니다 (예외 없이 실패)")
            listener.onFailure("페어링 실패. 코드가 만료되었을 수 있습니다. 새 코드를 받아 다시 시도하세요.")
            return
        }

        listener.onProgress("페어링 성공! 연결 정보를 찾는 중...")
        // 페어링 직후 곧바로 연결을 시도하면 기기 쪽 데몬이 아직 준비 중일 수 있어 잠깐 대기합니다.
        Thread.sleep(1500)

        // mDNS가 오래된(stale) 연결 포트를 알려주는 경우가 있어(ECONNREFUSED), 실패하면
        // 처음부터 다시 찾아서 재시도합니다. 한 번 실패했다고 바로 포기하지 않습니다.
        var connected = false
        var lastConnectError: String? = null

        repeat(3) { attempt ->
            listener.onProgress(if (attempt == 0) "연결 정보를 찾는 중..." else "연결 정보를 다시 찾는 중... (${attempt + 1}/3)")
            val connectService = discoverBlocking(isPairing = false)
            if (connectService == null) {
                lastConnectError = "연결 주소를 찾지 못했습니다."
                Thread.sleep(1000)
                return@repeat
            }

            listener.onProgress("연결 시도 중... (${connectService.host}:${connectService.port})")
            val success = try {
                runWithTimeout("connect") { sessionManager.connect(connectService.host, connectService.port) }
            } catch (e: TimeoutException) {
                Log.e(TAG, "connect() 시간 초과", e)
                lastConnectError = "연결 시간 초과."
                false
            } catch (e: java.net.ConnectException) {
                Log.e(TAG, "connect() 연결 거부 (시도 ${attempt + 1})", e)
                lastConnectError = "연결이 거부되었습니다 (오래된 포트 정보였을 수 있음)."
                false
            } catch (e: Exception) {
                Log.e(TAG, "connect() 실패", e)
                lastConnectError = describeError(e)
                false
            }

            if (success) {
                connected = true
                ConnectionPrefs.saveConnectAddress(appContext, connectService.host, connectService.port)
                return@repeat
            }
            Thread.sleep(1000)
        }

        if (!connected) {
            listener.onFailure("연결 실패: ${lastConnectError ?: "알 수 없는 오류"} 잠시 후 다시 시도하세요.")
            return
        }

        ConnectionPrefs.setMode(appContext, ConnectionPrefs.Mode.ADB_DIRECT)
        appContext.sendBroadcast(Intent(ACTION_CONNECTED).setPackage(appContext.packageName))
        listener.onSuccess()
    }

    /** discoverPairingService/discoverConnectService의 콜백 API를 동기 호출로 감쌉니다. */
    private fun discoverBlocking(isPairing: Boolean): AdbServiceDiscovery.DiscoveredService? {
        var result: AdbServiceDiscovery.DiscoveredService? = null
        val latch = java.util.concurrent.CountDownLatch(1)

        val onFound: (AdbServiceDiscovery.DiscoveredService) -> Unit = { service ->
            if (service.host != "127.0.0.1" && service.host != "::1") {
                result = service
            } else {
                Log.w(TAG, "루프백 주소(${service.host})가 발견되어 무시합니다.")
            }
            latch.countDown()
        }
        val onTimeout: () -> Unit = { latch.countDown() }

        if (isPairing) discovery.discoverPairingService(onFound = onFound, onTimeout = onTimeout)
        else discovery.discoverConnectService(onFound = onFound, onTimeout = onTimeout)

        latch.await(20, TimeUnit.SECONDS)
        return result
    }

    private fun <T> runWithTimeout(label: String, block: () -> T): T {
        val future = executor.submit<T> { block() }
        return try {
            future.get(OP_TIMEOUT_MS, TimeUnit.MILLISECONDS)
        } catch (e: TimeoutException) {
            future.cancel(true)
            Log.e(TAG, "$label 이(가) ${OP_TIMEOUT_MS}ms 안에 끝나지 않았습니다.")
            throw e
        } catch (e: java.util.concurrent.ExecutionException) {
            throw (e.cause as? Exception) ?: e
        }
    }

    /** message가 null인 예외(흔한 NullPointerException 등)라도 최소한 예외 클래스 이름은
     *  보여줍니다. 화면에 그냥 "null"만 뜨면 원인을 전혀 알 수 없어서 이렇게 바꿨습니다. */
    private fun describeError(e: Throwable): String {
        return e.message ?: e.javaClass.simpleName
    }
}
