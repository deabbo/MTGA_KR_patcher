package com.deabbo.mtgakrpatcher

import android.content.Context
import android.util.Log
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Shizuku 스타일 알림 페어링의 상태를 들고 있습니다. BroadcastReceiver는 매번 새
 * 인스턴스로 만들어지기 때문에, "지금 검색 중인지" "어떤 서비스를 찾았는지" 같은
 * 상태는 여기(object, 프로세스 생존 동안 유지)에 보관합니다.
 */
object NotificationPairFlow {

    private const val TAG = "NotificationPairFlow"
    private const val SEARCH_TIMEOUT_MS = 5 * 60_000L // 5분까지 계속 찾습니다

    @Volatile private var foundPairingService: AdbServiceDiscovery.DiscoveredService? = null
    private val cancelled = AtomicBoolean(false)
    private val searching = AtomicBoolean(false)

    /** "검색 중" 알림을 띄우고 백그라운드에서 계속 찾습니다. 찾으면 알림이 자동으로 바뀝니다. */
    fun startSearch(context: Context) {
        val appContext = context.applicationContext
        if (searching.get()) return // 이미 검색 중이면 중복 시작하지 않음

        cancelled.set(false)
        foundPairingService = null
        searching.set(true)
        PairingNotificationHelper.showSearching(appContext)

        Thread {
            val discovery = AdbServiceDiscovery(appContext)
            var found: AdbServiceDiscovery.DiscoveredService? = null
            val deadline = System.currentTimeMillis() + SEARCH_TIMEOUT_MS

            while (!cancelled.get() && found == null && System.currentTimeMillis() < deadline) {
                val latch = CountDownLatch(1)
                discovery.discoverPairingService(
                    timeoutMs = 8_000,
                    onFound = { svc ->
                        if (svc.host != "127.0.0.1" && svc.host != "::1") found = svc
                        latch.countDown()
                    },
                    onTimeout = { latch.countDown() }
                )
                latch.await(9, TimeUnit.SECONDS)
            }

            searching.set(false)

            when {
                cancelled.get() -> {
                    Log.d(TAG, "검색이 사용자에 의해 취소되었습니다.")
                    PairingNotificationHelper.dismiss(appContext)
                }
                found != null -> {
                    Log.d(TAG, "페어링 서비스를 찾았습니다: ${found?.host}:${found?.port}")
                    foundPairingService = found
                    PairingNotificationHelper.showFoundWaitingForCode(appContext)
                }
                else -> {
                    Log.w(TAG, "페어링 서비스를 시간 내에 찾지 못했습니다.")
                    PairingNotificationHelper.updateStatus(
                        appContext,
                        "페어링 서비스를 찾지 못했습니다. 무선 디버깅이 켜져 있는지 확인 후 다시 시도하세요.",
                        ongoing = false
                    )
                }
            }
        }.start()
    }

    fun cancelSearch(context: Context) {
        cancelled.set(true)
        searching.set(false)
        PairingNotificationHelper.dismiss(context.applicationContext)
    }

    /** 알림에서 코드를 입력했을 때 호출됩니다. 이미 찾아둔 페어링 서비스로 바로 이어갑니다. */
    fun onCodeSubmitted(context: Context, code: String) {
        val appContext = context.applicationContext
        val pairService = foundPairingService

        if (pairService == null) {
            PairingNotificationHelper.updateStatus(
                appContext, "먼저 페어링 서비스를 찾아야 합니다. 다시 시도해주세요.", ongoing = false
            )
            return
        }

        PairingNotificationHelper.updateStatus(appContext, "페어링을 시작합니다...", ongoing = true)

        Thread {
            QuickPairHelper(appContext).pairAndConnect(pairService, code, object : QuickPairHelper.Listener {
                override fun onProgress(message: String) {
                    PairingNotificationHelper.updateStatus(appContext, message, ongoing = true)
                }
                override fun onSuccess() {
                    foundPairingService = null
                    PairingNotificationHelper.updateStatus(appContext, "연결 성공! 앱으로 돌아가 패치를 실행하세요.", ongoing = false)
                }
                override fun onFailure(message: String) {
                    PairingNotificationHelper.updateStatus(appContext, message, ongoing = false)
                }
            })
        }.start()
    }
}
