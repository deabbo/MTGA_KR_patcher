package com.deabbo.mtgakrpatcher

import android.content.Context
import android.net.nsd.NsdManager
import android.net.nsd.NsdServiceInfo
import android.os.Handler
import android.os.Looper

/**
 * 무선 디버깅을 켜면 안드로이드가 로컬 네트워크에 자동으로 광고하는 mDNS
 * 서비스(_adb-tls-pairing._tcp, _adb-tls-connect._tcp)를 찾아 IP:포트를
 * 자동으로 알아냅니다. 사용자가 Developer Options 화면의 숫자를 직접
 * 옮겨 적을 필요가 없어집니다.
 *
 * 페어링 코드(6자리)는 화면에 표시만 되고 네트워크로 광고되지 않아서
 * 자동으로 알아낼 수 없습니다 — 이것만은 사용자가 직접 입력해야 합니다.
 */
class AdbServiceDiscovery(private val context: Context) {

    data class DiscoveredService(val host: String, val port: Int)

    private val nsdManager by lazy {
        context.applicationContext.getSystemService(Context.NSD_SERVICE) as NsdManager
    }
    private val mainHandler = Handler(Looper.getMainLooper())

    /** 페어링용 서비스(_adb-tls-pairing._tcp)를 찾습니다. */
    fun discoverPairingService(
        timeoutMs: Long = 15_000,
        onFound: (DiscoveredService) -> Unit,
        onTimeout: () -> Unit
    ) {
        discover("_adb-tls-pairing._tcp.", timeoutMs, onFound, onTimeout)
    }

    /** 연결용 서비스(_adb-tls-connect._tcp)를 찾습니다. 페어링 직후 사용합니다. */
    fun discoverConnectService(
        timeoutMs: Long = 15_000,
        onFound: (DiscoveredService) -> Unit,
        onTimeout: () -> Unit
    ) {
        discover("_adb-tls-connect._tcp.", timeoutMs, onFound, onTimeout)
    }

    private fun discover(
        serviceType: String,
        timeoutMs: Long,
        onFound: (DiscoveredService) -> Unit,
        onTimeout: () -> Unit
    ) {
        var finished = false
        lateinit var listener: NsdManager.DiscoveryListener

        val timeoutRunnable = Runnable {
            if (!finished) {
                finished = true
                safeStopDiscovery(listener)
                onTimeout()
            }
        }

        listener = object : NsdManager.DiscoveryListener {
            override fun onDiscoveryStarted(regType: String) {}

            override fun onServiceFound(service: NsdServiceInfo) {
                if (finished) return
                nsdManager.resolveService(service, object : NsdManager.ResolveListener {
                    override fun onResolveFailed(si: NsdServiceInfo, errorCode: Int) {
                        // 이 서비스는 못 읽었어도 다른 후보가 더 있을 수 있으니 계속 기다립니다.
                    }

                    override fun onServiceResolved(si: NsdServiceInfo) {
                        if (finished) return
                        val host = si.host?.hostAddress ?: return
                        finished = true
                        mainHandler.removeCallbacks(timeoutRunnable)
                        safeStopDiscovery(listener)
                        mainHandler.post { onFound(DiscoveredService(host, si.port)) }
                    }
                })
            }

            override fun onServiceLost(service: NsdServiceInfo) {}
            override fun onDiscoveryStopped(regType: String) {}

            override fun onStartDiscoveryFailed(regType: String, errorCode: Int) {
                if (!finished) {
                    finished = true
                    mainHandler.removeCallbacks(timeoutRunnable)
                    mainHandler.post { onTimeout() }
                }
            }

            override fun onStopDiscoveryFailed(regType: String, errorCode: Int) {}
        }

        nsdManager.discoverServices(serviceType, NsdManager.PROTOCOL_DNS_SD, listener)
        mainHandler.postDelayed(timeoutRunnable, timeoutMs)
    }

    private fun safeStopDiscovery(listener: NsdManager.DiscoveryListener) {
        try {
            nsdManager.stopServiceDiscovery(listener)
        } catch (e: Exception) {
            // 이미 멈춘 상태에서 또 멈추려 하면 예외가 날 수 있어 무시합니다.
        }
    }
}
