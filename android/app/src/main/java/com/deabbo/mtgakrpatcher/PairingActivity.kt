package com.deabbo.mtgakrpatcher

import android.Manifest
import android.app.Activity
import android.content.Intent
import android.content.pm.PackageManager
import android.os.Build
import android.os.Bundle
import android.provider.Settings
import android.widget.Button
import android.widget.EditText
import android.widget.TextView
import androidx.activity.result.contract.ActivityResultContracts
import androidx.appcompat.app.AlertDialog
import androidx.appcompat.app.AppCompatActivity
import androidx.core.content.ContextCompat

/**
 * Shizuku 앱 없이, 무선 디버깅 페어링을 우리 앱 안에서 직접 처리하는 화면.
 *
 * 코드 입력은 전부 "알림"에서만 받습니다(앱 화면에 코드 입력란이 없습니다) —
 * "페어링 시작"을 누르면 검색 알림이 뜨고 동시에 설정(무선 디버깅) 화면으로
 * 이동하며, 코드를 찾으면 알림이 자동으로 코드 입력창으로 바뀝니다.
 *
 * mDNS가 막힌 네트워크(일부 공유기의 AP 격리 등)를 위해 수동 입력도 폴백으로 남겨뒀고,
 * 그쪽은 이 화면 안에서 직접 IP:포트+코드를 입력합니다.
 */
class PairingActivity : AppCompatActivity() {

    private lateinit var sessionManager: AdbSessionManager
    private lateinit var statusText: TextView
    private lateinit var devOptionsStatusText: TextView

    private val notificationPermissionLauncher =
        registerForActivityResult(ActivityResultContracts.RequestPermission()) { granted ->
            if (granted) launchNotificationFlow()
            else setStatus("알림 권한이 없으면 코드를 입력할 방법이 없어 이 방식을 쓸 수 없습니다. 설정 > 앱 > MTGA 한글패치 > 알림에서 다시 켤 수 있습니다.")
        }

    /** 알림 경로 등 화면 밖에서 연결에 성공해도, 이 화면이 떠 있는 동안 이 방송을 받으면
     *  자동으로 성공 처리하고 닫습니다. */
    private val connectedReceiver = object : android.content.BroadcastReceiver() {
        override fun onReceive(context: android.content.Context, intent: Intent) {
            setStatus("연결 성공!")
            setResult(Activity.RESULT_OK)
            finish()
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_pairing)

        sessionManager = AdbSessionManager(applicationContext)
        statusText = findViewById(R.id.pairingStatusText)

        val toggleManualButton = findViewById<Button>(R.id.toggleManualButton)
        val manualSection = findViewById<android.widget.LinearLayout>(R.id.manualSection)
        val manualPairCodeInput = findViewById<EditText>(R.id.manualPairCodeInput)
        val pairHostPortInput = findViewById<EditText>(R.id.pairHostPortInput)
        val connectHostPortInput = findViewById<EditText>(R.id.connectHostPortInput)
        val pairButton = findViewById<Button>(R.id.pairButton)
        val connectButton = findViewById<Button>(R.id.connectButton)
        val devOptionsStatusText = findViewById<TextView>(R.id.devOptionsStatusText)
        val notifyInputButton = findViewById<Button>(R.id.notifyInputButton)
        this.devOptionsStatusText = devOptionsStatusText

        updateDevOptionsStatus()

        notifyInputButton.setOnClickListener {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU &&
                ContextCompat.checkSelfPermission(this, Manifest.permission.POST_NOTIFICATIONS)
                != PackageManager.PERMISSION_GRANTED
            ) {
                // 시스템 권한 팝업이 뜨기 전에, 왜 필요한지 먼저 설명합니다.
                // (안드로이드 13+에서는 이유 없이 권한을 요청하면 거부율이 높습니다.)
                AlertDialog.Builder(this)
                    .setTitle("알림 권한이 필요합니다")
                    .setMessage(
                        "페어링 코드를 앱 화면이 아니라 알림에서 바로 입력받는 방식이라, " +
                        "안드로이드의 알림 표시 권한이 필요합니다.\n\n" +
                        "다음 화면에서 '허용'을 눌러주세요. 거부하면 코드를 입력할 방법이 없어 " +
                        "이 연결 방식을 쓸 수 없습니다."
                    )
                    .setPositiveButton("계속") { _, _ ->
                        notificationPermissionLauncher.launch(Manifest.permission.POST_NOTIFICATIONS)
                    }
                    .setNegativeButton("취소", null)
                    .show()
            } else {
                launchNotificationFlow()
            }
        }

        toggleManualButton.setOnClickListener {
            manualSection.visibility =
                if (manualSection.visibility == android.view.View.GONE) android.view.View.VISIBLE
                else android.view.View.GONE
        }

        // --- 수동 페어링(폴백): mDNS가 막힌 네트워크에서 IP:포트+코드를 직접 입력 ---
        pairButton.setOnClickListener {
            val (host, port) = parseHostPort(pairHostPortInput.text.toString()) ?: run {
                setStatus("페어링 IP:포트 형식이 올바르지 않습니다.")
                return@setOnClickListener
            }
            val code = manualPairCodeInput.text.toString().trim()
            if (code.length != 6) {
                setStatus("페어링 코드는 6자리 숫자입니다.")
                return@setOnClickListener
            }
            pairButton.isEnabled = false
            setStatus("수동 페어링 시도 중...")
            Thread {
                val success = try {
                    sessionManager.pair(host, port, code)
                } catch (e: Exception) {
                    setStatus("페어링 오류: ${e.message ?: e.javaClass.simpleName}")
                    false
                }
                runOnUiThread {
                    pairButton.isEnabled = true
                    setStatus(if (success) "페어링 성공! 이제 2단계로 연결하세요." else "페어링 실패.")
                }
            }.start()
        }

        // --- 수동 연결(폴백) ---
        connectButton.setOnClickListener {
            val (host, port) = parseHostPort(connectHostPortInput.text.toString()) ?: run {
                setStatus("연결 IP:포트 형식이 올바르지 않습니다.")
                return@setOnClickListener
            }
            connectButton.isEnabled = false
            setStatus("연결 시도 중...")
            Thread { finishConnect(host, port, connectButton) }.start()
        }
    }

    /** 연결까지 성공하면 주소를 저장하고 화면을 닫습니다. UI 스레드가 아니어도 안전합니다. */
    private fun finishConnect(host: String, port: Int, triggerButton: Button) {
        val success = try {
            sessionManager.connect(host, port)
        } catch (e: Exception) {
            runOnUiThread { setStatus("연결 오류: ${e.message ?: e.javaClass.simpleName}") }
            false
        }
        runOnUiThread {
            triggerButton.isEnabled = true
            if (success) {
                ConnectionPrefs.saveConnectAddress(this, host, port)
                ConnectionPrefs.setMode(this, ConnectionPrefs.Mode.ADB_DIRECT)
                setStatus("연결 성공!")
                setResult(Activity.RESULT_OK)
                finish()
            } else {
                setStatus("연결 실패. 무선 디버깅이 켜져 있는지 확인하세요.")
            }
        }
    }

    private fun parseHostPort(raw: String): Pair<String, Int>? {
        val trimmed = raw.trim()
        val idx = trimmed.lastIndexOf(':')
        if (idx <= 0 || idx == trimmed.length - 1) return null
        val host = trimmed.substring(0, idx)
        val port = trimmed.substring(idx + 1).toIntOrNull() ?: return null
        return host to port
    }

    private fun openDevSettings() {
        val intent = Intent(Settings.ACTION_APPLICATION_DEVELOPMENT_SETTINGS)
        try {
            startActivity(intent)
        } catch (e: Exception) {
            try {
                startActivity(Intent(Settings.ACTION_SETTINGS))
            } catch (e2: Exception) {
                setStatus("설정 화면을 열지 못했습니다. 직접 설정 앱에서 찾아주세요.")
            }
        }
    }

    /** Shizuku 스타일: "검색 중" 알림을 띄우고 백그라운드에서 페어링 서비스를 찾은 뒤,
     *  곧바로 개발자 옵션(무선 디버깅) 화면으로 이동합니다. 설정에서 코드를 확인하고
     *  알림을 내려서 입력하면 됩니다. */
    private fun launchNotificationFlow() {
        NotificationPairFlow.startSearch(this)
        setStatus("검색을 시작했습니다. 설정에서 코드를 확인한 뒤 알림을 내려 입력하세요.")
        openDevSettings()
    }

    private fun setStatus(message: String) {
        runOnUiThread { statusText.text = message }
    }

    override fun onResume() {
        super.onResume()
        if (::devOptionsStatusText.isInitialized) updateDevOptionsStatus()

        // 알림 경로에서 이미 성공했는데, 그 시점에 이 화면이 백그라운드(예: 설정 화면에 가 있음)라
        // 브로드캐스트를 놓쳤을 수 있습니다. 화면이 다시 보일 때 "최근에 연결 성공했는지"를 직접
        // 확인합니다. hasSavedAddress()만 보면 오래된 연결 기록과 헷갈릴 수 있어 시간 기준으로 봅니다.
        if (ConnectionPrefs.getMode(this) == ConnectionPrefs.Mode.ADB_DIRECT &&
            ConnectionPrefs.connectedRecently(this)
        ) {
            setStatus("연결 성공!")
            setResult(Activity.RESULT_OK)
            finish()
            return
        }

        val filter = android.content.IntentFilter(QuickPairHelper.ACTION_CONNECTED)
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            registerReceiver(connectedReceiver, filter, android.content.Context.RECEIVER_NOT_EXPORTED)
        } else {
            @Suppress("UnspecifiedRegisterReceiverFlag")
            registerReceiver(connectedReceiver, filter)
        }
    }

    override fun onPause() {
        super.onPause()
        try { unregisterReceiver(connectedReceiver) } catch (e: Exception) { /* 등록 안 된 상태면 무시 */ }
    }

    private fun updateDevOptionsStatus() {
        val enabled = try {
            Settings.Global.getInt(contentResolver, Settings.Global.DEVELOPMENT_SETTINGS_ENABLED, 0) == 1
        } catch (e: Exception) {
            false
        }
        devOptionsStatusText.text = if (enabled) {
            "✓ 개발자 옵션이 켜져 있습니다. 아래 버튼을 누르면 검색 알림이 뜨고 무선 디버깅 화면으로 이동합니다."
        } else {
            "개발자 옵션이 아직 꺼져 있는 것 같습니다.\n설정 > 휴대전화 정보 > 빌드번호를 7번 연속 탭하면 켜집니다. 그다음 아래 버튼을 눌러주세요."
        }
    }
}
