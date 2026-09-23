package com.deabbo.mtgakrpatcher

import android.content.ComponentName
import android.content.Intent
import android.content.ServiceConnection
import android.content.pm.PackageManager
import android.net.Uri
import android.os.Bundle
import android.os.IBinder
import android.util.Log
import android.view.View
import android.widget.Button
import android.widget.CheckBox
import android.widget.TextView
import androidx.activity.result.contract.ActivityResultContracts
import androidx.appcompat.app.AlertDialog
import androidx.appcompat.app.AppCompatActivity
import rikka.shizuku.Shizuku
import java.io.File
import java.net.HttpURLConnection
import java.net.URL

/**
 * PC 버전과 동일한 3가지 옵션(오역/영문이름만/패치제거)에 더해, 연결 방식을
 * Shizuku 또는 앱 내장 직접 연결(빠른 연결) 중에서 고를 수 있습니다.
 * 어느 쪽이든 결과는 PrivilegedFileAccess 인터페이스로 통일해서 다루므로,
 * 아래 패치 로직은 연결 방식을 신경 쓰지 않습니다.
 */
class MainActivity : AppCompatActivity() {

    companion object {
        private const val TAG = "MainActivity"
        private const val SHIZUKU_PERMISSION_REQUEST_CODE = 1001
        private const val PREF_GUIDE_SHOWN = "guide_shown"

        private const val CLIENT_JSON_URL =
            "https://docs.google.com/uc?export=download&id=1oOqAmmoyJ9FJZsrWccMoLjMchAatWtou&confirm=t"
        private const val CARD_JSON_URL =
            "https://docs.google.com/uc?export=download&id=1pSF_YCV0NPuy240Rtt0bzOmr1GyE5HMd&confirm=t"
    }

    private lateinit var statusText: TextView
    private lateinit var logText: TextView
    private lateinit var mistranslationCheck: CheckBox
    private lateinit var englishNamesOnlyCheck: CheckBox
    private lateinit var removePatchCheck: CheckBox
    private lateinit var runButton: Button
    private lateinit var connectionButton: Button
    private lateinit var shizukuDownloadButton: Button

    /** 연결 방식과 무관하게, 패치 로직은 이 인터페이스로만 파일에 접근합니다. */
    private var fileAccess: PrivilegedFileAccess? = null

    private var adbSession: AdbSessionManager? = null

    private var isGuideShowing = false

    // --- Shizuku 전용 상태 ---
    private val userServiceConnection = object : ServiceConnection {
        override fun onServiceConnected(name: ComponentName?, binder: IBinder?) {
            if (binder != null && binder.pingBinder()) {
                val service = IUserService.Stub.asInterface(binder)
                fileAccess = ShizukuFileAccess(service)
                statusText.text = "준비 완료 (Shizuku). 옵션을 선택하고 버튼을 누르세요."
            }
        }

        override fun onServiceDisconnected(name: ComponentName?) {
            fileAccess = null
            statusText.text = "특권 프로세스 연결이 끊어졌습니다."
        }
    }

    private val permissionResultListener =
        Shizuku.OnRequestPermissionResultListener { requestCode, grantResult ->
            if (requestCode == SHIZUKU_PERMISSION_REQUEST_CODE) {
                if (grantResult == PackageManager.PERMISSION_GRANTED) {
                    onShizukuReady()
                } else {
                    statusText.text = "Shizuku 권한이 거부되었습니다. 앱을 다시 실행해 다시 시도하세요."
                }
            }
        }

    private val binderReceivedListener = Shizuku.OnBinderReceivedListener {
        checkShizukuPermission()
    }

    private val binderDeadListener = Shizuku.OnBinderDeadListener {
        fileAccess = null
        statusText.text = "Shizuku 연결이 끊어졌습니다. Shizuku 앱에서 다시 시작해주세요."
    }

    // 페어링 화면 결과를 받아서 이어받습니다. resultCode는 신뢰하지 않습니다 — 알림 경로로
    // 백그라운드에서 성공했을 수도 있으므로, 실제 저장된 연결 정보(ConnectionPrefs)를 기준으로 판단합니다.
    private val pairingLauncher = registerForActivityResult(ActivityResultContracts.StartActivityForResult()) { _ ->
        if (ConnectionPrefs.getMode(this) == ConnectionPrefs.Mode.ADB_DIRECT && ConnectionPrefs.hasSavedAddress(this)) {
            connectAdbDirect()
        } else {
            statusText.text = "연결되지 않았습니다."
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)

        statusText = findViewById(R.id.statusText)
        logText = findViewById(R.id.logText)
        mistranslationCheck = findViewById(R.id.mistranslationCheck)
        englishNamesOnlyCheck = findViewById(R.id.englishNamesOnlyCheck)
        removePatchCheck = findViewById(R.id.removePatchCheck)
        runButton = findViewById(R.id.runButton)
        connectionButton = findViewById(R.id.connectionButton)
        shizukuDownloadButton = findViewById(R.id.shizukuDownloadButton)
        shizukuDownloadButton.setOnClickListener { openShizukuPlayStorePage() }
        val helpButton = findViewById<Button>(R.id.helpButton)

        helpButton.setOnClickListener { showGuideDialog() }
        connectionButton.setOnClickListener { onConnectionButtonClicked() }

        mistranslationCheck.setOnCheckedChangeListener { _, _ -> updateUiState() }
        englishNamesOnlyCheck.setOnCheckedChangeListener { _, _ -> updateUiState() }
        removePatchCheck.setOnCheckedChangeListener { _, _ -> updateUiState() }
        runButton.setOnClickListener { runPatch() }
        updateUiState()

        Shizuku.addBinderReceivedListenerSticky(binderReceivedListener)
        Shizuku.addBinderDeadListener(binderDeadListener)
        Shizuku.addRequestPermissionResultListener(permissionResultListener)

        checkAppUpdate()

        // 첫 실행이면 가이드를 먼저 보여주고, 확인을 눌러야만 연결 방식 설정(UNSET일 때 자동으로
        // 뜨는 선택 창)으로 넘어갑니다. 가이드를 보고 있는 동안에는 자동 권한 요청으로 인한 화면 깜빡임을 방지합니다.
        if (!getPreferences(MODE_PRIVATE).getBoolean(PREF_GUIDE_SHOWN, false)) {
            isGuideShowing = true
            showGuideDialog(onConfirm = {
                getPreferences(MODE_PRIVATE).edit().putBoolean(PREF_GUIDE_SHOWN, true).apply()
                isGuideShowing = false
                setupConnectionForCurrentMode()
            })
        } else {
            setupConnectionForCurrentMode()
        }
    }

    private var isFirstResume = true

    override fun onResume() {
        super.onResume()
        // onCreate가 이미 최초 연결 시도를 하므로, 첫 onResume은 건너뛰어 중복 시도를 막습니다.
        if (isFirstResume) {
            isFirstResume = false
            return
        }
        // 알림 경로 등으로 앱 밖에서 연결이 이미 성공했을 수 있으니, 돌아올 때마다 다시 확인합니다.
        // auto=true: 실패해도 페어링 화면을 강제로 띄우지 않습니다 (무한 루프 방지).
        if (fileAccess == null && ConnectionPrefs.getMode(this) == ConnectionPrefs.Mode.ADB_DIRECT) {
            connectAdbDirect(auto = true)
        }
    }

    override fun onDestroy() {
        super.onDestroy()
        Shizuku.removeBinderReceivedListener(binderReceivedListener)
        Shizuku.removeBinderDeadListener(binderDeadListener)
        Shizuku.removeRequestPermissionResultListener(permissionResultListener)
        if (ConnectionPrefs.getMode(this) == ConnectionPrefs.Mode.SHIZUKU && fileAccess != null) {
            Shizuku.unbindUserService(buildUserServiceArgs(), userServiceConnection, true)
        }
        adbSession?.disconnect()
    }

    // ============ 연결 방식 선택/설정 ============

    private fun setupConnectionForCurrentMode() {
        // 현재 배포 버전은 Shizuku 전용으로 작동하도록 모드를 SHIZUKU로 고정합니다.
        if (ConnectionPrefs.getMode(this) != ConnectionPrefs.Mode.SHIZUKU) {
            ConnectionPrefs.setMode(this, ConnectionPrefs.Mode.SHIZUKU)
        }
        connectionButton.text = "연결: Shizuku (탭하여 상태 재확인)"
        shizukuDownloadButton.visibility = View.VISIBLE
        checkShizukuPermission()
    }

    /** Play 스토어의 Shizuku 페이지를 엽니다. Play 스토어 앱이 없으면 웹 브라우저로 대체합니다. */
    private fun openShizukuPlayStorePage() {
        val packageId = "moe.shizuku.privileged.api"
        try {
            startActivity(Intent(Intent.ACTION_VIEW, Uri.parse("market://details?id=$packageId")))
        } catch (e: Exception) {
            try {
                startActivity(Intent(Intent.ACTION_VIEW, Uri.parse("https://play.google.com/store/apps/details?id=$packageId")))
            } catch (e2: Exception) {
                appendLog("Play 스토어를 열지 못했습니다: ${e2.message}")
            }
        }
    }

    private fun onConnectionButtonClicked() {
        checkShizukuPermission()
    }

    private fun showConnectionModeChooser() {
        // [추후 빠른 연결(ADB_DIRECT) 기능 추가 시 아래 주석을 해제하여 사용합니다]
        /*
        AlertDialog.Builder(this)
            .setTitle("연결 방식 선택")
            .setMessage(
                "Shizuku: 별도 앱 설치가 필요하지만 한 번 설정해두면 안정적입니다.\n\n" +
                "빠른 연결: 이 앱 안에서 바로 페어링합니다. 별도 앱이 필요 없지만, " +
                "기기를 재부팅하면 다시 연결해야 합니다."
            )
            .setPositiveButton("Shizuku 사용") { _, _ ->
                ConnectionPrefs.setMode(this, ConnectionPrefs.Mode.SHIZUKU)
                fileAccess = null
                setupConnectionForCurrentMode()
            }
            .setNegativeButton("빠른 연결 사용") { _, _ ->
                ConnectionPrefs.setMode(this, ConnectionPrefs.Mode.ADB_DIRECT)
                fileAccess = null
                setupConnectionForCurrentMode()
            }
            .setNeutralButton("취소", null)
            .show()
        */
        checkShizukuPermission()
    }

    // --- Shizuku 경로 ---
    private fun checkShizukuPermission() {
        if (ConnectionPrefs.getMode(this) != ConnectionPrefs.Mode.SHIZUKU) return
        if (isGuideShowing) return
        if (!Shizuku.pingBinder()) {
            statusText.text = "Shizuku가 아직 실행되지 않았습니다. Shizuku 앱을 열어 '무선 디버깅으로 시작'을 눌러주세요."
            return
        }
        if (Shizuku.isPreV11()) {
            statusText.text = "Shizuku 버전이 너무 낮습니다. Shizuku 앱을 최신 버전으로 업데이트하세요."
            return
        }
        if (Shizuku.checkSelfPermission() == PackageManager.PERMISSION_GRANTED) {
            onShizukuReady()
        } else {
            statusText.text = "Shizuku 권한을 요청합니다..."
            Shizuku.requestPermission(SHIZUKU_PERMISSION_REQUEST_CODE)
        }
    }

    private fun onShizukuReady() {
        statusText.text = "Shizuku 준비 완료. 특권 프로세스에 연결하는 중..."
        Shizuku.bindUserService(buildUserServiceArgs(), userServiceConnection)
    }

    private fun buildUserServiceArgs(): Shizuku.UserServiceArgs {
        return Shizuku.UserServiceArgs(ComponentName(packageName, UserServiceImpl::class.java.name))
            .daemon(false)
            .processNameSuffix("patcher")
            .debuggable(false)
            .version(1)
    }

    // --- 빠른 연결(직접 ADB) 경로 ---
    // auto=true면 onResume 등 백그라운드 재확인 용도로, 실패해도 화면을 강제로 띄우지 않습니다.
    // (이전에 자동으로 페어링 화면을 다시 띄우던 게 onResume과 맞물려 무한 루프를 만들었습니다.)
    private fun connectAdbDirect(auto: Boolean = false) {
        val session = adbSession ?: AdbSessionManager(applicationContext).also { adbSession = it }

        // 알림 경로 등에서 이미 이 프로세스 안에 연결이 살아있을 수 있습니다.
        // 그 상태에서 connect()를 또 부르면 라이브러리가 실패를 반환하는 문제가 있어,
        // 이미 연결돼 있으면 다시 connect()를 호출하지 않고 바로 재사용합니다.
        if (session.isConnected()) {
            fileAccess = AdbDirectFileAccess(session)
            statusText.text = "준비 완료 (빠른 연결). 옵션을 선택하고 버튼을 누르세요."
            return
        }

        if (!ConnectionPrefs.hasSavedAddress(this)) {
            statusText.text = "아직 페어링된 적이 없습니다. '연결 방식 설정'에서 다시 진행하세요."
            if (!auto) pairingLauncher.launch(Intent(this, PairingActivity::class.java))
            return
        }

        val host = ConnectionPrefs.getConnectHost(this)!!
        val port = ConnectionPrefs.getConnectPort(this)
        statusText.text = "저장된 연결 정보로 다시 연결하는 중..."
        Thread {
            val success = try { session.connect(host, port) } catch (e: Exception) { false }
            runOnUiThread {
                if (success) {
                    fileAccess = AdbDirectFileAccess(session)
                    statusText.text = "준비 완료 (빠른 연결). 옵션을 선택하고 버튼을 누르세요."
                } else {
                    val baseMsg = "재연결 실패 (기기 재부팅 등으로 끊어졌을 수 있습니다)."
                    if (!auto) {
                        // 사용자가 직접 "빠른 연결 사용"을 누른 경우에만 페어링 화면을 열어줍니다.
                        // onResume의 자동 재확인(auto=true)에서는 절대 열지 않습니다 (무한 루프 방지).
                        statusText.text = "$baseMsg 다시 페어링해주세요."
                        pairingLauncher.launch(Intent(this, PairingActivity::class.java))
                    } else {
                        statusText.text = "$baseMsg '연결 방식 설정'에서 다시 페어링하세요."
                    }
                }
            }
        }.start()
    }

    // ============ 패치 옵션 UI ============

    private fun updateUiState() {
        val isRemove = removePatchCheck.isChecked

        if (isRemove) {
            mistranslationCheck.isChecked = false
            englishNamesOnlyCheck.isChecked = false
            mistranslationCheck.isEnabled = false
            englishNamesOnlyCheck.isEnabled = false
            runButton.isEnabled = true
            runButton.text = "패치 제거 시작"
            return
        }

        mistranslationCheck.isEnabled = true
        englishNamesOnlyCheck.isEnabled = true

        val isMistranslation = mistranslationCheck.isChecked
        val isEnglishOnly = englishNamesOnlyCheck.isChecked
        runButton.isEnabled = isMistranslation || isEnglishOnly

        val labels = mutableListOf<String>()
        if (isMistranslation) labels.add("오역")
        if (isEnglishOnly) labels.add("영문 이름")

        runButton.text = if (labels.isNotEmpty()) "${labels.joinToString(" & ")} 패치 시작"
                          else "옵션을 선택하세요"
    }

    // ============ 패치 실행 (연결 방식 무관) ============

    private fun runPatch() {
        val access = fileAccess
        if (access == null) {
            appendLog("아직 연결되지 않았습니다. '연결 방식 설정' 버튼을 확인하세요.")
            return
        }

        val removePatch = removePatchCheck.isChecked
        val doMistranslation = !removePatch && mistranslationCheck.isChecked
        val doEnglishNamesOnly = !removePatch && englishNamesOnlyCheck.isChecked

        logText.text = ""
        runButton.isEnabled = false
        appendLog(if (removePatch) "패치 제거를 시작합니다..." else "패치를 시작합니다...")

        Thread {
            try {
                appendLog("MTG 아레나 설치 경로를 확인하는 중...")
                val (cardPath, clientPath) = access.locateDbPaths()

                if (cardPath == null && clientPath == null) {
                    appendLog("*** 필수 파일을 찾지 못해 작업을 중단합니다. ***")
                    return@Thread
                }
                cardPath?.let { appendLog("성공: 카드 데이터베이스를 찾았습니다: ${File(it).name}") }
                clientPath?.let { appendLog("성공: 클라이언트 로컬라이제이션 파일을 찾았습니다: ${File(it).name}") }

                if (removePatch) {
                    var removedAny = false
                    clientPath?.let { removedAny = deleteOne(access, it) || removedAny }
                    cardPath?.let { removedAny = deleteOne(access, it) || removedAny }
                    appendLog(
                        if (removedAny) "패치 제거가 완료되었습니다. 게임을 재시작하여 파일을 복구하세요."
                        else "*** 삭제할 파일을 찾지 못했습니다. ***"
                    )
                    return@Thread
                }

                var clientJson: String? = null
                var cardJson: String? = null
                if (doMistranslation) {
                    appendLog("서버에서 번역 데이터를 받아오는 중...")
                    clientJson = fetchText(CLIENT_JSON_URL)
                    cardJson = fetchText(CARD_JSON_URL)
                    if (clientJson == null && cardJson == null) {
                        appendLog("경고: 서버에서 데이터를 받아오지 못했습니다. 네트워크 상태를 확인하세요.")
                    }
                }

                if (doMistranslation) {
                    clientPath?.let { remote ->
                        val local = pullToLocal(access, remote)
                        LocalizationPatcher.patchClientLocalization(local, clientJson, ::appendLog)
                        pushFromLocal(access, local, remote)
                        local.delete()
                    }
                    cardPath?.let { remote ->
                        val local = pullToLocal(access, remote)
                        LocalizationPatcher.patchCardText(local, cardJson, ::appendLog)
                        LocalizationPatcher.patchSneakKeyword(local, ::appendLog)
                        LocalizationPatcher.patchVanishingKeyword(local, ::appendLog)
                        LocalizationPatcher.patchNoTranslationNeeded(local, ::appendLog)
                        pushFromLocal(access, local, remote)
                        local.delete()
                    }
                }

                if (doEnglishNamesOnly) {
                    cardPath?.let { remote ->
                        val local = pullToLocal(access, remote)
                        LocalizationPatcher.patchEnglishNamesOnly(local, ::appendLog)
                        pushFromLocal(access, local, remote)
                        local.delete()
                    }
                }

                appendLog("*** 모든 작업이 완료되었습니다. ***")
            } catch (e: android.os.RemoteException) {
                Log.e(TAG, "패치 중 RemoteException", e)
                appendLog("*** 특권 프로세스와의 연결이 끊어졌습니다. 연결 상태를 확인하고 다시 시도하세요. (${e.message}) ***")
            } catch (e: Exception) {
                Log.e(TAG, "패치 중 오류", e)
                appendLog("*** 패치 중 오류 발생: ${e.message} ***")
            } finally {
                runOnUiThread { runButton.isEnabled = true }
            }
        }.start()
    }

    private fun deleteOne(access: PrivilegedFileAccess, remotePath: String): Boolean {
        val deleted = access.deleteIfExists(remotePath)
        appendLog(
            if (deleted) "  - 삭제됨: ${File(remotePath).name}"
            else "  - 파일이 없어 건너뜁니다: ${File(remotePath).name}"
        )
        return deleted
    }

    private fun pullToLocal(access: PrivilegedFileAccess, remotePath: String): File {
        val local = File(cacheDir, File(remotePath).name)
        appendLog("  - 기기에서 파일을 가져오는 중: ${local.name}")
        access.pullToLocal(remotePath, local)
        return local
    }

    private fun pushFromLocal(access: PrivilegedFileAccess, local: File, remotePath: String) {
        appendLog("  - 패치된 파일을 기기에 다시 적용하는 중: ${File(remotePath).name}")
        access.pushFromLocal(local, remotePath)
    }

    // ============ 앱 업데이트 체크 ============

    private fun checkAppUpdate() {
        Thread {
            val currentVersion = BuildConfig.VERSION_NAME
            val update = VersionChecker.checkForUpdate(currentVersion) ?: return@Thread
            runOnUiThread {
                if (isFinishing) return@runOnUiThread
                val message = buildString {
                    append("새로운 버전 (${update.latestVersion})이 있습니다.\n(현재 버전: $currentVersion)")
                    if (!update.notes.isNullOrBlank()) append("\n\n${update.notes}")
                }
                AlertDialog.Builder(this)
                    .setTitle("업데이트 안내")
                    .setMessage(message)
                    .setPositiveButton("다운로드 페이지 열기") { _, _ ->
                        try {
                            startActivity(Intent(Intent.ACTION_VIEW, Uri.parse(update.releaseUrl)))
                        } catch (e: Exception) {
                            appendLog("다운로드 페이지를 여는 데 실패했습니다: ${e.message}")
                        }
                    }
                    .setNegativeButton("나중에", null)
                    .show()
            }
        }.start()
    }

    // ============ 도움말 ============

    private fun showGuideDialog(onConfirm: (() -> Unit)? = null) {
        val message = """
            이 앱은 MTG 아레나(안드로이드)의 한글 오역을 고쳐주는 도구입니다.
            게임 파일을 수정하려면 Shizuku 앱을 통한 권한 설정이 필요합니다.

            [Shizuku 설정 방법]
            1. 아래 'Shizuku 앱 설치/열기' 버튼을 누르거나 Play 스토어에서 'Shizuku'를 설치하세요.
            2. 설정 > 개발자 옵션 > 무선 디버깅을 켜세요.
               (개발자 옵션이 안 보이면: 설정 > 휴대전화 정보 > 빌드번호를 7번 연속 탭)
            3. Shizuku 앱을 열고 '무선 디버깅으로 시작' 버튼을 누르세요.
            4. 이 앱으로 돌아와 권한 요청 팝업이 뜨면 '항상 허용'을 누르세요.

            [패치 실행]
            - MTG 아레나를 한 번 이상 실행해서 게임 데이터가 받아져 있어야 합니다.
            - 원하는 패치 옵션(오역 패치 / 카드이름만 영어로 / 패치 제거)을 고르고
              버튼을 누르면 자동으로 진행됩니다.

            [주의사항]
            - 기기를 재부팅하면 Shizuku 연결이 끊어집니다. Shizuku 앱에서 다시 시작해주세요.
            - '패치 제거'를 누르면 수정된 파일이 삭제되고, 다음에 게임을 켜면
              정식 원본 파일로 새로 다운로드됩니다.
        """.trimIndent()

        val builder = AlertDialog.Builder(this)
            .setTitle("사용 가이드")
            .setMessage(message)
            .setPositiveButton("확인") { _, _ ->
                onConfirm?.invoke()
            }

        if (onConfirm != null) {
            builder.setCancelable(false)
        }

        builder.show()
    }

    // ============ 유틸 ============

    private fun fetchText(urlString: String): String? {
        return try {
            val conn = (URL(urlString).openConnection() as HttpURLConnection).apply {
                connectTimeout = 15_000
                readTimeout = 15_000
                requestMethod = "GET"
            }
            val code = conn.responseCode
            if (code != HttpURLConnection.HTTP_OK) {
                appendLog("  - 서버 응답 오류 (HTTP $code): $urlString")
                return null
            }
            conn.inputStream.bufferedReader().use { it.readText() }
        } catch (e: java.net.SocketTimeoutException) {
            Log.e(TAG, "fetchText 시간 초과: $urlString", e)
            appendLog("  - 서버 응답 시간 초과: $urlString")
            null
        } catch (e: java.io.IOException) {
            Log.e(TAG, "fetchText IO 오류: $urlString", e)
            appendLog("  - 네트워크 오류: ${e.message}")
            null
        } catch (e: Exception) {
            Log.e(TAG, "fetchText 알 수 없는 오류: $urlString", e)
            appendLog("  - 알 수 없는 오류로 데이터를 받지 못했습니다: ${e.message}")
            null
        }
    }

    private fun appendLog(message: String) {
        runOnUiThread { logText.append("$message\n") }
    }
}
