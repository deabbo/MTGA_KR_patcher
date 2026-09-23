package com.deabbo.mtgakrpatcher

import org.json.JSONObject
import java.net.HttpURLConnection
import java.net.URL

/**
 * PC 버전의 check_for_updates()에 대응하지만, 안드로이드는 앱이 스스로를
 * 덮어쓸 수 없기 때문에 "새 버전이 있다"는 사실만 알려주고, 실제 설치는
 * 사용자가 GitHub Releases 페이지에서 직접 받도록 안내합니다.
 *
 * version.json은 android/version.json 경로에 두고, 새 버전을 낼 때마다
 * 이 파일의 "version" 값만 올리면 됩니다. (raw.githubusercontent.com 주소는
 * 저장소에 푸시하는 즉시 몇 분 내로 갱신됩니다. 캐시가 오래 남아있는 경우
 * URL 끝에 ?t=현재시간 같은 캐시 무효화 파라미터를 붙일 수도 있습니다.)
 */
object VersionChecker {

    private const val VERSION_CHECK_URL =
        "https://raw.githubusercontent.com/deabbo/MTGA_KR_patcher/main/android/version.json"

    data class UpdateInfo(
        val latestVersion: String,
        val releaseUrl: String,
        val notes: String?
    )

    /** 새 버전이 있으면 UpdateInfo를, 없거나 확인에 실패하면 null을 돌려줍니다. */
    fun checkForUpdate(currentVersion: String): UpdateInfo? {
        val json = fetchJson() ?: return null
        val latest = json.optString("version", null) ?: return null
        val releaseUrl = json.optString(
            "releaseUrl",
            "https://github.com/deabbo/MTGA_KR_patcher/releases/latest"
        )
        val notes = json.optString("notes", null)

        return if (isNewer(latest, currentVersion)) {
            UpdateInfo(latest, releaseUrl, notes)
        } else {
            null
        }
    }

    /**
     * "1.2.10" 같은 점(.)으로 구분된 버전 문자열을 숫자 단위로 비교합니다.
     * PC 버전은 단순 문자열 비교(latest_version > __version__)를 쓰는데,
     * 이 방식은 "1.10.0"과 "1.9.0"을 비교하면 문자열상 "1.9.0"이 더 크다고
     * 잘못 판단하는 문제가 있어(자릿수가 안 맞음), 안드로이드에서는 실제
     * 숫자로 쪼개서 비교하도록 개선했습니다.
     */
    private fun isNewer(latest: String, current: String): Boolean {
        val latestParts = latest.split(".").map { it.toIntOrNull() ?: 0 }
        val currentParts = current.split(".").map { it.toIntOrNull() ?: 0 }
        val maxLen = maxOf(latestParts.size, currentParts.size)
        for (i in 0 until maxLen) {
            val l = latestParts.getOrElse(i) { 0 }
            val c = currentParts.getOrElse(i) { 0 }
            if (l != c) return l > c
        }
        return false
    }

    private fun fetchJson(): JSONObject? {
        return try {
            val conn = (URL(VERSION_CHECK_URL).openConnection() as HttpURLConnection).apply {
                connectTimeout = 8_000
                readTimeout = 8_000
                requestMethod = "GET"
            }
            if (conn.responseCode != HttpURLConnection.HTTP_OK) return null
            val text = conn.inputStream.bufferedReader().use { it.readText() }
            JSONObject(text)
        } catch (e: Exception) {
            null // 버전 확인 실패는 조용히 넘어갑니다 (네트워크 문제로 패치 자체를 막으면 안 됨)
        }
    }
}
