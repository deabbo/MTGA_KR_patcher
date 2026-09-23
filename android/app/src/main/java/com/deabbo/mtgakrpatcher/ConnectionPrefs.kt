package com.deabbo.mtgakrpatcher

import android.content.Context

/** 사용자가 고른 연결 방식(Shizuku / 앱 내장 직접 연결)과, 직접 연결 시
 *  마지막으로 성공했던 주소를 저장해두는 곳. */
object ConnectionPrefs {
    private const val PREFS_NAME = "connection_prefs"
    private const val KEY_MODE = "connection_mode"
    private const val KEY_CONNECT_HOST = "connect_host"
    private const val KEY_CONNECT_PORT = "connect_port"
    private const val KEY_LAST_CONNECTED_AT = "last_connected_at"

    enum class Mode { SHIZUKU, ADB_DIRECT, UNSET }

    fun getMode(context: Context): Mode {
        val raw = prefs(context).getString(KEY_MODE, null) ?: return Mode.UNSET
        return try { Mode.valueOf(raw) } catch (e: Exception) { Mode.UNSET }
    }

    fun setMode(context: Context, mode: Mode) {
        prefs(context).edit().putString(KEY_MODE, mode.name).apply()
    }

    fun saveConnectAddress(context: Context, host: String, port: Int) {
        prefs(context).edit()
            .putString(KEY_CONNECT_HOST, host)
            .putInt(KEY_CONNECT_PORT, port)
            .putLong(KEY_LAST_CONNECTED_AT, System.currentTimeMillis())
            .apply()
    }

    fun getConnectHost(context: Context): String? = prefs(context).getString(KEY_CONNECT_HOST, null)
    fun getConnectPort(context: Context): Int = prefs(context).getInt(KEY_CONNECT_PORT, -1)

    fun hasSavedAddress(context: Context): Boolean =
        getConnectHost(context) != null && getConnectPort(context) > 0

    /** 아주 최근(기본 2분 이내)에 성공적으로 연결됐는지. 오래된 저장값과 구분하기 위한 용도입니다 —
     *  hasSavedAddress()만으로는 "오래전에 한 번 됐던 적 있음"과 "방금 막 성공함"을 구분할 수 없습니다. */
    fun connectedRecently(context: Context, withinMs: Long = 2 * 60_000L): Boolean {
        val lastConnectedAt = prefs(context).getLong(KEY_LAST_CONNECTED_AT, 0L)
        return lastConnectedAt > 0 && (System.currentTimeMillis() - lastConnectedAt) < withinMs
    }

    private fun prefs(context: Context) =
        context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)
}
