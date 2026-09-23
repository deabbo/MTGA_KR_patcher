package com.deabbo.mtgakrpatcher

import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.content.Context
import android.content.Intent
import android.os.Build
import androidx.core.app.NotificationCompat
import androidx.core.app.RemoteInput

/**
 * Shizuku의 "무선 디버깅으로 시작" 알림과 같은 방식:
 *  1) 검색 중 — 계속 떠 있는 알림 + "검색 중지" 버튼
 *  2) 찾음 — 같은 알림이 "코드 입력" 칸으로 바뀜 (RemoteInput)
 *  3) 진행 중 / 결과 — 같은 알림 ID를 계속 갱신
 */
object PairingNotificationHelper {

    const val CHANNEL_ID = "quick_pair"
    const val NOTIFICATION_ID = 1001
    const val KEY_PAIRING_CODE = "pairing_code_input"
    const val ACTION_SUBMIT_CODE = "com.deabbo.mtgakrpatcher.ACTION_SUBMIT_PAIRING_CODE"
    const val ACTION_CANCEL_SEARCH = "com.deabbo.mtgakrpatcher.ACTION_CANCEL_PAIRING_SEARCH"

    private const val TITLE = "MTGA 한글패치 - 빠른 연결"

    fun ensureChannel(context: Context) {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.O) return
        val manager = context.getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
        if (manager.getNotificationChannel(CHANNEL_ID) == null) {
            val channel = NotificationChannel(
                CHANNEL_ID, "빠른 연결", NotificationManager.IMPORTANCE_HIGH
            ).apply {
                description = "무선 디버깅 페어링 진행 상태와 코드 입력창을 보여줍니다."
            }
            manager.createNotificationChannel(channel)
        }
    }

    /** 1) 검색 중 상태: 계속 떠 있고, "검색 중지" 버튼이 있습니다. */
    fun showSearching(context: Context) {
        ensureChannel(context)
        val cancelIntent = Intent(ACTION_CANCEL_SEARCH).setPackage(context.packageName)
        val cancelPendingIntent = PendingIntent.getBroadcast(
            context, 1, cancelIntent,
            PendingIntent.FLAG_MUTABLE or PendingIntent.FLAG_UPDATE_CURRENT
        )
        val cancelAction = NotificationCompat.Action.Builder(
            android.R.drawable.ic_menu_close_clear_cancel, "검색 중지", cancelPendingIntent
        ).build()

        val notification = NotificationCompat.Builder(context, CHANNEL_ID)
            .setSmallIcon(android.R.drawable.stat_sys_download)
            .setContentTitle(TITLE)
            .setContentText("페어링 서비스 검색 중...")
            .setPriority(NotificationCompat.PRIORITY_HIGH)
            .setOngoing(true)
            .setOnlyAlertOnce(true)
            .addAction(cancelAction)
            .build()

        androidx.core.app.NotificationManagerCompat.from(context).notify(NOTIFICATION_ID, notification)
    }

    /** 2) 찾음 상태: 같은 알림이 코드 입력창으로 바뀝니다. */
    fun showFoundWaitingForCode(context: Context) {
        ensureChannel(context)

        val remoteInput = RemoteInput.Builder(KEY_PAIRING_CODE)
            .setLabel("6자리 페어링 코드")
            .build()

        val submitIntent = Intent(ACTION_SUBMIT_CODE).setPackage(context.packageName)
        val submitPendingIntent = PendingIntent.getBroadcast(
            context, 0, submitIntent,
            PendingIntent.FLAG_MUTABLE or PendingIntent.FLAG_UPDATE_CURRENT
        )
        val codeAction = NotificationCompat.Action.Builder(
            android.R.drawable.ic_menu_edit, "코드 입력", submitPendingIntent
        ).addRemoteInput(remoteInput).build()

        val cancelIntent = Intent(ACTION_CANCEL_SEARCH).setPackage(context.packageName)
        val cancelPendingIntent = PendingIntent.getBroadcast(
            context, 1, cancelIntent,
            PendingIntent.FLAG_MUTABLE or PendingIntent.FLAG_UPDATE_CURRENT
        )
        val cancelAction = NotificationCompat.Action.Builder(
            android.R.drawable.ic_menu_close_clear_cancel, "취소", cancelPendingIntent
        ).build()

        val notification = NotificationCompat.Builder(context, CHANNEL_ID)
            .setSmallIcon(android.R.drawable.stat_sys_download)
            .setContentTitle(TITLE)
            .setContentText("페어링 서비스를 찾았습니다. 6자리 코드를 입력하세요.")
            .setPriority(NotificationCompat.PRIORITY_HIGH)
            .setOngoing(true)
            .setOnlyAlertOnce(false)
            .addAction(codeAction)
            .addAction(cancelAction)
            .build()

        androidx.core.app.NotificationManagerCompat.from(context).notify(NOTIFICATION_ID, notification)
    }

    /** 3) 진행 상태 갱신 (페어링 중.../연결 중... 등). */
    fun updateStatus(context: Context, message: String, ongoing: Boolean) {
        val notification = NotificationCompat.Builder(context, CHANNEL_ID)
            .setSmallIcon(android.R.drawable.stat_sys_download)
            .setContentTitle(TITLE)
            .setContentText(message)
            .setPriority(NotificationCompat.PRIORITY_HIGH)
            .setOngoing(ongoing)
            .build()
        androidx.core.app.NotificationManagerCompat.from(context).notify(NOTIFICATION_ID, notification)
    }

    fun dismiss(context: Context) {
        androidx.core.app.NotificationManagerCompat.from(context).cancel(NOTIFICATION_ID)
    }
}
