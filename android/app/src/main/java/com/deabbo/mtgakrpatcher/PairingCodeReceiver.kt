package com.deabbo.mtgakrpatcher

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import androidx.core.app.RemoteInput

/**
 * 알림의 "코드 입력" 액션에서 사용자가 입력한 코드를 받습니다.
 * 이 시점엔 NotificationPairFlow가 이미 페어링 서비스를 찾아둔 상태이므로,
 * 그 정보로 바로 페어링~연결을 이어갑니다.
 */
class PairingCodeReceiver : BroadcastReceiver() {

    override fun onReceive(context: Context, intent: Intent) {
        val input = RemoteInput.getResultsFromIntent(intent)
            ?.getCharSequence(PairingNotificationHelper.KEY_PAIRING_CODE)
            ?.toString()
            ?.trim()

        if (input.isNullOrEmpty()) {
            PairingNotificationHelper.updateStatus(context, "코드가 입력되지 않았습니다.", ongoing = false)
            return
        }

        NotificationPairFlow.onCodeSubmitted(context, input)
    }
}
