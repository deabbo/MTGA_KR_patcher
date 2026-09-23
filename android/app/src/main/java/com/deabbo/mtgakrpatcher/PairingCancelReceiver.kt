package com.deabbo.mtgakrpatcher

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent

/** 알림의 "검색 중지"/"취소" 버튼을 받아 검색을 멈추고 알림을 닫습니다. */
class PairingCancelReceiver : BroadcastReceiver() {
    override fun onReceive(context: Context, intent: Intent) {
        NotificationPairFlow.cancelSearch(context)
    }
}
