package com.deabbo.mtgakrpatcher

import android.content.Context
import android.os.Build
import android.util.Base64
import io.github.muntashirakon.adb.AbsAdbConnectionManager
import android.sun.security.x509.CertAndKeyGen
import android.sun.security.x509.X500Name
import java.security.KeyFactory
import java.security.PrivateKey
import java.security.cert.Certificate
import java.security.cert.CertificateFactory
import java.security.cert.X509Certificate
import java.security.spec.PKCS8EncodedKeySpec
import java.util.Date

class AdbConnectionManager private constructor(context: Context) : AbsAdbConnectionManager() {
    private val prefs = context.getSharedPreferences("adb_keys", Context.MODE_PRIVATE)
    private var privateKey: PrivateKey? = null
    private var certificate: Certificate? = null

    init {
        setApi(Build.VERSION.SDK_INT)
        loadOrGenerateKeys()
    }

    private fun loadOrGenerateKeys() {
        val privStr = prefs.getString("private_key", null)
        val certStr = prefs.getString("certificate", null)

        if (privStr != null && certStr != null) {
            try {
                val privBytes = Base64.decode(privStr, Base64.DEFAULT)
                val keyFactory = KeyFactory.getInstance("RSA")
                privateKey = keyFactory.generatePrivate(PKCS8EncodedKeySpec(privBytes))

                val certBytes = Base64.decode(certStr, Base64.DEFAULT)
                val certFactory = CertificateFactory.getInstance("X.509")
                certificate = certFactory.generateCertificate(certBytes.inputStream())
                return
            } catch (e: Exception) {
                e.printStackTrace()
            }
        }

        generateNewKeys()
    }

    private fun generateNewKeys() {
        try {
            val keyGen = CertAndKeyGen("RSA", "SHA256WithRSA")
            keyGen.generate(2048)
            
            val dn = X500Name("CN=MTGAKRPatcher, O=Deabbo, C=KR")
            val validity = 10L * 365 * 24 * 60 * 60 // 10 years in seconds
            val cert = keyGen.getSelfCertificate(dn, Date(), validity)

            privateKey = keyGen.privateKey
            certificate = cert

            prefs.edit().apply {
                putString("private_key", Base64.encodeToString(privateKey?.encoded, Base64.DEFAULT))
                putString("certificate", Base64.encodeToString(certificate?.encoded, Base64.DEFAULT))
                apply()
            }
        } catch (e: Exception) {
            throw RuntimeException("Failed to generate ADB keys", e)
        }
    }

    override fun getPrivateKey(): PrivateKey = privateKey!!

    override fun getCertificate(): Certificate = certificate!!

    override fun getDeviceName(): String = "MTGAKRPatcher_${Build.MODEL}"

    companion object {
        @Volatile
        private var INSTANCE: AdbConnectionManager? = null

        fun getInstance(context: Context): AdbConnectionManager {
            return INSTANCE ?: synchronized(this) {
                INSTANCE ?: AdbConnectionManager(context.applicationContext).also { INSTANCE = it }
            }
        }
    }
}
