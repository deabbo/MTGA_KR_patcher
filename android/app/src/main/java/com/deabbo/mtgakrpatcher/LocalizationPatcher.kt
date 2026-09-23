package com.deabbo.mtgakrpatcher

import android.database.sqlite.SQLiteDatabase
import android.database.sqlite.SQLiteException
import org.json.JSONArray
import java.io.File
import java.text.Normalizer

/**
 * PC 버전(mtga_KR_patcher.py, 사용자가 직접 수정한 버전)의
 * run_localization_patch() 파이프라인을 이식:
 *   1) patchClientLocalization   <- UI 문자열
 *   2) patchCardText             <- 카드 오역 + 이스케이프 정리
 *   3) patchSneakKeyword         <- '기습' -> '잠행'  (순서 변경됨)
 *   4) patchVanishingKeyword     <- '소실' -> '사라짐' (순서 변경됨)
 *   5) patchNoTranslationNeeded  <- '#NoTranslationNeeded' 영문 복구 (순서 변경됨)
 *
 * [예외처리 보강]
 * 각 함수는 이제 withDatabase() 헬퍼로 감싸서, 게임 업데이트로 테이블/컬럼이
 * 바뀌어 SQLiteException이 나더라도 그 함수만 실패 처리되고 파이프라인의
 * 나머지 단계는 계속 진행됩니다. (이전에는 예외가 MainActivity까지 튀어
 * 올라가서 이후 단계가 전부 취소됐습니다.)
 */
object LocalizationPatcher {

    /**
     * DB 열기 -> 트랜잭션 시작 -> block 실행 -> 커밋/롤백 -> 닫기 까지
     * 공통으로 처리하는 헬퍼. block 안에서 발생한 예외는 여기서 잡아
     * 로그로만 남기고, 트랜잭션은 안전하게 롤백됩니다.
     *
     * @return 성공하면 true, 예외로 중단됐으면 false
     */
    private fun withDatabase(
        dbFile: File,
        log: (String) -> Unit,
        label: String,
        block: (SQLiteDatabase) -> Unit
    ): Boolean {
        var db: SQLiteDatabase? = null
        return try {
            db = SQLiteDatabase.openDatabase(dbFile.path, null, SQLiteDatabase.OPEN_READWRITE)
            db.beginTransaction()
            block(db)
            db.setTransactionSuccessful()
            true
        } catch (e: SQLiteException) {
            log("    - [$label] 데이터베이스 오류: ${e.message}")
            log("    - 게임이 업데이트되어 파일 구조가 바뀌었을 수 있습니다. 패치 도구 업데이트가 필요할 수 있습니다.")
            false
        } catch (e: Exception) {
            log("    - [$label] 예상치 못한 오류: ${e.message}")
            false
        } finally {
            try {
                if (db?.isOpen == true && db.inTransaction()) db.endTransaction()
            } catch (e: Exception) {
                log("    - [$label] 트랜잭션 정리 중 오류: ${e.message}")
            }
            try {
                if (db?.isOpen == true) db.close()
            } catch (e: Exception) {
                log("    - [$label] 파일 닫기 중 오류: ${e.message}")
            }
        }
    }

    // --- Raw_ClientLocalization_*.mtga : UI 문자열 패치 ---
    fun patchClientLocalization(dbFile: File, clientJson: String?, log: (String) -> Unit) {
        if (clientJson.isNullOrBlank()) {
            log("UI 번역 데이터가 없어 이 단계는 건너뜁니다.")
            return
        }
        val items = try {
            JSONArray(clientJson)
        } catch (e: Exception) {
            log("UI 번역 데이터 형식이 올바르지 않습니다: ${e.message}")
            return
        }

        var updated = 0
        val success = withDatabase(dbFile, log, "UI 번역") { db ->
            for (i in 0 until items.length()) {
                val item = items.getJSONObject(i)
                val key = item.optString("Key", null) ?: continue
                val koKR = item.optString("KoKR", null) ?: continue
                db.execSQL("UPDATE Loc SET koKR = ? WHERE key = ?", arrayOf(koKR, key))
                updated++
            }
        }
        if (success) log("UI 번역 업데이트 완료. (${updated}건)")
    }

    // --- Raw_CardDatabase_*.mtga : 카드 텍스트 오역 패치 ---
    fun patchCardText(dbFile: File, cardJson: String?, log: (String) -> Unit) {
        log("=== 카드 텍스트 변경 시작 ===")

        withDatabase(dbFile, log, "카드 텍스트") { db ->
            // 1. 이스케이프된 태그(&lt; &gt;) 정리 + Formatted 값별 정규화
            val toFix = mutableListOf<Triple<String, Long, Int>>()
            db.rawQuery(
                "SELECT LocId, Loc, Formatted FROM Localizations_koKR WHERE Loc LIKE '%&lt;%' OR Loc LIKE '%&gt;%'",
                null
            ).use { cursor ->
                while (cursor.moveToNext()) {
                    val locId = cursor.getLong(0)
                    val locText = cursor.getString(1)
                    val formatted = cursor.getInt(2)
                    var newText = locText.replace("&lt;", "<").replace("&gt;", ">")
                    newText = if (formatted == 2) formattingFor2(newText)
                              else Normalizer.normalize(newText, Normalizer.Form.NFC)
                    if (newText != locText) toFix.add(Triple(newText, locId, formatted))
                }
            }
            toFix.forEach { (text, locId, formatted) ->
                db.execSQL(
                    "UPDATE Localizations_koKR SET Loc = ? WHERE LocId = ? AND Formatted = ?",
                    arrayOf(text, locId, formatted)
                )
            }
            log("  - 이스케이프/정규화 정리: ${toFix.size}건")

            // 2. 서버에서 받아온 카드 오역 데이터 적용
            if (cardJson.isNullOrBlank()) {
                log("  - 카드 오역 데이터가 없어 이 단계는 건너뜁니다.")
            } else {
                val cards = try {
                    JSONArray(cardJson)
                } catch (e: Exception) {
                    log("  - 카드 오역 데이터 형식이 올바르지 않습니다: ${e.message}")
                    JSONArray()
                }
                var patched = 0
                for (i in 0 until cards.length()) {
                    val card = cards.getJSONObject(i)
                    val locId = card.optString("LocId", null) ?: continue
                    val v0 = card.optString("Formatted_0", null) ?: continue
                    val v1 = card.optString("Formatted_1", "").ifEmpty { v0 }
                    val v2 = formattingFor2(v0)

                    db.rawQuery(
                        "SELECT Formatted FROM Localizations_koKR WHERE LocId = ?",
                        arrayOf(locId)
                    ).use { cursor ->
                        while (cursor.moveToNext()) {
                            val formattedValue = cursor.getInt(0)
                            val newValue = when (formattedValue) {
                                0 -> v0
                                1 -> v1
                                2 -> v2
                                else -> null
                            }
                            if (!newValue.isNullOrEmpty()) {
                                db.execSQL(
                                    "UPDATE Localizations_koKR SET Loc = ? WHERE LocId = ? AND Formatted = ?",
                                    arrayOf(newValue, locId, formattedValue)
                                )
                                patched++
                            }
                        }
                    }
                }
                log("  - 카드 오역 데이터 적용: $patched 건")
            }
        }

        log("=== 카드 텍스트 변경 완료 ===")
    }

    // --- '기습' -> '잠행' 키워드 패치 ---
    // PC의 patch_sneak_keyword()에 대응. ReferencedAbilityIds는 "394,394" 같은
    // 콤마 구분 문자열일 수 있어 정확히 일치(=)가 아니라 LIKE '%394%'로 찾습니다.
    fun patchSneakKeyword(dbFile: File, log: (String) -> Unit) {
        log("  - '기습' -> '잠행' 키워드 패치 시작...")
        withDatabase(dbFile, log, "기습(Sneak)") { db ->
            patchKeywordReplacement(
                db = db,
                abilityQuery = "SELECT Id, TextId FROM Abilities WHERE Id = 394 OR BaseId = 394 OR ReferencedAbilityIds LIKE '%394%'",
                abilityArgs = emptyArray(),
                targetNfc = "기습",
                replaceNfc = "잠행",
                keywordLabel = "기습(Sneak)",
                log = log
            )
        }
    }

    // --- '소실' -> '사라짐' 키워드 패치 ---
    // PC의 patch_vanishing_keyword()에 대응
    fun patchVanishingKeyword(dbFile: File, log: (String) -> Unit) {
        log("  - '소실' -> '사라짐' 키워드 패치 시작...")
        withDatabase(dbFile, log, "소실") { db ->
            try {
                patchKeywordReplacement(
                    db = db,
                    abilityQuery = "SELECT Id, TextId FROM Abilities WHERE AbilityWord = 53",
                    abilityArgs = emptyArray(),
                    targetNfc = "소실",
                    replaceNfc = "사라짐",
                    keywordLabel = "소실",
                    log = log
                )
            } catch (e: SQLiteException) {
                // 'AbilityWord' 컬럼이 없는 예전/다른 스키마일 수 있음: 이 단계만 건너뜁니다.
                log("    - 'Abilities' 테이블에 'AbilityWord' 컬럼이 없거나 쿼리 오류: ${e.message}")
            }
        }
    }

    // --- '#NoTranslationNeeded' 영문 복구 ---
    // PC의 patch_no_translation_needed()에 대응
    fun patchNoTranslationNeeded(dbFile: File, log: (String) -> Unit) {
        log("  - '#NoTranslationNeeded' 영문 텍스트 복구 시작...")
        withDatabase(dbFile, log, "영문 복구") { db ->
            val count = db.rawQuery(
                "SELECT COUNT(*) FROM Localizations_koKR WHERE Loc = '#NoTranslationNeeded'", null
            ).use { it.moveToFirst(); it.getInt(0) }

            if (count == 0) {
                log("    - 변경할 '#NoTranslationNeeded' 텍스트가 없습니다.")
                return@withDatabase
            }
            log("    - 총 ${count}개의 누락된 텍스트를 영문으로 복구합니다.")

            db.execSQL(
                """
                UPDATE Localizations_koKR
                SET Loc = (
                    SELECT Loc FROM Localizations_enUS
                    WHERE Localizations_enUS.LocId = Localizations_koKR.LocId
                      AND Localizations_enUS.Formatted = Localizations_koKR.Formatted
                )
                WHERE Loc = '#NoTranslationNeeded'
                  AND EXISTS (
                      SELECT 1 FROM Localizations_enUS
                      WHERE Localizations_enUS.LocId = Localizations_koKR.LocId
                        AND Localizations_enUS.Formatted = Localizations_koKR.Formatted
                  )
                """.trimIndent()
            )
            val updatedCount = db.rawQuery("SELECT changes()", null)
                .use { it.moveToFirst(); it.getInt(0) }
            log("  - 총 ${updatedCount}개 항목 복구 완료.")
        }
    }

    /**
     * '기습'/'소실' 패치가 공유하는 공통 로직:
     *  1) 조건에 맞는 Abilities 조회
     *  2) 해당 TextId들의 현재 koKR 텍스트 확인 ('#NoTranslationNeeded'면 리다이렉트 후보로)
     *  3) 리다이렉트 후보는 Cards.AbilityIds의 "A:B" 패턴에서 실제 타겟 LocId(B) 탐색
     *  4) 최종 대상 LocId들에서 문자열 치환 (Formatted=2는 NFD 기준)
     */
    private fun patchKeywordReplacement(
        db: SQLiteDatabase,
        abilityQuery: String,
        abilityArgs: Array<String>,
        targetNfc: String,
        replaceNfc: String,
        keywordLabel: String,
        log: (String) -> Unit
    ) {
        val abilityRows = mutableListOf<Pair<Long, String?>>()
        db.rawQuery(abilityQuery, abilityArgs).use { c ->
            while (c.moveToNext()) {
                val id = c.getLong(0)
                val textId = if (c.isNull(1)) null else c.getString(1)
                abilityRows.add(id to textId)
            }
        }
        if (abilityRows.isEmpty()) {
            log("    - '$keywordLabel' 관련 능력을 찾지 못했습니다.")
            return
        }

        val textIdsToCheck = abilityRows.mapNotNull { it.second }.filter { it.isNotEmpty() }.toSet()
        val locLookup = mutableMapOf<String, String>()
        if (textIdsToCheck.isNotEmpty()) {
            val placeholders = textIdsToCheck.joinToString(",") { "?" }
            db.rawQuery(
                "SELECT LocId, Loc FROM Localizations_koKR WHERE LocId IN ($placeholders)",
                textIdsToCheck.toTypedArray()
            ).use { c ->
                while (c.moveToNext()) {
                    val locId = c.getString(0)
                    val locText = c.getString(1)
                    if (!locLookup.containsKey(locId) || locText == "#NoTranslationNeeded") {
                        locLookup[locId] = locText
                    }
                }
            }
        }

        val targetLocIds = mutableSetOf<String>()
        val redirectCheckAbilityIds = mutableSetOf<Long>()

        abilityRows.forEach { (abId, textId) ->
            if (textId.isNullOrEmpty()) return@forEach
            when (val value = locLookup[textId]) {
                "#NoTranslationNeeded" -> redirectCheckAbilityIds.add(abId)
                null -> { /* koKR DB에 없음: 건너뜀 */ }
                else -> targetLocIds.add(textId)
            }
        }

        if (redirectCheckAbilityIds.isNotEmpty()) {
            db.rawQuery(
                "SELECT AbilityIds FROM Cards WHERE AbilityIds IS NOT NULL AND AbilityIds LIKE '%:%'",
                null
            ).use { c ->
                while (c.moveToNext()) {
                    val abIdsStr = c.getString(0) ?: continue
                    abIdsStr.split(",").forEach { segmentRaw ->
                        val segment = segmentRaw.trim()
                        if (segment.contains(":")) {
                            val parts = segment.split(":", limit = 2)
                            if (parts.size == 2) {
                                val aId = parts[0].trim().toLongOrNull()
                                val bId = parts[1].trim()
                                if (aId != null && aId in redirectCheckAbilityIds) {
                                    targetLocIds.add(bId)
                                }
                            }
                        }
                    }
                }
            }
        }

        if (targetLocIds.isEmpty()) {
            log("    - 수정할 텍스트 ID를 찾지 못했습니다.")
            return
        }

        val targetNfd = Normalizer.normalize(targetNfc, Normalizer.Form.NFD)
        val replaceNfd = Normalizer.normalize(replaceNfc, Normalizer.Form.NFD)
        val updates = mutableListOf<Triple<String, String, Int>>()

        val placeholders = targetLocIds.joinToString(",") { "?" }
        db.rawQuery(
            "SELECT LocId, Loc, Formatted FROM Localizations_koKR WHERE LocId IN ($placeholders)",
            targetLocIds.toTypedArray()
        ).use { c ->
            while (c.moveToNext()) {
                val locId = c.getString(0)
                val locText = c.getString(1)
                val formatted = c.getInt(2)
                var newText = locText
                if (formatted == 2) {
                    if (locText.contains(targetNfd)) newText = locText.replace(targetNfd, replaceNfd)
                } else {
                    if (locText.contains(targetNfc)) newText = locText.replace(targetNfc, replaceNfc)
                }
                if (newText != locText) updates.add(Triple(newText, locId, formatted))
            }
        }

        if (updates.isNotEmpty()) {
            updates.forEach { (text, locId, formatted) ->
                db.execSQL(
                    "UPDATE Localizations_koKR SET Loc = ? WHERE LocId = ? AND Formatted = ?",
                    arrayOf(text, locId, formatted)
                )
            }
            log("  - 총 ${updates.size}개 항목에서 '$targetNfc'을(를) '$replaceNfc'(으)로 변경했습니다.")
        } else {
            log("  - 변경할 '$targetNfc' 텍스트가 없거나 이미 패치되었습니다.")
        }
    }

    // --- 카드 이름만 영어로 ---
    fun patchEnglishNamesOnly(dbFile: File, log: (String) -> Unit) {
        log("=== 카드 이름 영문화 패치 시작 ===")

        withDatabase(dbFile, log, "영문 이름") { db ->
            val titleIds = mutableSetOf<String>()
            db.rawQuery(
                """
                SELECT DISTINCT titleId FROM Cards
                WHERE titleId IS NOT NULL AND isToken = 0
                AND titleId NOT IN (SELECT TextId FROM Abilities WHERE Category = 3 AND TextId IS NOT NULL)
                """.trimIndent(),
                null
            ).use { cursor ->
                while (cursor.moveToNext()) titleIds.add(cursor.getString(0))
            }

            if (titleIds.isEmpty()) {
                log("  - 카드 테이블에서 타이틀 ID를 찾을 수 없습니다.")
                return@withDatabase
            }
            log("  - ${titleIds.size}개의 고유한 카드 타이틀 ID를 찾았습니다.")

            val placeholders = titleIds.joinToString(",") { "?" }
            val enNames = mutableListOf<Pair<String, String>>()
            db.rawQuery(
                "SELECT LocId, Loc FROM Localizations_enUS WHERE LocId IN ($placeholders)",
                titleIds.toTypedArray()
            ).use { cursor ->
                while (cursor.moveToNext()) {
                    enNames.add(cursor.getString(0) to cursor.getString(1))
                }
            }

            if (enNames.isEmpty()) {
                log("  - 업데이트할 카드 이름을 찾지 못했습니다.")
                return@withDatabase
            }
            log("  - ${enNames.size}개의 카드 이름을 영어로 덮어씁니다...")

            val aMinusSprite = "<sprite=\"SpriteSheet_MiscIcons\" name=\"arena_a\">"
            var updatedRows = 0
            var aMinusCount = 0

            enNames.forEach { (locId, name) ->
                db.execSQL("UPDATE Localizations_koKR SET Loc = ? WHERE LocId = ?", arrayOf(name, locId))
                updatedRows++
                if (name.startsWith("A-")) {
                    aMinusCount++
                    val newText = aMinusSprite + name.substring(2)
                    db.execSQL(
                        "UPDATE Localizations_koKR SET Loc = ? WHERE LocId = ? AND Formatted = 1",
                        arrayOf(newText, locId)
                    )
                }
            }

            log("  - 총 ${updatedRows}개 항목이 업데이트되었습니다.")
            if (aMinusCount > 0) log("  - 그 중 ${aMinusCount}개의 'A-' 카드를 특별 아이콘으로 처리했습니다.")
            log("=== 카드 이름 영문화 패치 완료 ===")
        }
    }

    private fun formattingFor2(text: String): String = Normalizer.normalize(text, Normalizer.Form.NFD)
}
