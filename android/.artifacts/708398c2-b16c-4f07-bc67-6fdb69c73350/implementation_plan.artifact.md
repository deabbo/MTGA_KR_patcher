# Fix Unresolved Reference 'AdbConnectionManager'

The build error `Unresolved reference 'AdbConnectionManager'` occurs because the `libadb-android` library provides an abstract base class `AbsAdbConnectionManager` but expects the developer to provide a concrete implementation for managing RSA keys and certificates.

## Proposed Changes

### [MTGA KR Patcher App]

#### [NEW] [AdbConnectionManager.kt](file:///C:/Users/lhs00/Desktop/Cording/MTGLocalization/MTGA_KR_Patcher/android/app/src/main/java/com/deabbo/mtgakrpatcher/AdbConnectionManager.kt)
Create a concrete implementation of `AbsAdbConnectionManager` that:
- Generates a 2048-bit RSA key pair and a self-signed X.509 certificate using the `sun-security-android` library.
- Persists these credentials in `SharedPreferences` so they survive app restarts (preventing frequent re-pairing).
- Implements the Singleton pattern with a `getInstance(context)` method.

#### [MODIFY] [AdbSessionManager.kt](file:///C:/Users/lhs00/Desktop/Cording/MTGLocalization/MTGA_KR_Patcher/android/app/src/main/java/com/deabbo/mtgakrpatcher/AdbSessionManager.kt)
- Update the import to point to the local `AdbConnectionManager` instead of the non-existent library package.

## Verification Plan

### Automated Tests
- Run `./gradlew :app:compileDebugKotlin` to verify the unresolved reference is fixed.

### Manual Verification
- Deploy the app to a device.
- Navigate to the Pairing screen and attempt to pair with a Wireless Debugging session.
- Verify that the keys are generated and stored correctly.
