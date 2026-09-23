package com.deabbo.mtgakrpatcher;

interface IUserService {

    // Shizuku가 프로세스를 정리할 때 내부적으로 호출하는 예약 메서드입니다.
    // 이 시그니처와 숫자(16777114)는 반드시 이 그대로 유지하세요. 직접 호출하지 않습니다.
    void destroy() = 16777114;

    /**
     * Raw_CardDatabase / Raw_ClientLocalization 파일의 실제 경로를 찾아 돌려줍니다.
     * 순서: [카드DB 경로 또는 "", 클라이언트DB 경로 또는 ""]
     */
    String[] locateDbPaths() = 1;

    /** remotePath 파일을 읽기 위한 파일 디스크립터. 없는 파일이면 예외가 던져집니다. */
    ParcelFileDescriptor openForRead(String remotePath) = 2;

    /** remotePath 파일에 쓰기 위한 파일 디스크립터(기존 내용은 덮어씁니다). */
    ParcelFileDescriptor openForWrite(String remotePath) = 3;

    /**
     * remotePath 파일을 삭제합니다. PC 버전과 동일하게 "패치 제거"는 백업을
     * 복원하는 게 아니라 파일을 지워서, 게임이 다음 실행 시 새로 받게 만듭니다.
     * 삭제에 성공하면 true, 파일이 이미 없었거나 실패하면 false.
     */
    boolean deleteIfExists(String remotePath) = 4;
}
