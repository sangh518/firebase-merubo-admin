// 이 스크립트는 'my-soop-api/' 루트 폴더에서 실행해야 합니다.
// 실행 명령어: node scripts/importRawIds.js

const fs = require("fs");
const path = require("path");
const csv = require("csv-parser");
const admin = require("firebase-admin");

// 중요: 서비스 계정 키 파일 경로
// 1. Firebase 콘솔 > 프로젝트 설정 > 서비스 계정 탭으로 이동
// 2. '새 비공개 키 생성' 버튼을 눌러 json 파일을 다운로드
// 3. 다운로드한 파일을 프로젝트 루트 폴더에 놓고, 아래 파일 이름을 맞게 수정하세요.
const serviceAccount = require("../merubo-admin-firebase-adminsdk-fbsvc-8b2ebbd415.json");

// Firebase Admin SDK 초기화
admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});

const db = admin.firestore();
const idDataList = [];
const csvFilePath = path.join(__dirname, "..", "raw-ids.csv");

fs.createReadStream(csvFilePath)
  .pipe(csv())
  .on("data", (row) => {
    // CSV 파일의 각 줄(row)을 streamers 배열에 추가
    if (row.soopId && row.name && row.channelId) {
      idDataList.push({
        soopId: row.soopId.trim(),
        name: row.name.trim(),
        channelId: row.channelId.trim(),
      });
    } else {
      console.warn("CSV 데이터 누락:", row);
    }
  })
  .on("end", async () => {
    console.log("CSV 파일 읽기 완료. Firestore에 업로드를 시작합니다...");

    if (idDataList.length === 0) {
      console.warn("CSV에서 추가할 스트리머를 찾지 못했습니다.");
      return;
    }

    // 1. CSV에 있는 스트리머 이름 목록을 Set으로 만듭니다. (빠른 비교를 위해)
    //    Firestore의 문서 ID가 'name'을 사용하므로 'name'을 기준으로 합니다.
    const csvStreamerNames = new Set(idDataList.map((data) => data.name));

    // 2. Firestore 컬렉션 참조
    const collectionRef = db.collection("wakchidong/vuster/data");

    // 3. Firestore의 모든 문서 가져오기
    let firestoreDocs;
    try {
      firestoreDocs = await collectionRef.get();
    } catch (error) {
      console.error("Firestore에서 기존 문서를 가져오는 중 오류:", error);
      return;
    }

    // 4. Batch 작업 생성
    const batch = db.batch();
    let deleteCount = 0;
    let upsertCount = 0;

    // 5. [삭제 로직] Firestore 문서를 순회하며 CSV에 없는 문서를 찾습니다.
    firestoreDocs.docs.forEach((doc) => {
      const streamerName = doc.id; // 문서 ID가 곧 스트리머 이름
      if (!csvStreamerNames.has(streamerName)) {
        // Firestore에는 있지만 CSV에는 없는 이름 -> 삭제 대상
        batch.delete(doc.ref);
        deleteCount++;
      }
    });

    // 6. [추가/업데이트 로직] CSV 데이터를 순회하며 문서를 추가(set)합니다.
    for (const idData of idDataList) {
      // 문서 ID를 'name'으로 설정
      const docRef = collectionRef.doc(idData.name);
      batch.set(docRef, {
        soopId: idData.soopId,
        name: idData.name,
        channelId: idData.channelId,
      });
      upsertCount++;
    }

    // 7. Batch 작업 실행
    try {
      await batch.commit();
      console.log("Firestore 동기화 성공!");
      console.log(`- 추가/업데이트: ${upsertCount}명`);
      console.log(`- 삭제: ${deleteCount}명`);
    } catch (error) {
      console.error("Firestore에 동기화 중 오류가 발생했습니다:", error);
    }
  });
