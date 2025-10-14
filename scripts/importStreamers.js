// 이 스크립트는 'my-soop-api/' 루트 폴더에서 실행해야 합니다.
// 실행 명령어: node scripts/importStreamers.js

const fs = require("fs");
const path = require("path");
const csv = require("csv-parser");
const admin = require("firebase-admin");

// 중요: 서비스 계정 키 파일 경로
// 1. Firebase 콘솔 > 프로젝트 설정 > 서비스 계정 탭으로 이동
// 2. '새 비공개 키 생성' 버튼을 눌러 json 파일을 다운로드
// 3. 다운로드한 파일을 프로젝트 루트 폴더에 놓고, 아래 파일 이름을 맞게 수정하세요.
const serviceAccount = require("../merubo-admin-firebase-adminsdk-fbsvc-8b2ebbd415.json"); // ←←←←← 파일 이름 수정!

// Firebase Admin SDK 초기화
admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});

const db = admin.firestore();
const streamers = [];
const csvFilePath = path.join(__dirname, "..", "streamers.csv");

fs.createReadStream(csvFilePath)
  .pipe(csv())
  .on("data", (row) => {
    // CSV 파일의 각 줄(row)을 streamers 배열에 추가
    if (row.id && row.name) {
      streamers.push({ id: row.id.trim(), name: row.name.trim() });
    }
  })
  .on("end", async () => {
    console.log("CSV 파일 읽기 완료. Firestore에 업로드를 시작합니다...");

    if (streamers.length === 0) {
      console.log("CSV에서 추가할 스트리머를 찾지 못했습니다.");
      return;
    }

    // 여러 문서를 한 번에 쓰기 위한 Batch 작업 생성
    const batch = db.batch();

    for (const streamer of streamers) {
      const docRef = db
        .collection("wakchidong/data/streamers")
        .doc(streamer.id);
      batch.set(docRef, {
        streamerId: streamer.id,
        name: streamer.name,
        isLive: false,
        viewers: 0,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      });
    }

    // Batch 작업 실행
    try {
      await batch.commit();
      console.log(
        `성공! 총 ${streamers.length}명의 스트리머를 Firestore에 추가/업데이트했습니다.`
      );
    } catch (error) {
      console.error("Firestore에 업로드 중 오류가 발생했습니다:", error);
    }
  });
