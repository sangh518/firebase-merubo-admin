// /functions/youtubeVideoSync.js
const { onRequest } = require("firebase-functions/v2/https");
const { onSchedule } = require("firebase-functions/v2/scheduler");
const { logger } = require("firebase-functions");
const { defineSecret } = require("firebase-functions/params"); // v2 방식의 비밀 관리
const admin = require("firebase-admin");
const { google } = require("googleapis");
const axios = require("axios"); // [추가] 썸네일 다운로드에 필요
const sharp = require("sharp"); // [추가] 이미지 처리에 필요

// admin.initializeApp()는 index.js에서 이미 호출했으므로 여기서는 db만 가져옵니다.
const db = admin.firestore();
// Firebase Storage 버킷 가져오기
const bucket = admin.storage().bucket();
// YouTube API 키를 Firebase Secret Manager를 통해 안전하게 관리합니다.
// (설정 방법은 아래 '중요: 설정 단계'를 참고하세요)
const YOUTUBE_API_KEY = defineSecret("YOUTUBE_API_KEY");

async function runYouTubeSyncLogic() {
  const startTime = Date.now(); // 1. 시작 시간 기록

  logger.info("YouTube 동기화 [Core Logic] 시작.");
  try {
    // 1. YouTube API 클라이언트 초기화
    const apiKey = YOUTUBE_API_KEY.value();
    if (!apiKey) {
      logger.error("YOUTUBE_API_KEY가 Secret Manager에 설정되지 않았습니다.");
      throw new Error("YOUTUBE_API_KEY is not set.");
    }
    const youtube = google.youtube({
      version: "v3",
      auth: apiKey,
    });

    // 2. 동기화할 스트리머 목록 가져오기
    const streamersRef = db.collection("wakchidong/vuster/data");
    const snapshot = await streamersRef.get();

    if (snapshot.empty) {
      logger.warn("동기화할 스트리머가 'wakchidong/vuster/data'에 없습니다.");
      return; // 로직 종료
    }

    const processPromises = snapshot.docs.map((streamerDoc) =>
      processStreamer(streamerDoc, youtube)
    );

    // 3. 모든 스트리머의 작업이 끝날 때까지 대기
    await Promise.all(processPromises);

    // 4. 모든 작업 완료 후 메타데이터 문서에 최종 업데이트 시간 기록
    const metadataRef = db.doc("wakchidong/vuster");
    await metadataRef.set(
      {
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );
    logger.info("모든 스트리머 동기화 완료.");
  } catch (error) {
    // 5. 에러가 발생하면 로그를 남기고
    logger.error("runYouTubeSyncLogic 실행 중 오류:", error);
    // 상위 호출자(HTTP 트리거)가 에러를 인지할 수 있도록 다시 던집니다.
    throw error;
  } finally {
    // 6. (가장 중요) 성공/실패 여부와 관계없이 항상 실행됩니다.
    const endTime = Date.now();
    const durationInMs = endTime - startTime;
    const durationInSeconds = (durationInMs / 1000).toFixed(2); // 초 단위 (소수점 2자리)
    logger.info(
      `YouTube 동기화 [Core Logic] 완료. 총 소요 시간: ${durationInSeconds}초 (${durationInMs}ms)`
    );
  }
}
// -----------------------------------------------------------------
// [트리거 1] 기존의 스케줄 함수 (수정됨)
// -----------------------------------------------------------------
/**
 * [TRIGGER 1: Schedule] 주기적으로 동기화를 실행합니다.
 */
exports.syncYouTubeVideos = onSchedule(
  {
    schedule: "every 1 hours",
    region: "asia-northeast3",
    secrets: [YOUTUBE_API_KEY],
    timeoutSeconds: 540,
    memory: "512MiB",
  },
  async (event) => {
    logger.info("SCHEDULED YouTube 동기화 작업을 시작합니다.");
    try {
      // 핵심 로직 호출
      await runYouTubeSyncLogic();
      logger.info("SCHEDULED YouTube 동기화 작업이 성공적으로 완료되었습니다.");
    } catch (error) {
      logger.error("SCHEDULED YouTube 동기화 작업 중 오류 발생:", error);
    }
    return null; // onSchedule 함수는 null 또는 Promise 반환
  }
);

// -----------------------------------------------------------------
// [트리거 2] 수동 실행을 위한 HTTP 함수 (신규)
// -----------------------------------------------------------------
/**
 * [TRIGGER 2: HTTP] 수동으로 YouTube 동기화를 즉시 실행하는 API
 * (보안을 위해 인증을 추가하는 것이 좋습니다)
 */
exports.triggerYouTubeSync = onRequest(
  {
    region: "asia-northeast3",
    secrets: [YOUTUBE_API_KEY], // 핵심 로직이 API 키를 사용하므로 동일하게 필요
    timeoutSeconds: 540, // 스케줄 작업과 동일하게 넉넉한 타임아웃
    memory: "512MiB",
    cors: true, // 웹사이트에서 호출할 수 있도록 CORS 허용
  },
  async (req, res) => {
    logger.info("MANUAL YouTube 동기화 요청 수신.");

    // [보안 권장] 간단한 비밀키(secret)를 쿼리 파라미터로 받아 인증합니다.
    // 예: /triggerYouTubeSync?secret=MY_VERY_SECRET_KEY
    if (req.query.secret !== "merubo999") {
      // <-- 이 부분 수정!
      logger.warn("MANUAL 동기화 실패: 승인되지 않은 요청입니다.");
      return res
        .status(401)
        .json({ status: "error", message: "Unauthorized." });
    }

    try {
      // 핵심 로직 호출 (await를 사용해야 함수가 완료될 때까지 기다립니다)
      await runYouTubeSyncLogic();

      logger.info("MANUAL YouTube 동기화 작업이 성공적으로 완료되었습니다.");
      res.status(200).json({
        status: "success",
        message: "YouTube video sync completed.",
      });
    } catch (error) {
      logger.error("MANUAL YouTube 동기화 작업 중 오류 발생:", error);
      res.status(500).json({
        status: "error",
        message: "Failed to sync YouTube videos.",
        error: error.message,
      });
    }
  }
);

/**
 * 특정 스트리머의 동영상 정보를 처리하는 헬퍼 함수
 * @param {admin.firestore.DocumentSnapshot} streamerDoc - 스트리머 문서
 * @param {object} youtube - YouTube API 클라이언트
 */
async function processStreamer(streamerDoc, youtube) {
  const streamerData = streamerDoc.data();
  const streamerName = streamerDoc.id; // 문서 ID를 이름으로 사용
  const channelId = streamerData.channelId; // 문서에 'channelId' 필드가 있어야 함

  if (!channelId) {
    logger.warn(
      `'${streamerName}' 스트리머에 'channelId'가 없습니다. 건너뜁니다.`
    );
    return;
  }

  logger.info(`'${streamerName}' (Channel ID: ${channelId}) 처리 시작...`);

  try {
    // 1. 최근 1달간의 모든 비디오 ID 목록 가져오기
    const videoIds = await fetchRecentVideoIds(channelId, youtube);
    if (videoIds.length === 0) {
      logger.info(`'${streamerName}'의 최근 동영상이 없습니다.`);
      // 영상이 없어도 기존 목록은 삭제해야 하므로 계속 진행
    }

    // 2. 비디오 ID 목록으로 상세 정보(조회수, 길이 등) 가져오기
    const videoDetails = await fetchVideoDetails(videoIds, youtube);

    // 3. Firestore 서브컬렉션 참조
    const videosRef = streamerDoc.ref.collection("videos");

    // 4. 기존 비디오 목록 삭제
    await deleteSubcollection(videosRef);

    // 5. 새 비디오 목록 추가 (Batch 사용)
    await addNewVideos(videosRef, videoDetails);

    logger.info(
      `'${streamerName}' 처리 완료: ${videoDetails.length}개 동영상 추가.`
    );
  } catch (error) {
    logger.error(
      `'${streamerName}' 처리 중 오류 발생:`,
      error.message,
      error.stack
    );
  }
}

/**
 * [YouTube API] 1. 최근 1달간 업로드된 동영상의 ID 목록을 가져옵니다. (search.list)
 */
async function fetchRecentVideoIds(channelId, youtube) {
  const oneMonthAgo = new Date();
  oneMonthAgo.setMonth(oneMonthAgo.getMonth() - 1);

  const videoIds = [];
  let nextPageToken = null;

  try {
    do {
      const response = await youtube.search.list({
        part: "snippet",
        channelId: channelId,
        order: "date",
        type: "video",
        publishedAfter: oneMonthAgo.toISOString(), // 1달 전
        maxResults: 50,
        pageToken: nextPageToken,
      });

      if (response.data.items) {
        response.data.items.forEach((item) => {
          videoIds.push(item.id.videoId);
        });
      }
      nextPageToken = response.data.nextPageToken;
    } while (nextPageToken);

    return videoIds;
  } catch (error) {
    logger.error(
      `[YouTube API Error] search.list (${channelId}):`,
      error.message
    );
    // 쿼터 초과 등의 에러가 발생해도 빈 배열을 반환하여 다음 로직 진행
    return [];
  }
}

/**
 * [YouTube API] 2. 동영상 ID 목록으로 상세 정보를 가져옵니다. (videos.list)
 */
async function fetchVideoDetails(videoIds, youtube) {
  if (videoIds.length === 0) return [];

  const videoDetails = [];
  // YouTube API는 ID를 50개씩만 조회 가능하므로, 50개 단위로 나눕니다.
  const chunks = [];
  for (let i = 0; i < videoIds.length; i += 50) {
    chunks.push(videoIds.slice(i, i + 50));
  }

  try {
    // 50개 묶음을 병렬로 요청
    const chunkPromises = chunks.map(async (chunk) => {
      const response = await youtube.videos.list({
        part: "snippet,contentDetails,statistics",
        id: chunk.join(","), // 콤마(,)로 구분된 ID 문자열
      });

      if (response.data.items) {
        response.data.items.forEach((item) => {
          const durationSeconds = parseISODuration(
            item.contentDetails.duration
          );
          videoDetails.push({
            videoId: item.id, // 문서 ID로 사용하기 위해 ID도 포함
            title: item.snippet.title,
            views: parseInt(item.statistics.viewCount) || 0,
            isShorts: durationSeconds > 0 && durationSeconds <= 180, // 1분 이하
            publishedAt: new Date(item.snippet.publishedAt), // 업로드 시간
          });
        });
      }
    });

    await Promise.all(chunkPromises);
    return videoDetails;
  } catch (error) {
    logger.error(`[YouTube API Error] videos.list :`, error.message);
    return []; // 오류 시 빈 배열 반환
  }
}

/**
 * Firestore 서브컬렉션을 삭제합니다. (500개씩 배치 삭제)
 */
async function deleteSubcollection(collectionRef) {
  let snapshot;
  do {
    snapshot = await collectionRef.limit(500).get();
    if (snapshot.empty) break;

    const batch = db.batch();
    snapshot.docs.forEach((doc) => {
      batch.delete(doc.ref);
    });
    await batch.commit();
  } while (!snapshot.empty);
}

/**
 * Firestore에 새 동영상 목록을 추가합니다. (500개씩 배치 추가)
 */
async function addNewVideos(collectionRef, videoDetails) {
  if (videoDetails.length === 0) return;

  let batch = db.batch();
  let count = 0;

  for (const video of videoDetails) {
    // videoId를 문서 ID로 사용하여 중복 방지
    const docRef = collectionRef.doc(video.videoId);
    batch.set(docRef, {
      title: video.title,
      views: video.views,
      isShorts: video.isShorts,
      publishedAt: video.publishedAt,
    });
    count++;

    // 500개 단위로 배치 커밋
    if (count % 500 === 0) {
      await batch.commit();
      batch = db.batch(); // 새 배치 시작
    }
  }

  // 남은 항목 커밋
  if (count % 500 !== 0) {
    await batch.commit();
  }
}

/**
 * YouTube API의 ISO 8601 Duration(예: "PT1M30S")을 초 단위로 변환합니다.
 */
function parseISODuration(duration) {
  const regex = /PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?/;
  const matches = duration.match(regex);
  if (!matches) return 0;

  const hours = parseInt(matches[1] || 0);
  const minutes = parseInt(matches[2] || 0);
  const seconds = parseInt(matches[3] || 0);

  return hours * 3600 + minutes * 60 + seconds;
}
