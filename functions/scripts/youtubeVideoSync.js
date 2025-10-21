// /functions/youtubeVideoSync.js
const { onRequest } = require("firebase-functions/v2/https");
// const { onSchedule } = require("firebase-functions/v2/scheduler");
const { logger } = require("firebase-functions");
const { defineSecret } = require("firebase-functions/params");
const admin = require("firebase-admin");
const { google } = require("googleapis");
const { getStorage } = require("firebase-admin/storage"); // <-- [신규] Firebase Storage
const sharp = require("sharp"); // <-- [신규] 이미지 처리 라이브러리
const axios = require("axios"); // <-- [신규] 이미지 다운로드 라이브러리

// admin.initializeApp()는 index.js에서 이미 호출
const db = admin.firestore();
const storage = getStorage(); // <-- [신규] 스토리지 초기화
const YOUTUBE_API_KEY = defineSecret("YOUTUBE_API_KEY");

// --- [신규] 아틀라스 설정 ---
const ATLAS_SIZE = 2048; // 아틀라스 크기 (정사각형)
const THUMB_WIDTH = 128; // 개별 썸네일 너비
const THUMB_HEIGHT = 72; // 개별 썸네일 높이
const THUMBS_PER_ROW = Math.floor(ATLAS_SIZE / THUMB_WIDTH); // 한 줄에 16개
const MAX_THUMBS = THUMBS_PER_ROW * Math.floor(ATLAS_SIZE / THUMB_HEIGHT); // 16 * 28 = 총 448개

/**
 * [핵심 로직] 모든 스트리머의 동영상을 동기화하고 이미지 아틀라스를 생성합니다.
 */
async function runYouTubeSyncLogic() {
  const startTime = Date.now();
  logger.info("YouTube 동기화 [Core Logic] 시작.");
  let atlasUrl = null; // 아틀라스 URL 저장 변수

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
      return;
    }

    // 3. [1단계] 모든 스트리머의 비디오 정보 병렬 처리 (Firestore에 저장)
    const processPromises = snapshot.docs.map((streamerDoc) =>
      processStreamer(streamerDoc, youtube)
    );
    await Promise.all(processPromises);
    logger.info("1단계: 모든 스트리머의 비디오 정보 동기화 완료.");

    // 4. [2단계] 저장된 비디오 정보를 기반으로 이미지 아틀라스 생성 및 인덱스 업데이트
    // (snapshot.docs를 전달하여 어떤 스트리머를 처리했는지 알림)
    logger.info("2단계: 이미지 아틀라스 생성 및 인덱스 업데이트 시작...");
    atlasUrl = await generateAtlasAndUpdateFirestore(snapshot.docs);
    if (atlasUrl) {
      logger.info(`2단계: 아틀라스 생성 완료. URL: ${atlasUrl}`);
    } else {
      logger.info("2단계: 아틀라스를 생성할 비디오가 없습니다.");
    }

    // 5. [3단계] 모든 작업 완료 후 메타데이터 문서에 최종 업데이트 시간 및 atlasUrl 기록
    const metadataRef = db.doc("wakchidong/vuster");
    const metadataUpdate = {
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    };
    // atlasUrl이 성공적으로 생성된 경우에만 필드 추가
    if (atlasUrl) {
      metadataUpdate.atlasUrl = atlasUrl;
    }
    await metadataRef.set(metadataUpdate, { merge: true });

    logger.info("모든 동기화 작업 및 메타데이터 업데이트 완료.");
  } catch (error) {
    logger.error("runYouTubeSyncLogic 실행 중 오류:", error);
    throw error;
  } finally {
    const endTime = Date.now();
    const durationInMs = endTime - startTime;
    const durationInSeconds = (durationInMs / 1000).toFixed(2);
    logger.info(
      `YouTube 동기화 [Core Logic] 완료. 총 소요 시간: ${durationInSeconds}초 (${durationInMs}ms)`
    );
  }
}
// -----------------------------------------------------------------
// [트리거 1] 스케줄 함수 (주석 처리됨 - 필요시 해제)
// -----------------------------------------------------------------
// exports.syncYouTubeVideos = onSchedule( ... );

// -----------------------------------------------------------------
// [트리거 2] 수동 실행을 위한 HTTP 함수 (기존과 동일)
// -----------------------------------------------------------------
exports.triggerYouTubeSync = onRequest(
  {
    region: "asia-northeast3",
    secrets: [YOUTUBE_API_KEY],
    timeoutSeconds: 540,
    memory: "1GiB", // [수정 권장] 이미지 처리를 위해 메모리 증가 (예: 1GiB)
    cors: true,
  },
  async (req, res) => {
    logger.info("MANUAL YouTube 동기화 요청 수신.");

    if (req.query.secret !== "merubo999") {
      logger.warn("MANUAL 동기화 실패: 승인되지 않은 요청입니다.");
      return res
        .status(401)
        .json({ status: "error", message: "Unauthorized." });
    }

    try {
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
 * [헬퍼] 특정 스트리머의 동영상 정보를 처리하는 헬퍼 함수
 * (Firestore에 1차 저장)
 */
async function processStreamer(streamerDoc, youtube) {
  const streamerData = streamerDoc.data();
  const streamerName = streamerDoc.id;
  const channelId = streamerData.channelId;

  if (!channelId) {
    logger.warn(
      `'${streamerName}' 스트리머에 'channelId'가 없습니다. 건너뜁니다.`
    );
    return;
  }

  logger.info(`'${streamerName}' (Channel ID: ${channelId}) 처리 시작...`);

  try {
    // [신규] 1. 채널 구독자 수 가져오기 (비디오 처리와 병렬 실행)
    const subscriberPromise = fetchChannelSubscriberCount(channelId, youtube);

    // [신규] 2. 비디오 ID 및 상세 정보 가져오기 (기존 로직)
    const videoPromise = (async () => {
      const videoIds = await fetchRecentVideoIds(channelId, youtube);
      if (videoIds.length === 0) {
        logger.info(`'${streamerName}'의 최근 동영상이 없습니다.`);
        return []; // 비어있는 배열 반환
      }
      return await fetchVideoDetails(videoIds, youtube);
    })();

    // [신규] 1번과 2번 병렬 실행
    const [subscriberCount, videoDetails] = await Promise.all([
      subscriberPromise,
      videoPromise,
    ]);

    // [신규] 3. 구독자 수 Firestore에 업데이트
    // (null이 아닌 경우에만 업데이트. null은 API 오류 또는 채널 없음)
    if (subscriberCount !== null) {
      await streamerDoc.ref.update({
        subscriberCount: subscriberCount,
      });
      logger.info(`'${streamerName}' 구독자 수 업데이트: ${subscriberCount}`);
    } else {
      logger.warn(`'${streamerName}'의 구독자 수를 가져오지 못했습니다.`);
    }
    // 3. Firestore 서브컬렉션 참조
    const videosRef = streamerDoc.ref.collection("videos");

    // 4. 기존 비디오 목록 삭제
    await deleteSubcollection(videosRef);

    // 5. 새 비디오 목록 추가 (Batch 사용)
    await addNewVideos(videosRef, videoDetails);

    logger.info(
      `'${streamerName}' 처리 완료: ${videoDetails.length}개 동영상 1차 저장.`
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
 * [YouTube API] 1. 업로드 재생목록을 조회하여 최근 1달간 동영상 ID 목록을 가져옵니다.
 * (기존과 동일)
 */
async function fetchRecentVideoIds(channelId, youtube) {
  const oneMonthAgo = new Date();
  oneMonthAgo.setMonth(oneMonthAgo.getMonth() - 1);
  const uploadsPlaylistId = channelId.replace("UC", "UU");
  const videoIds = [];
  let nextPageToken = null;

  try {
    do {
      const response = await youtube.playlistItems.list({
        part: "snippet",
        playlistId: uploadsPlaylistId,
        maxResults: 50,
        pageToken: nextPageToken,
      });

      let keepPaginating = true;

      if (response.data.items) {
        for (const item of response.data.items) {
          const publishedAt = new Date(item.snippet.publishedAt);
          if (publishedAt >= oneMonthAgo) {
            videoIds.push(item.snippet.resourceId.videoId);
          } else {
            keepPaginating = false;
            break;
          }
        }
      } else {
        keepPaginating = false;
      }

      nextPageToken = response.data.nextPageToken;
      if (!keepPaginating || !nextPageToken) {
        nextPageToken = null;
      }
    } while (nextPageToken);

    return videoIds;
  } catch (error) {
    logger.error(
      `[YouTube API Error] playlistItems.list (${uploadsPlaylistId}):`,
      error.message
    );
    return [];
  }
}

/**
 * [YouTube API] 2. 동영상 ID 목록으로 상세 정보를 가져옵니다.
 * [수정됨] 썸네일 URL 추가, 쇼츠 기준 60초로 변경
 */
async function fetchVideoDetails(videoIds, youtube) {
  if (videoIds.length === 0) return [];

  const videoDetails = [];
  const chunks = [];
  for (let i = 0; i < videoIds.length; i += 50) {
    chunks.push(videoIds.slice(i, i + 50));
  }

  try {
    const chunkPromises = chunks.map(async (chunk) => {
      const response = await youtube.videos.list({
        part: "snippet,contentDetails,statistics",
        id: chunk.join(","),
      });

      if (response.data.items) {
        response.data.items.forEach((item) => {
          const durationSeconds = parseISODuration(
            item.contentDetails.duration
          );
          // [수정] 썸네일 URL 추가 (medium: 320x180, 없으면 default: 120x90)
          const thumbnailUrl =
            item.snippet.thumbnails?.medium?.url ||
            item.snippet.thumbnails?.default?.url ||
            null;

          videoDetails.push({
            videoId: item.id,
            title: preprocessTitle(item.snippet.title),
            rawTitle: item.snippet.title,
            views: parseInt(item.statistics.viewCount) || 0,
            // [수정] 쇼츠 기준 60초 이하
            isShorts: durationSeconds > 0 && durationSeconds <= 180,
            publishedAt: new Date(item.snippet.publishedAt),
            thumbnailUrl: thumbnailUrl, // [신규] 썸네일 URL
          });
        });
      }
    });

    await Promise.all(chunkPromises);
    return videoDetails;
  } catch (error) {
    logger.error(`[YouTube API Error] videos.list :`, error.message);
    return [];
  }
}

/**
 * [YouTube API] 3. 채널 ID로 구독자 수를 가져옵니다.
 * (신규 추가된 함수)
 * @returns {Promise<number|null>} 구독자 수 또는 실패 시 null
 */
async function fetchChannelSubscriberCount(channelId, youtube) {
  try {
    const response = await youtube.channels.list({
      part: "statistics",
      id: channelId,
    });

    if (response.data.items && response.data.items.length > 0) {
      const stats = response.data.items[0].statistics;

      // 구독자 수를 비공개한 채널 처리
      if (stats.hiddenSubscriberCount) {
        logger.info(`'${channelId}' 채널은 구독자 수를 비공개했습니다.`);
        return 0; // 비공개 시 0으로 처리 (혹은 -1 등 특별한 값)
      }

      return parseInt(stats.subscriberCount) || 0;
    } else {
      logger.warn(`[YouTube API] 채널을 찾을 수 없습니다: ${channelId}`);
      return null;
    }
  } catch (error) {
    logger.error(
      `[YouTube API Error] channels.list (${channelId}):`,
      error.message
    );
    return null; // API 오류 시 null 반환
  }
}

/**
 * Firestore 서브컬렉션을 삭제합니다. (500개씩 배치 삭제)
 * (기존과 동일)
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
 * Firestore에 새 동영상 목록을 추가합니다.
 * [수정됨] thumbnailUrl 필드 추가
 */
async function addNewVideos(collectionRef, videoDetails) {
  if (videoDetails.length === 0) return;

  let batch = db.batch();
  let count = 0;

  for (const video of videoDetails) {
    const docRef = collectionRef.doc(video.videoId);
    // [수정] 저장할 데이터 객체 생성
    const data = {
      title: video.title,
      views: video.views,
      isShorts: video.isShorts,
      publishedAt: video.publishedAt,
    };
    // [신규] 썸네일 URL이 있는 경우에만 추가 (아틀라스 생성을 위해 임시 저장)
    if (video.thumbnailUrl) {
      data.thumbnailUrl = video.thumbnailUrl;
    }

    batch.set(docRef, data);
    count++;

    if (count % 500 === 0) {
      await batch.commit();
      batch = db.batch();
    }
  }

  if (count % 500 !== 0) {
    await batch.commit();
  }
}

/**
 * ISO 8601 Duration을 초 단위로 변환합니다.
 * (기존과 동일)
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

// -----------------------------------------------------------------
// [신규] 아틀라스 생성 및 Firestore 업데이트 함수들
// -----------------------------------------------------------------

/**
 * [신규] 2단계: 아틀라스를 생성하고 Firestore 문서를 업데이트합니다.
 * @param {Array<admin.firestore.DocumentSnapshot>} streamerDocs - 처리할 스트리머 문서 목록
 * @returns {Promise<string|null>} 생성된 아틀라스의 공개 URL 또는 null
 */
async function generateAtlasAndUpdateFirestore(streamerDocs) {
  logger.info("아틀라스 생성을 위해 모든 비-쇼츠 동영상 수집 중...");

  // 1. 모든 스트리머의 'videos' 서브컬렉션에서 아틀라스에 포함할 비디오 쿼리
  const allNonShorts = [];
  const queryPromises = streamerDocs.map(async (streamerDoc) => {
    const videosRef = streamerDoc.ref.collection("videos");
    // isShorts가 false이고 thumbnailUrl이 존재하는 문서만 쿼리
    const videosSnapshot = await videosRef
      .where("isShorts", "==", false)
      .where("thumbnailUrl", "!=", null)
      .get();

    videosSnapshot.forEach((doc) => {
      allNonShorts.push({
        ref: doc.ref, // 문서 참조 (업데이트 시 필요)
        data: doc.data(), // 문서 데이터 (썸네일 URL)
      });
    });
  });

  await Promise.all(queryPromises);

  // 최신순으로 정렬 (모든 스트리머 통합)
  allNonShorts.sort(
    (a, b) => b.data.publishedAt.toDate() - a.data.publishedAt.toDate()
  );

  logger.info(
    `총 ${allNonShorts.length}개의 썸네일 수집 완료 (최대 ${MAX_THUMBS}개 처리).`
  );

  if (allNonShorts.length === 0) {
    return null; // 아틀라스 생성할 비디오 없음
  }

  // 2. 아틀라스 최대 크기에 맞게 비디오 목록 자르기
  const videosForAtlas = allNonShorts.slice(0, MAX_THUMBS);

  try {
    // 3. 이미지 아틀라스 생성 (Buffer)
    const atlasBuffer = await createAtlas(
      videosForAtlas,
      THUMB_WIDTH,
      THUMB_HEIGHT,
      ATLAS_SIZE,
      THUMBS_PER_ROW
    );

    // 4. Firebase Storage에 아틀라스 업로드
    const atlasUrl = await uploadAtlas(atlasBuffer);
    logger.info(`아틀라스 Storage 업로드 완료. URL: ${atlasUrl}`);

    // 5. Firestore 문서에 `thumbnailIndex` 업데이트 (Batch 사용)
    logger.info("Firestore에 thumbnailIndex 업데이트 시작...");
    let batch = db.batch();
    let writeCount = 0;

    for (let i = 0; i < videosForAtlas.length; i++) {
      const video = videosForAtlas[i];
      const index = i; // 아틀라스 내 인덱스

      // thumbnailIndex 추가, 임시 thumbnailUrl 필드 삭제
      batch.update(video.ref, {
        thumbnailIndex: index,
        thumbnailUrl: admin.firestore.FieldValue.delete(),
      });

      writeCount++;
      if (writeCount % 499 === 0) {
        // 500개 제한
        await batch.commit();
        batch = db.batch();
      }
    }

    // 남은 배치 커밋
    if (writeCount % 499 !== 0) {
      await batch.commit();
    }

    logger.info(
      `${writeCount}개 비디오의 thumbnailIndex 업데이트 및 임시 URL 삭제 완료.`
    );

    // 6. 생성된 URL 반환
    return atlasUrl;
  } catch (error) {
    logger.error("아틀라스 생성 또는 Firestore 업데이트 중 오류:", error);
    return null;
  }
}

/**
 * [신규] 썸네일 URL 목록을 받아 이미지 아틀라스 버퍼를 생성합니다.
 * @param {Array<object>} videosForAtlas - { ref, data: { thumbnailUrl, ... } } 객체 배열
 * @returns {Promise<Buffer>} PNG 이미지 버퍼
 */
async function createAtlas(
  videosForAtlas,
  thumbWidth,
  thumbHeight,
  atlasSize,
  thumbsPerRow
) {
  // 1. 모든 썸네일 이미지 병렬로 다운로드
  const imageBuffers = await Promise.all(
    videosForAtlas.map((v) => fetchImage(v.data.thumbnailUrl))
  );

  // 2. Sharp composite 연산에 사용할 입력 배열 생성
  const composites = [];
  for (let i = 0; i < imageBuffers.length; i++) {
    const buffer = imageBuffers[i];
    if (!buffer) continue; // 이미지 다운로드 실패 시 건너뛰기

    // 3. 아틀라스 내 위치 계산
    const x = (i % thumbsPerRow) * thumbWidth;
    const y = Math.floor(i / thumbsPerRow) * thumbHeight;

    try {
      // 4. 이미지 리사이즈 (128x72로 고정, 찌그러짐 방지 'cover')
      const resizedBuffer = await sharp(buffer)
        .resize(thumbWidth, thumbHeight, { fit: "cover" })
        .toBuffer();

      composites.push({
        input: resizedBuffer,
        top: y,
        left: x,
      });
    } catch (resizeError) {
      logger.warn(
        `썸네일 리사이즈 실패 (URL: ${videosForAtlas[i].data.thumbnailUrl}):`,
        resizeError.message
      );
    }
  }

  logger.info(
    `${composites.length}개의 썸네일을 아틀라스에 합성합니다. (전체 크기: ${atlasSize}x${atlasSize})`
  );

  // 5. 2048x2048 투명 배경 캔버스 생성 후 이미지 합성
  const atlasBuffer = await sharp({
    create: {
      width: atlasSize,
      height: atlasSize,
      channels: 3, // 3채널 (RGB)
      background: { r: 0, g: 0, b: 0 },
    },
  })
    .composite(composites) // 준비된 썸네일들 합성
    .jpeg({ quality: 80 }) // [수정] JPEG 포맷, 품질 80% (75~85 권장)
    .toBuffer();

  return atlasBuffer;
}

/**
 * [신규] 이미지 URL을 받아 ArrayBuffer로 다운로드합니다.
 */
async function fetchImage(url) {
  try {
    const response = await axios.get(url, {
      responseType: "arraybuffer", // 버퍼로 받기
    });
    return response.data;
  } catch (error) {
    logger.error(
      `이미지 다운로드 실패: ${url}`,
      error.response?.status || error.message
    );
    return null; // 실패 시 null 반환
  }
}

/**
 * [신규] 이미지 버퍼를 Firebase Storage에 업로드하고 공개 URL을 반환합니다.
 */
async function uploadAtlas(buffer) {
  const bucket = storage.bucket(); // 기본 버킷 사용
  const filePath = `vuster-atlas/thumbnails.jpg`; // 파일 경로
  const file = bucket.file(filePath);

  // 1. 파일 업로드
  await file.save(buffer, {
    metadata: {
      contentType: "image/jpeg",
    },
  });

  // 2. 파일 공개
  await file.makePublic();

  // 3. 공개 URL 반환
  return file.publicUrl();
}

/**
 * [HELPER] 비디오 제목을 정규화하고 이모지를 제거합니다.
 *
 * 1. (NFKD) 유니코드 정규화:
 * - 스타일리시한 문자(𝒪)를 일반 문(O)으로 변환합니다.
 * - 악센트 문자(É)를 (E + ´)로 분해합니다.
 * - 완성형 한글(강)을 자모(ㄱ + ㅏ + ㅇ)로 분해합니다.
 * 2. (replace) 결합 문자(악센트 등)를 제거합니다. (E + ´) -> E
 * 3. (NFC) 유니코드 정규화:
 * - 분해된 한글 자모(ㄱ + ㅏ + ㅇ)를 다시 완성형 한글(강)으로 조합합니다.
 * - (파자된 한글 'ㄱㅏㅇ'도 '강'으로 합쳐집니다.)
 * 4. (replace) 이모지를 제거합니다. (Unicode Property Escapes 사용)
 * 5. (replace) 연속된 공백을 하나로 합치고 앞뒤 공백을 제거합니다.
 *
 * @param {string} title - 원본 비디오 제목
 * @returns {string} - 전처리된 제목
 */
function preprocessTitle(title) {
  if (typeof title !== "string" || !title) {
    return title;
  }

  let processedTitle = title;

  // 1, 2, 3: 특수 알파벳 정규화, 악센트 제거, 파자된 한글 조합
  processedTitle = processedTitle
    .normalize("NFKD") // 1. 분해
    .replace(/[\u0300-\u036f]/g, "") // 2. 악센트 등 결합 문자 제거
    .normalize("NFC"); // 3. 한글 등 재조합

  // 4. 이모지 제거
  // \p{Emoji_Presentation}: 표준 이모지 (e.g., 😊)
  // \p{Extended_Pictographic}: 확장 그림 문자 (e.g., 🧑‍💻, 깃발 등)
  const emojiRegex = /[\p{Emoji_Presentation}\p{Extended_Pictographic}]/gu;
  processedTitle = processedTitle.replace(emojiRegex, "");

  // 5. 공백 정리 (이모지 제거 등으로 생긴 연속 공백 처리)
  processedTitle = processedTitle.replace(/\s+/g, " ").trim();

  return processedTitle;
}
