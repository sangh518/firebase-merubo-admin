// /functions/videoListApi.js

const { onRequest } = require("firebase-functions/v2/https");
const { logger } = require("firebase-functions");
const admin = require("firebase-admin");

// index.js에서 initializeApp()을 이미 호출했으므로 db만 가져옵니다.
const db = admin.firestore();

/**
 * [API] 동기화된 모든 스트리머와 비디오 목록을 집계하여 반환합니다.
 * 요청한 JSON 구조에 맞춰 데이터를 조립(aggregate)합니다.
 */
exports.getAggregatedList = onRequest(
  {
    region: "asia-northeast3",
    cors: true, // 웹사이트 등에서 호출할 수 있도록 CORS 허용
  },
  async (req, res) => {
    logger.info("데이터 집계 목록(getAggregatedList) 요청 수신");

    try {
      // 1. 최종 업데이트 시간 가져오기 (from wakchidong/vuster 문서)
      const metadataRef = db.doc("wakchidong/vuster");
      const metadataDoc = await metadataRef.get();

      const updatedAtTimestamp = metadataDoc.exists
        ? metadataDoc.data().updatedAt
        : null;

      const atlasUrl = metadataDoc.exists ? metadataDoc.data().atlasUrl : null;

      // 2. 모든 스트리머 목록 가져오기 (from wakchidong/vuster/data 컬렉션)
      const streamersRef = db.collection("wakchidong/vuster/data");
      const streamersSnapshot = await streamersRef.get();

      if (streamersSnapshot.empty) {
        logger.info("데이터가 없습니다. 빈 목록을 반환합니다.");
        res.status(200).json({
          updatedAt: updatedAtTimestamp
            ? updatedAtTimestamp.toDate().toISOString()
            : null,
          atlasUrl: atlasUrl,
          list: [],
        });
        return;
      }

      // 3. 각 스트리머의 'videos' 서브컬렉션을 병렬로 조회합니다. (N+1 쿼리)
      const streamerListPromises = streamersSnapshot.docs.map(
        async (streamerDoc) => {
          const streamerData = streamerDoc.data();

          // 3-1. 'videos' 서브컬렉션 조회
          const videosRef = streamerDoc.ref.collection("videos");
          const videosSnapshot = await videosRef
            .orderBy("publishedAt", "desc")
            .get();

          // 3-2. 'videos' 목록을 'shorts'와 'longs'로 분리
          const shortsList = [];
          const longsList = [];

          videosSnapshot.docs.forEach((videoDoc) => {
            const videoData = videoDoc.data();

            // isShorts가 없는 객체를 생성
            const videoObject = {
              title: videoData.title,
              views: videoData.views,
              // (참고) 필요하다면 videoData.publishedAt 등 다른 정보도 추가 가능
            };

            if (videoData.isShorts) {
              shortsList.push(videoObject);
            } else {
              videoObject.thumbnailIndex = videoData.thumbnailIndex ?? null;
              longsList.push(videoObject);
            }
          });

          // 3-3. 최종 스트리머 객체 포맷
          return {
            name: streamerData.name,
            soopId: streamerData.soopId,
            subscriberCount: streamerData.subscriberCount ?? 0,
            shorts: shortsList,
            longs: longsList,
          };
        }
      );

      // 4. 모든 병렬 조회 작업이 완료될 때까지 대기
      const aggregatedList = await Promise.all(streamerListPromises);

      // 5. 최종 JSON 응답 전송
      res.status(200).json({
        updatedAt: updatedAtTimestamp
          ? updatedAtTimestamp.toDate().toISOString()
          : null,
        atlasUrl: atlasUrl,
        list: aggregatedList,
      });
    } catch (error) {
      logger.error("데이터 집계 목록 조회 중 심각한 오류 발생:", error);
      res.status(500).json({
        status: "error",
        message: "Failed to retrieve aggregated list.",
        error: error.message,
      });
    }
  }
);
