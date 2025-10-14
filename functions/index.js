// firebase deploy --only functions

// v2 API를 사용하기 위해 가져오는 방식이 변경되었습니다.
const { onSchedule } = require("firebase-functions/v2/scheduler");
const { logger } = require("firebase-functions"); // console.log 대신 logger를 권장합니다.
const admin = require("firebase-admin");
const axios = require("axios");
const { onRequest } = require("firebase-functions/v2/https");

// Firebase 앱 초기화
admin.initializeApp();
const db = admin.firestore();

// v2 방식으로 스케줄 함수를 정의합니다.
exports.updateStreamerStatus = onSchedule(
  {
    schedule: "every 1 minutes",
    region: "asia-northeast3", // 옵션을 객체 안에 넣습니다.
  },
  async (event) => {
    logger.info("스트리머 상태 확인 작업을 시작합니다.");

    const streamersRef = db.collection("wakchidong/data/streamers");
    const snapshot = await streamersRef.get();

    if (snapshot.empty) {
      logger.info("DB에 등록된 스트리머가 없습니다.");
      return null;
    }

    const updatePromises = snapshot.docs.map((doc) => {
      const streamerId = doc.id;
      const soopApiUrl = `https://api-channel.sooplive.co.kr/v1.1/channel/${streamerId}/home/section/broad`;

      return axios
        .get(soopApiUrl)
        .then((response) => {
          const streamerDocRef = doc.ref;
          const now = admin.firestore.FieldValue.serverTimestamp();

          if (response.data && response.data.currentSumViewer) {
            const currentSumViewer = response.data.currentSumViewer;
            logger.info(
              `${streamerId}: 방송 중. 시청자 수: ${currentSumViewer}`
            );
            return streamerDocRef.update({
              isLive: true,
              viewers: currentSumViewer,
              updatedAt: now,
            });
          } else {
            logger.info(`${streamerId}: 방송 중이 아님.`);
            return streamerDocRef.update({
              isLive: false,
              viewers: 0,
              updatedAt: now,
            });
          }
        })
        .catch((error) => {
          logger.error(
            `${streamerId}의 상태 확인 중 에러 발생:`,
            error.message
          );
        });
    });

    await Promise.all(updatePromises);
    logger.info("모든 스트리머 상태 확인 작업이 완료되었습니다.");
    return null;
  }
);

// 스트리머 전체 목록을 조회하는 API 함수
exports.getStreamerList = onRequest(
  {
    region: "asia-northeast3",
    // 웹사이트 등 다른 도메인에서 이 API를 호출하려면 CORS 허용이 필수입니다.
    cors: true,
  },
  async (req, res) => {
    logger.info("스트리머 목록 조회 요청 수신");

    try {
      const streamersRef = db.collection("wakchidong/data/streamers");
      const snapshot = await streamersRef.get();

      if (snapshot.empty) {
        logger.info("DB에 스트리머 데이터가 없습니다.");
        // 데이터가 없어도 성공 응답으로 빈 배열을 보냅니다.
        res.status(200).json({ data: [] });
        return;
      }

      // Firestore 문서들을 깔끔한 객체 배열로 변환합니다.
      const streamerList = snapshot.docs.map((doc) => {
        const data = doc.data();
        return {
          streamerId: data.streamerId,
          name: data.name,
          isLive: data.isLive,
          viewers: data.viewers,
          // JavaScript에서 사용할 수 있도록 Timestamp를 ISO 문자열로 변환
          updatedAt: data.updatedAt.toDate().toISOString(),
        };
      });

      // 성공적으로 데이터를 조회했으면 JSON 형태로 응답합니다.
      res.status(200).json({ data: streamerList });
    } catch (error) {
      logger.error("스트리머 목록 조회 중 에러 발생:", error);
      res.status(500).json({
        status: "error",
        message: "Failed to retrieve streamer list.",
      });
    }
  }
);
