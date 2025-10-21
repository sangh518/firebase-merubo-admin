// /functions/soopStatusApi.js

// v2 API를 사용하기 위해 가져오는 방식
const { onSchedule } = require("firebase-functions/v2/scheduler");
const { onRequest } = require("firebase-functions/v2/https");
const { logger } = require("firebase-functions");
const admin = require("firebase-admin");
const axios = require("axios");

// index.js에서 이미 initializeApp()을 호출했으므로 db만 가져옵니다.
const db = admin.firestore();

/**
 * [SCHEDULE] 1분마다 스트리머의 SOOP 방송 상태를 DB에 업데이트합니다.
 */
const updateStreamerStatus = onSchedule(
  {
    schedule: "every 1 minutes",
    region: "asia-northeast3",
  },
  async (event) => {
    logger.info("스트리머 상태 확인 작업을 시작합니다.");

    // [수정됨] 경로 확인: wakchidong/vuster/data
    // (이전 코드에서는 wakchidong/data/streamers 였는데,
    //  vuster 프로젝트와 맞추려면 이 경로가 맞는지 확인 필요)
    const streamersRef = db.collection("wakchidong/data/streamers");
    const snapshot = await streamersRef.get();

    if (snapshot.empty) {
      logger.info("DB에 등록된 스트리머가 없습니다.");
      return null;
    }

    const updatePromises = snapshot.docs.map((doc) => {
      const streamerId = doc.id; // 문서 ID가 SOOP ID인 것으로 보임
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

/**
 * [API] DB에 저장된 스트리머 전체 목록을 조회하는 API
 */
const getStreamerList = onRequest(
  {
    region: "asia-northeast3",
    cors: true,
  },
  async (req, res) => {
    logger.info("스트리머 목록 조회 요청 수신");

    try {
      const streamersRef = db.collection("wakchidong/data/streamers");
      const snapshot = await streamersRef.get();

      if (snapshot.empty) {
        logger.info("DB에 스트리머 데이터가 없습니다.");
        res.status(200).json({ data: [] });
        return;
      }

      const streamerList = snapshot.docs.map((doc) => {
        const data = doc.data();
        return {
          streamerId: data.streamerId,
          name: data.name,
          isLive: data.isLive,
          viewers: data.viewers,
          updatedAt: data.updatedAt.toDate().toISOString(),
        };
      });

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

/**
 * [API] 특정 스트리머의 현재 SOOP 상태를 '즉시' 조회하여 반환하는 API
 */
const getStreamerStatus = onRequest(
  {
    region: "asia-northeast3",
    cors: true,
  },
  async (req, res) => {
    const { streamerId } = req.query;
    if (!streamerId) {
      return res
        .status(400)
        .json({ error: "Query parameter 'streamerId' is required." });
    }

    logger.info(`실시간 상태 조회 요청: ${streamerId}`);
    const soopApiUrl = `https://api-channel.sooplive.co.kr/v1.1/channel/${streamerId}/home/section/broad`;

    try {
      const response = await axios.get(soopApiUrl);
      let isLive = false;
      let viewers = 0;

      if (response.data && response.data.currentSumViewer) {
        isLive = true;
        viewers = response.data.currentSumViewer;
      }

      res.status(200).json({
        streamerId: streamerId,
        isLive: isLive,
        viewers: viewers,
        retrievedAt: new Date().toISOString(),
      });
    } catch (error) {
      logger.error(
        `실시간 상태 조회 중 에러 발생 (${streamerId}):`,
        error.message
      );
      res.status(500).json({
        status: "error",
        message: `Failed to retrieve status for ${streamerId}.`,
      });
    }
  }
);

// 정의된 함수들을 모듈로 내보냅니다.
module.exports = {
  updateStreamerStatus,
  getStreamerList,
  getStreamerStatus,
};
