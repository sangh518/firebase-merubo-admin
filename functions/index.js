// v2 API를 사용하기 위해 가져오는 방식이 변경되었습니다.
const { onSchedule } = require("firebase-functions/v2/scheduler");
const { logger } = require("firebase-functions"); // console.log 대신 logger를 권장합니다.
const admin = require("firebase-admin");
const axios = require("axios");

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

          if (response.data && response.data.live) {
            const liveData = response.data.live;
            logger.info(
              `${streamerId}: 방송 중. 시청자 수: ${liveData.concurrent_user_count}`
            );
            return streamerDocRef.update({
              isLive: true,
              viewers: liveData.concurrent_user_count,
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

// HTTP 요청으로 실행되는 함수를 가져옵니다.
const { onRequest } = require("firebase-functions/v2/https");

// 스트리머를 추가하는 API 함수
exports.addStreamer = onRequest(
  {
    region: "asia-northeast3",
    cors: true, // 만약 웹사이트에서 이 API를 호출하려면 이 줄의 주석을 푸세요.
  },
  async (req, res) => {
    // 1. 요청에서 streamerId와 name을 추출합니다.
    // 예시: /addStreamer?id=ecvhao&name=우왁굳
    const streamerId = req.query.id;
    const streamerName = req.query.name;

    // 2. id나 name이 없으면 에러를 반환합니다.
    if (!streamerId || !streamerName) {
      logger.error("스트리머 ID와 이름이 모두 필요합니다.");
      res.status(400).json({
        status: "error",
        message: "Query parameters 'id' and 'name' are required.",
      });
      return;
    }

    try {
      // 3. Firestore에 저장할 데이터를 준비합니다.
      const streamerRef = db
        .collection("wakchidong/data/streamers")
        .doc(streamerId);
      await streamerRef.set({
        streamerId: streamerId,
        name: streamerName,
        isLive: false, // 기본값
        viewers: 0, // 기본값
        updatedAt: admin.firestore.FieldValue.serverTimestamp(), // 현재 시간
      });

      logger.info(`새로운 스트리머 추가 성공: ${streamerId} (${streamerName})`);
      res.status(200).json({
        status: "success",
        message: `Streamer ${streamerId} added successfully.`,
      });
    } catch (error) {
      logger.error(`스트리머 추가 중 에러 발생:`, error);
      res.status(500).json({
        status: "error",
        message: "Failed to add streamer.",
      });
    }
  }
);
