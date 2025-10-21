// /functions/index.js

const admin = require("firebase-admin");

// Firebase 앱 초기화 (가장 먼저 한 번만 실행)
admin.initializeApp();

// --- YouTube 동기화 관련 ---
const { triggerYouTubeSync } = require("./scripts/youtubeVideoSync");
const { getAggregatedList } = require("./scripts/videosListApi");

// --- SOOP 스트리머 상태 관련 ---
// const {
//   // updateStreamerStatus,
//   getStreamerList,
//   getStreamerStatus,
// } = require("./scripts/soopStatusApi"); // [신규] 방금 만든 파일 임포트

// ===================================================================
// Cloud Functions에 함수 등록
// ===================================================================

// YouTube 동기화
exports.triggerYouTubeSync = triggerYouTubeSync;
exports.getAggregatedList = getAggregatedList;

// SOOP 스트리머 상태
// exports.updateStreamerStatus = updateStreamerStatus;
// exports.getStreamerList = getStreamerList;
// exports.getStreamerStatus = getStreamerStatus;
