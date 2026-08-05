"use strict";

const WebSocket = require("ws");
const { encode, decode } = require("@msgpack/msgpack");

const FISH_TTS_WS_URL = "wss://api.fish.audio/v1/tts/live";

class FishTTSClient {
  constructor({ apiKey, model, referenceId, onAudio, onError, onClose, onFinish }) {
    this.apiKey = apiKey;
    this.model = model;
    this.referenceId = referenceId || null;
    this.onAudio = onAudio;
    this.onError = onError;
    this.onClose = onClose;
    this.onFinish = onFinish;
    this.ws = null;
    this.closed = false;
  }

  open() {
    return new Promise((resolve, reject) => {
      if (this.closed) {
        reject(new Error("Fish TTS client is already closed"));
        return;
      }

      const headers = {
        Authorization: `Bearer ${this.apiKey}`,
        model: this.model,
      };

      this.ws = new WebSocket(FISH_TTS_WS_URL, { headers });

      let settled = false;

      this.ws.on("open", () => {
        if (settled) {
          return;
        }
        settled = true;
        resolve();
      });

      this.ws.on("message", (data, isBinary) => {
        this.handleMessage(data, isBinary);
      });

      this.ws.on("error", (err) => {
        if (!settled) {
          settled = true;
          reject(err);
        } else {
          this.handleError(err);
        }
      });

      this.ws.on("close", () => {
        if (!settled) {
          settled = true;
          reject(new Error("Fish TTS connection closed before opening"));
          return;
        }
        if (this.onClose) {
          this.onClose();
        }
      });
    });
  }

  start(request) {
    this.send({ event: "start", request });
  }

  sendText(text) {
    if (text) {
      this.send({ event: "text", text });
    }
  }

  flush() {
    this.send({ event: "flush" });
  }

  stop() {
    this.send({ event: "stop" });
  }

  send(obj) {
    // Guard against sending on a socket we've already told to close -
    // otherwise a late-arriving delta right after a barge-in can throw
    // or, worse, race a brand new session on the same call.
    if (this.closed) {
      return;
    }
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(encode(obj));
    }
  }

  handleMessage(data, isBinary) {
    if (!isBinary) {
      return;
    }
    let msg;
    try {
      msg = decode(data);
    } catch (err) {
      this.handleError(err);
      return;
    }
    if (!msg || typeof msg !== "object") {
      return;
    }
    if (msg.event === "audio") {
      if (this.onAudio) {
        const audio = Buffer.isBuffer(msg.audio) ? msg.audio : Buffer.from(msg.audio);
        this.onAudio(audio);
      }
    } else if (msg.event === "finish") {
      if (msg.reason && msg.reason !== "stop" && this.onError) {
        this.handleError(new Error(`Fish TTS finished with reason: ${msg.reason}`));
      }
      if (this.onFinish) {
        this.onFinish(msg.reason);
      }
    }
  }

  handleError(err) {
    if (this.onError) {
      this.onError(err);
    } else {
      console.error(`[fish] websocket error: ${err.message}`);
    }
  }

  close() {
    this.closed = true;
    if (this.ws) {
      try {
        this.ws.close();
      } catch (err) {}
    }
  }
}

module.exports = { FishTTSClient, FISH_TTS_WS_URL };