"use strict";

const { mulaw } = require("alawmulaw");

const WS_OPEN = 1;
const TWILIO_SAMPLE_RATE = 8000;

class TwilioAudioPump {
  constructor(ws) {
    this.ws = ws;
    this.streamSid = null;
    this.queue = [];
    this.timer = null;
    this.wallStart = null;
    this.timelineEndMs = 0;
    this.stopped = false;
    this.playing = false;
  }

  setStreamSid(streamSid) {
    this.streamSid = streamSid;
  }

  start() {
    this.stopped = false;
    this.playing = false;
  }

  pushPcm16(pcmBuffer) {
    let buf = pcmBuffer;
    if (buf.byteLength % 2 !== 0) {
      buf = buf.subarray(0, buf.byteLength - 1);
    }
    const samples = new Int16Array(buf.buffer, buf.byteOffset, buf.byteLength / 2);
    const ulaw = mulaw.encode(samples);
    const payload = Buffer.from(ulaw.buffer, ulaw.byteOffset, ulaw.byteLength).toString("base64");
    this.pushBase64(payload);
  }

  pushBase64(payload) {
    if (this.stopped) {
      return;
    }
    const bytes = Buffer.from(payload, "base64").length;
    const durationMs = (bytes / TWILIO_SAMPLE_RATE) * 1000;
    const now = Date.now();
    const idle = this.wallStart === null || now >= this.wallStart + this.timelineEndMs;

    this.playing = true;

    if (idle) {
      this._send(payload);
      this.queue = [];
      this.wallStart = now;
      this.timelineEndMs = durationMs;
      this._startTimer();
      return;
    }

    const startMs = this.timelineEndMs;
    this.queue.push({ payload, startMs, endMs: startMs + durationMs });
    this.timelineEndMs = startMs + durationMs;
    this._startTimer();
  }

  /**
   * True while audio this pump sent is still being played out on the call,
   * even after the source (Fish TTS) has finished generating/sending all
   * of it. Generation finishing and playback finishing are NOT the same
   * moment - the pump paces audio out in real time, so playback can lag
   * generation by seconds. Barge-in decisions must key off THIS, not off
   * whether a TTS/LLM generation is still technically in flight.
   */
  isPlaying() {
    return this.playing && !this.stopped;
  }

  _startTimer() {
    if (this.timer) {
      return;
    }
    this.timer = setInterval(() => this._tick(), 20);
  }

  _tick() {
    if (this.stopped) {
      this._clearTimer();
      return;
    }
    if (!this.ws || this.ws.readyState !== WS_OPEN || !this.streamSid) {
      this.queue = [];
      this._clearTimer();
      return;
    }
    const now = Date.now();
    const playhead = now - this.wallStart;
    while (this.queue.length && this.queue[0].startMs <= playhead) {
      const chunk = this.queue.shift();
      this._send(chunk.payload);
    }
    if (!this.queue.length && now >= this.wallStart + this.timelineEndMs) {
      this.wallStart = null;
      this.timelineEndMs = 0;
      this._clearTimer();
      this.playing = false;
    }
  }

  _send(payload) {
    if (this.stopped || !this.streamSid || !this.ws || this.ws.readyState !== WS_OPEN) {
      return;
    }
    try {
      this.ws.send(
        JSON.stringify({
          event: "media",
          streamSid: this.streamSid,
          media: { payload, track: "outbound" },
        })
      );
    } catch (err) {
      console.error(`[twilio-audio] failed to send media: ${err.message}`);
    }
  }

  /**
   * Tells Twilio to immediately discard any audio it has already buffered
   * for playback on the call. Without this, stopping locally does nothing
   * about frames Twilio already received before the barge-in was detected,
   * so the caller keeps hearing the AI talk for a moment (or longer).
   * This MUST be sent on every interruption, not just on final stop.
   */
  clear() {
    if (!this.streamSid || !this.ws || this.ws.readyState !== WS_OPEN) {
      return;
    }
    try {
      this.ws.send(
        JSON.stringify({
          event: "clear",
          streamSid: this.streamSid,
        })
      );
    } catch (err) {
      console.error(`[twilio-audio] failed to send clear: ${err.message}`);
    }
  }

  /**
   * Stop pushing further audio AND flush whatever Twilio already has
   * buffered so playback halts immediately on the call.
   */
  stop() {
    this.stopped = true;
    this.queue = [];
    this.wallStart = null;
    this.timelineEndMs = 0;
    this.playing = false;
    this._clearTimer();
    this.clear();
  }

  _clearTimer() {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }
}

module.exports = { TwilioAudioPump };