require("dotenv").config();

const http = require("http");
const express = require("express");
const twilio = require("twilio");
const { WebSocketServer } = require("ws");
const { DeepgramClient } = require("@deepgram/sdk");
const { OpenAI } = require("openai");
const { mulaw } = require("alawmulaw");
const { Resampler } = require("@eliware/resampler");
const { FishTTSClient } = require("./fish-tts");
const { TwilioAudioPump } = require("./audio-pump");

const DESTINATION_PHONE = "+918510994751";
// Public-facing host used for TwiML/webhook URLs. On Railway/Render the
// platform injects its own public URL, so prefer those over a hardcoded
// ngrok domain. Any scheme is stripped since the code below adds
// https:// and wss:// itself. Override with PUBLIC_DOMAIN if needed.
const PUBLIC_DOMAIN =
  process.env.PUBLIC_DOMAIN ||
  (process.env.RAILWAY_PUBLIC_DOMAIN || "").replace(/^https?:\/\//, "") ||
  (process.env.RAILWAY_STATIC_URL || "").replace(/^https?:\/\//, "") ||
  (process.env.RENDER_EXTERNAL_URL || "").replace(/^https?:\/\//, "") ||
  process.env.NGROK_DOMAIN ||
  "unrailroaded-grandfatherly-ian.ngrok-free.dev";
const PORT = process.env.PORT || 3000;

const DEEPGRAM_MODEL = process.env.DEEPGRAM_MODEL || "flux-general-multi";
const DEEPGRAM_ENCODING = "linear16";
const DEEPGRAM_SAMPLE_RATE = 16000;
const DEEPGRAM_LANGUAGE_HINTS = (process.env.DEEPGRAM_LANGUAGE_HINTS || "en,hi")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

const TWILIO_SAMPLE_RATE = 8000;
const TWILIO_CHANNELS = 1;

const FISH_API_KEY = process.env.FISH_API_KEY;
const FISH_MODEL = process.env.FISH_MODEL || "s2.1-pro-free";
const FISH_REFERENCE_ID = process.env.FISH_REFERENCE_ID || "";
const FISH_LATENCY = process.env.FISH_LATENCY || "low";
const FISH_CHUNK_LENGTH = Number(process.env.FISH_CHUNK_LENGTH) || 60;
const FISH_MIN_CHUNK_LENGTH = Number(process.env.FISH_MIN_CHUNK_LENGTH) || 20;
const FISH_TTS_MAX_BUFFER = Number(process.env.FISH_TTS_MAX_BUFFER) || 20;

// How long a pre-warmed, idle Fish session is allowed to sit before we
// proactively replace it. Fish (like most streaming TTS providers) can
// drop idle sockets server-side after some period of inactivity, and a
// dead warm session is worse than no warm session (it'd force a cold
// reconnect right when we thought we were fast). Keep this comfortably
// under whatever Fish's own idle timeout is.
const FISH_WARM_REFRESH_MS = Number(process.env.FISH_WARM_REFRESH_MS) || 10000;

// Deepgram Flux's StartOfTurn event is purpose-built and reliable for
// barge-in (Deepgram guarantees it never fires with an empty transcript),
// so by default we act on it with ZERO delay - the whole point is to stop
// the instant the caller starts talking, not after a guard window.
const FISH_BARGE_IN_GUARD_MS = Number(process.env.FISH_BARGE_IN_GUARD_MS) || 0;
const FISH_FINISH_TIMEOUT_MS = Number(process.env.FISH_FINISH_TIMEOUT_MS) || 30000;

const DG_OPEN = 1;

// Groq's own pitch is sub-200-300ms time-to-first-token on their classic
// Llama models. If your LLM TTFT logs are consistently well above that,
// the model itself (not your pipeline) is very likely the bottleneck -
// try swapping OPENAI_MODEL to something like "llama-3.1-8b-instant" and
// compare the [latency] LLM TTFT lines against 70b-versatile.
const OPENAI_MODEL = process.env.OPENAI_MODEL || "llama-3.1-8b-instant";

// Caps how much conversation history gets sent to the LLM on every turn
// (system message is always kept). Unbounded history means every turn on
// a long call sends more prompt tokens than the last, which grows prompt
// processing time and therefore TTFT as the call goes on. This trims to
// a recent rolling window instead.
const MAX_HISTORY_MESSAGES = Number(process.env.MAX_HISTORY_MESSAGES) || 6;

const SYSTEM_PROMPT = `You are Riya, a customer service voice agent for a Samsung authorized service center in India, talking to a customer on a phone call.

How you talk:
- Sound like a real human support agent, not a script. Warm, natural, a little conversational - use small acknowledgements like "okay", "got it", "sure" where natural.
- Keep every reply under 400 characters. Shorter is better - this is a live phone call, not a chat. Say only what's needed to move the conversation forward, then let the customer respond.
- Use short sentences. Long run-on sentences delay your own voice on this call, so break ideas into separate short sentences instead of one long one.
- Never use markdown, bullet points, asterisks, numbered lists, or emojis - this is spoken audio, not text on a screen.
- Ask one question at a time. Don't stack multiple questions in one reply.
- Speak plainly - no corporate jargon, no reading out long disclaimers unless the customer specifically asks for policy details.

What you help with:
- Samsung product issues (phones, TVs, appliances, tablets, wearables) - basic troubleshooting, understanding the problem, and when needed, guiding the customer to book a repair or visit the service center.
- Checking on repair/service status, warranty questions, and general service center info (hours, what to bring, estimated timelines) at a general level.
- If you don't have specific account or ticket details in front of you, say so plainly and offer to note it down or connect them further, rather than guessing or inventing order numbers, ticket IDs, or repair costs.
- Stay on topic - you're here for Samsung service center support, not general chit-chat, though a brief friendly moment is fine.

Ending the call:
- You have an end_call function. Call it once the conversation is genuinely finished - the customer says goodbye, confirms there's nothing else they need, or explicitly asks to hang up.
- Always say a short, warm goodbye in your spoken reply FIRST, then call end_call in that same turn. Never call end_call silently without saying goodbye.
- Don't call end_call while the customer still seems to be asking something or is mid-thought.`;

// Spoken the moment the call connects, before the caller says anything.
// Kept as fixed text (not an LLM call) so it starts playing as fast as
// possible and is 100% predictable for a first impression.
const GREETING_TEXT =
  "Hi, thanks for calling Samsung service support, this is Riya. How can I help you today?";
const SEPARATOR = "=".repeat(36);

// Standard OpenAI-compatible function calling (Chat Completions format).
// We rely ENTIRELY on the native structured `tool_calls` field in the
// stream - no text-markup scanning of the reply content. That means the
// model's spoken text (delta.content) is always safe to feed straight to
// TTS with zero parsing overhead, and end_call is only ever detected via
// delta.tool_calls / the final tool_calls accumulator.
const TOOLS = [
  {
    type: "function",
    function: {
      name: "end_call",
      description:
        "Ends the phone call. Call this once the conversation is naturally finished - e.g. the customer says goodbye, confirms they don't need anything else, or explicitly asks to hang up. Always say a brief, polite goodbye in your reply text in the SAME turn before calling this.",
      parameters: {
        type: "object",
        properties: {
          reason: {
            type: "string",
            description: "Short internal note on why the call is ending, e.g. 'customer said goodbye'.",
          },
        },
        required: [],
      },
    },
  },
];

const REQUIRED_ENV = [
  "TWILIO_ACCOUNT_SID",
  "TWILIO_AUTH_TOKEN",
  "TWILIO_PHONE_NUMBER",
  "DEEPGRAM_API_KEY",
  "GROQ_API_KEY",
  "FISH_API_KEY",
];

for (const key of REQUIRED_ENV) {
  if (!process.env[key]) {
    console.error(`Missing required environment variable: ${key}`);
    process.exit(1);
  }
}

const app = express();
const server = http.createServer(app);

app.use(express.json());

app.get("/health", (req, res) => {
  res.json({ status: "ok" });
});

const client = twilio(process.env.TWILIO_ACCOUNT_SID, process.env.TWILIO_AUTH_TOKEN);
const deepgram = new DeepgramClient({ apiKey: process.env.DEEPGRAM_API_KEY });
const groq = new OpenAI({
  apiKey: process.env.GROQ_API_KEY,
  baseURL: "https://api.groq.com/openai/v1",
});

const conversations = new Map();

function initConversation(callSid) {
  if (!conversations.has(callSid)) {
    conversations.set(callSid, [{ role: "system", content: SYSTEM_PROMPT }]);
  }
}

function deleteConversation(callSid) {
  if (callSid && conversations.delete(callSid)) {
    console.log(`[groq] conversation cleared for call ${callSid}`);
  }
}

// Keeps the system message plus only the most recent MAX_HISTORY_MESSAGES
// messages. Prevents prompt size - and therefore LLM TTFT - from growing
// unbounded over the course of a long call.
function trimHistory(history) {
  if (history.length <= MAX_HISTORY_MESSAGES + 1) {
    return history;
  }
  const system = history[0];
  const recent = history.slice(-MAX_HISTORY_MESSAGES);
  return [system, ...recent];
}

function findSentenceEnd(text) {
  for (let i = 0; i < text.length; i++) {
    const ch = text[i];
    if (
      ch === "." ||
      ch === "!" ||
      ch === "?" ||
      ch === ";" ||
      ch === ":" ||
      ch === "。" ||
      ch === "！" ||
      ch === "？"
    ) {
      return i;
    }
  }
  return -1;
}

// Common entry point for native tool_call detection. Kicks off the
// hangup-once-audio-finishes sequence for end_call.
function handleFunctionCall(ctx, call) {
  if (!ctx || !call || call.name !== "end_call") {
    return;
  }
  let reason = "";
  try {
    const parsed = call.args ? JSON.parse(call.args) : {};
    reason = parsed.reason || "";
  } catch (err) {
    // Arguments weren't valid JSON - not fatal, just skip the reason.
  }
  console.log(`[call ${ctx.callSid}] end_call tool invoked${reason ? `: ${reason}` : ""}`);
  ctx.pendingHangup = true;
  scheduleHangupIfNeeded(ctx);
}

// Feeds LLM text to the live Fish session, sentence-by-sentence, flushing
// after each send so Fish starts synthesizing immediately instead of
// buffering the whole reply. Logs the very first flush of a turn so you
// can see the gap between "LLM produced text" and "we handed text to TTS".
function feedTtsText(ctx, genId, tts, buffer, delta) {
  buffer += delta;
  let sentSomething = false;
  while (buffer.length > 0) {
    const idx = findSentenceEnd(buffer);
    if (idx === -1) {
      break;
    }
    const segment = buffer.slice(0, idx + 1).trim();
    buffer = buffer.slice(idx + 1);
    if (segment) {
      tts.sendText(segment);
      sentSomething = true;
    }
  }
  if (buffer.length >= FISH_TTS_MAX_BUFFER) {
    const segment = buffer.trim();
    buffer = "";
    if (segment) {
      tts.sendText(segment);
      sentSomething = true;
    }
  }
  if (sentSomething) {
    // CRITICAL for latency: Fish only starts synthesizing text once it's
    // flushed (or once its own internal chunk_length threshold is hit).
    tts.flush();
    if (ctx.firstFlushLoggedGen !== genId) {
      ctx.firstFlushLoggedGen = genId;
      const sinceTurnStart = Date.now() - (ctx.turnStartedAt || Date.now());
      console.log(
        `[latency] call ${ctx.callSid}: first text flushed to TTS at +${sinceTurnStart}ms from turn start`
      );
    }
  }
  return buffer;
}

function endTurn(ctx, genId) {
  if (ctx.currentGeneration === genId) {
    ctx.currentGeneration = 0;
    ctx.abortController = null;
    // The AI has gone quiet and is now waiting on the caller - this is
    // exactly the idle window we want to spend on the next Fish
    // handshake, so it's off the critical path when the caller responds.
    prewarmFish(ctx);
  }
}

function closeFishSession(ctx, session) {
  if (ctx.fishSession === session) {
    ctx.fishSession = null;
  }
  if (session) {
    try {
      session.close();
    } catch (err) {}
  }
}

// Returns true if there was actually something to interrupt.
function interruptAssistant(ctx, reason) {
  if (!ctx) {
    return false;
  }
  // Don't gate this on ctx.currentGeneration alone. Generation/TTS can
  // finish (currentGeneration already reset to 0) while the pump is still
  // physically playing out queued audio on the call for seconds afterward
  // - that's still audio the caller is hearing and needs to be able to
  // interrupt. So: anything to abort, a live TTS session, OR audio still
  // playing all separately justify a barge-in.
  const hasActiveGeneration = ctx.currentGeneration > 0;
  const hasPlayingAudio = !!(ctx.audioPump && ctx.audioPump.isPlaying());
  if (!hasActiveGeneration && !hasPlayingAudio) {
    return false;
  }
  console.log(`[call ${ctx.callSid}] barge-in: ${reason}`);
  if (ctx.abortController) {
    try {
      ctx.abortController.abort();
    } catch (err) {}
    ctx.abortController = null;
  }
  if (ctx.fishSession) {
    closeFishSession(ctx, ctx.fishSession);
  }
  // audioPump.stop() also sends Twilio a "clear" event so any audio Twilio
  // already buffered for playback gets discarded immediately, instead of
  // just quietly finishing whatever it was already holding.
  if (ctx.audioPump) {
    ctx.audioPump.stop();
  }
  ctx.currentGeneration = 0;
  // A caller talking over the AI is, by definition, not hanging up -
  // cancel any pending auto-hangup from an end_call tool call that may
  // have fired on a now-interrupted turn.
  ctx.pendingHangup = false;
  ctx.hangupScheduled = false;
  return true;
}

function teardownAssistant(ctx) {
  if (!ctx) {
    return;
  }
  ctx.stopped = true;
  if (ctx.warmRefreshTimer) {
    clearInterval(ctx.warmRefreshTimer);
    ctx.warmRefreshTimer = null;
  }
  if (ctx.abortController) {
    try {
      ctx.abortController.abort();
    } catch (err) {}
    ctx.abortController = null;
  }
  if (ctx.fishSession) {
    try {
      ctx.fishSession.close();
    } catch (err) {}
    ctx.fishSession = null;
  }
  if (ctx.warmFish) {
    const warm = ctx.warmFish;
    ctx.warmFish = null;
    warm.promise
      .then((tts) => {
        if (tts) {
          try {
            tts.close();
          } catch (err) {}
        }
      })
      .catch(() => {});
  }
  if (ctx.audioPump) {
    ctx.audioPump.stop();
  }
  ctx.currentGeneration = 0;
}

// Low-level: opens a Fish TTS websocket and issues "start" on it. Handlers
// are bound up front - callers that want no-op handlers (pre-warming) or
// real handlers (an active turn) both go through this same path so the
// connect/start behavior never drifts between the two. Logs how long the
// raw connect + start handshake itself took.
function openFishSession(handlers, label) {
  const startedAt = Date.now();
  return new Promise((resolve, reject) => {
    const tts = new FishTTSClient({
      apiKey: FISH_API_KEY,
      model: FISH_MODEL,
      referenceId: FISH_REFERENCE_ID,
      ...handlers,
    });

    tts
      .open()
      .then(() => {
        const request = {
          text: "",
          format: "pcm",
          sample_rate: TWILIO_SAMPLE_RATE,
          latency: FISH_LATENCY,
          chunk_length: FISH_CHUNK_LENGTH,
          min_chunk_length: FISH_MIN_CHUNK_LENGTH,
        };
        if (tts.referenceId) {
          request.reference_id = tts.referenceId;
        }
        tts.start(request);
        console.log(`[latency] fish ${label || "connect"}: handshake done in ${Date.now() - startedAt}ms`);
        resolve(tts);
      })
      .catch((err) => {
        try {
          tts.close();
        } catch (err2) {}
        reject(err);
      });
  });
}

// Rebinds a Fish client's callbacks to a specific turn. FishTTSClient
// stores these as plain instance properties, so a session opened earlier
// (e.g. while pre-warmed, with no-op handlers) can be handed to a real
// turn without reconnecting - we just swap in the real handlers here.
function bindFishHandlers(ctx, tts, genId) {
  tts.onAudio = (pcm) => {
    if (!ctx.audioPump || ctx.currentGeneration !== genId) {
      return;
    }
    if (ctx.turnStartedAt && ctx.firstAudioLoggedGen !== genId) {
      ctx.firstAudioLoggedGen = genId;
      const latencyMs = Date.now() - ctx.turnStartedAt;
      console.log(
        `[latency] call ${ctx.callSid}: ${(latencyMs / 1000).toFixed(2)}s from caller finishing speech to first AI audio`
      );
      if (ctx.firstFlushAtGen === genId && ctx.firstFlushAtMs) {
        console.log(
          `[latency] call ${ctx.callSid}: ${Date.now() - ctx.firstFlushAtMs}ms from first TTS flush to first audio back (Fish synth+network)`
        );
      }
    }
    try {
      ctx.audioPump.pushPcm16(pcm);
    } catch (err) {
      console.error(`[fish] failed to encode audio: ${err.message}`);
    }
  };
  tts.onError = (err) => {
    console.error(`[fish] tts error: ${err.message}`);
  };
  tts.onFinish = () => {
    closeFishSession(ctx, tts);
    endTurn(ctx, genId);
  };
  tts.onClose = () => {
    if (ctx.fishSession === tts) {
      ctx.fishSession = null;
    }
    endTurn(ctx, genId);
  };
}

// Cold path: opens a brand-new Fish session bound directly to this turn.
// Used as a fallback whenever no usable pre-warmed session is available
// (e.g. very first turn of the call, or the warm session failed).
function connectFishTTS(ctx, genId) {
  const handlers = {
    onAudio: () => {},
    onError: (err) => console.error(`[fish] tts error: ${err.message}`),
    onFinish: () => {},
    onClose: () => {},
  };
  return openFishSession(handlers, "cold-connect").then((tts) => {
    bindFishHandlers(ctx, tts, genId);
    return tts;
  });
}

// Opens (or refreshes) an idle, pre-warmed Fish session while the
// assistant has nothing to say. Uses no-op handlers since nothing should
// be produced from an idle session; a real turn later claims it and
// rebinds the handlers via bindFishHandlers. Keeping this off the
// critical path of an actual turn is the main latency win.
function prewarmFish(ctx) {
  if (ctx.stopped || ctx.warmFish) {
    return;
  }

  const createdAt = Date.now();
  const handlers = {
    onAudio: () => {},
    onError: (err) => console.error(`[fish] warm session error: ${err.message}`),
    onFinish: () => {},
    onClose: () => {
      if (ctx.warmFish && ctx.warmFish.createdAt === createdAt) {
        ctx.warmFish = null;
      }
    },
  };

  console.log(`[fish] call ${ctx.callSid}: pre-warming a new session`);
  const promise = openFishSession(handlers, "prewarm").catch((err) => {
    console.error(`[fish] prewarm failed: ${err.message}`);
    if (ctx.warmFish && ctx.warmFish.createdAt === createdAt) {
      ctx.warmFish = null;
    }
    return null;
  });

  ctx.warmFish = { promise, createdAt };

  ensureWarmRefreshTimer(ctx);
}

// While the AI is idle waiting on the caller (which can last a while if
// the caller is mid-sentence for a long time), periodically replace an
// aging warm session so it never goes stale right before it's needed.
function ensureWarmRefreshTimer(ctx) {
  if (ctx.warmRefreshTimer || ctx.stopped) {
    return;
  }
  ctx.warmRefreshTimer = setInterval(() => {
    if (ctx.stopped) {
      clearInterval(ctx.warmRefreshTimer);
      ctx.warmRefreshTimer = null;
      return;
    }
    // Only refresh if idle (no active generation) and the current warm
    // session has been sitting around long enough to risk going stale.
    if (ctx.currentGeneration === 0 && ctx.warmFish) {
      const age = Date.now() - ctx.warmFish.createdAt;
      if (age >= FISH_WARM_REFRESH_MS) {
        console.log(`[fish] call ${ctx.callSid}: warm session aged out (${age}ms), refreshing`);
        const stale = ctx.warmFish;
        ctx.warmFish = null;
        stale.promise
          .then((tts) => {
            if (tts) {
              try {
                tts.close();
              } catch (err) {}
            }
          })
          .catch(() => {});
        prewarmFish(ctx);
      }
    }
  }, 5000);
  ctx.warmRefreshTimer.unref();
}

// Hands a pre-warmed, already-open session to a real turn, rebinding its
// callbacks to that turn's genId. Falls back to the cold path if no warm
// session exists yet or it failed to come up in time.
//
// FIX: previously the next warm session was only started in endTurn(),
// which fires once the *current* turn's Fish session fully closes. If the
// caller replied quickly, claimWarmFish could be called again before that
// happened, finding warmFish === null and falling back to a cold connect
// (this is what produced the 1826ms "TTS ready" outlier in the logs).
// Now we re-arm the next warm session the INSTANT this one is claimed, so
// there's always a connection in flight rather than one that only starts
// after the previous turn's audio has entirely finished.
async function claimWarmFish(ctx, genId) {
  const warm = ctx.warmFish;
  ctx.warmFish = null;

  // Re-arm immediately - don't wait for this turn to end.
  prewarmFish(ctx);

  if (warm) {
    const tts = await warm.promise;
    if (tts && !tts.closed && ctx.currentGeneration === genId) {
      bindFishHandlers(ctx, tts, genId);
      return tts;
    }
    if (tts) {
      try {
        tts.close();
      } catch (err) {}
    }
  }

  console.log(`[fish] call ${ctx.callSid}: no usable warm session, falling back to cold connect`);
  return connectFishTTS(ctx, genId);
}

// Actually terminates the phone call: ends the Twilio call leg via the
// REST API, tears down our own assistant state, and closes the media
// websocket. Idempotent - safe to call more than once.
async function hangupCall(ctx) {
  if (!ctx || ctx.hungUp) {
    return;
  }
  ctx.hungUp = true;
  console.log(`[call ${ctx.callSid}] ending call via end_call tool`);
  try {
    await client.calls(ctx.callSid).update({ status: "completed" });
  } catch (err) {
    console.error(`[twilio] failed to hang up call ${ctx.callSid}: ${err.message}`);
  }
  teardownAssistant(ctx);
  if (ctx.ws && ctx.ws.readyState === ctx.ws.OPEN) {
    try {
      ctx.ws.close();
    } catch (err) {}
  }
}

// Called as soon as the LLM invokes end_call. Doesn't hang up immediately
// - the whole point of end_call is that the model just said a goodbye
// line that still needs to finish playing out over the call. Polls until
// audio actually stops playing (not just until Fish stops generating -
// playback trails generation, same distinction audioPump.isPlaying()
// exists for elsewhere in this file), then hangs up.
function scheduleHangupIfNeeded(ctx) {
  if (!ctx.pendingHangup || ctx.hangupScheduled || ctx.hungUp) {
    return;
  }
  ctx.hangupScheduled = true;
  const deadline = Date.now() + 15000;
  let sawAudio = false;

  const poll = () => {
    if (ctx.hungUp || !ctx.pendingHangup) {
      return;
    }
    const playing = !!(ctx.audioPump && ctx.audioPump.isPlaying());
    if (playing) {
      sawAudio = true;
    }
    const stillExpectingAudio = !sawAudio && ctx.currentGeneration !== 0;
    if ((playing || stillExpectingAudio) && Date.now() < deadline) {
      setTimeout(poll, 200);
      return;
    }
    hangupCall(ctx);
  };

  setTimeout(poll, 300);
}

async function streamAssistantResponse(ctx, userText, genId) {
  const callSid = ctx.callSid;
  const abort = new AbortController();
  ctx.abortController = abort;
  ctx.audioPump.start();
  ctx.firstFlushLoggedGen = 0;
  ctx.firstFlushAtGen = 0;
  ctx.firstFlushAtMs = 0;

  const llmStart = Date.now();
  let llmFirstTokenLogged = false;

  let tts = null;
  let ttsReady = false;
  let ttsFailed = false;

  try {
    let history = conversations.get(callSid) || [];
    history.push({ role: "user", content: userText });
    history = trimHistory(history);
    conversations.set(callSid, history);

    console.log("Assistant:");

    let fullResponse = "";
    let ttsBuffer = "";
    // Text received from the LLM that hasn't been handed to TTS yet because
    // the Fish socket isn't claimed yet. Plain spoken text only - no
    // markup parsing needed since end_call arrives via native tool_calls.
    let pending = "";
    // Accumulates a native (structured) tool_call across chat-completions
    // deltas.
    let toolCallAccum = null;

    // Claim a pre-warmed Fish session (or fall back to a cold connect)
    // and the LLM stream at the SAME TIME, instead of one after the
    // other. Any deltas that arrive before the TTS socket is ready get
    // buffered and flushed the moment it's claimed, so no audio is lost.
    const ttsConnectStart = Date.now();
    console.log(`[latency] call ${callSid}: starting TTS claim`);
    const ttsConnectPromise = claimWarmFish(ctx, genId)
      .then((session) => {
        if (abort.signal.aborted || ctx.currentGeneration !== genId) {
          closeFishSession(ctx, session);
          return null;
        }
        tts = session;
        ctx.fishSession = session;
        ttsReady = true;
        console.log(`[latency] call ${callSid}: TTS ready in ${Date.now() - ttsConnectStart}ms`);
        // Flush anything the LLM already produced while we were connecting.
        if (pending) {
          const text = pending;
          pending = "";
          ttsBuffer = feedTtsText(ctx, genId, tts, ttsBuffer, text);
        }
        if (ttsBuffer) {
          ttsBuffer = feedTtsText(ctx, genId, tts, "", ttsBuffer);
        }
        return session;
      })
      .catch((err) => {
        console.error(`[fish] tts session failed: ${err.message}`);
        console.error("[fish] continuing the conversation without audio");
        ttsFailed = true;
        return null;
      });

    const stream = await groq.chat.completions.create(
      {
        model: OPENAI_MODEL,
        stream: true,
        messages: history,
        tools: TOOLS,
        tool_choice: "auto",
      },
      { signal: abort.signal }
    );

    for await (const chunk of stream) {
      if (abort.signal.aborted) {
        break;
      }
      const choice = chunk.choices && chunk.choices[0];
      if (!choice) {
        continue;
      }
      const delta = choice.delta || {};

      // Native structured tool call deltas - this is the ONLY place
      // end_call is detected from. No text scanning required.
      if (delta.tool_calls && delta.tool_calls.length) {
        for (const tc of delta.tool_calls) {
          if (!toolCallAccum) {
            toolCallAccum = { index: tc.index != null ? tc.index : 0, name: "", args: "" };
          }
          if (tc.function) {
            if (tc.function.name) {
              toolCallAccum.name = tc.function.name;
            }
            if (tc.function.arguments) {
              toolCallAccum.args += tc.function.arguments;
            }
          }
        }
        continue;
      }

      if (!delta.content) {
        continue;
      }

      if (!llmFirstTokenLogged) {
        llmFirstTokenLogged = true;
        console.log(`[latency] call ${callSid}: LLM TTFT ${Date.now() - llmStart}ms`);
      }
      process.stdout.write(delta.content);
      fullResponse += delta.content;

      // Plain spoken text, straight to TTS - nothing to strip out.
      if (ttsReady && tts && ctx.currentGeneration === genId && ctx.fishSession === tts) {
        ttsBuffer = feedTtsText(ctx, genId, tts, ttsBuffer, delta.content);
        if (ctx.firstFlushLoggedGen === genId && !ctx.firstFlushAtMs) {
          ctx.firstFlushAtGen = genId;
          ctx.firstFlushAtMs = Date.now();
        }
      } else if (!ttsFailed) {
        // TTS socket isn't claimed yet - hold the text, it'll be flushed
        // as soon as ttsConnectPromise resolves above.
        pending += delta.content;
      }
    }

    // A native tool_call (if the model emitted one) triggers the hangup path.
    if (toolCallAccum && toolCallAccum.name === "end_call") {
      handleFunctionCall(ctx, { name: toolCallAccum.name, args: toolCallAccum.args });
    }

    // Make sure we know the final TTS connect outcome before deciding
    // what to do with any leftover buffered text.
    await ttsConnectPromise;

    if (abort.signal.aborted) {
      return;
    }

    if (fullResponse.trim()) {
      history.push({ role: "assistant", content: fullResponse.trim() });
      conversations.set(callSid, history);
    }

    console.log("\nStreaming complete.");
    console.log("");
    console.log(SEPARATOR);
    console.log("Response complete.");
    console.log("Waiting for caller...");
    console.log(SEPARATOR);

    if (tts && ctx.currentGeneration === genId && ctx.fishSession === tts) {
      const leftover = (ttsBuffer + pending).trim();
      if (leftover) {
        tts.sendText(leftover);
      }
      tts.flush();
      tts.stop();
      setTimeout(() => {
        if (ctx.fishSession === tts) {
          closeFishSession(ctx, tts);
          endTurn(ctx, genId);
        }
      }, FISH_FINISH_TIMEOUT_MS).unref();
    } else if (ctx.currentGeneration === genId) {
      // No usable TTS session for this turn (failed to connect) - nothing
      // left to wait on, so free up the turn immediately.
      endTurn(ctx, genId);
    }

    // In case audio was already finished (or never started, e.g. ttsFailed)
    // by the time end_call was seen, re-check now rather than only at
    // detection time.
    if (ctx.pendingHangup) {
      scheduleHangupIfNeeded(ctx);
    }
  } catch (err) {
    if (abort.signal.aborted) {
      console.log("[groq] response interrupted by barge-in");
    } else {
      console.error(`[groq] request failed: ${err.message}`);
      // Groq occasionally rejects its own streamed end_call (e.g. a
      // truncated tool name in the validation phase) AFTER the model has
      // already written its goodbye. That text is sitting in fullResponse/
      // ttsBuffer and would otherwise be lost, leaving the caller on a live
      // line forever. Since end_call is the only tool, a failure like this
      // after a goodbye was produced means the call is over: speak the
      // goodbye, then hang up once the audio finishes.
      const isToolFail = /failed to call a function/i.test(err.message || "");
      if (
        isToolFail &&
        fullResponse.trim() &&
        tts &&
        ctx.currentGeneration === genId &&
        ctx.fishSession === tts
      ) {
        const leftover = (ttsBuffer + fullResponse).trim();
        if (leftover) {
          tts.sendText(leftover);
          tts.flush();
        }
        tts.stop();
        setTimeout(() => {
          if (ctx.fishSession === tts) {
            closeFishSession(ctx, tts);
            endTurn(ctx, genId);
          }
        }, FISH_FINISH_TIMEOUT_MS).unref();
        console.log("[groq] end_call aborted by Groq mid-call - treating the goodbye as the end of the call");
        ctx.pendingHangup = true;
        scheduleHangupIfNeeded(ctx);
        return;
      }
      console.error("[groq] continuing to listen for the next utterance...");
      if (tts) {
        closeFishSession(ctx, tts);
        tts = null;
      }
    }
  } finally {
    if (ctx.currentGeneration === genId) {
      ctx.abortController = null;
      if (!tts) {
        endTurn(ctx, genId);
      }
    }
  }
}

// Plays the fixed greeting the instant the call connects, using the same
// TTS/audio-pump pipeline as a normal turn but skipping the LLM entirely
// (nothing to generate - the text is fixed) so it starts as fast as
// possible. The greeting is also recorded into conversation history so the
// model has correct context for anything the caller says next.
async function speakGreeting(ctx) {
  const genId = (ctx.generationSeq = (ctx.generationSeq || 0) + 1);
  ctx.currentGeneration = genId;
  ctx.turnStartedAt = Date.now();
  ctx.audioPump.start();

  const history = conversations.get(ctx.callSid) || [];
  history.push({ role: "assistant", content: GREETING_TEXT });
  conversations.set(ctx.callSid, history);

  console.log(`[call ${ctx.callSid}] greeting: ${GREETING_TEXT}`);

  let tts = null;
  try {
    // Nothing to pre-warm yet on the very first turn of the call, so this
    // always takes the cold path.
    tts = await connectFishTTS(ctx, genId);
    if (ctx.currentGeneration !== genId) {
      // Caller already barged in before the greeting even connected.
      closeFishSession(ctx, tts);
      return;
    }
    ctx.fishSession = tts;
    tts.sendText(GREETING_TEXT);
    tts.flush();
    tts.stop();
    setTimeout(() => {
      if (ctx.fishSession === tts) {
        closeFishSession(ctx, tts);
        endTurn(ctx, genId);
      }
    }, FISH_FINISH_TIMEOUT_MS).unref();
  } catch (err) {
    console.error(`[fish] greeting tts failed: ${err.message}`);
    if (tts) {
      closeFishSession(ctx, tts);
    }
    endTurn(ctx, genId);
  }
}

function buildTwiml() {
  const voiceResponse = new twilio.twiml.VoiceResponse();
  const connect = voiceResponse.connect();
  connect.stream({
    url: `wss://${PUBLIC_DOMAIN}/media`,
    track: "inbound_track",
  });
  return voiceResponse.toString();
}

app.all("/call", async (req, res) => {
  try {
    const call = await client.calls.create({
      to: DESTINATION_PHONE,
      from: process.env.TWILIO_PHONE_NUMBER,
      url: `https://${PUBLIC_DOMAIN}/twiml`,
      method: "GET",
    });
    console.log(`[twilio] outbound call created: ${call.sid} -> ${DESTINATION_PHONE}`);
    res.json({ success: true, callSid: call.sid });
  } catch (err) {
    console.error(`[twilio] failed to create call: ${err.message}`);
    res.status(500).json({ success: false, message: err.message });
  }
});

app.all("/twiml", (req, res) => {
  res.type("text/xml");
  res.send(buildTwiml());
});

const wss = new WebSocketServer({ server, path: "/media" });

wss.on("connection", (ws) => {
  let streamSid = null;
  let callSid = null;
  let ctx = null;
  let dg = null;
  let resampler = null;
  let dgPending = false;
  let droppedFrames = 0;

  const log = (...args) => console.log(`[${callSid || "call"}]`, ...args);

  function handleTurnInfo(info) {
    const transcript = (info.transcript || "").trim();
    if (!transcript) {
      return;
    }
    if (info.event === "EndOfTurn") {
      const detected = Array.isArray(info.languages) ? info.languages : [];
      if (detected.length > 0) {
        console.log(`[deepgram] languages detected: ${detected.join(", ")}`);
      }
      if (ctx) {
        initConversation(ctx.callSid);
        // No pre-check on currentGeneration here - interruptAssistant now
        // decides on its own whether there's anything to stop (active
        // generation OR audio still physically playing), so just call it
        // and let it no-op if there's truly nothing happening.
        interruptAssistant(ctx, "caller finished a new turn while AI was speaking");
        if (transcript === ctx.lastUserText) {
          console.log(`[groq] duplicate transcript ignored: "${transcript}"`);
          return;
        }
      }
      console.log(SEPARATOR);
      console.log("Caller:");
      console.log(transcript);
      console.log("");
      if (ctx) {
        const genId = (ctx.generationSeq = (ctx.generationSeq || 0) + 1);
        ctx.currentGeneration = genId;
        ctx.lastUserText = transcript;
        ctx.turnStartedAt = Date.now();
        streamAssistantResponse(ctx, transcript, genId).catch((err) => {
          console.error(`[groq] unexpected error: ${err.message}`);
        });
      }
    } else {
      // StartOfTurn AND Update both mean the caller is producing speech
      // right now. Flux doesn't always cleanly emit a StartOfTurn for
      // every interruption (e.g. speech that follows closely on the heels
      // of the AI's own reply) - relying on StartOfTurn alone meant some
      // barge-ins were missed entirely until the caller's turn fully
      // ended. Treat ANY transcript-bearing event as a barge-in signal
      // while the AI is talking, so we never wait past the very first
      // recognized word.
      if (ctx) {
        const elapsed = Date.now() - (ctx.turnStartedAt || 0);
        if (elapsed >= FISH_BARGE_IN_GUARD_MS) {
          interruptAssistant(ctx, `caller speaking mid-response (${info.event})`);
        }
      }
      const label = info.event === "StartOfTurn" ? "start of turn" : "update";
      console.log(`Caller (${label}): ${transcript}`);
    }
  }

  function setupDeepgram() {
    if (dgPending) {
      return;
    }
    dgPending = true;

    (async () => {
      let connection = null;
      try {
        connection = await deepgram.listen.v2.connect({
          model: DEEPGRAM_MODEL,
          encoding: DEEPGRAM_ENCODING,
          sample_rate: DEEPGRAM_SAMPLE_RATE,
          eot_threshold: "0.7",
          eot_timeout_ms: 5000,
          language_hint: DEEPGRAM_LANGUAGE_HINTS,
          Authorization: `Token ${process.env.DEEPGRAM_API_KEY}`,
        });

        connection.on("open", () => {
          log("[deepgram] websocket open");
        });

        connection.on("error", (err) => {
          console.error(`[deepgram] error: ${err.message}`);
        });

        connection.on("close", (event) => {
          log(
            `[deepgram] connection closed (code ${event && event.code}, reason "${(event && event.reason) || ""}")`
          );
          dg = null;
          if (resampler) {
            try {
              resampler.end();
            } catch (err) {}
            resampler = null;
          }
          if (ws.readyState === ws.OPEN) {
            setTimeout(() => {
              if (ws.readyState === ws.OPEN && !dg) {
                setupDeepgram();
              }
            }, 1000);
          }
        });

        connection.on("message", (message) => {
          if (message.type === "Connected") {
            log("[deepgram] connected, ready for audio");
          } else if (message.type === "TurnInfo") {
            handleTurnInfo(message);
          } else if (message.type === "Error") {
            console.error(
              `[deepgram] server error: ${message.code} - ${message.description}`
            );
          }
        });

        connection.connect();
        await connection.waitForOpen();

        if (ws.readyState !== ws.OPEN) {
          try {
            connection.close();
          } catch (err) {}
          return;
        }

        dg = connection;

        resampler = new Resampler({
          inRate: TWILIO_SAMPLE_RATE,
          outRate: DEEPGRAM_SAMPLE_RATE,
          inChannels: TWILIO_CHANNELS,
          outChannels: 1,
        });

        resampler.on("data", (chunk) => {
          if (dg && dg.readyState === DG_OPEN) {
            dg.sendMedia(chunk);
          }
        });

        resampler.on("error", (err) => {
          console.error(`[resampler] error: ${err.message}`);
        });

        log("[deepgram] streaming started");
      } catch (err) {
        console.error(`[deepgram] connection failed: ${err.message}`);
        if (connection) {
          try {
            connection.close();
          } catch (err2) {}
        }
      } finally {
        dgPending = false;
      }
    })();
  }

  function teardownDeepgram() {
    if (resampler) {
      try {
        resampler.end();
      } catch (err) {}
      resampler = null;
    }
    if (dg) {
      try {
        dg.sendCloseStream({ type: "CloseStream" });
      } catch (err) {}
      try {
        dg.close();
      } catch (err) {}
      dg = null;
    }
  }

  ws.on("error", (err) => {
    console.error(`[twilio] websocket error: ${err.message}`);
  });

  ws.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch (err) {
      return;
    }

    switch (msg.event) {
      case "connected":
        log(`[twilio] connected (protocol ${msg.protocol}, version ${msg.version})`);
        break;

      case "start":
        streamSid = msg.start.streamSid;
        callSid = msg.start.callSid;
        initConversation(callSid);
        ctx = {
          callSid,
          ws,
          audioPump: new TwilioAudioPump(ws),
          currentGeneration: 0,
          generationSeq: 0,
          abortController: null,
          fishSession: null,
          warmFish: null,
          warmRefreshTimer: null,
          stopped: false,
          lastUserText: null,
          turnStartedAt: 0,
          firstAudioLoggedGen: 0,
          firstFlushLoggedGen: 0,
          firstFlushAtGen: 0,
          firstFlushAtMs: 0,
          pendingHangup: false,
          hangupScheduled: false,
          hungUp: false,
        };
        ctx.audioPump.setStreamSid(streamSid);
        log(
          `[twilio] stream started ${streamSid} (${msg.start.mediaFormat.encoding}, ${msg.start.mediaFormat.sampleRate} Hz, ${msg.start.mediaFormat.channels} channel)`
        );
        setupDeepgram();
        // Warm a TTS session in parallel with the greeting so the caller's
        // FIRST real turn doesn't pay a cold Fish connect on the critical
        // path (the greeting itself cold-connects; this gives it ~5s of
        // lead time to be ready).
        prewarmFish(ctx);
        speakGreeting(ctx).catch((err) => {
          console.error(`[greeting] unexpected error: ${err.message}`);
        });
        break;

      case "media":
        if (msg.media && msg.media.track && msg.media.track !== "inbound") {
          return;
        }
        if (!dg || !resampler || dg.readyState !== DG_OPEN) {
          droppedFrames++;
          return;
        }
        try {
          const ulawBuffer = Buffer.from(msg.media.payload, "base64");
          const pcm = mulaw.decode(ulawBuffer);
          resampler.write(Buffer.from(pcm.buffer, pcm.byteOffset, pcm.byteLength));
        } catch (err) {
          console.error(`[audio] failed to process media chunk: ${err.message}`);
        }
        break;

      case "stop":
        log(`[twilio] stream stopped (call ${msg.stop && msg.stop.callSid})`);
        teardownAssistant(ctx);
        teardownDeepgram();
        deleteConversation(callSid);
        break;

      default:
        break;
    }
  });

  ws.on("close", () => {
    const droppedNote = droppedFrames > 0 ? `, ${droppedFrames} frames dropped before Deepgram was ready` : "";
    log(`[twilio] websocket closed${droppedNote}`);
    teardownAssistant(ctx);
    teardownDeepgram();
    deleteConversation(callSid);
  });
});

server.listen(PORT, () => {
  console.log(`Server listening on http://localhost:${PORT}`);
  console.log(`[deepgram] STT model: ${DEEPGRAM_MODEL}, language hints: ${DEEPGRAM_LANGUAGE_HINTS.join(", ")}`);
  console.log(`[groq] model: ${OPENAI_MODEL}`);
  console.log(`Trigger a call:       http://localhost:${PORT}/call`);
  console.log(`TwiML endpoint:       https://${PUBLIC_DOMAIN}/twiml`);
  console.log(`Media stream socket:  wss://${PUBLIC_DOMAIN}/media`);
});