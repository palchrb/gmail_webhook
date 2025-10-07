/*************************************************************
 *  InReach → Matrix via Apps Script + Pub/Sub
 *  - Admin API (subscribe/unsubscribe/list/check) via Script Properties
 *  - Pub/Sub push: processes INBOX unread with a label-based state machine
 *  - Posts to room webhook with Bearer token; payload includes plain+html body
 *************************************************************/

/** ========= CONFIG ========= */
const CONFIG = {
  ENDPOINT_TOKEN: 'some_random_secret_here',  // ?token=... for Pub/Sub push (optional)
  ADMIN_TOKEN:   'some_random_secret_here',      // ?admin_token=... for admin API

  // Server-side Gmail query: exclude state labels so we don't re-process.
  INBOX_QUERY: 'in:inbox is:unread -label:MatrixProcessing -label:MatrixDone -label:MatrixSkipped',

  BODY_MAX_PLAIN: 5000,
  BODY_MAX_HTML:  15000,

  // Alias must be subscribed before delivery
  REQUIRE_SUBSCRIPTION: true,

  // Optional: require SPF/DKIM/DMARC
  REQUIRE_AUTH_RESULTS: false,

  // Per-alias rate limit (events/min)
  RATE_PER_MIN: 20,

  // (Optional) Gmail Watch settings if you want to manage watch from script:
  WATCH_TOPIC:  'projects/webhook-473808/topics/webhookz',
  WATCH_LABELS: ['INBOX']
};

const SP = PropertiesService.getScriptProperties();
const KEY_SUBSCRIPTIONS = 'subscriptions'; // JSON: alias -> { webhook, bearer_token }

/** State labels (auto-created if missing) */
const LBL_PROCESSING = 'MatrixProcessing';
const LBL_DONE       = 'MatrixDone';
const LBL_SKIPPED    = 'MatrixSkipped'; // not used in the main flow, kept for compatibility

/** ========= ENTRYPOINT ========= */
/**
 * doPost handles:
 * - Admin API (returns ContentService JSON)
 * - Pub/Sub push (returns "OK"/"BAD_REQUEST"/"ERROR" in body; Apps Script can't set status)
 */
function doPost(e) {
  try {
    Logger.log('[doPost] called %s', new Date().toISOString());

    // --- Admin API (returns ContentService JSON; does NOT affect Pub/Sub ack) ---
    if (e && e.parameter && e.parameter.admin_token) {
      Logger.log('[admin] action=%s bodyLen=%s',
        String(e.parameter.action || '').toLowerCase(),
        e && e.postData && e.postData.contents ? e.postData.contents.length : 0);
      return handleAdmin(e);
    }

    // --- Pub/Sub push: return numeric HTTP codes for proper acking ---
    if (!e || !e.postData || !e.postData.contents) {
      Logger.log('[pubsub] missing body');
      // 400 Bad Request if postData is missing
      return 400;
    }

    var postBody;
    try {
      postBody = JSON.parse(e.postData.contents);
      Logger.log('[pubsub] request body parsed');
    } catch (jsonErr) {
      Logger.log('[pubsub] invalid json: %s', jsonErr && jsonErr.message);
      // 400 Bad Request if JSON is invalid
      return 400;
    }

    if (!postBody.message || !postBody.message.data) {
      Logger.log('[pubsub] missing message.data');
      // 400 Bad Request if Pub/Sub envelope is malformed
      return 400;
    }

    // Decode base64 data (optional; useful for debugging/historyId inspection)
    try {
      var decodedData = Utilities
        .newBlob(Utilities.base64Decode(postBody.message.data))
        .getDataAsString();
      Logger.log('[pubsub] decoded %s bytes', decodedData.length);
    } catch (decErr) {
      Logger.log('[pubsub] base64 decode failed: %s', decErr && decErr.message);
      // 400 if the message data can't be decoded
      return 400;
    }

    // Kick off the label-based processor (hybrid: Advanced Gmail state + GmailApp bodies)
    Logger.log('[pubsub] starting processUnreadInboxOnce()');
    processUnreadInboxOnce();
    Logger.log('[pubsub] processUnreadInboxOnce() completed');

    // 200 OK to ack the Pub/Sub message
    return 200;

  } catch (error) {
    Logger.log('[doPost] ERROR: %s\nStack: %s', error && error.message, error && error.stack);
    // 500 Internal Server Error to signal Pub/Sub for retry
    return 500;
  }
}


/** Also allow admin via GET and a basic ping */
function doGet(e) {
  if (e && e.parameter && e.parameter.admin_token) {
    return handleAdmin(e);
  }
  return ContentService.createTextOutput(
    JSON.stringify({ ok: true, ts: new Date().toISOString() })
  ).setMimeType(ContentService.MimeType.JSON);
}

/** ========= MAIN PROCESSOR ========= */
/**
 * Process all unread messages:
 * - Claim atomically with MatrixProcessing (Advanced Gmail).
 * - Build payload with bodies from GmailApp (robust), headers from Advanced Gmail.
 * - Gate: subscription, optional auth, per-alias rate.
 * - Deliver with bearer + idempotency key (msg.id).
 * - Success → add MatrixDone, mark read, remove Processing.
 * - Failure → keep Processing for the sweeper to retry.
 */
function processUnreadInboxOnce() {
  const lock = LockService.getScriptLock();
  lock.waitLock(30000); // serialize runs; label claims still guard per-message concurrency
  try {
    const userId = 'me';
    const ids = ensureStateLabels(); // { processingId, doneId, skippedId }
    const subs = loadSubs();

    // 1) List unread candidates (server-side filters)
    const msgIds = [];
    let pageToken = null;
    do {
      const res = Gmail.Users.Messages.list(userId, {
        q: CONFIG.INBOX_QUERY,
        maxResults: 100,
        pageToken
      });
      (res.messages || []).forEach(m => msgIds.push(m.id));
      pageToken = res.nextPageToken || null;
    } while (pageToken);

    if (!msgIds.length) {
      Logger.log('[proc] No unread candidates.');
      return;
    }
    Logger.log('[proc] Found unread candidates: %s', msgIds.length);

    // 2) Atomically claim (do NOT remove UNREAD yet)
    const claimed = [];
    msgIds.forEach(id => {
      try {
        Gmail.Users.Messages.modify({
          addLabelIds: [ids.processingId],
          removeLabelIds: []
        }, userId, id);
        claimed.push(id);
      } catch (e) {
        Logger.log('[proc] Claim failed for %s: %s', id, e);
      }
    });
    if (!claimed.length) {
      Logger.log('[proc] No messages could be claimed.');
      return;
    }

    // === FIFO: sorter claimed etter internalDate og prosesser sekvensielt ===
    const orderedClaimed = orderByInternalDate_(claimed);

    // 3) Process each claimed message SEQUENTIALLY (FIFO)
    for (let i = 0; i < orderedClaimed.length; i++) {
      const id = orderedClaimed[i];
      let adv, headers, basic, bodies;
      try {
        adv = Gmail.Users.Messages.get(userId, id, { format: 'full' });
        headers = headerMap_(adv.payload.headers || []);
      } catch (e) {
        Logger.log('[proc] Fetch (Advanced) failed for %s: %s', id, e);
        // Give up claim, leave UNREAD so a later run can reattempt
        try { Gmail.Users.Messages.modify({ addLabelIds: [], removeLabelIds: [ids.processingId] }, userId, id); } catch (_){}
        continue;
      }

      // Prefer GmailApp for bodies (robust)
      bodies = getBodiesHybrid_(id, adv);   // { plain, html, snippet, to }
      basic  = getBasicFieldsFromHeaders_(headers); // { from, to, subject, date }

      // Alias: from headers first, fallback to GmailApp "To:"
      let alias = extractPlusAlias(headers);
      if (!alias) alias = extractPlusAliasFromString_(bodies.to || basic.to || '');

      // Gate 1: alias must be subscribed
      const entry = alias ? subs[alias] : null;
      if (!alias || !entry) {
        Logger.log('[proc] No subscribed alias → mark read. id=%s alias=%s', id, alias);
        safeMarkRead_('me', id, ids.processingId);
        continue;
      }

      // Gate 2: optional SPF/DKIM/DMARC
      if (CONFIG.REQUIRE_AUTH_RESULTS && !passesAuth(headers)) {
        Logger.log('[proc] AuthResults failed → mark read. id=%s', id);
        safeMarkRead_('me', id, ids.processingId);
        continue;
      }

      // Gate 3: per-alias rate limit
      if (!allowRate(alias, CONFIG.RATE_PER_MIN)) {
        Logger.log('[proc] Rate-limited alias=%s → mark read. id=%s', alias, id);
        safeMarkRead_('me', id, ids.processingId);
        continue;
      }

      // Build payload
      const payload = {
        type: 'inreach.email',
        alias,
        from: basic.from || '',
        to:   basic.to   || '',
        subject: basic.subject || '(no subject)',
        date: basic.date || '',
        snippet: bodies.snippet || '',
        body: {
          plain: (bodies.plain || '').slice(0, CONFIG.BODY_MAX_PLAIN),
          html:  (bodies.html  || '').slice(0, CONFIG.BODY_MAX_HTML)
        },
        gmail: { id: adv.id, thread: adv.threadId }
      };

      try {
        postJsonAuth(entry.webhook, payload, entry.bearer_token || '', adv.id);
        Gmail.Users.Messages.modify({
          addLabelIds: [ids.doneId],
          removeLabelIds: ['UNREAD', ids.processingId]
        }, userId, id);

        // === PRIVACY: permanent delete etter vellykket levering ===
        try {
          Gmail.Users.Messages.remove(userId, id);
          Logger.log('[proc] Delivered OK and permanently deleted. id=%s alias=%s', id, alias);
        } catch (delErr) {
          Logger.log('[proc] Permanent delete failed for %s: %s', id, delErr);
        }

      } catch (sendErr) {
        // Keep Processing → sweeper will retry
        Logger.log('[proc] Delivery failed (kept Processing) id=%s err=%s', id, sendErr);
      }
    }

  } finally {
    lock.releaseLock();
  }
}


/** ========= SWEEPER (retry stuck Processing) ========= */
function retryProcessingStuck() {
  const { processingId, doneId } = ensureStateLabels();
  const subs = loadSubs();

  const MAX_AGE_MS = 10 * 60 * 1000;
  const now = Date.now();

  let pageToken = null;
  do {
    const res = Gmail.Users.Messages.list('me', {
      labelIds: [processingId],
      maxResults: 100,
      pageToken
    });

    (res.messages || []).forEach(m => {
      const full = Gmail.Users.Messages.get('me', m.id, { format: 'full' });
      const age = now - Number(full.internalDate || 0);
      if (age < MAX_AGE_MS) return;

      const headers = headerMap_(full.payload.headers || []);
      const basic   = getBasicFieldsFromHeaders_(headers);

      // Alias
      let alias = extractPlusAlias(headers);
      if (!alias) {
        try {
          const toStr = GmailApp.getMessageById(full.id).getTo() || '';
          alias = extractPlusAliasFromString_(toStr);
        } catch (_) {}
      }
      const entry = alias ? subs[alias] : null;

      if (!entry) {
        // Not subscribed anymore: mark read & drop Processing
        safeMarkRead_('me', full.id, processingId);
        return;
      }

      // Bodies via GmailApp (fallback to Advanced if needed)
      const bodies = getBodiesHybrid_(full.id, full);
      const payload = {
        type: 'inreach.email',
        alias,
        from: basic.from || '',
        to:   basic.to   || '',
        subject: basic.subject || '(no subject)',
        date: basic.date || '',
        snippet: bodies.snippet || '',
        body: { 
          plain: (bodies.plain || '').slice(0, CONFIG.BODY_MAX_PLAIN), 
          html:  (bodies.html  || '').slice(0, CONFIG.BODY_MAX_HTML) 
        },
        gmail: { id: full.id, thread: full.threadId }
      };

      try {
        postJsonAuth(entry.webhook, payload, entry.bearer_token || '', full.id);
        Gmail.Users.Messages.modify({
          addLabelIds: [doneId],
          removeLabelIds: ['UNREAD', processingId]
        }, 'me', full.id);

        // === PRIVACY: permanent delete etter vellykket levering i sweeper ===
        try {
          Gmail.Users.Messages.remove('me', full.id);
          Logger.log('[sweeper] Delivered OK and permanently deleted. id=%s alias=%s', full.id, alias);
        } catch (delErr) {
          Logger.log('[sweeper] Permanent delete failed for %s: %s', full.id, delErr);
        }

      } catch (e) {
        Logger.log('[sweeper] Delivery failed (kept Processing) id=%s err=%s', full.id, e);
      }
    });

    pageToken = res.nextPageToken || null;
  } while (pageToken);
}


/** ========= LABELS ========= */
function ensureStateLabels() {
  const labels = Gmail.Users.Labels.list('me').labels || [];
  const byName = {};
  labels.forEach(l => byName[l.name] = l);

  function ensureLabel(name) {
    if (byName[name]) return byName[name].id;
    const created = Gmail.Users.Labels.create({
      name,
      labelListVisibility: 'labelShow',
      messageListVisibility: 'show'
    }, 'me');
    return created.id;
  }

  const processingId = ensureLabel(LBL_PROCESSING);
  const doneId       = ensureLabel(LBL_DONE);
  const skippedId    = ensureLabel(LBL_SKIPPED);
  return { processingId, doneId, skippedId };
}

/** ========= BODY/HEADER HELPERS ========= */

/** Map header array → lowercase name → value */
function headerMap_(headersArr) {
  const m = {};
  (headersArr || []).forEach(h => m[String(h.name || '').toLowerCase()] = h.value || '');
  return m;
}

/** Pull basic fields from headers */
function getBasicFieldsFromHeaders_(hmap) {
  return {
    from:    hmap['from']    || '',
    to:      hmap['to']      || '',
    subject: hmap['subject'] || '(no subject)',
    date:    hmap['date']    || ''
  };
}

/**
 * Prefer GmailApp for bodies (simple & robust).
 * If something goes wrong, fall back to Advanced MIME extraction.
 */
function getBodiesHybrid_(messageId, advMsg) {
  let plain = '', html = '', snippet = '', to = '';
  try {
    const ga = GmailApp.getMessageById(messageId);
    plain = ga.getPlainBody() || '';
    html  = ga.getBody()      || '';
    to    = ga.getTo()        || '';
    snippet = (plain || advMsg.snippet || '')
      .slice(0, 120)
      .replace(/\s+/g, ' ')
      .trim();
    return { plain, html, snippet, to };
  } catch (e) {
    Logger.log('[hybrid] GmailApp body fetch failed for %s: %s', messageId, e);
  }

  // Fallback: Advanced MIME extraction
  try {
    plain = extractTextPart(messageId, advMsg.payload, 'text/plain') || '';
    html  = extractTextPart(messageId, advMsg.payload, 'text/html')  || '';
    snippet = (plain || advMsg.snippet || '')
      .slice(0, 120)
      .replace(/\s+/g, ' ')
      .trim();
  } catch (e2) {
    Logger.log('[hybrid] Advanced MIME fallback failed for %s: %s', messageId, e2);
  }
  return { plain, html, snippet, to: '' };
}

/** Extract +alias from headers (To:/Delivered-To:) */
function extractPlusAlias(headers) {
  const to        = headers['to'] || '';
  const delivered = headers['delivered-to'] || '';
  const cands = [to, delivered];
  for (const line of cands) {
    const m = line.match(/matrix\.vibb\.me\+([a-z0-9._-]+)@gmail\.com/i);
    if (m) return m[1].toLowerCase();
  }
  return null;
}

/** Extract +alias from a raw To: string like "matrix.vibb.me+alias@gmail.com" */
function extractPlusAliasFromString_(toStr) {
  if (!toStr) return null;
  const m = String(toStr).match(/matrix\.vibb\.me\+([a-z0-9._-]+)@gmail\.com/i);
  return m ? m[1].toLowerCase() : null;
}

/** Advanced MIME: recursively extract a desired text/* part */
function extractTextPart(messageId, part, wanted) {
  if (!part) return '';
  const mime = (part.mimeType || '').toLowerCase();
  const charset = detectCharsetFromPart(part) || 'utf-8';

  // Direct hit
  if (mime.indexOf(wanted) === 0) {
    // inline data
    if (part.body && part.body.data) {
      return decodePartDataToString(part.body.data, charset);
    }
    // attachment ref
    if (part.body && part.body.attachmentId) {
      try {
        const att = Gmail.Users.Messages.Attachments.get('me', messageId, part.body.attachmentId);
        if (att && att.data) {
          return decodePartDataToString(att.data, charset);
        }
      } catch (e) {
        Logger.log('[extractTextPart] Attachment fetch failed %s: %s', part.body.attachmentId, e);
      }
    }
    return '';
  }

  // Search children
  const parts = part.parts || [];
  for (let i = 0; i < parts.length; i++) {
    const found = extractTextPart(messageId, parts[i], wanted);
    if (found) return found;
  }
  return '';
}

/** Base64 (std + url-safe) with padding fix → string in charset */
function decodePartDataToString(b64, charset) {
  if (!b64) return '';
  // Normalize to standard base64
  let s = String(b64).replace(/-/g, '+').replace(/_/g, '/');
  const pad = s.length % 4;
  if (pad) s += '='.repeat(4 - pad);

  // Try standard base64
  try {
    const bytes = Utilities.base64Decode(s);
    return Utilities.newBlob(bytes).getDataAsString(charset || 'utf-8');
  } catch (e1) {
    // Fallback: web-safe
    try {
      const bytes2 = Utilities.base64DecodeWebSafe(b64);
      return Utilities.newBlob(bytes2).getDataAsString(charset || 'utf-8');
    } catch (e2) {
      Logger.log('[decodePartDataToString] Failed both base64 and base64url. e1=%s e2=%s', e1, e2);
      throw e2;
    }
  }
}

/** Detect charset from part headers (e.g., Content-Type: text/plain; charset="iso-8859-1") */
function detectCharsetFromPart(part) {
  if (!part || !part.headers) return null;
  const h = {};
  (part.headers || []).forEach(x => h[String(x.name || '').toLowerCase()] = x.value || '');
  const ct = h['content-type'] || '';
  const m = ct.match(/charset\s*=\s*"?([A-Za-z0-9._-]+)"?/i);
  return m ? m[1] : null;
}

/** ========= AUTH / RATE / DELIVERY ========= */

/** Require dmarc=pass and (spf=pass || dkim=pass) if CONFIG.REQUIRE_AUTH_RESULTS = true */
function passesAuth(headers) {
  const ar = headers['authentication-results'] || '';
  const okDMARC = /dmarc\s*=\s*pass/i.test(ar);
  const okSPF   = /spf\s*=\s*pass/i.test(ar);
  const okDKIM  = /dkim\s*=\s*pass/i.test(ar);
  return okDMARC && (okSPF || okDKIM);
}

/** Per-alias rate limit (CacheService) */
function allowRate(key, perMin) {
  const cache = CacheService.getScriptCache();
  const k = 'rate_' + key;
  const val = Number(cache.get(k) || '0');
  if (val >= perMin) return false;
  cache.put(k, String(val + 1), 60); // TTL 60s
  return true;
}

/** Mark read + drop Processing */
function safeMarkRead_(userId, msgId, processingId) {
  try {
    Gmail.Users.Messages.modify({
      addLabelIds: [],
      removeLabelIds: ['UNREAD', processingId]
    }, userId, msgId);
  } catch (e) {
    Logger.log('[proc] Mark read cleanup failed for %s: %s', msgId, e);
  }
}

/** POST to room webhook with Bearer + Idempotency */
function postJsonAuth(url, obj, bearerToken, idempotencyKey) {
  const headers = { 'X-Idempotency-Key': idempotencyKey };
  if (bearerToken) headers['Authorization'] = 'Bearer ' + bearerToken;

  const options = {
    method: 'post',
    contentType: 'application/json',
    headers,
    payload: JSON.stringify(obj),
    muteHttpExceptions: true
  };
  const res = UrlFetchApp.fetch(url, options);
  const code = res.getResponseCode();
  if (code < 200 || code >= 300) {
    throw new Error('Webhook failed ' + code + ': ' + res.getContentText());
  }
}

/** ========= ADMIN API (subscriptions) ========= */

function handleAdmin(e) {
  const token = e.parameter.admin_token;
  if (token !== CONFIG.ADMIN_TOKEN) return jsonOut({ error: 'forbidden' }, 403);

  const action = (e.parameter.action || '').toLowerCase();
  const body = (e.postData && e.postData.contents) ? parseJsonSafe(e.postData.contents) : {};

  if (action === 'subscribe')   return adminSubscribe_(body);
  if (action === 'unsubscribe') return adminUnsubscribe_(body);
  if (action === 'list')        return adminList_();
  if (action === 'check')       return adminCheck_(body);
  return jsonOut({ error: 'unknown action' }, 400);
}

function adminSubscribe_(body) {
  if (!body || !body.alias || !body.webhook) return jsonOut({error:'alias and webhook required'}, 400);
  const alias = normAlias(body.alias);
  if (!alias) return jsonOut({error:'invalid alias'}, 400);
  if (!isValidUrl(body.webhook)) return jsonOut({error:'invalid webhook url'}, 400);

  saveSubsAtomic(subs => { subs[alias] = { webhook: body.webhook, bearer_token: body.bearer_token || '' }; });
  const now = loadSubs()[alias];
  return jsonOut({ ok:true, alias, webhook: now.webhook, has_bearer: !!now.bearer_token }, 200);
}

function adminUnsubscribe_(body) {
  if (!body || !body.alias) return jsonOut({error:'alias required'}, 400);
  const alias = normAlias(body.alias);
  saveSubsAtomic(subs => { delete subs[alias]; });
  return jsonOut({ ok:true, alias }, 200); // idempotent
}

function adminList_() {
  const subs = loadSubs();
  // Viktig: ikke returner bearer_token
  return jsonOut({ ok: true, subscriptions: sanitizeSubs_(subs) }, 200);
}

// --- remove bearer token for admin_list ---
function sanitizeSubs_(subs) {
  const out = {};
  Object.keys(subs || {}).forEach(alias => {
    const s = subs[alias] || {};
    out[alias] = {
      webhook: s.webhook || '',
      has_bearer: !!s.bearer_token
    };
  });
  return out;
}


function adminCheck_(body) {
  if (!body || !body.alias) return jsonOut({error:'alias required'}, 400);
  const alias = normAlias(body.alias);
  const entry = loadSubs()[alias] || null;
  return jsonOut({ ok:true, alias, subscribed: !!entry, webhook: entry && entry.webhook, has_bearer: !!(entry && entry.bearer_token) }, 200);
}

/** ========= STORAGE (Script Properties) ========= */

function loadSubs() {
  const raw = SP.getProperty(KEY_SUBSCRIPTIONS) || '{}';
  try { return JSON.parse(raw); } catch (e) { return {}; }
}
function saveSubs(obj) {
  SP.setProperty(KEY_SUBSCRIPTIONS, JSON.stringify(obj));
}
function saveSubsAtomic(mutator) {
  const lock = LockService.getScriptLock();
  lock.waitLock(30000);
  try {
    const cur = loadSubs();
    mutator(cur);
    saveSubs(cur);
  } finally {
    lock.releaseLock();
  }
}
function normAlias(a) {
  const m = String(a || '').toLowerCase().match(/^[a-z0-9._-]{1,64}$/);
  return m ? m[0] : null;
}
function isValidUrl(u) {
  if (typeof u !== 'string') return false;
  u = u.trim();
  if (!/^https:\/\//i.test(u)) return false; // force https
  const re = /^https:\/\/[A-Za-z0-9.-]+(?::\d+)?(?:\/[\w\-./?%&=:+#@]*)?$/;
  return re.test(u);
}

/** ========= ContentService helpers (Admin) ========= */
function jsonOut(obj /*, codeIgnored */) {
  return ContentService.createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}
function parseJsonSafe(s) { if (!s) return null; try { return JSON.parse(s); } catch(e) { return null; } }

/** ========= (Optional) Gmail Watch helpers ========= */
/**
 * Start/renew Gmail Watch using raw REST (robust).
 * - Uses CONFIG.WATCH_TOPIC (full resource name: projects/<proj>/topics/<name>)
 * - Uses CONFIG.WATCH_LABELS (names or IDs). Names are resolved to IDs.
 * - Uses labelFilterAction: "include" if you provided labels; otherwise watches everything.
 */
function renewWatch() {
  const url = "https://gmail.googleapis.com/gmail/v1/users/me/watch";

  // Resolve label identifiers: accept names like "INBOX" or actual label IDs like "Label_123..."
  const labelIds = resolveLabelIds_(CONFIG.WATCH_LABELS || []);

  const payload = {
    topicName: CONFIG.WATCH_TOPIC
  };

  if (labelIds.length > 0) {
    payload.labelIds = labelIds;
    // Gmail API expects lowercase "include" / "exclude"
    payload.labelFilterAction = "include";
  }

  const options = {
    method: "post",
    contentType: "application/json",
    headers: { Authorization: "Bearer " + ScriptApp.getOAuthToken() },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  };

  try {
    const res = UrlFetchApp.fetch(url, options);
    const code = res.getResponseCode();
    const body = res.getContentText();
    Logger.log("[watch] status=%s body=%s", code, body);
    if (code < 200 || code >= 300) {
      throw new Error("Watch failed " + code + ": " + body);
    }
  } catch (e) {
    Logger.log("[watch] ERROR: %s", e.message);
    throw e;
  }
}

/**
 * Stop Gmail Watch using raw REST.
 */
function stopGmailWatch() {
  const url = "https://gmail.googleapis.com/gmail/v1/users/me/stop";
  const options = {
    method: "post",
    headers: { Authorization: "Bearer " + ScriptApp.getOAuthToken() },
    muteHttpExceptions: true
  };
  const res = UrlFetchApp.fetch(url, options);
  Logger.log("[watch] stop status=%s body=%s", res.getResponseCode(), res.getContentText());
}

/**
 * Resolve a mix of label names or IDs to a pure ID list.
 * - Accepts system names like "INBOX", "SENT", "DRAFT", etc.
 * - If input already looks like a Label_* ID, it is kept as-is.
 */
function resolveLabelIds_(namesOrIds) {
  if (!namesOrIds || !namesOrIds.length) return [];

  const labels = Gmail.Users.Labels.list('me').labels || [];
  const byName = {};
  labels.forEach(l => { byName[String(l.name).toUpperCase()] = l.id; });

  const out = [];
  namesOrIds.forEach(x => {
    if (!x) return;
    // If it already looks like an ID, keep it
    if (/^Label_\d+/.test(String(x))) {
      out.push(String(x));
      return;
    }
    // Try resolve by name (case-insensitive)
    const id = byName[String(x).toUpperCase()];
    if (id) out.push(id);
    else Logger.log("[watch] Could not resolve label '%s' to an ID; skipping.", x);
  });
  return out;
}

function orderByInternalDate_(ids) {
  const enriched = [];
  ids.forEach(id => {
    try {
      const msg = Gmail.Users.Messages.get('me', id, { format: 'metadata', metadataHeaders: ['Date'] });
      let ts = Number(msg.internalDate || 0);
      if (!ts && msg.payload && msg.payload.headers) {
        const h = headerMap_(msg.payload.headers);
        const d = h['date'] ? Date.parse(h['date']) : 0;
        ts = isNaN(d) ? 0 : d;
      }
      enriched.push({ id, ts });
    } catch (e) {
      Logger.log('[fifo] metadata fail %s: %s', id, e);
      enriched.push({ id, ts: 0 });
    }
  });
  enriched.sort((a, b) => a.ts - b.ts);
  return enriched.map(x => x.id);
}
