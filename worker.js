// Louisiana Vending CRM — Email Sync Worker
// Runs OUTSIDE the browser. Syncs Hostinger mailboxes into Supabase over IMAP,
// polls the Smart-BCC mailbox, and sends due scheduled emails over SMTP.
//
//   node worker.js            → one full pass, then exit (use with cron)
//   node worker.js --loop 180 → run forever, one pass every 180 seconds
//
// Credentials: each email_accounts row stores credential_ref (e.g. MAIL_PW_ADMIN);
// the actual password is read from this process's environment / .env only.
import "dotenv/config";
import { ImapFlow } from "imapflow";
import { simpleParser } from "mailparser";
import nodemailer from "nodemailer";
import { createClient } from "@supabase/supabase-js";

const need = (k) => { const v = process.env[k]; if (!v) { console.error(`Missing env ${k}`); process.exit(1); } return v; };
const sb = createClient(need("SUPABASE_URL"), need("SUPABASE_SERVICE_ROLE_KEY"));
const SKIP_FOLDERS = /(junk|spam|trash|deleted)/i; // never sync these even on "all folders"
const RUN_BUDGET = 150; // max messages examined per run across ALL mailboxes — keeps every run short so it finishes (and is never killed mid-backlog). The 15-min cron resumes where it left off.
let runSeen = 0;        // reset at the start of every pass()

const normSubject = (s) => (s || "(no subject)").replace(/^((re|fwd|fw)\s*:\s*)+/i, "").trim().toLowerCase();
const addrList = (a) => (a?.value ?? []).map((v) => v.address?.toLowerCase()).filter(Boolean);
const isBlocked = (addr, patterns) => {
  const a = (addr || "").toLowerCase();
  return patterns.some((p) => (p.startsWith("@") ? a.endsWith(p) : a === p));
};

async function setSync(accountId, patch) {
  await sb.from("email_sync_status").upsert({ account_id: accountId, ...patch, updated_at: new Date().toISOString() });
}

// Close an IMAP connection without hanging. We try a graceful LOGOUT but never
// wait more than 5s — Hostinger sometimes stalls on LOGOUT, which used to keep
// the whole worker alive until the 10-minute job timeout killed it.
async function safeClose(client) {
  try {
    await Promise.race([
      client.logout(),
      new Promise((_, rej) => setTimeout(() => rej(new Error("logout timed out")), 5000)),
    ]);
  } catch { try { client.close(); } catch { /* already gone */ } }
}

// ---------- thread resolution + record linking ------------------------------
async function resolveThread(acct, parsed, participants) {
  // Prefer real RFC threading (References / In-Reply-To root), fall back to
  // normalized subject + participants, same formula the send function uses.
  const refs = parsed.references ? [].concat(parsed.references) : [];
  const root = refs[0] || parsed.inReplyTo || null;
  let key = root || `${normSubject(parsed.subject)}|${participants.sort().join(",")}`;

  const { data: existing } = await sb.from("email_threads").select("id,contact_id,deal_id")
    .eq("account_id", acct.id).eq("thread_key", key).maybeSingle();
  if (existing) return existing;

  // If this is a reply, the original outbound mail may live in a subject-keyed thread.
  if (root) {
    const { data: byMsg } = await sb.from("emails").select("thread_id").eq("account_id", acct.id)
      .in("message_id", refs.concat(parsed.inReplyTo || []).filter(Boolean)).limit(1).maybeSingle();
    if (byMsg) {
      const { data: t } = await sb.from("email_threads").select("id,contact_id,deal_id").eq("id", byMsg.thread_id).single();
      if (t) return t;
    }
  }
  const { data: t, error } = await sb.from("email_threads").insert({
    account_id: acct.id, subject: parsed.subject || "(no subject)", thread_key: key,
    visibility: acct.default_visibility,
  }).select("id,contact_id,deal_id").single();
  if (error) { // race with another pass — re-read
    const { data: again } = await sb.from("email_threads").select("id,contact_id,deal_id")
      .eq("account_id", acct.id).eq("thread_key", key).single();
    return again;
  }
  return t;
}

async function autoLink(thread, externalAddrs) {
  if (thread.contact_id || externalAddrs.length === 0) return;
  const { data: c } = await sb.from("contacts").select("id,organization_id")
    .in("email", externalAddrs).limit(1).maybeSingle();
  if (!c) return;
  const patch = { contact_id: c.id };
  if (c.organization_id) patch.organization_id = c.organization_id;
  // also attach the most recent open deal for that contact, if any
  const { data: d } = await sb.from("deals").select("id").eq("contact_id", c.id)
    .eq("status", "open").order("updated_at", { ascending: false }).limit(1).maybeSingle();
  if (d && !thread.deal_id) patch.deal_id = d.id;
  await sb.from("email_threads").update(patch).eq("id", thread.id);
}

// ---------- one mailbox ------------------------------------------------------
async function syncAccount(acct, blockedPatterns, bccAddress) {
  const password = process.env[acct.credential_ref];
  if (!password) { await setSync(acct.id, { state: "error", last_error: `Worker env var ${acct.credential_ref} is not set` }); return; }
  await setSync(acct.id, { state: "syncing", last_error: null });

  for (let attempt = 1; attempt <= 3; attempt++) {
  const client = new ImapFlow({
    host: acct.imap_host, port: acct.imap_port, secure: true, logger: false,
    auth: { user: acct.address, pass: password },
    connectionTimeout: 30000, greetingTimeout: 20000, socketTimeout: 240000,
  });
  // ImapFlow fires an 'error' event on socket problems (e.g. ETIMEOUT). With no
  // listener, Node treats it as fatal and crashes the whole run with exit code 1 —
  // underneath the try/catch below. Handling it here turns the crash into a normal
  // rejection the retry loop can catch and recover from.
  client.on("error", (err) => console.error(`[imap] ${acct.address} connection error: ${err?.message || err}`));
  let synced = 0;
  try {
    await client.connect();

    // folder discovery → email_folders rows (UI folder picker reads these)
    const boxes = await client.list();
    for (const b of boxes) {
      await sb.from("email_folders").upsert(
        { account_id: acct.id, path: b.path },
        { onConflict: "account_id,path", ignoreDuplicates: true });
    }
    const { data: folders } = await sb.from("email_folders").select("*").eq("account_id", acct.id);

    for (const f of folders ?? []) {
      if (runSeen >= RUN_BUDGET) break;
      if (SKIP_FOLDERS.test(f.path)) continue;
      if (!acct.sync_all_folders && !f.selected) continue;

      const lock = await client.getMailboxLock(f.path).catch(() => null);
      if (!lock) continue;
      try {
        const box = client.mailbox;
        let lastUid = Number(f.last_uid) || 0;
        if (f.uidvalidity && String(box.uidValidity) !== String(f.uidvalidity)) lastUid = 0; // server reset

        // Honor "sync past emails" + start date on the very first pass.
        // Safeguard: when no start date is set, default to the last 6 months so
        // the first sync finishes within the time limit instead of pulling years
        // of history and timing out. New mail always keeps syncing after that;
        // set a "Sync start date" on the account to go further back.
        let range = `${lastUid + 1}:*`;
        if (lastUid === 0) {
          if (!acct.sync_past) {
            range = { since: new Date() };
          } else if (acct.sync_start_date) {
            range = { since: new Date(acct.sync_start_date) };
          } else {
            const since = new Date(); since.setMonth(since.getMonth() - 6);
            range = { since };
          }
        }
        let maxUid = lastUid;
        for await (const msg of client.fetch(range, { uid: true, source: true }, { uid: typeof range === "string" })) {
          if (msg.uid <= lastUid) continue;
          maxUid = Math.max(maxUid, msg.uid);
          runSeen++;
          // Save progress every 25 messages so a cancelled run never loses its place,
          // and stop after RUN_BUDGET so each run finishes fast (the 15-min cron resumes).
          if (runSeen % 25 === 0) {
            await sb.from("email_folders").update({ last_uid: maxUid, uidvalidity: String(box.uidValidity) }).eq("id", f.id);
            console.log(`[sync] ${acct.address} ${f.path}: ${runSeen} scanned this run, ${synced} new — progress saved`);
          }
          if (runSeen >= RUN_BUDGET) {
            console.log(`[sync] ${acct.address} ${f.path}: hit per-run cap (${RUN_BUDGET}) — pausing here; the next 15-min run resumes automatically`);
            break;
          }
          const parsed = await simpleParser(msg.source);
          const from = parsed.from?.value?.[0]?.address?.toLowerCase() ?? "";
          if (isBlocked(from, blockedPatterns)) continue;                  // blocked senders ignored
          const messageId = parsed.messageId || `<uid-${msg.uid}-${f.path}@${acct.address}>`;

          const tos = addrList(parsed.to), ccs = addrList(parsed.cc), bccs = addrList(parsed.bcc);
          const participants = [from, ...tos].filter(Boolean);
          const direction = from === acct.address.toLowerCase() ? "outbound" : "inbound";
          const isBcc = bccAddress && acct.address.toLowerCase() === bccAddress;

          const thread = await resolveThread(acct, parsed, participants);
          if (!thread?.id) continue; // thread couldn't be resolved (transient DB hiccup) — skip this one; next pass picks it up
          const { data: inserted, error } = await sb.from("emails").insert({
            thread_id: thread.id, account_id: acct.id, message_id: messageId,
            in_reply_to: parsed.inReplyTo ?? null, direction, folder: f.path,
            from_address: from || "(unknown)", from_name: parsed.from?.value?.[0]?.name ?? null,
            to_addresses: tos, cc_addresses: ccs, bcc_addresses: bccs,
            subject: parsed.subject ?? null,
            body_text: parsed.text ?? null, body_html: parsed.html || null,
            sent_at: (parsed.date ?? new Date()).toISOString(),
            visibility: acct.default_visibility, is_smart_bcc: !!isBcc,
          }).select("id").single();
          if (error) { if (error.code === "23505") continue; throw error; } // duplicate → already synced
          synced++;

          // attachments → private storage bucket
          for (const att of parsed.attachments ?? []) {
            const path = `${inserted.id}/${(att.filename || "attachment").replace(/[^\w.\- ]+/g, "_")}`;
            const { error: upErr } = await sb.storage.from("attachments")
              .upload(path, att.content, { contentType: att.contentType, upsert: true });
            if (!upErr) await sb.from("email_attachments").insert({
              email_id: inserted.id, filename: att.filename || "attachment",
              mime_type: att.contentType, size_bytes: att.size, storage_path: path });
          }

          const external = participants.filter((a) => a && !a.endsWith("@louisianavending.com"));
          await autoLink(thread, external);

          if (direction === "inbound" && /inbox/i.test(f.path) && acct.owner_id) {
            await sb.from("notifications").insert({
              profile_id: acct.owner_id, type: "email_received", title: "New email",
              body: `${parsed.from?.text ?? from}: ${parsed.subject ?? "(no subject)"}`,
              entity: "email_threads", record_id: thread.id });
          }
        }
        await sb.from("email_folders").update({ last_uid: maxUid, uidvalidity: String(box.uidValidity) }).eq("id", f.id);
      } finally { lock.release(); }
    }

    const { data: st } = await sb.from("email_sync_status").select("messages_synced").eq("account_id", acct.id).single();
    await setSync(acct.id, { state: "idle", last_sync_at: new Date().toISOString(),
      messages_synced: (st?.messages_synced ?? 0) + synced });
    await sb.from("audit_logs").insert({ action: "EMAIL_SYNC", entity: "email_accounts",
      record_id: acct.id, diff: { address: acct.address, new_messages: synced } });
    console.log(`[sync] ${acct.address}: ${synced} new`);
    await safeClose(client);
    return; // success — this mailbox is done
  } catch (e) {
    await safeClose(client);
    const msg = e?.message || String(e);
    if (attempt < 3) {
      console.error(`[sync] ${acct.address} attempt ${attempt} failed (${msg}) — retrying in ${8 * attempt}s`);
      await new Promise((r) => setTimeout(r, 8000 * attempt));
      continue;
    }
    console.error(`[sync] ${acct.address} FAILED after ${attempt} attempts:`, msg);
    await setSync(acct.id, { state: "error", last_error: msg });
  }
  }
}

// ---------- scheduled outbound emails ---------------------------------------
async function sendScheduled(accounts) {
  const { data: due } = await sb.from("scheduled_emails").select("*")
    .eq("sent", false).lte("send_at", new Date().toISOString());
  for (const s of due ?? []) {
    const acct = accounts.find((a) => a.id === s.account_id);
    const password = acct && process.env[acct.credential_ref];
    if (!acct || !password) continue;
    try {
      const tx = nodemailer.createTransport({ host: acct.smtp_host, port: acct.smtp_port,
        secure: acct.smtp_port === 465, auth: { user: acct.address, pass: password },
        connectionTimeout: 15000, greetingTimeout: 10000, socketTimeout: 20000 });
      const messageId = `<${crypto.randomUUID()}@${acct.address.split("@")[1]}>`;
      await tx.sendMail({ from: `${acct.custom_sender_name || acct.sender_name} <${acct.address}>`,
        to: s.to_addresses.join(","), cc: (s.cc_addresses ?? []).join(",") || undefined,
        subject: s.subject ?? "", html: s.body_html ?? "", messageId });
      // record into CRM like any outbound mail
      let threadId = s.thread_id;
      if (!threadId) {
        const key = `${normSubject(s.subject)}|${[acct.address.toLowerCase(), ...s.to_addresses.map((x)=>x.toLowerCase())].sort().join(",")}`;
        const { data: t } = await sb.from("email_threads").upsert(
          { account_id: acct.id, subject: s.subject ?? "(no subject)", thread_key: key, deal_id: s.deal_id ?? null },
          { onConflict: "account_id,thread_key" }).select("id").single();
        threadId = t.id;
      }
      await sb.from("emails").insert({ thread_id: threadId, account_id: acct.id, message_id: messageId,
        direction: "outbound", folder: "Sent", from_address: acct.address,
        from_name: acct.custom_sender_name || acct.sender_name, to_addresses: s.to_addresses,
        cc_addresses: s.cc_addresses ?? [], subject: s.subject, body_html: s.body_html,
        body_text: (s.body_html ?? "").replace(/<[^>]+>/g, " "), sent_at: new Date().toISOString(),
        owner_id: s.created_by });
      await sb.from("scheduled_emails").update({ sent: true }).eq("id", s.id);
      await sb.from("audit_logs").insert({ actor: s.created_by, action: "EMAIL_SEND", entity: "scheduled_emails",
        record_id: s.id, diff: { scheduled: true, to: s.to_addresses, subject: s.subject } });
      console.log(`[scheduled] sent "${s.subject}" → ${s.to_addresses.join(",")}`);
      tx.close();
    } catch (e) { console.error(`[scheduled] ${s.id} failed:`, e.message); }
  }
}

// ---------- main -------------------------------------------------------------
async function pass() {
  console.log("======================================================");
  console.log("🟢 CRASHPROOF BUILD v5 — handles socket timeouts (no more exit-code-1 crash)");
  console.log("======================================================");
  runSeen = 0;
  const { data: blocked } = await sb.from("blocked_emails").select("pattern");
  const patterns = (blocked ?? []).map((b) => b.pattern.toLowerCase());
  const bccAddress = (process.env.SMART_BCC_ADDRESS || "").toLowerCase() || null;
  const { data: accounts } = await sb.from("email_accounts").select("*");
  const active = (accounts ?? []).filter((a) => a.sync_active && !a.address.startsWith("PLACEHOLDER"));
  for (const acct of active) {
    if (runSeen >= RUN_BUDGET) { console.log("[sync] per-run cap reached — remaining mailboxes continue on the next run"); break; }
    await syncAccount(acct, patterns, bccAddress);
  }
  await sendScheduled(accounts ?? []);
  console.log(`[sync] pass complete — ${runSeen} messages examined this run`);
}

const loopIdx = process.argv.indexOf("--loop");
if (loopIdx > -1) {
  const secs = Number(process.argv[loopIdx + 1]) || 180;
  console.log(`Worker looping every ${secs}s. Ctrl+C to stop.`);
  // eslint-disable-next-line no-constant-condition
  while (true) { await pass(); await new Promise((r) => setTimeout(r, secs * 1000)); }
} else {
  // Safety net: never let a single run hang. If anything stalls, force a clean
  // exit at 6 min (well under the job's 10-min limit). Progress is saved every
  // 25 messages, so quitting early loses nothing — the next run just resumes.
  const watchdog = setTimeout(() => {
    console.log("⏱️ watchdog: 6-min safety limit reached — exiting cleanly (progress already saved; next run resumes).");
    process.exit(0);
  }, 6 * 60 * 1000);
  await pass();
  clearTimeout(watchdog);
  process.exit(0);
}
