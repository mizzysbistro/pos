/* MY POS cloud adapter. Publishable Supabase key only; never put service-role secrets here. */
import { createClient } from 'https://esm.sh/@supabase/supabase-js@2.118.0';

const SUPABASE_URL = 'https://tjjvdsjrdjzqszzdtosk.supabase.co';
const SUPABASE_PUBLISHABLE_KEY = 'sb_publishable_pY0SamFnI8cvutUXL8E3GQ_mgRRXxON';
const OUTBOX_KEY = 'mypos-cloud-outbox-v1';
function randomUuid() {
  if (globalThis.crypto?.randomUUID) return globalThis.crypto.randomUUID();
  const bytes = new Uint8Array(16);
  if (globalThis.crypto?.getRandomValues) globalThis.crypto.getRandomValues(bytes);
  else for (let i = 0; i < bytes.length; i++) bytes[i] = Math.floor(Math.random() * 256);
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = [...bytes].map(b => b.toString(16).padStart(2, '0')).join('');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
const supabase = createClient(SUPABASE_URL, SUPABASE_PUBLISHABLE_KEY, {
  auth: { persistSession: true, autoRefreshToken: true, detectSessionInUrl: true, flowType: 'pkce' },
  realtime: { params: { eventsPerSecond: 10 } }
});
const isPreviewMode = ['localhost', '127.0.0.1', '::1'].includes(location.hostname) || location.hostname === '' || location.protocol === 'file:' || new URLSearchParams(location.search).has('preview');
const cloud = {
  configured: true, isPreviewMode, active: false, connected: false, connecting: false,
  user: null, role: null, storeId: null, storeName: null, team: { members: [], invites: [] }, lastError: '',
  client: supabase
};
window.MYPOS_CLOUD = cloud;
let subscription = null;
let queuedIncoming = [];
let acceptIncoming = false;
let bootstrapInProgress = false;
let pendingAccess = null;
let subscriptionReady = Promise.resolve(false);

function currentState() { return window.MYPOS_STATE || {}; }
function notify(message, ok = true) {
  if (typeof window.MYPOS_TOAST === 'function') window.MYPOS_TOAST(message, ok);
  else console[ok ? 'info' : 'warn']('[MY POS]', message);
}
function safeSnapshot(input = currentState(), role = cloud.role) {
  const snapshot = JSON.parse(JSON.stringify(input || {}));
  snapshot.staff = [];
  snapshot.currentOrder = [];
  snapshot.orderNote = '';
  snapshot.currentCustomerId = null;
  snapshot.user = { role: role || snapshot.role || 'Manager' };
  snapshot.role = role || snapshot.role || 'Manager';
  snapshot.settings = { ...(snapshot.settings || {}), storeId: cloud.storeId || snapshot.settings?.storeId };
  const assignment = `${role || snapshot.role || 'Staff'} assignment`;
  if (snapshot.shift && typeof snapshot.shift === 'object') {
    snapshot.shift = { ...snapshot.shift, employee: assignment };
    delete snapshot.shift.employeeName; delete snapshot.shift.openedBy; delete snapshot.shift.closedBy;
    if (Array.isArray(snapshot.shift.openingHistory)) snapshot.shift.openingHistory = snapshot.shift.openingHistory.map(x => ({ ...x, by: assignment, employee: assignment }));
  }
  if (Array.isArray(snapshot.stockHistory)) snapshot.stockHistory = snapshot.stockHistory.map(x => { const y = { ...x, by: assignment }; delete y.employee; delete y.staffName; return y; });
  if (Array.isArray(snapshot.transactions)) snapshot.transactions = snapshot.transactions.map(x => { const y = { ...x, by: assignment }; delete y.employee; delete y.staffName; delete y.email; return y; });
  // Explicitly exclude any local PIN records or local session credentials.
  delete snapshot.pin; delete snapshot.session; delete snapshot.access_token; delete snapshot.refresh_token;
  return snapshot;
}
function safeEvent(event) {
  const out = JSON.parse(JSON.stringify(event || {}));
  out.storeId = cloud.storeId;
  out.actor = { role: cloud.role || currentState().role || 'System' };
  if (out.payload?.transactions) out.payload.transactions = out.payload.transactions.map(t => {
    const x = { ...t, by: `${cloud.role || currentState().role || 'Staff'} assignment` };
    delete x.employee; delete x.staffName; delete x.email;
    return x;
  });
  if (Array.isArray(out.payload?.stockHistory)) out.payload.stockHistory = out.payload.stockHistory.map(t => { const x={...t,by:`${cloud.role||currentState().role||'Staff'} assignment`}; delete x.employee; delete x.staffName; return x; });
  if (out.payload?.order) {
    delete out.payload.order.createdBy;
    delete out.payload.order.cancelledBy;
    delete out.payload.order.staffName;
  }
  return out;
}
function readOutbox() {
  try { const rows = JSON.parse(localStorage.getItem(OUTBOX_KEY) || '[]'); return Array.isArray(rows) ? rows : []; } catch { return []; }
}
function writeOutbox(rows) { try { localStorage.setItem(OUTBOX_KEY, JSON.stringify(rows.slice(-250))); } catch { /* storage can be unavailable */ } }
async function reconcileCanonicalSnapshot() {
  const { data, error } = await supabase.rpc('get_pos_snapshot', { p_store_id: cloud.storeId });
  if (error) {
    cloud.lastError = error.message || 'Could not refresh canonical state';
    updateStatus();
    return false;
  }
  if (!data) return true;
  const s = currentState();
  // Refresh only shared business fields; never overwrite an unfinished local cart or PIN/session state.
  for (const key of ['products','orders','tables','stockHistory','transactions','settings','shift','customers']) {
    if (Object.prototype.hasOwnProperty.call(data, key)) s[key] = key === 'settings' ? { ...(s.settings || {}), ...(data.settings || {}) } : data[key];
  }
  s.settings = { ...(s.settings || {}), storeId: cloud.storeId, storeName: cloud.storeName };
  window.MYPOS_STATE = s;
  window.MYPOS_PERSIST?.();
  if (!document.querySelector('#modal-root .modal')) window.MYPOS_RENDER?.();
  else window.__MYPOS_SYNC_PENDING_REFRESH = true;
  return true;
}
async function publish(event, snapshot) {
  if (!cloud.active || !cloud.storeId) throw new Error('Sign in and unlock a staff assignment before syncing.');
  if (event?.storeId && String(event.storeId) !== String(cloud.storeId)) {
    throw new Error('This queued action belongs to a different store and was not sent. Sign in to that store to retry it.');
  }
  const ev = safeEvent(event);
  const snap = safeSnapshot(snapshot || currentState());
  if (ev.type === 'order.paid' && !['Cash'].includes(ev.payload?.order?.paymentMethod || 'Cash')) {
    throw new Error('Online synchronization currently confirms cash only. Card/GCash require a verified payment provider.');
  }
  const { data, error } = await supabase.rpc('publish_pos_event', { p_event: ev, p_snapshot: snap });
  if (error) {
    const retryable = !error.code || ['PGRST000','PGRST002','PGRST003','502','503','504','ECONNRESET','ETIMEDOUT'].includes(String(error.code));
    if (retryable) {
      const queue = readOutbox();
      if (!queue.some(x => x.event?.id === ev.id)) queue.push({ event: ev, snapshot: snap });
      writeOutbox(queue);
      cloud.connected = false;
    } else {
      // Reconcile a locally optimistic view after a business/security rejection.
      await reconcileCanonicalSnapshot().catch(() => false);
    }
    cloud.lastError = error.message;
    updateStatus();
    throw error;
  }
  cloud.lastError = '';
  // This is a follow-up to a user action, never a timer or polling loop. It reconciles server-validated prices/stock.
  await reconcileCanonicalSnapshot();
  updateStatus();
  return data;
}
async function flushOutbox() {
  if (!cloud.active || !cloud.storeId) return;
  const queue = readOutbox(); if (!queue.length) return;
  // Keep other stores' queued events isolated. Never relabel one store's event as another store's event.
  const otherStores = queue.filter(row => !row.event?.storeId || String(row.event.storeId) !== String(cloud.storeId));
  const ownStore = queue.filter(row => row.event?.storeId && String(row.event.storeId) === String(cloud.storeId));
  const left = [];
  for (const row of ownStore) {
    try { await publish(row.event, row.snapshot); }
    catch { left.push(row); }
  }
  writeOutbox([...otherStores, ...left]);
  if (ownStore.length && left.length === 0) notify('Pending cloud actions synchronized.');
}
function updateStatus() {
  document.querySelectorAll('.cloud-status').forEach(el => {
    el.innerHTML = cloud.active
      ? `<i class="fa-solid fa-circle" style="color:${cloud.connected ? '#79e2a8' : '#ffcf6e'}"></i> ${cloud.connected ? 'Cloud connected' : 'Cloud sync pending'}`
      : '<i class="fa-solid fa-circle" style="color:#f9d78c"></i> Preview / signed out';
    el.title = cloud.active ? `Store ${cloud.storeName || cloud.storeId}; action-triggered sync` : 'Sign in with Google to sync across devices';
  });
}
function waitForSubscribed(channel) {
  return new Promise(resolve => {
    let done = false;
    const timer = setTimeout(() => { if (!done) { done = true; resolve(false); } }, 8000);
    channel.subscribe(status => {
      if (status === 'SUBSCRIBED' || status === 'CHANNEL_ERROR' || status === 'TIMED_OUT' || status === 'CLOSED') {
        if (!done) { done = true; clearTimeout(timer); resolve(status === 'SUBSCRIBED'); }
      }
    });
  });
}
function subscribe(onEvent, { storeId, mode }) {
  if (mode !== 'event-driven') throw new Error('MY POS cloud sync only supports event-driven updates.');
  if (subscription) supabase.removeChannel(subscription);
  const kitchenOnly = cloud.role === 'Kitchen';
  const table = kitchenOnly ? 'pos_kitchen_events' : 'pos_events';
  const channel = supabase.channel(`my-pos-${storeId}-${cloud.role}-${randomUuid()}`)
    .on('postgres_changes', { event: 'INSERT', schema: 'public', table, filter: `store_id=eq.${storeId}` }, change => {
      const row = change.new;
      const event = { id: row.event_id, type: row.event_type, storeId: row.store_id, origin: row.origin, actor: { role: row.actor_role || 'Kitchen' }, occurredAt: row.created_at, payload: row.payload || {} };
      if (!acceptIncoming) queuedIncoming.push(event); else {
        onEvent(event);
        // Event-triggered canonical reconciliation restores the viewer's permitted fields and server-calculated stock.
        if (['order.saved','order.paid','order.cancelled','kitchen.status','products.updated','inventory.updated','tables.updated'].includes(event.type)) {
          reconcileCanonicalSnapshot().catch(error => { cloud.lastError = error.message || 'Canonical refresh failed'; updateStatus(); });
        }
      }
      cloud.connected = true; updateStatus();
    });
  subscription = channel;
  subscriptionReady = waitForSubscribed(channel).then(ok => { cloud.connected = !!ok; updateStatus(); return !!ok; });
  return () => { if (subscription === channel) subscription = null; supabase.removeChannel(channel); };
}
function syncAdapter() { return { publish, subscribe }; }
function applyCloudSnapshot(snapshot, membership, user, store) {
  const role = membership.role;
  const existing = currentState();
  const { _cloudMeta, ...businessSnapshot } = snapshot || {};
  const merged = {
    ...existing,
    ...businessSnapshot,
    settings: { ...(existing.settings || {}), ...(snapshot.settings || {}), storeId: store.id, storeName: store.name },
    staff: existing.staff || [],
    currentOrder: [], orderNote: '', role,
    user: { id: user.id, email: user.email, name: role, role },
    page: role === 'Kitchen' ? 'Kitchen' : 'Sell',
    activeSettings: role === 'Manager' ? (existing.activeSettings || 'General') : 'My account'
  };
  // Respect role-specific settings; the DB also checks role on every mutation.
  window.MYPOS_STATE = merged;
  window.MYPOS_PERSIST?.();
  window.MYPOS_RENDER?.();
}
async function refreshTeam() {
  if (!cloud.active || cloud.role !== 'Manager') return cloud.team;
  const { data, error } = await supabase.rpc('get_pos_team', { p_store_id: cloud.storeId });
  if (error) throw error;
  cloud.team = data || { members: [], invites: [] };
  return cloud.team;
}
async function loadMemberships(user) {
  const { data: membershipRows, error } = await supabase.from('pos_store_memberships')
    .select('store_id,role,is_active,created_at').eq('user_id', user.id).eq('is_active', true).order('created_at', { ascending: true });
  if (error) throw error;
  if (!membershipRows?.length) return null;
  const ids = membershipRows.map(m => m.store_id);
  const { data: stores, error: storeError } = await supabase.from('pos_stores').select('id,name').in('id', ids);
  if (storeError) throw storeError;
  const savedStoreId = currentState().settings?.storeId;
  const membership = membershipRows.find(m => m.store_id === savedStoreId) || membershipRows[0];
  const store = (stores || []).find(s => s.id === membership.store_id);
  if (!store) throw new Error('Assigned store is unavailable. Ask a manager to check this staff assignment.');
  return { membership, store };
}
function safeText(value) {
  return String(value ?? '').replace(/[&<>\"']/g, c => ({ '&':'&amp;', '<':'&lt;', '>':'&gt;', '\"':'&quot;', "'":'&#39;' }[c]));
}
function showPinSetupModal() {
  if (!pendingAccess) return;
  window.MYPOS_SHOW_MODAL?.('Create staff PIN', `<p class="muted small">Set a private 4–8 digit PIN for this staff assignment. The PIN is stored as a salted server-side hash, never as plain text.</p><div class="field"><label>New PIN</label><input id="cloud-pin-new" type="password" inputmode="numeric" autocomplete="new-password" maxlength="8" /></div><div class="field"><label>Confirm PIN</label><input id="cloud-pin-confirm" type="password" inputmode="numeric" autocomplete="new-password" maxlength="8" /></div><div id="cloud-pin-error" class="small" style="color:#b42318"></div>`, `<button class="btn btn-quiet" data-action="cloud-signout">Cancel</button><button class="btn btn-primary" data-action="cloud-save-own-pin">Save PIN</button>`, true);
}
function showPinVerifyModal(pinStatus = {}) {
  if (!pendingAccess) return;
  if (pinStatus.locked) {
    window.MYPOS_SHOW_MODAL?.('Staff assignment temporarily locked', `<p class="muted small">Too many incorrect PIN attempts. This account is temporarily locked. Try again after the lock expires, or contact the store Manager for a PIN reset.</p>`, `<button class="btn btn-primary" data-action="cloud-signout">Sign out</button>`, true);
    return;
  }
  window.MYPOS_SHOW_MODAL?.('Unlock staff assignment', `<p class="muted small">Google confirms the account; this PIN unlocks the assigned ${safeText(pendingAccess.membership.role)} workspace on this device.</p><div class="field"><label>Staff PIN</label><input id="cloud-pin-entry" type="password" inputmode="numeric" autocomplete="current-password" maxlength="8" autofocus /></div><div id="cloud-pin-error" class="small" style="color:#b42318"></div>`, `<button class="btn btn-quiet" data-action="cloud-signout">Sign out</button><button class="btn btn-primary" data-action="cloud-verify-pin">Unlock</button>`, true);
}
function setPinError(message) {
  const el = document.getElementById('cloud-pin-error');
  if (el) el.textContent = message;
  else notify(message, false);
}
async function resumeAssignedAccess() {
  const access = pendingAccess;
  if (!access || bootstrapInProgress) return;
  bootstrapInProgress = true; cloud.connecting = true; cloud.active = true; cloud.connected = false; updateStatus();
  try {
    const { user, membership, store } = access;
    cloud.user = user; cloud.role = membership.role; cloud.storeId = store.id; cloud.storeName = store.name;
    const draft = currentState();
    draft.settings = { ...(draft.settings || {}), storeId: store.id, storeName: store.name };
    draft.role = membership.role; draft.user = { id: user.id, email: user.email, name: membership.role, role: membership.role };
    draft.page = membership.role === 'Kitchen' ? 'Kitchen' : 'Sell';
    window.MYPOS_STATE = draft;
    window.MYPOS_SYNC_BRIDGE?.connect(syncAdapter());

    let { data: snapshot, error } = await supabase.rpc('get_pos_snapshot', { p_store_id: store.id });
    if (error) throw error;
    // Initialization is a server-side one-time marker; an intentionally empty catalogue is still a valid store.
    if (!snapshot?._cloudMeta?.initialized) {
      if (membership.role === 'Manager') {
        const initEvent = { id: randomUuid(), type: 'initial.snapshot', storeId: store.id, origin: `web-${randomUuid()}`, actor: { role: 'Manager' }, occurredAt: new Date().toISOString(), payload: {} };
        const safe = safeSnapshot(currentState(), 'Manager');
        const { error: initError } = await supabase.rpc('publish_pos_event', { p_event: initEvent, p_snapshot: safe });
        if (initError) throw initError;
      } else {
        throw new Error('Store has not been initialized by a Manager.');
      }
    }
    const canonicalResult = await supabase.rpc('get_pos_snapshot', { p_store_id: store.id });
    if (canonicalResult.error) throw canonicalResult.error;
    if (canonicalResult.data) applyCloudSnapshot(canonicalResult.data, membership, user, store);
    acceptIncoming = true;
    for (const event of queuedIncoming.splice(0)) window.MYPOS_SYNC_BRIDGE?.receive(event);
    cloud.connecting = false; cloud.lastError = '';
    // The successful database read does not by itself prove Realtime is subscribed.
    cloud.connected = await subscriptionReady;
    updateStatus();
    pendingAccess = null;
    if (membership.role === 'Manager') await refreshTeam().catch(() => {});
    await flushOutbox();
    notify(cloud.connected ? `Connected to ${store.name} · ${membership.role}` : `Signed in to ${store.name}; live updates are reconnecting.` , cloud.connected);
  } catch (error) {
    cloud.active = false; cloud.connected = false; cloud.connecting = false; cloud.lastError = error.message || String(error); updateStatus();
    notify(`Cloud access failed: ${cloud.lastError}`, false);
  } finally { bootstrapInProgress = false; }
}
async function enterAssignedStore(session) {
  if (!session?.user || bootstrapInProgress) return;
  if (cloud.active && cloud.user?.id === session.user.id) return;
  if (pendingAccess?.user?.id === session.user.id) return;
  bootstrapInProgress = true; cloud.connecting = true; cloud.lastError = ''; updateStatus();
  try {
    const user = session.user;
    const { error: inviteError } = await supabase.rpc('accept_pos_invites_for_current_user');
    if (inviteError) throw inviteError;
    const assignment = await loadMemberships(user);
    if (!assignment) {
      cloud.user = user; cloud.active = false; cloud.connecting = false; pendingAccess = null;
      showCreateStoreDialog(user); return;
    }
    pendingAccess = { user, membership: assignment.membership, store: assignment.store };
    cloud.user = user; cloud.role = assignment.membership.role; cloud.storeId = assignment.store.id; cloud.storeName = assignment.store.name;
    cloud.active = false; cloud.connected = false;
    const { data: pinStatus, error: pinError } = await supabase.rpc('get_pos_pin_status', { p_store_id: assignment.store.id });
    if (pinError) throw pinError;
    updateStatus();
    if (!pinStatus?.configured || pinStatus?.needsSetup) showPinSetupModal(); else showPinVerifyModal(pinStatus);
  } catch (error) {
    pendingAccess = null; cloud.active = false; cloud.connected = false; cloud.connecting = false; cloud.lastError = error.message || String(error); updateStatus();
    notify(`Cloud connection failed: ${cloud.lastError}`, false);
  } finally { bootstrapInProgress = false; cloud.connecting = false; updateStatus(); }
}
async function verifyPin() {
  if (!pendingAccess) throw new Error('Sign in with the assigned Google account first.');
  const pin = document.getElementById('cloud-pin-entry')?.value?.trim() || '';
  if (!/^\d{4,8}$/.test(pin)) { setPinError('Enter the 4–8 digit PIN.'); return; }
  const { data, error } = await supabase.rpc('verify_pos_staff_pin', { p_store_id: pendingAccess.store.id, p_pin: pin });
  if (error) throw error;
  if (!data?.verified) {
    if (data?.locked) setPinError('Too many attempts. This assignment is temporarily locked; try again after the lock expires.');
    else setPinError(`PIN not correct. ${data?.attemptsRemaining ?? 0} attempt(s) remaining before temporary lock.`);
    return;
  }
  if (window.MYPOS_CLOSE_MODAL) window.MYPOS_CLOSE_MODAL();
  await resumeAssignedAccess();
}
async function setOwnPin() {
  if (!pendingAccess) throw new Error('Sign in with the assigned Google account first.');
  const pin = document.getElementById('cloud-pin-new')?.value?.trim() || '';
  const confirm = document.getElementById('cloud-pin-confirm')?.value?.trim() || '';
  if (!/^\d{4,8}$/.test(pin)) { setPinError('PIN must contain 4–8 digits.'); return; }
  if (pin !== confirm) { setPinError('PIN entries do not match.'); return; }
  const { error } = await supabase.rpc('set_pos_staff_pin', { p_store_id: pendingAccess.store.id, p_user_id: pendingAccess.user.id, p_pin: pin, p_old_pin: null });
  if (error) throw error;
  if (window.MYPOS_CLOSE_MODAL) window.MYPOS_CLOSE_MODAL();
  await resumeAssignedAccess();
}
async function changeOwnPin(oldPin, pin, confirmation) {
  if (!cloud.active) throw new Error('Unlock the cloud account first.');
  if (!/^\d{4,8}$/.test(pin) || pin !== confirmation) throw new Error('New PIN must match and contain 4–8 digits.');
  const { error } = await supabase.rpc('set_pos_staff_pin', { p_store_id: cloud.storeId, p_user_id: cloud.user.id, p_pin: pin, p_old_pin: oldPin || null });
  if (error) throw error;
  notify('Staff PIN updated.');
}
async function setMemberPin(userId, pin) {
  if (!cloud.active || cloud.role !== 'Manager') throw new Error('Manager permission required.');
  if (!/^\d{4,8}$/.test(pin)) throw new Error('PIN must contain 4–8 digits.');
  const { error } = await supabase.rpc('set_pos_staff_pin', { p_store_id: cloud.storeId, p_user_id: userId, p_pin: pin, p_old_pin: null });
  if (error) throw error;
  notify('PIN saved for this staff assignment.');
}
function showCreateStoreDialog(user) {
  const show = window.MYPOS_SHOW_MODAL;
  if (!show) { notify('No store assignment found. A Manager must invite this Google account.', false); return; }
  show('Set up MY POS store', `<p class="muted small">Signed in as ${String(user.email || 'Google account').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]))}. This account will become the first Manager for the new store.</p><div class="field"><label>Business / store name</label><input id="cloud-new-store-name" maxlength="120" value="${String(currentState().settings?.storeName || "Mizzy's Bistro").replace(/&/g,'&amp;').replace(/"/g,'&quot;')}" /></div><div class="sync-banner"><i class="fa-solid fa-shield-halved"></i><div>Products, prices, stock, tables and settings from this browser will be copied as the initial store setup. Unfinished cart, local staff PINs, and personal session data are excluded.</div></div>`, `<button class="btn" data-action="close-modal">Cancel</button><button class="btn btn-primary" data-action="cloud-create-store">Create secure store</button>`, true);
}
async function createStore() {
  if (!cloud.user) throw new Error('Sign in with Google first.');
  const name = document.getElementById('cloud-new-store-name')?.value?.trim() || currentState().settings?.storeName || "Mizzy's Bistro";
  const { data: storeId, error } = await supabase.rpc('create_pos_store', { p_name: name });
  if (error) throw error;
  cloud.storeId = storeId; cloud.storeName = name; cloud.role = 'Manager'; cloud.active = true;
  const s = currentState(); s.settings = { ...(s.settings || {}), storeId, storeName: name }; s.role = 'Manager'; s.user = { id: cloud.user.id, email: cloud.user.email, name: 'Manager', role: 'Manager' }; window.MYPOS_STATE = s;
  const event = { id: randomUuid(), type: 'initial.snapshot', storeId, origin: `web-${randomUuid()}`, actor: { role: 'Manager' }, occurredAt: new Date().toISOString(), payload: {} };
  const snapshot = safeSnapshot(s, 'Manager');
  const { error: initError } = await supabase.rpc('publish_pos_event', { p_event: event, p_snapshot: snapshot });
  if (initError) throw initError;
  if (window.MYPOS_CLOSE_MODAL) window.MYPOS_CLOSE_MODAL();
  cloud.active = false; cloud.connecting = false; await enterAssignedStore((await supabase.auth.getSession()).data.session);
}
async function inviteStaff(email, role) {
  if (!cloud.active || cloud.role !== 'Manager') throw new Error('Only a signed-in Manager can invite staff.');
  const { data, error } = await supabase.rpc('invite_pos_member', { p_store_id: cloud.storeId, p_email: email, p_role: role });
  if (error) throw error;
  await refreshTeam(); return data;
}
async function signInWithGoogle() {
  cloud.lastError = '';
  const redirectTo = `${location.origin}${location.pathname}`;
  const { error } = await supabase.auth.signInWithOAuth({ provider: 'google', options: { redirectTo, queryParams: { prompt: 'select_account' } } });
  if (error) { cloud.lastError = error.message; updateStatus(); throw error; }
}
async function signOut() {
  try { window.MYPOS_CLOSE_MODAL?.(); } catch {}
  try { if (subscription) await supabase.removeChannel(subscription); } catch {}
  subscription = null; acceptIncoming = false; queuedIncoming = [];
  pendingAccess = null; cloud.active = false; cloud.connected = false; cloud.connecting = false; cloud.role = null; cloud.storeId = null; cloud.storeName = null; cloud.user = null; cloud.team = { members: [], invites: [] };
  await supabase.auth.signOut();
  const s = currentState(); s.role = null; s.user = null; s.page = 'Sell'; window.MYPOS_STATE = s; window.MYPOS_PERSIST?.(); window.MYPOS_RENDER?.(); updateStatus();
}
cloud.signInWithGoogle = signInWithGoogle;
cloud.signOut = signOut;
cloud.createStore = createStore;
cloud.inviteStaff = inviteStaff;
cloud.refreshTeam = refreshTeam;
cloud.verifyPin = verifyPin;
cloud.setOwnPin = setOwnPin;
cloud.changeOwnPin = changeOwnPin;
cloud.setMemberPin = setMemberPin;
cloud.status = () => ({ configured: cloud.configured, active: cloud.active, connected: cloud.connected, connecting: cloud.connecting, role: cloud.role, storeId: cloud.storeId, storeName: cloud.storeName, lastError: cloud.lastError, queuedActions: readOutbox().length });

document.addEventListener('click', async event => {
  const el = event.target.closest('[data-action]');
  if (!el) return;
  const action = el.dataset.action;
  if (!['cloud-verify-pin','cloud-save-own-pin','cloud-open-own-pin-change','cloud-save-changed-pin','cloud-open-member-pin','cloud-save-member-pin'].includes(action)) return;
  event.preventDefault(); event.stopImmediatePropagation();
  try {
    if (action === 'cloud-verify-pin') await cloud.verifyPin();
    else if (action === 'cloud-save-own-pin') await cloud.setOwnPin();
    else if (action === 'cloud-open-own-pin-change') {
      window.MYPOS_SHOW_MODAL?.('Change staff PIN', `<p class="muted small">Choose a new 4–8 digit PIN for your current ${safeText(cloud.role)} assignment.</p><div class="field"><label>Current PIN</label><input id="cloud-own-old-pin" type="password" inputmode="numeric" maxlength="8" autocomplete="current-password" /></div><div class="field"><label>New PIN</label><input id="cloud-own-new-pin" type="password" inputmode="numeric" maxlength="8" autocomplete="new-password" /></div><div class="field"><label>Confirm new PIN</label><input id="cloud-own-confirm-pin" type="password" inputmode="numeric" maxlength="8" autocomplete="new-password" /></div>`, `<button class="btn" data-action="close-modal">Cancel</button><button class="btn btn-primary" data-action="cloud-save-changed-pin">Update PIN</button>`, true);
    } else if (action === 'cloud-save-changed-pin') {
      await cloud.changeOwnPin(document.getElementById('cloud-own-old-pin')?.value?.trim(), document.getElementById('cloud-own-new-pin')?.value?.trim(), document.getElementById('cloud-own-confirm-pin')?.value?.trim());
      window.MYPOS_CLOSE_MODAL?.();
    } else if (action === 'cloud-open-member-pin') {
      if (!cloud.active || cloud.role !== 'Manager') throw new Error('Manager permission required.');
      window.__cloudPinTargetUserId = el.dataset.userId;
      window.MYPOS_SHOW_MODAL?.('Set staff assignment PIN', `<p class="muted small">The assigned staff member can change this PIN after signing in. Only a salted hash is stored on the server.</p><div class="field"><label>New PIN</label><input id="cloud-member-pin" type="password" inputmode="numeric" maxlength="8" autocomplete="new-password" /></div><div class="field"><label>Confirm PIN</label><input id="cloud-member-pin-confirm" type="password" inputmode="numeric" maxlength="8" autocomplete="new-password" /></div>`, `<button class="btn" data-action="close-modal">Cancel</button><button class="btn btn-primary" data-action="cloud-save-member-pin">Save PIN</button>`, true);
    } else if (action === 'cloud-save-member-pin') {
      const pin = document.getElementById('cloud-member-pin')?.value?.trim() || '';
      const confirm = document.getElementById('cloud-member-pin-confirm')?.value?.trim() || '';
      if (pin !== confirm) throw new Error('PIN entries do not match.');
      await cloud.setMemberPin(window.__cloudPinTargetUserId, pin);
      window.__cloudPinTargetUserId = null;
      window.MYPOS_CLOSE_MODAL?.();
    }
  } catch (error) {
    setPinError(error.message || 'PIN action failed.');
  }
}, true);

let authListenerInitialized = false;
function initializeCloudAuth() {
  if (authListenerInitialized) return;
  authListenerInitialized = true;
  supabase.auth.onAuthStateChange((event, session) => {
    if (event === 'SIGNED_OUT') {
      const wasCloudSession = cloud.active || !!cloud.user;
      try { if (subscription) supabase.removeChannel(subscription); } catch {}
      subscription = null; acceptIncoming = false; queuedIncoming = [];
      pendingAccess = null; cloud.active = false; cloud.connected = false; cloud.connecting = false; cloud.role = null; cloud.storeId = null; cloud.storeName = null; cloud.user = null; updateStatus();
      if (wasCloudSession || !isPreviewMode) {
        const s = currentState(); s.role = null; s.user = null; s.page = 'Sell';
        window.MYPOS_STATE = s; window.MYPOS_PERSIST?.(); window.MYPOS_RENDER?.();
      }
      return;
    }
    if (session && ['SIGNED_IN','INITIAL_SESSION','TOKEN_REFRESHED','USER_UPDATED'].includes(event)) setTimeout(() => enterAssignedStore(session), 0);
  });
  supabase.auth.getSession().then(({ data }) => { if (data.session) enterAssignedStore(data.session); else updateStatus(); });
}
window.addEventListener('mypos:ready', initializeCloudAuth, { once: true });
if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', initializeCloudAuth, { once: true });
else initializeCloudAuth();
window.addEventListener('online', () => { if (cloud.active) flushOutbox(); });
