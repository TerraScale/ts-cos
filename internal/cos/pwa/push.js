// Turns notifications on or off for this browser. The settings panel calls window.cosPush.refresh()
// on every render; the handler is assigned with onclick so a re-rendered panel never stacks listeners.
const iOS = /iPhone|iPad/.test(navigator.userAgent);
const standalone = navigator.standalone === true || matchMedia('(display-mode: standalone)').matches;

function line(text) {
  document.getElementById('push-device').textContent = text;
}

const turnOn = attempt('Could not turn on notifications', async () => {
  if ((await Notification.requestPermission()) !== 'granted') return line('Notifications were not allowed for this site');
  await subscribe(await navigator.serviceWorker.ready, await keyText());
  await refresh();
});

const turnOff = attempt('Could not turn off notifications', async () => {
  const sub = await (await navigator.serviceWorker.ready).pushManager.getSubscription();
  if (sub) await unsubscribe(sub);
  await refresh();
});

// toggle sets the one device button; null action leaves it disabled (cos.css buttons ignore the hidden attribute).
function toggle(label, action) {
  const button = document.getElementById('push-toggle');
  button.textContent = label;
  button.disabled = !action;
  button.onclick = action;
}

function attempt(failed, action) {
  return async () => {
    try {
      await action();
    } catch (err) {
      line(`${failed}: ${err.message}`);
    }
  };
}

async function refresh() {
  toggle('Turn on for this device', null);
  if (iOS && !standalone) return line('On iPhone and iPad, add COS to the Home Screen first, then open it from there');
  if (!('serviceWorker' in navigator) || !('PushManager' in window) || !('Notification' in window)) return line('This browser does not support notifications');
  if (Notification.permission === 'denied') return line("Notifications are blocked in this browser's site settings");
  const reg = await navigator.serviceWorker.ready;
  const sub = await reg.pushManager.getSubscription();
  if (!sub) {
    toggle('Turn on for this device', turnOn);
    return line('Notifications are off for this device');
  }
  const key = await keyText();
  if (Notification.permission === 'granted' && !sameKey(sub, key)) {
    // The cluster's VAPID key changed: move this device to the new key without asking again.
    await unsubscribe(sub);
    await subscribe(reg, key);
    return refresh();
  }
  toggle('Turn off for this device', turnOff);
  line('Notifications are on for this device');
}

async function keyText() {
  const res = await fetch('/push/key', {credentials: 'same-origin'});
  if (!res.ok) throw new Error(await res.text());
  return res.text();
}

async function subscribe(reg, key) {
  const sub = await reg.pushManager.subscribe({userVisibleOnly: true, applicationServerKey: keyBytes(key)});
  const res = await fetch('/push/subscribe', {
    method: 'POST',
    credentials: 'same-origin',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(sub.toJSON()),
  });
  if (!res.ok) {
    await sub.unsubscribe();
    throw new Error(await res.text());
  }
}

async function unsubscribe(sub) {
  await fetch('/push/unsubscribe', {
    method: 'POST',
    credentials: 'same-origin',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({endpoint: sub.endpoint}),
  });
  await sub.unsubscribe();
}

// keyBytes decodes the base64url VAPID public key the server serves.
function keyBytes(key) {
  const b64 = key.replace(/-/g, '+').replace(/_/g, '/');
  return Uint8Array.from(atob(b64.padEnd(Math.ceil(b64.length / 4) * 4, '=')), (c) => c.charCodeAt(0));
}

function sameKey(sub, key) {
  const have = sub.options?.applicationServerKey;
  if (!have) return false;
  const b64 = btoa(String.fromCharCode(...new Uint8Array(have)));
  return b64.replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '') === key;
}

window.cosPush = {
  refresh: () => {
    return refresh().catch((err) => line(`Could not check notifications: ${err.message}`));
  },
};
window.cosPush.refresh();
