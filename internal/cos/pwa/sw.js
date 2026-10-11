// __VERSION__ is replaced at serve time with a hash of this file and offline.html.
const SHELL = 'cos-shell-__VERSION__';
const PRECACHE = ['/pwa/offline', '/manifest.webmanifest', '/static/icon-192.png', '/static/icon.svg'];
const OFFLINE = '/pwa/offline';
const DATASTAR = 'https://cdn.jsdelivr.net/gh/starfederation/datastar@v1.0.4/bundles/datastar.js';
const LINK_ORIGIN = 'https://git.terrascale.tech';

self.addEventListener('install', (event) => {
  event.waitUntil(caches.open(SHELL).then((cache) => cache.addAll(PRECACHE)).then(() => self.skipWaiting()));
});

self.addEventListener('activate', (event) => {
  event.waitUntil((async () => {
    for (const name of await caches.keys()) {
      if (name !== SHELL) await caches.delete(name);
    }
    if (self.registration.navigationPreload) await self.registration.navigationPreload.enable();
    await self.clients.claim();
  })());
});

self.addEventListener('fetch', (event) => {
  const req = event.request;
  if (req.method !== 'GET') return;
  if (req.mode === 'navigate') {
    // Pages are never cached: a stale page would look like live data.
    event.respondWith((async () => {
      try {
        return (await event.preloadResponse) || (await fetch(req));
      } catch {
        return caches.match(OFFLINE);
      }
    })());
    return;
  }
  const url = new URL(req.url);
  const shell = url.origin === self.location.origin && (url.pathname.startsWith('/static/') || url.pathname.startsWith('/pwa/'));
  if (shell || req.url === DATASTAR) event.respondWith(cacheFirst(req));
});

async function cacheFirst(req) {
  const cache = await caches.open(SHELL);
  const hit = await cache.match(req);
  if (hit) return hit;
  const res = await fetch(req);
  // During a rolling deploy /static/ may answer another node's hash with an uncached file; keep only immutable ones.
  const keep = !new URL(req.url).pathname.startsWith('/static/') || (res.headers.get('Cache-Control') || '').includes('immutable');
  if (res.ok && keep) await cache.put(req, res.clone());
  return res;
}

self.addEventListener('push', (event) => {
  event.waitUntil((async () => {
    const msg = event.data.json();
    if (msg.category === 'chat') {
      const windows = await self.clients.matchAll({type: 'window', includeUncontrolled: true});
      // Browsers let a push skip its notification while a page of this origin is visible.
      if (windows.some((w) => w.focused && w.visibilityState === 'visible')) return;
    }
    await self.registration.showNotification(msg.title, {
      body: msg.body,
      tag: msg.tag,
      renotify: !!msg.tag,
      requireInteraction: !!msg.urgent,
      icon: '/static/icon-192.png',
      data: {url: msg.url},
    });
  })());
});

self.addEventListener('notificationclick', (event) => {
  event.notification.close();
  const url = safeURL(event.notification.data?.url);
  event.waitUntil((async () => {
    const windows = await self.clients.matchAll({type: 'window', includeUncontrolled: true});
    const win = windows[0];
    if (win) {
      await win.focus();
      // WindowClient.navigate rejects cross-origin URLs, so those open a new window instead.
      if (new URL(url, self.location.origin).origin === self.location.origin) return win.navigate(url);
    }
    return self.clients.openWindow(url);
  })());
});

self.addEventListener('pushsubscriptionchange', (event) => {
  if (!event.oldSubscription) return;
  event.waitUntil((async () => {
    const sub = await self.registration.pushManager.subscribe(event.oldSubscription.options);
    await fetch('/push/subscribe', {
      method: 'POST',
      credentials: 'same-origin',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(sub.toJSON()),
    });
  })());
});

function safeURL(raw) {
  try {
    const url = new URL(raw || '/', self.location.origin);
    if (url.origin === self.location.origin || url.origin === LINK_ORIGIN) return url.href;
  } catch {}
  return '/';
}
