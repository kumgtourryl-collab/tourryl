// ============================================================
// TouRryl Service Worker — KUMG
// ============================================================
const CACHE_NAME = "tourryl-v3";
const CORE_ASSETS = [
  "/tourryl.html",
  "/manifest.webmanifest"
];

// ================= INSTALL =================
self.addEventListener("install", (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => {
      return cache.addAll(CORE_ASSETS).catch(() => {});
    }).then(() => self.skipWaiting())
  );
});

// ================= ACTIVATE =================
self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys().then((keys) => {
      return Promise.all(
        keys.filter(k => k !== CACHE_NAME).map(k => caches.delete(k))
      );
    }).then(() => self.clients.claim())
  );
});

// ================= FETCH =================
// Network-first strategy. If network fails, fall back to cache.
// NEVER intercept API calls, uploads, or WebSocket.
self.addEventListener("fetch", (event) => {
  const url = new URL(event.request.url);

  // Never intercept anything except our own domain's static assets
  if (event.request.method !== "GET") return;
  if (url.origin !== self.location.origin) return;

  // Skip API endpoints, uploads, websockets, docs
  const skipPrefixes = [
    "/auth/", "/me", "/feed/", "/posts", "/comments", "/users",
    "/listings", "/offers", "/orders", "/notifications", "/badges",
    "/conversations", "/messages", "/stories", "/hashtags",
    "/search", "/verify", "/devices", "/reports", "/settings",
    "/admin/", "/upload", "/uploads/", "/ws", "/stripe/",
    "/privacy", "/health", "/docs", "/openapi.json"
  ];
  if (skipPrefixes.some(p => url.pathname.startsWith(p))) return;

  // Serve the app shell
  event.respondWith(
    fetch(event.request)
      .then((response) => {
        // Cache successful responses for offline
        if (response && response.status === 200) {
          const copy = response.clone();
          caches.open(CACHE_NAME).then((cache) => {
            cache.put(event.request, copy).catch(() => {});
          });
        }
        return response;
      })
      .catch(() => {
        // Offline — try cache
        return caches.match(event.request).then((cached) => {
          if (cached) return cached;
          // Fallback to app shell for navigation requests
          if (event.request.mode === "navigate") {
            return caches.match("/tourryl.html");
          }
          return new Response("Offline", { status: 503, statusText: "Offline" });
        });
      })
  );
});

// ================= NOTIFICATIONS =================
self.addEventListener("message", (event) => {
  const d = event.data || {};
  if (d.type === "notify") {
    self.registration.showNotification(d.title || "TouRryl", {
      body: d.body || "",
      icon: "/uploads/logo.png",
      badge: "/uploads/logo.png",
      tag: d.tag || "tourryl",
      data: d.data || {},
      vibrate: [100, 50, 100]
    });
  }
});

// ================= NOTIFICATION CLICK =================
self.addEventListener("notificationclick", (event) => {
  event.notification.close();
  event.waitUntil(
    self.clients.matchAll({ type: "window", includeUncontrolled: true })
      .then((clientList) => {
        // Focus an existing tab if open
        for (const client of clientList) {
          if (client.url.includes("tourryl") && "focus" in client) {
            return client.focus();
          }
        }
        // Otherwise open a new one
        if (self.clients.openWindow) {
          return self.clients.openWindow("/tourryl.html");
        }
      })
  );
});

// ================= PUSH (future) =================
self.addEventListener("push", (event) => {
  let data = { title: "TouRryl", body: "New activity" };
  try {
    if (event.data) data = event.data.json();
  } catch (e) {}
  event.waitUntil(
    self.registration.showNotification(data.title || "TouRryl", {
      body: data.body || "",
      icon: "/uploads/logo.png",
      badge: "/uploads/logo.png",
      data: data.data || {}
    })
  );
});