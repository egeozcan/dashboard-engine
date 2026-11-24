try {
  var meta = document.querySelector('meta[name="color-scheme"]');
  if (meta) {
    meta.content = localStorage.getItem("color-scheme") || "light dark";
  }
} catch (_) {}
