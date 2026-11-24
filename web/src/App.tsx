import React, { useEffect, useState } from "react";
import { apiFetch, setCsrfToken, type MeResponse } from "./lib/api";
import { getOrCreateClientId } from "./lib/clientState";
import { DashboardView } from "./views/DashboardView";
import { GraphEditorView } from "./views/GraphEditorView";
import { PackageStudioView } from "./views/PackageStudioView";
import { DiagnosticsView } from "./views/DiagnosticsView";

type AppView = "dashboard" | "graph" | "studio" | "diagnostics";

export function applyColorScheme(pref: "system" | "light" | "dark") {
  const meta = document.querySelector<HTMLMetaElement>('meta[name="color-scheme"]');
  const val = pref === "system" ? "light dark" : pref;
  if (meta) meta.content = val;
  try {
    if (pref === "system") {
      localStorage.removeItem("color-scheme");
    } else {
      localStorage.setItem("color-scheme", pref);
    }
  } catch {
    // ignore
  }
}

export const App: React.FC = () => {
  const [me, setMe] = useState<MeResponse | null>(null);
  const [checking, setChecking] = useState(true);
  const [view, setView] = useState<AppView>("dashboard");
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [showPw, setShowPw] = useState(false);
  const [loginErr, setLoginErr] = useState<string | null>(null);
  const [scheme, setScheme] = useState<"system" | "light" | "dark">(() => {
    try {
      const s = localStorage.getItem("color-scheme");
      return s === "light" || s === "dark" ? s : "system";
    } catch {
      return "system";
    }
  });
  const [clientId] = useState(() => getOrCreateClientId());

  useEffect(() => {
    const onStorage = (ev: StorageEvent) => {
      if (ev.key === "color-scheme") {
        const next = ev.newValue === "light" || ev.newValue === "dark" ? ev.newValue : "system";
        setScheme(next);
        applyColorScheme(next);
      }
    };
    window.addEventListener("storage", onStorage);
    return () => window.removeEventListener("storage", onStorage);
  }, []);

  useEffect(() => {
    apiFetch<MeResponse>("/api/auth/me")
      .then((res) => {
        if (res.csrf_token) setCsrfToken(res.csrf_token);
        setMe(res);
      })
      .catch(() => setMe(null))
      .finally(() => setChecking(false));
  }, []);

  const handleLogin = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoginErr(null);
    try {
      const res = await apiFetch<{ username: string; principal_kind: string; csrf_token: string }>(
        "/api/auth/login",
        {
          method: "POST",
          body: JSON.stringify({ username, password }),
        }
      );
      setCsrfToken(res.csrf_token);
      setMe({
        authenticated: true,
        principal_id: res.username,
        principal_kind: res.principal_kind,
        csrf_token: res.csrf_token,
      });
      setPassword("");
    } catch (err) {
      setLoginErr(String(err));
    }
  };

  const handleLogout = async () => {
    await apiFetch("/api/auth/logout", { method: "POST" });
    setCsrfToken("");
    setUsername("");
    setPassword("");
    setMe(null);
  };

  if (checking) {
    return <div className="login-shell">Loading dash...</div>;
  }

  if (!me) {
    return (
      <main className="login-shell">
        <div className="card login-card">
          <h1 style={{ marginTop: 0, fontSize: 22 }}>dash Sign In</h1>
          <p style={{ color: "var(--text-muted)", fontSize: 13 }}>
            Self-hosted Universal Dashboard Engine
          </p>
          {loginErr && <div className="badge danger" style={{ marginBottom: 12 }}>{loginErr}</div>}
          <form className="col" onSubmit={(e) => void handleLogin(e)}>
            <label htmlFor="dash-username">Username</label>
            <input
              id="dash-username"
              name="username"
              type="text"
              placeholder="admin"
              autoComplete="username"
              spellCheck={false}
              autoCapitalize="none"
              autoCorrect="off"
              required
              value={username}
              onChange={(e) => setUsername(e.target.value)}
            />
            <label htmlFor="dash-password">Password</label>
            <input
              id="dash-password"
              name="password"
              type={showPw ? "text" : "password"}
              autoComplete="current-password"
              required
              value={password}
              onChange={(e) => setPassword(e.target.value)}
            />
            <label className="row" style={{ fontSize: 12 }}>
              <input
                type="checkbox"
                checked={showPw}
                onChange={(e) => setShowPw(e.target.checked)}
              />
              <span>Show password</span>
            </label>
            <button type="submit" className="btn primary" style={{ marginTop: 6 }}>
              Sign In
            </button>
          </form>
        </div>
      </main>
    );
  }

  return (
    <div className="app-shell">
      <header className="topbar">
        <div className="brand">
          <span>dash</span>
          <span className="badge">{me.principal_kind}: {me.principal_id}</span>
          <span className="badge" title="Browser profile Client ID">Client: {clientId.slice(0, 12)}</span>
        </div>

        <nav className="nav-links" aria-label="Primary sections">
          {(
            [
              ["dashboard", "Dashboards"],
              ["graph", "Dependency Graph"],
              ["studio", "Package Studio"],
              ["diagnostics", "Execution & Security"],
            ] as const
          ).map(([id, label]) => (
            <button
              key={id}
              type="button"
              aria-current={view === id ? "page" : undefined}
              onClick={() => setView(id)}
            >
              {label}
            </button>
          ))}
        </nav>

        <div className="row">
          <label className="row" style={{ fontSize: 12 }}>
            <span className="sr-only">Color scheme</span>
            <select
              aria-label="Color scheme"
              value={scheme}
              onChange={(e) => {
                const next = e.target.value as "system" | "light" | "dark";
                setScheme(next);
                applyColorScheme(next);
              }}
            >
              <option value="system">Theme: System</option>
              <option value="light">Theme: Light</option>
              <option value="dark">Theme: Dark</option>
            </select>
          </label>
          <button type="button" className="btn" onClick={() => void handleLogout()}>
            Sign Out
          </button>
        </div>
      </header>

      <main className="main-content">
        {view === "dashboard" && <DashboardView />}
        {view === "graph" && <GraphEditorView />}
        {view === "studio" && <PackageStudioView />}
        {view === "diagnostics" && <DiagnosticsView />}
      </main>
    </div>
  );
};

export default App;
