// The theme choice is a per-viewer convenience: kept in localStorage when it
// is available, never required.
export type ThemeChoice = "system" | "light" | "dark";

const STORAGE_KEY = "objectstore.theme";

export function loadTheme(): ThemeChoice {
  try {
    const v = localStorage.getItem(STORAGE_KEY);
    return v === "light" || v === "dark" ? v : "system";
  } catch {
    return "system";
  }
}

export function applyTheme(choice: ThemeChoice) {
  const root = document.documentElement;
  if (choice === "system") {
    delete root.dataset.theme;
  } else {
    root.dataset.theme = choice;
  }
  try {
    localStorage.setItem(STORAGE_KEY, choice);
  } catch {
    // private window or storage blocked: the choice lasts for this page only
  }
}
