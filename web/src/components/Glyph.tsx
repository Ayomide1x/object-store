// State glyphs as inline SVG, so they look the same in every font. Always
// decorative (aria-hidden): every glyph is shown next to its word.

export type GlyphKind = "alive" | "suspect" | "dead" | "unknown" | "healthy" | "repair" | "bad" | "absent" | "warn";

export function Glyph({ kind }: { kind: GlyphKind }) {
  return (
    <svg className={`glyph glyph-${kind}`} viewBox="0 0 16 16" width="16" height="16" aria-hidden="true" focusable="false">
      {shape(kind)}
    </svg>
  );
}

function shape(kind: GlyphKind) {
  switch (kind) {
    case "alive":
      return <circle cx="8" cy="8" r="5.5" fill="currentColor" />;
    case "suspect":
      return (
        <>
          <circle cx="8" cy="8" r="5" fill="none" stroke="currentColor" strokeWidth="1.5" />
          <path d="M8 3a5 5 0 0 1 0 10z" fill="currentColor" />
        </>
      );
    case "dead":
      return <circle cx="8" cy="8" r="5" fill="none" stroke="currentColor" strokeWidth="1.5" />;
    case "unknown":
      return (
        <path
          d="M5.5 6a2.5 2.5 0 1 1 3.4 2.3c-.6.3-.9.7-.9 1.3V10M8 12.4v.1"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.6"
          strokeLinecap="round"
        />
      );
    case "healthy":
      return <rect x="3" y="3" width="10" height="10" rx="1.5" fill="currentColor" />;
    case "repair":
      return <path d="M8 1.8 14.2 8 8 14.2 1.8 8z" fill="currentColor" />;
    case "bad":
      return <path d="M4 4l8 8M12 4l-8 8" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />;
    case "absent":
      return (
        <rect x="3.5" y="3.5" width="9" height="9" rx="1.5" fill="none" stroke="currentColor" strokeWidth="1" strokeDasharray="1.5 1.5" />
      );
    case "warn":
      return (
        <>
          <path d="M8 2 14.5 13.5h-13z" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinejoin="round" />
          <path d="M8 6.5v3M8 11.6v.1" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
        </>
      );
  }
}
