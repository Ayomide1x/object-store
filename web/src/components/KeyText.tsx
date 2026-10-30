// A key is text a user typed, possibly hostile. It is only ever rendered as a
// React text child, never as markup (lint forbids the alternatives).
export function KeyText({ value }: { value: string }) {
  return <span className="key">{value}</span>;
}
