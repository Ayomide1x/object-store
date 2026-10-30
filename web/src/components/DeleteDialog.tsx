import { useEffect, useRef, useState } from "react";
import { ApiError, deleteObject } from "../api";
import { objectPath } from "../keys";
import { KeyText } from "./KeyText";

interface Props {
  objectKey: string;
  onDeleted: () => void;
  onClose: () => void;
}

// A native modal dialog: the browser traps focus, closes it on Esc and puts
// focus back on the button that opened it.
export function DeleteDialog({ objectKey, onDeleted, onClose }: Props) {
  const ref = useRef<HTMLDialogElement>(null);
  const keep = useRef<HTMLButtonElement>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    const d = ref.current;
    if (d && !d.open) {
      d.showModal();
      keep.current?.focus();
    }
  }, []);

  async function confirm() {
    setBusy(true);
    setError(null);
    try {
      await deleteObject(objectPath(objectKey));
      ref.current?.close();
      onDeleted();
    } catch (e) {
      setError(e instanceof ApiError ? `Not deleted: ${e.message}` : "Not deleted: the request failed.");
      setBusy(false);
    }
  }

  return (
    <dialog ref={ref} className="dialog" aria-labelledby="delete-title" onClose={onClose}>
      <h2 id="delete-title">
        Delete <KeyText value={objectKey} />?
      </h2>
      <p>It disappears from reads immediately; its chunks are removed in the background.</p>
      {error !== null ? (
        <p className="notice" data-tone="error" role="alert">
          {error}
        </p>
      ) : null}
      <div className="dialog-actions">
        <button type="button" ref={keep} onClick={() => ref.current?.close()}>
          Keep it
        </button>
        <button type="button" className="danger" disabled={busy} onClick={() => void confirm()}>
          Delete object
        </button>
      </div>
    </dialog>
  );
}
