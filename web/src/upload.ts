// Uploads go through XMLHttpRequest, not fetch: only XHR reports upload
// progress. The File is the body, so the browser streams it from disk and
// sets Content-Length itself.
import { objectPath } from "./keys";
import { parseRetryAfter, type UploadOutcome } from "./uploadMessages";

export interface Progress {
  sent: number;
  total: number;
}

export interface RunningUpload {
  done: Promise<UploadOutcome>;
  abort: () => void;
}

export function startUpload(key: string, file: File, onProgress: (p: Progress) => void): RunningUpload {
  const xhr = new XMLHttpRequest();
  // upload.onload fires once the browser has sent the whole body.
  let sentAll = false;
  const done = new Promise<UploadOutcome>((resolve) => {
    xhr.upload.onprogress = (e) => onProgress({ sent: e.loaded, total: e.lengthComputable ? e.total : file.size });
    xhr.upload.onload = () => {
      sentAll = true;
      onProgress({ sent: file.size, total: file.size });
    };
    xhr.onload = () => {
      if (xhr.status >= 200 && xhr.status < 300) {
        const body = JSON.parse(xhr.responseText) as { object_id: string; size_bytes: number };
        resolve({ kind: "stored", objectId: body.object_id, sizeBytes: body.size_bytes });
        return;
      }
      let code = "";
      let message = xhr.statusText;
      try {
        const body = JSON.parse(xhr.responseText) as { code?: string; error?: string };
        code = body.code ?? "";
        message = body.error ?? message;
      } catch {
        // not JSON: keep the status text
      }
      resolve({
        kind: "http",
        status: xhr.status,
        code,
        message,
        retryAfterSeconds: parseRetryAfter(xhr.getResponseHeader("Retry-After")),
      });
    };
    xhr.onerror = () => resolve({ kind: "dropped", sentAll });
    xhr.onabort = () => resolve({ kind: "dropped", sentAll });
  });
  xhr.open("PUT", objectPath(key));
  xhr.setRequestHeader("Content-Type", "application/octet-stream");
  xhr.send(file);
  return { done, abort: () => xhr.abort() };
}
