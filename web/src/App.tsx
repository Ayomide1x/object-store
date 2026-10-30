import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { ApiError, getJSON, type Cluster, type ObjectList as ListPage, type ObjectSummary, type Stat } from "./api";
import { ActivityFeed } from "./components/ActivityFeed";
import { DeleteDialog } from "./components/DeleteDialog";
import { Header, type PollState } from "./components/Header";
import { ObjectDetail } from "./components/ObjectDetail";
import { ObjectList } from "./components/ObjectList";
import { PlacementGrid } from "./components/PlacementGrid";
import { Uploader } from "./components/Uploader";
import { deriveGrid } from "./grid";
import { browserUnreachable, objectPath } from "./keys";
import { startPoller } from "./poller";
import { applyTheme, loadTheme, type ThemeChoice } from "./theme";

const PAGE = 100;

function messageOf(e: unknown): string {
  if (e instanceof ApiError) {
    return e.status === 0 ? "The gateway didn't answer." : `${e.message}.`;
  }
  return "The request failed.";
}

function initialKey(): string | null {
  const k = new URLSearchParams(window.location.search).get("key");
  return k === null || k === "" || browserUnreachable(k) ? null : k;
}

export function App() {
  const [theme, setTheme] = useState<ThemeChoice>(loadTheme);
  const [cluster, setCluster] = useState<Cluster | null>(null);
  const [poll, setPoll] = useState<PollState>({ updatedAt: null, error: null, retryInMs: 0 });
  const [now, setNow] = useState(Date.now);

  const [selected, setSelected] = useState<string | null>(initialKey);
  const [stat, setStat] = useState<Stat | null>(null);
  const [statError, setStatError] = useState<string | null>(null);
  const [deleting, setDeleting] = useState(false);
  const [notice, setNotice] = useState<string | null>(null);

  const [prefix, setPrefix] = useState("");
  const [objects, setObjects] = useState<ObjectSummary[]>([]);
  const [cursor, setCursor] = useState("");
  const [listLoading, setListLoading] = useState(false);
  const [listError, setListError] = useState<string | null>(null);
  const [listVersion, setListVersion] = useState(0);
  // Wait for a pause in typing before listing a new prefix; otherwise list at once.
  const [typing, setTyping] = useState(false);
  // Only the latest list request may update the list: an older one (say, for
  // the previous prefix) can answer after it.
  const listSeq = useRef(0);

  useEffect(() => applyTheme(theme), [theme]);

  // The cluster, every 2 s while the tab is visible.
  useEffect(
    () =>
      startPoller({
        load: (signal) => getJSON<Cluster>("/cluster", signal),
        onData: (data) => {
          setCluster(data as Cluster);
          setNow(Date.now());
          setPoll({ updatedAt: Date.now(), error: null, retryInMs: 0 });
        },
        onError: (e, retryInMs) => {
          setNow(Date.now());
          setPoll((p) => ({ ...p, error: e instanceof ApiError && e.status === 0 ? "The gateway didn't answer." : `The cluster is unreachable: ${messageOf(e)}`, retryInMs }));
        },
      }),
    [],
  );

  // The selected object's placement, every 2 s while it is selected.
  useEffect(() => {
    const url = new URL(window.location.href);
    if (selected === null) {
      url.searchParams.delete("key");
    } else {
      url.searchParams.set("key", selected);
    }
    window.history.replaceState(null, "", url);
    if (selected === null) {
      return;
    }
    return startPoller({
      load: (signal) => getJSON<Stat>(`${objectPath(selected)}/stat`, signal),
      onData: (data) => {
        setStat(data as Stat);
        setStatError(null);
      },
      onError: (e) => {
        if (e instanceof ApiError && e.status === 404) {
          setStat(null);
          setStatError("This object no longer exists: its key was deleted.");
        } else {
          setStatError(`Couldn't load this object: ${messageOf(e)}`);
        }
      },
    });
  }, [selected]);

  // The object list: the first page whenever the prefix changes (after a
  // short pause in typing) or a refresh is asked for; more pages on request.
  const loadPage = useCallback(
    async (after: string, append: boolean) => {
      const seq = ++listSeq.current;
      setListLoading(true);
      try {
        const q = new URLSearchParams({ prefix, limit: String(PAGE) });
        if (after !== "") {
          q.set("start_after", after);
        }
        const page = await getJSON<ListPage>(`/objects?${q.toString()}`);
        if (seq !== listSeq.current) {
          return;
        }
        setObjects((prev) => (append ? [...prev, ...page.objects] : page.objects));
        setCursor(page.next_start_after);
        setListError(null);
      } catch (e) {
        if (seq === listSeq.current) {
          setListError(`Couldn't list objects: ${messageOf(e)}`);
        }
      } finally {
        if (seq === listSeq.current) {
          setListLoading(false);
        }
      }
    },
    [prefix],
  );

  useEffect(() => {
    const t = setTimeout(() => void loadPage("", false), typing ? 250 : 0);
    return () => clearTimeout(t);
  }, [loadPage, listVersion, typing]);

  const refreshList = () => {
    setTyping(false);
    setListVersion((v) => v + 1);
  };

  function select(key: string | null) {
    setSelected(key);
    setStat(null);
    setStatError(null);
  }

  const grid = useMemo(() => (stat === null ? null : deriveGrid(stat, cluster?.nodes ?? [])), [stat, cluster]);

  return (
    <div className="app">
      <a className="skip" href="#placement">
        Skip to the placement view
      </a>
      <Header cluster={cluster} poll={poll} now={now} theme={theme} onTheme={setTheme} />
      <aside className="rail">
        <Uploader
          prefix={prefix}
          onSettled={(key) => {
            refreshList();
            select(key);
          }}
        />
        {notice !== null ? (
          <p className="notice" data-tone="ok" role="status">
            {notice}
          </p>
        ) : null}
        <ObjectList
          prefix={prefix}
          onPrefix={(p) => {
            // The old prefix's keys and cursor no longer apply.
            listSeq.current++;
            setObjects([]);
            setCursor("");
            setTyping(true);
            setPrefix(p);
          }}
          objects={objects}
          hasMore={cursor !== ""}
          loading={listLoading}
          error={listError}
          selected={selected}
          onSelect={(k) => {
            setNotice(null);
            select(k);
          }}
          onMore={() => void loadPage(cursor, true)}
          onRefresh={refreshList}
        />
      </aside>
      <main className="main" id="placement" tabIndex={-1}>
        {stat !== null ? <ObjectDetail stat={stat} onDelete={() => setDeleting(true)} /> : null}
        {statError !== null ? (
          <p className="notice" data-tone="error" role="alert">
            {statError}
          </p>
        ) : null}
        <PlacementGrid
          nodes={cluster?.nodes ?? []}
          grid={grid}
          now={now}
          stale={poll.error !== null}
          empty={
            selected === null
              ? "Choose an object to see where its chunks are stored."
              : statError === null
                ? "Loading…"
                : " "
          }
        />
      </main>
      <aside className="side">
        <ActivityFeed cluster={cluster} />
      </aside>
      {deleting && stat !== null ? (
        <DeleteDialog
          objectKey={stat.key}
          onClose={() => setDeleting(false)}
          onDeleted={() => {
            setNotice(`Deleted ${stat.key}.`);
            select(null);
            refreshList();
          }}
        />
      ) : null}
    </div>
  );
}
