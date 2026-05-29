"""
Update Polymarket v2 data:

  1. Pull markets (Gamma keyset API)  ─┐
                                       ├─ in parallel
  2. Pull order events (Polygon RPC)  ─┘
  3. Process orders → labeled trades

Both fetchers are independently resumable; subsequent runs only pull deltas.
"""

import signal
import threading
from concurrent.futures import ThreadPoolExecutor

from update_utils.update_markets import update_markets
from update_utils.update_chain import update_chain
from update_utils.process_live import process_live


def _run(name, fn):
    print(f"[{name}] starting")
    try:
        fn()
        print(f"[{name}] done")
    except Exception as e:
        print(f"[{name}] FAILED: {e}")
        raise


if __name__ == "__main__":
    stop = threading.Event()

    def _on_sigint(sig, frame):
        print("\nCtrl-C received — stopping after current batch …", flush=True)
        stop.set()

    signal.signal(signal.SIGINT, _on_sigint)

    with ThreadPoolExecutor(max_workers=2) as ex:
        f_markets = ex.submit(_run, "markets", update_markets)
        f_chain = ex.submit(_run, "chain", lambda: update_chain(stop))
        # Surface exceptions; both must succeed before processing.
        try:
            f_markets.result()
            f_chain.result()
        except Exception:
            pass

    if stop.is_set():
        print("Interrupted — run again to resume.")
    else:
        print("\n[process] joining orders ↔ markets")
        process_live()
