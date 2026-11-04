# SURGICAL PATCH SET — Signal Pipeline (`5_wake`, `asset_signal_stack`, `position_anchors`, scraper, evaluator)
# Repo: kbsingh1399/Trading_2 @ main (commit dac2218)

---

## PATCH W1 — `5_wake_on_footprint_signal.py`: bar-variant-aware clustering (Range 10 ≠ Range 200 ≠ Vol 100K ≠ Vol 250K)

**WHY (severity: HIGH):** Signals are grouped by `(symbol, bar_type, time_ms)`. `bar_type`
is only `"range"` / `"vol"` — the variant (Range 10, Range 200, 100K, 250K, 5K) is NOT in
the key. Consequences today: (a) two *different* bar constructions closing at the same
instant get MERGED into one "Multi-Signal Cluster" — awarding the +15/+20 intra-candle
cluster boost for what is actually ONE volume event seen by two aggregators (inflated,
correlated confluence); (b) the evaluated-ledger key (PATCH S1) suppresses the second
variant after the first is reviewed — **systematic under-sampling of Volume-250K/5K and
Range-200 signals during bursts, precisely when absorption/exhaustion fire**.

**FIND (lines 316–322):**

```python
        # 1. Group signals on the exact same (symbol, bar_type, time_ms) candle/bar
        candle_clusters = defaultdict(list)
        for s in raw_signals:
            s_sym = s.get("symbol", "N/A")
            s_bt = str(s.get("bar_type", "time")).lower()
            s_t = int(s.get("time_ms") or 0)
            candle_clusters[(s_sym, s_bt, s_t)].append(s)
```

**REPLACE WITH:**

```python
        # 1. PATCH W1: group on (symbol, bar_type, bar_variant, time_ms).
        # bar_variant (bar_label: 'Range 10' vs 'Range 200' vs '100K' vs '250K' vs '5K')
        # MUST be part of the identity: different bar constructions closing at the same
        # instant are DIFFERENT candles. Merging them awarded the multi-signal cluster
        # boost (+15/+20) for one volume event seen twice, and the ledger then
        # suppressed the second variant permanently.
        candle_clusters = defaultdict(list)
        for s in raw_signals:
            s_sym = s.get("symbol", "N/A")
            s_bt = str(s.get("bar_type", "time")).lower()
            s_t = int(s.get("time_ms") or 0)
            s_var = str(s.get("bar_label") or s.get("bar_variant") or "").strip() if s_bt != "time" else ""
            candle_clusters[(s_sym, s_bt, s_var, s_t)].append(s)
```

**FIND (line 324):**

```python
        for (sym, bar_type, t_ms), cluster in candle_clusters.items():
```

**REPLACE WITH:**

```python
        for (sym, bar_type, bar_variant, t_ms), cluster in candle_clusters.items():
```

**Then EVERY ledger call in this loop must pass the variant. FIND (line 338):**

```python
                if self.stack.ledger.is_evaluated(sym, open_ms, bar_type=bar_type):
```

**REPLACE WITH:**

```python
                if self.stack.ledger.is_evaluated(sym, open_ms, bar_type=bar_type, bar_variant=bar_variant):
```

**FIND (lines 347–356, unlisted-asset suppression):**

```python
                    self.stack.ledger.mark_evaluated(
                        symbol=sym,
                        open_time_ms=open_ms,
                        signal_id=generate_signal_id(sym, open_ms, unlisted_dir, sig_codes),
                        confluence_score=0.0,
                        action="SUPPRESSED_UNLISTED_VENUE",
                        sig_name=str(cluster[0].get("sig_name", "Unlisted")),
                        direction=unlisted_dir,
                        bar_type=bar_type
                    )
```

**REPLACE WITH (adds `bar_variant=bar_variant`):**

```python
                    self.stack.ledger.mark_evaluated(
                        symbol=sym,
                        open_time_ms=open_ms,
                        signal_id=generate_signal_id(sym, open_ms, unlisted_dir, sig_codes),
                        confluence_score=0.0,
                        action="SUPPRESSED_UNLISTED_VENUE",
                        sig_name=str(cluster[0].get("sig_name", "Unlisted")),
                        direction=unlisted_dir,
                        bar_type=bar_type,
                        bar_variant=bar_variant
                    )
```

**FIND (lines 364–373, stale veto) — same treatment:**

```python
                    self.stack.ledger.mark_evaluated(
                        symbol=sym,
                        open_time_ms=open_ms,
                        signal_id=generate_signal_id(sym, open_ms, stale_dir, sig_codes),
                        confluence_score=0.0,
                        action="DISCARDED_STALE",
                        sig_name=str(cluster[0].get("sig_name", "Stale")),
                        direction=stale_dir,
                        bar_type=bar_type
                    )
```

**REPLACE WITH:**

```python
                    self.stack.ledger.mark_evaluated(
                        symbol=sym,
                        open_time_ms=open_ms,
                        signal_id=generate_signal_id(sym, open_ms, stale_dir, sig_codes),
                        confluence_score=0.0,
                        action="DISCARDED_STALE",
                        sig_name=str(cluster[0].get("sig_name", "Stale")),
                        direction=stale_dir,
                        bar_type=bar_type,
                        bar_variant=bar_variant
                    )
```

**FIND (lines 479–489, the final mark after dispatch outcome):**

```python
                self.stack.ledger.mark_evaluated(
                    symbol=sym,
                    open_time_ms=open_ms,
                    signal_id=sig_id,
                    confluence_score=score,
                    action=final_action,
                    sig_name=sig_name,
                    direction=direction,
                    bar_type=bar_type
                )
```

**REPLACE WITH (see PATCH W2 for the surrounding context change):**

```python
                if final_action != "PARKED_IN_FLIGHT_FORMING":
                    self.stack.ledger.mark_evaluated(
                        symbol=sym,
                        open_time_ms=open_ms,
                        signal_id=sig_id,
                        confluence_score=score,
                        action=final_action,
                        sig_name=sig_name,
                        direction=direction,
                        bar_type=bar_type,
                        bar_variant=bar_variant
                    )
```

---

## PATCH W2 — `5_wake_on_footprint_signal.py`: park in-flight forming bars instead of consuming them

**WHY (severity: CRITICAL):** The spec says execution happens on confirmed closes, with
in-flight bars analyzed live for situational awareness. Today an actionable signal on a
forming bar is (a) withheld from dispatch, then (b) **marked evaluated in the ledger** —
so when the bar closes, the Single-Evaluation invariant SUPPRESSES it forever. Actionable
intra-bar signals never execute. `self.pending_signals` was declared (line 272) and never
used. This patch parks the cluster and drains it at bar close.

**FIND (lines 442–445):**

```python
                final_action = action_str
                if is_actionable and direction in ("BUY", "SELL"):
                    if not is_bar_closed:
                        logger.info(f"⏳ [IN-FLIGHT FORMING BAR] {sym} {bar_type} signal evaluated live. Live Delta execution withheld until bar close.")
                        final_action = "ANALYZED_IN_FLIGHT_FORMING"
                    else:
```

**REPLACE WITH:**

```python
                final_action = action_str
                if is_actionable and direction in ("BUY", "SELL"):
                    if not is_bar_closed:
                        # PATCH W2: PARK the actionable forming-bar cluster instead of
                        # consuming it. Previously it was ledger-marked here, so the
                        # Single-Evaluation invariant suppressed it at bar close —
                        # actionable intra-bar signals NEVER executed.
                        park_key = (sym, open_ms, bar_type, bar_variant)
                        self.pending_signals[park_key] = {
                            "cluster": cluster,
                            "close_ms": close_ms,
                            "bar_type": bar_type,
                            "bar_variant": bar_variant,
                            "open_ms": open_ms,
                            "parked_at_ms": now_ms,
                        }
                        logger.info(
                            f"⏳ [IN-FLIGHT FORMING BAR] {sym} {bar_type}{('/' + bar_variant) if bar_variant else ''} "
                            f"actionable cluster PARKED (score {score:.1f}); will dispatch at bar close (close_ms={close_ms})."
                        )
                        final_action = "PARKED_IN_FLIGHT_FORMING"
                    else:
```

**FIND — the daemon loop, `run_daemon` (lines 684–694):**

```python
                    # 1. Read newly appended signals and immediately evaluate
                    new_signals = self.read_new_raw_signals()
                    if new_signals:
                        logger.info(f"⚡ Discovered {len(new_signals)} new footprint signal(s) [time/range/vol]. Immediate quantitative evaluation...")
                        await self.evaluate_and_dispatch(new_signals)

                    # Advance durable offset after processing signals
                    if hasattr(self, "_pending_offset") and self._pending_offset > self.state.get("offset", 0):
                        self.state["offset"] = self._pending_offset
                        self.state["processed_signals_count"] += len(new_signals)
                        save_state(self.state)
```

**REPLACE WITH:**

```python
                    # 1. Read newly appended signals and immediately evaluate
                    new_signals = self.read_new_raw_signals()
                    if new_signals:
                        logger.info(f"⚡ Discovered {len(new_signals)} new footprint signal(s) [time/range/vol]. Immediate quantitative evaluation...")
                        await self.evaluate_and_dispatch(new_signals)

                    # PATCH W2: drain parked forming-bar clusters once their bar closes
                    await self._drain_pending_forming_bars()

                    # Advance durable offset after processing signals
                    if hasattr(self, "_pending_offset") and self._pending_offset > self.state.get("offset", 0):
                        self.state["offset"] = self._pending_offset
                        self.state["processed_signals_count"] += len(new_signals)
                        save_state(self.state)
```

**INSERT — new method, directly above `def run_once` (line ~624):**

```python
    async def _drain_pending_forming_bars(self):
        """
        PATCH W2: re-evaluates parked actionable forming-bar clusters at bar close.
        A parked cluster older than 2h without a close is discarded as stale.
        Ledger-marking happens ONLY here (after close), preserving the
        Single-Evaluation invariant while making close-confirmed dispatch possible.
        """
        if not self.pending_signals:
            return
        now_ms = time.time() * 1000
        for park_key in list(self.pending_signals.keys()):
            parked = self.pending_signals[park_key]
            close_ms = int(parked.get("close_ms") or 0)
            if close_ms <= 0:
                self.pending_signals.pop(park_key, None)
                continue
            if (now_ms - close_ms) > 7_200_000:   # > 2h stale
                logger.info(f"⏳ [PARKED SIGNAL STALE] {park_key} never confirmed closed; discarding.")
                self.pending_signals.pop(park_key, None)
                continue
            if now_ms < close_ms:
                continue   # bar still forming
            cluster = parked.get("cluster") or []
            if not cluster:
                self.pending_signals.pop(park_key, None)
                continue
            logger.info(f"🔔 [BAR CLOSED] Re-evaluating parked cluster {park_key} for confirmed-close dispatch.")
            self.pending_signals.pop(park_key, None)
            try:
                await self.evaluate_and_dispatch(cluster)
            except Exception as e:
                logger.error(f"Error draining parked cluster {park_key}: {e}", exc_info=True)
```

---

## PATCH S1 — `Terminal/asset_signal_stack.py`: ledger key gains the bar variant

**FIND (lines 80–84):**

```python
    def _key(self, symbol: str, open_time_ms: int, bar_type: str = "time") -> str:
        bt = str(bar_type or "time").lower()
        if bt == "time":
            return f"{symbol.upper()}:{open_time_ms}"
        return f"{symbol.upper()}:{bt}:{open_time_ms}"
```

**REPLACE WITH:**

```python
    def _key(self, symbol: str, open_time_ms: int, bar_type: str = "time", bar_variant: str = "") -> str:
        # PATCH S1: variant-aware key — 'range' bars (Range 10 vs Range 200) and 'vol'
        # bars (100K vs 250K vs 5K) are distinct candle constructions and must never
        # share a dedup key, else the second variant is silently suppressed after
        # the first is reviewed (systematic signal loss during volume bursts).
        bt = str(bar_type or "time").lower()
        if bt == "time":
            return f"{symbol.upper()}:{open_time_ms}"
        var = str(bar_variant or "").strip().replace(" ", "_")
        return f"{symbol.upper()}:{bt}:{var}:{open_time_ms}"
```

**FIND (lines 116–118):**

```python
    def is_evaluated(self, symbol: str, open_time_ms: int, bar_type: str = "time") -> bool:
        """Returns True if this exact candle has already undergone evaluation."""
        return self._key(symbol, open_time_ms, bar_type) in self._evaluated
```

**REPLACE WITH:**

```python
    def is_evaluated(self, symbol: str, open_time_ms: int, bar_type: str = "time", bar_variant: str = "") -> bool:
        """Returns True if this exact candle (bar_type + variant) has already undergone evaluation."""
        if self._key(symbol, open_time_ms, bar_type, bar_variant) in self._evaluated:
            return True
        # Backward compatibility: a pre-patch ledger record without the variant still
        # suppresses the same bar for legacy-keyed (range/vol) entries.
        bt = str(bar_type or "time").lower()
        if bt != "time" and not str(bar_variant or "").strip():
            return f"{symbol.upper()}:{bt}:{open_time_ms}" in self._evaluated
        return False
```

**FIND (lines 120–132, `mark_evaluated` signature) — extend with `bar_variant: str = ""`
and pass it to `_key`. Update the record dict to store it:**

```python
    def mark_evaluated(
        self,
        symbol: str,
        open_time_ms: int,
        signal_id: str,
        confluence_score: float,
        action: str,
        sig_name: str = "",
        direction: str = "",
        bar_type: str = "time"
    ):
        """Records the candle as evaluated and durably persists."""
        key = self._key(symbol, open_time_ms, bar_type)
        record = {
            "symbol": symbol.upper(),
            "open_time_ms": open_time_ms,
            "bar_type": str(bar_type or "time").lower(),
```

**REPLACE WITH:**

```python
    def mark_evaluated(
        self,
        symbol: str,
        open_time_ms: int,
        signal_id: str,
        confluence_score: float,
        action: str,
        sig_name: str = "",
        direction: str = "",
        bar_type: str = "time",
        bar_variant: str = ""
    ):
        """Records the candle as evaluated and durably persists."""
        key = self._key(symbol, open_time_ms, bar_type, bar_variant)
        record = {
            "symbol": symbol.upper(),
            "open_time_ms": open_time_ms,
            "bar_type": str(bar_type or "time").lower(),
            "bar_variant": str(bar_variant or "").strip(),
```

*(No other changes needed — `load()` reads `bar_type` from records; old records simply
lack the variant and the back-compat branch in `is_evaluated` handles them.)*

---

## PATCH A1 — `Terminal/position_anchors.py`: fix the cold-start R0 recovery math

**WHY (severity: HIGH):** On daemon restart at **Tier 2** (SL locks +1.0R0, TP widened to
+3.0R0), the old hierarchy inferred `r0 = TP_dist / 2 = 1.5 × r0_true`. Every subsequent
trailing threshold was then computed against a phantom 50%-inflated R0: Tier 3 fired at
+4.5R instead of +3R, and the Tier-3 SL lock landed at +3.0R instead of +2.0R — the
trailing engine silently degraded. The fix uses a **tick-agnostic ratio discriminator**:
the profit-side SL distance `s` relative to TP distance is itself tier information
(BE ≈ 0, Tier2 ≈ 1/3, Tier3 ≈ 1/2).

**FIND (lines 141–158) — the Cold-Start Recovery Hierarchy:**

```python
    # Cold-Start Recovery Hierarchy
    dir_mult = 1.0 if direction.upper() == "BUY" else -1.0
    is_sl_loss_side = (entry_price - cur_sl_price) * dir_mult > 0

    if is_sl_loss_side and cur_sl_price > 0:
        # Step 1: SL is still on loss side -> direct derivation
        r0 = abs(entry_price - cur_sl_price)
        recovered_tier = 0
    elif cur_tp_price and cur_tp_price > 0:
        # Step 2: Infer from 2.0:1 Take-Profit target
        reward_dist = abs(cur_tp_price - entry_price)
        r0 = reward_dist / 2.0
        recovered_tier = 1 if not is_sl_loss_side else 0
    else:
        # Step 3: Canonical institutional risk envelope cap ($25.00)
        eff_size = max(1, abs(size))
        r0 = max_risk_usd / (eff_size * max(1e-6, contract_val))
        recovered_tier = 0
```

**REPLACE WITH:**

```python
    # PATCH A1: Cold-Start Recovery Hierarchy — tier-aware R0 inference.
    # The profit-side SL is itself tier information:
    #   Tier 1: SL = breakeven (+1 tick)  -> |SL-lock| / TP-dist ~ 0
    #   Tier 2: SL locks +1.0R0, TP widened to +3.0R0 -> ratio ~ 1/3
    #   Tier 3: SL locks +2.0R0, TP at +4.0R0           -> ratio ~ 1/2
    # The ratio discriminator is TICK-AGNOSTIC (a 1bp price tolerance missed the
    # +1-tick BE buffer on small-tick instruments like HBAR). The old TP/2
    # inference recovered r0 = 1.5x-true at Tier 2, corrupting every subsequent
    # trailing threshold after a mid-trade daemon restart.
    dir_mult = 1.0 if direction.upper() == "BUY" else -1.0
    sl_dist_signed = (entry_price - cur_sl_price) * dir_mult if cur_sl_price > 0 else 0.0
    is_sl_loss_side = cur_sl_price > 0 and sl_dist_signed > 0
    tp_dist = abs(cur_tp_price - entry_price) if (cur_tp_price and cur_tp_price > 0) else 0.0
    eff_size = max(1, abs(size))
    env_r0 = max_risk_usd / (eff_size * max(1e-6, contract_val))

    if cur_sl_price <= 0:
        # No stop present (naked recovery): r0 from the unwidened 2R0 TP, else envelope.
        if tp_dist > 0:
            r0 = tp_dist / 2.0
            recovered_tier = 1
        else:
            r0 = env_r0
            recovered_tier = 0
    elif is_sl_loss_side:
        # Step 1: SL still on the loss side -> direct derivation (Tier 0)
        r0 = abs(entry_price - cur_sl_price)
        recovered_tier = 0
    else:
        # SL on the profit side: s = locked distance in R0 units.
        s = abs(sl_dist_signed)
        ratio = (s / tp_dist) if tp_dist > 0 else 1.0   # no TP -> treat as conservative
        if ratio < 0.05:
            # Tier 1 signature: SL at ~breakeven (+1 tick); r0 from unwidened 2R0 TP
            r0 = (tp_dist / 2.0) if tp_dist > 0 else env_r0
            recovered_tier = 1
        elif tp_dist > 0 and abs(ratio - (1.0 / 3.0)) <= 0.15 * (1.0 / 3.0):
            # Tier 2 signature: SL locks +1R0 (ratio 1/3 of the +3R0 widened TP)
            r0 = s
            recovered_tier = 2
        elif tp_dist > 0 and abs(ratio - 0.5) <= 0.15 * 0.5:
            # Tier 3 signature: SL locks +2R0, TP at +4R0 (ratio 1/2)
            r0 = s / 2.0
            recovered_tier = 3
        else:
            # Ambiguous (TP edit may have failed mid-widen): conservative default.
            # r0 = s reproduces exact math for Tier 2 and only *understates* r_gain
            # for Tier 3 — whose SL is already locked at the maximum tier, so no
            # further ratchet is possible anyway. Never overstate r0.
            r0 = s
            recovered_tier = 2
```

**AND — in the same function, FIND (the closing register call):**

```python
    return register_position_anchor(
        symbol=clean_sym,
        product_id=product_id,
        direction=direction,
        entry_price=entry_price,
        initial_sl=cur_sl_price if is_sl_loss_side else entry_price - (dir_mult * r0),
        initial_tp=cur_tp_price if cur_tp_price else entry_price + (dir_mult * 2.0 * r0),
        size=size,
        initial_risk_dist=r0,
        current_tier=recovered_tier
    )
```

**REPLACE WITH (ratchet level must persist for the naked-position breach check):**

```python
    return register_position_anchor(
        symbol=clean_sym,
        product_id=product_id,
        direction=direction,
        entry_price=entry_price,
        initial_sl=cur_sl_price if cur_sl_price > 0 else entry_price - (dir_mult * r0),
        initial_tp=cur_tp_price if cur_tp_price else entry_price + (dir_mult * 2.0 * r0),
        size=size,
        initial_risk_dist=r0,
        current_tier=recovered_tier
    )
```

---

## PATCH C1 — `1_coinglass_footprint_scraper.py`: variant in the dedup key + true close time for Range/Volume bars

**FIND (lines 355–356):**

```python
        t_ms = int(s_copy.get("time_ms") or 0)
        c_ms = t_ms + 900000
```

**REPLACE WITH:**

```python
        t_ms = int(s_copy.get("time_ms") or 0)
        # PATCH C1: Range/Volume bars are discrete completed bars — their close time IS
        # their emission time; fabricating close = open + 15m corrupted close_time_utc
        # for every non-time bar in the parquet ledger.
        if str(s_copy.get("bar_type") or "time").lower() in ("range", "vol", "volume"):
            c_ms = t_ms
        else:
            itv_ms_probe = 300_000 if str(s_copy.get("bar_label", "")).lower().lstrip().startswith("5m") else 900_000
            c_ms = t_ms + int(s_copy.get("bar_interval_ms") or itv_ms_probe)
        s_copy["bar_interval_ms"] = int(c_ms - t_ms)
```

**FIND (lines 426–428):**

```python
        key = (s["symbol"], str(s.get("sig_code") if s.get("sig_code") is not None else ""), bar_type, t_ms)
        if key not in KNOWN_SIGNAL_KEYS:
            KNOWN_SIGNAL_KEYS.add(key)
            newly_discovered.append(s)
```

**REPLACE WITH:**

```python
        # PATCH C1: variant-aware key. Vol 100K and Vol 250K bars closing on the same
        # burst previously collided (same sig_code, same bar_type, same t_ms) and the
        # second variant was silently dropped — exactly during the volume spikes the
        # footprint stack is built to harvest.
        s_variant = str(s.get("bar_label") or "").strip() if bar_type != "time" else ""
        key = (s["symbol"], str(s.get("sig_code") if s.get("sig_code") is not None else ""), bar_type, s_variant, t_ms)
        if key not in KNOWN_SIGNAL_KEYS:
            KNOWN_SIGNAL_KEYS.add(key)
            newly_discovered.append(s)
```

**AND (line 121, `load_known_signals`):**

```python
                    if sym and t_ms > 0:
                        KNOWN_SIGNAL_KEYS.add((sym, sig, bt, t_ms))
```

**REPLACE WITH:**

```python
                    if sym and t_ms > 0:
                        var = str(data.get("bar_label") or "").strip() if bt != "time" else ""
                        KNOWN_SIGNAL_KEYS.add((sym, sig, bt, var, t_ms))
```

*(The freshness loop at lines 407–414 keys `is_closed` off `bar_label.startswith("5m")`
— with the new `bar_interval_ms` field written at emission you may harden it to
`itv_ms = int(s.get("bar_interval_ms") or (300_000 if startswith("5m") else 900_000))`;
the scraper-only-emits-closed-bars contract makes this advisory, not blocking.)*

---

## PATCH E1 — `Terminal/realtime_signal_evaluator.py`: safe Delta import (no crash on venue-module failure)

**FIND (lines 923–925):**

```python
        # Primary Execution Venue: Delta Exchange India Discrete Contract Solver
        from Terminal.delta_exchange_live_trader import delta_client
        delta_prod = delta_client.resolve_product(sym)
```

**REPLACE WITH:**

```python
        # PATCH E1: venue resolution must never take down signal evaluation.
        # (a hard import failure here previously crashed evaluate_signal for EVERY
        #  incoming signal — e.g. transient module-level init errors)
        try:
            from Terminal.delta_exchange_live_trader import delta_client
            delta_prod = delta_client.resolve_product(sym)
        except Exception as exc:
            logger.warning("Delta trader unavailable — evaluation-only mode for %s: %s", sym, exc)
            delta_prod = None
```

---

## PATCH E2 — `Terminal/realtime_signal_evaluator.py`: Hyperdash staleness guard + Whale Sweep-Target pillar

**FIND (lines 669–700) — section 3.9:**

```python
        # 3.9 Hyperdash Institutional Smart Money & Crowd Squeeze Intelligence
        if hd_intel and hd_intel.get("data_valid"):
            hd_smart_bias = float(hd_intel.get("smart_bias", 0.0))
            hd_crowd_bias = float(hd_intel.get("crowd_bias", 0.0))
            hd_signals = hd_intel.get("signals", []) or []

            # Smart Money Alignment
            if direction == "BUY":
                if hd_smart_bias >= 15.0:
                    confluence_score += 10.0
                    reasons.append(f"Hyperdash Smart Money Accumulation (Apex/Profitable cohorts net long +{hd_smart_bias:.1f}%)")
                elif hd_smart_bias <= -20.0:
                    confluence_score -= 15.0
                    reasons.append(f"Hyperdash Opposing Smart Money Distribution (Apex/Profitable cohorts net short {hd_smart_bias:.1f}%)")

                # Crowd Squeeze
                if "CROWD_SQUEEZE_LONG" in hd_signals or (hd_smart_bias >= 10.0 and hd_crowd_bias <= -20.0):
                    confluence_score += 10.0
                    reasons.append(f"Hyperdash Retail Short Squeeze Fuel (Crowd bias {hd_crowd_bias:+.1f}%)")

            elif direction == "SELL":
                if hd_smart_bias <= -15.0:
                    confluence_score += 10.0
                    reasons.append(f"Hyperdash Smart Money Distribution (Apex/Profitable cohorts net short {hd_smart_bias:.1f}%)")
                elif hd_smart_bias >= 20.0:
                    confluence_score -= 15.0
                    reasons.append(f"Hyperdash Opposing Smart Money Accumulation (Apex/Profitable cohorts net long +{hd_smart_bias:.1f}%)")

                # Crowd Squeeze
                if "CROWD_SQUEEZE_SHORT" in hd_signals or (hd_smart_bias <= -10.0 and hd_crowd_bias >= 20.0):
                    confluence_score += 10.0
                    reasons.append(f"Hyperdash Retail Long Squeeze Fuel (Crowd bias {hd_crowd_bias:+.1f}%)")
```

**REPLACE WITH:**

```python
        # 3.9 Hyperdash Institutional Smart Money & Crowd Squeeze Intelligence
        # PATCH E2: (a) staleness guard — stale/dead intel is neutralized instead of
        # blindly trusted; (b) WHALE LIQUIDATION SWEEP-TARGETS wired into Pillar 4.
        hd_intel_age = self._finite(hd_intel.get("intel_age_sec"), None) if hd_intel else None
        if hd_intel and hd_intel.get("data_valid"):
            hd_smart_bias = float(hd_intel.get("smart_bias", 0.0))
            hd_crowd_bias = float(hd_intel.get("crowd_bias", 0.0))
            hd_signals = hd_intel.get("signals", []) or []

            # Smart Money Alignment
            if direction == "BUY":
                if hd_smart_bias >= 15.0:
                    confluence_score += 10.0
                    reasons.append(f"Hyperdash Smart Money Accumulation (Apex/Profitable cohorts net long +{hd_smart_bias:.1f}%)")
                elif hd_smart_bias <= -20.0:
                    confluence_score -= 15.0
                    reasons.append(f"Hyperdash Opposing Smart Money Distribution (Apex/Profitable cohorts net short {hd_smart_bias:.1f}%)")

                # Crowd Squeeze
                if "CROWD_SQUEEZE_LONG" in hd_signals or (hd_smart_bias >= 10.0 and hd_crowd_bias <= -20.0):
                    confluence_score += 10.0
                    reasons.append(f"Hyperdash Retail Short Squeeze Fuel (Crowd bias {hd_crowd_bias:+.1f}%)")

            elif direction == "SELL":
                if hd_smart_bias <= -15.0:
                    confluence_score += 10.0
                    reasons.append(f"Hyperdash Smart Money Distribution (Apex/Profitable cohorts net short {hd_smart_bias:.1f}%)")
                elif hd_smart_bias >= 20.0:
                    confluence_score -= 15.0
                    reasons.append(f"Hyperdash Opposing Smart Money Accumulation (Apex/Profitable cohorts net long +{hd_smart_bias:.1f}%)")

                # Crowd Squeeze
                if "CROWD_SQUEEZE_SHORT" in hd_signals or (hd_smart_bias <= -10.0 and hd_crowd_bias >= 20.0):
                    confluence_score += 10.0
                    reasons.append(f"Hyperdash Retail Long Squeeze Fuel (Crowd bias {hd_crowd_bias:+.1f}%)")

            # WHALE LIQUIDATION SWEEP-TARGETS (new Pillar 4 alpha):
            # SHORT whale-liq clusters above price = BUY-side squeeze magnets;
            # LONG whale-liq clusters below price = SELL-side cascade magnets.
            sweep_targets = hd_intel.get("sweep_targets") or []
            if direction == "BUY":
                fwd_fuel = [t for t in sweep_targets if t.get("side") == "SHORT" and 0.3 <= float(t.get("dist_pct", 99)) <= 5.0]
                if fwd_fuel:
                    best_fuel = max(fwd_fuel, key=lambda t: t.get("sweep_score", 0.0))
                    confluence_score += 8.0
                    reasons.append(
                        f"Whale Short-Liq Sweep Target Above: ${best_fuel.get('liq_price'):,.2f} "
                        f"(+{best_fuel.get('dist_pct'):.1f}%, ${best_fuel.get('notional_musd', 0):.1f}M whale fuel)"
                    )
                stop_fuel = [t for t in sweep_targets if t.get("side") == "LONG" and -1.2 <= float(t.get("dist_pct", -99)) <= -0.15]
                if stop_fuel:
                    worst = max(stop_fuel, key=lambda t: t.get("sweep_score", 0.0))
                    confluence_score -= 8.0
                    reasons.append(
                        f"Whale Long-Liq Magnet Below Entry: ${worst.get('liq_price'):,.2f} "
                        f"({worst.get('dist_pct'):.1f}%, ${worst.get('notional_musd', 0):.1f}M) — stop-out fuel (-8)"
                    )
            elif direction == "SELL":
                fwd_fuel = [t for t in sweep_targets if t.get("side") == "LONG" and -5.0 <= float(t.get("dist_pct", -99)) <= -0.3]
                if fwd_fuel:
                    best_fuel = max(fwd_fuel, key=lambda t: t.get("sweep_score", 0.0))
                    confluence_score += 8.0
                    reasons.append(
                        f"Whale Long-Liq Sweep Target Below: ${best_fuel.get('liq_price'):,.2f} "
                        f"({best_fuel.get('dist_pct'):.1f}%, ${best_fuel.get('notional_musd', 0):.1f}M whale fuel)"
                    )
                stop_fuel = [t for t in sweep_targets if t.get("side") == "SHORT" and 0.15 <= float(t.get("dist_pct", 99)) <= 1.2]
                if stop_fuel:
                    worst = max(stop_fuel, key=lambda t: t.get("sweep_score", 0.0))
                    confluence_score -= 8.0
                    reasons.append(
                        f"Whale Short-Liq Magnet Above Entry: ${worst.get('liq_price'):,.2f} "
                        f"(+{worst.get('dist_pct'):.1f}%, ${worst.get('notional_musd', 0):.1f}M) — stop-out fuel (-8)"
                    )
        elif hd_intel and hd_intel.get("data_valid") is False and hd_intel_age is not None:
            reasons.append(
                f"Hyperdash intel stale (age {hd_intel_age:.0f}s) — neutralized (no boost, no penalty)"
            )
```

**AND — make the evaluator pass the live signal price as the sweep-target mark hint.
FIND (lines 283–289):**

```python
        hd_intel = {}
        if getattr(self, "hyperdash", None):
            try:
                hd_intel = self.hyperdash.get_asset_intel(sym) or {}
            except Exception as exc:
                logger.debug("Hyperdash intel fetch failed for %s: %s", sym, exc)
                hd_intel = {}
```

**REPLACE WITH:**

```python
        hd_intel = {}
        if getattr(self, "hyperdash", None):
            try:
                # PATCH E2: mark_price_hint anchors whale-liq cluster distances to the
                # live signal price instead of deriving the mark from whale UPnL.
                hd_intel = self.hyperdash.get_asset_intel(sym, mark_price_hint=(sig_price if sig_price > 0 else None)) or {}
            except Exception as exc:
                logger.debug("Hyperdash intel fetch failed for %s: %s", sym, exc)
                hd_intel = {}
```

---

## PATCH E3 — `Terminal/realtime_signal_evaluator.py`: commission-aware grid sizing in the advisory path

**FIND (lines 919–942):**

```python
        solved_lot, realized_risk, vol_status = None, 0.0, 'BROKER_VALIDATION_REQUIRED'

        geom_valid = False

        # Primary Execution Venue: Delta Exchange India Discrete Contract Solver
        from Terminal.delta_exchange_live_trader import delta_client
        delta_prod = delta_client.resolve_product(sym)

        if delta_prod:
            contract_val = float(delta_prod.get('contract_value', 1.0) or 1.0)
            risk_dist = abs(entry - sl)
            single_risk = risk_dist * contract_val
            if single_risk > 0 and single_risk <= MAX_RISK_USD:
                solved_lot = max(1, int(MAX_RISK_USD / single_risk))
                realized_risk = min(MAX_RISK_USD, solved_lot * single_risk)
                effective_max_risk = MAX_RISK_USD
                geom_valid = True
                vol_status = 'DELTA_QUALIFIED'
            else:
                solved_lot = 1
                realized_risk = single_risk
                effective_max_risk = MAX_RISK_USD
                geom_valid = (single_risk <= MAX_RISK_USD)
                vol_status = 'DELTA_QUALIFIED' if geom_valid else 'RISK_CAP_EXCEEDED'
        else:
```

**REPLACE WITH:**

```python
        solved_lot, realized_risk, vol_status = None, 0.0, 'BROKER_VALIDATION_REQUIRED'

        geom_valid = False

        # Primary Execution Venue: Delta Exchange India Discrete Contract Solver
        # PATCH E1: safe venue import; PATCH E3: fee-aware grid via SSOT helper
        try:
            from Terminal.delta_exchange_live_trader import delta_client
            delta_prod = delta_client.resolve_product(sym)
        except Exception as exc:
            logger.warning("Delta trader unavailable — evaluation-only mode for %s: %s", sym, exc)
            delta_prod = None

        if delta_prod:
            contract_val = float(delta_prod.get('contract_value', 1.0) or 1.0)
            risk_dist = abs(entry - sl)
            # PATCH E3: fee-aware discrete grid via the dispatch SSOT helper
            # (solve_fee_aware_grid — k_max form, commissions inside the cap).
            from Terminal.delta_exchange_live_trader import solve_fee_aware_grid
            grid = solve_fee_aware_grid(
                entry=entry,
                stop_loss=sl,
                contract_value=contract_val,
                taker_fee_rate=float(delta_prod.get('taker_fee_rate', 0.00075) or 0.00075),
                cap_usd=MAX_RISK_USD,
                max_contracts=100
            )
            if grid["ok"]:
                solved_lot = int(grid["size"])
                realized_risk = min(MAX_RISK_USD, float(grid["realized_risk_usd"]))
                effective_max_risk = MAX_RISK_USD
                geom_valid = True
                vol_status = 'DELTA_QUALIFIED'
            else:
                reason = grid["reason"]
                if "SINGLE_CONTRACT_ALL_IN_RISK_EXCEEDS_CAP" in reason or "INVALID_RISK" in reason:
                    solved_lot = 1
                    realized_risk = float(grid.get("loss_per_contract") or 0.0)
                    effective_max_risk = MAX_RISK_USD
                    geom_valid = False
                    vol_status = 'RISK_CAP_EXCEEDED'
                else:
                    solved_lot = None
                    realized_risk = 0.0
                    geom_valid = False
                    vol_status = f'NO_ADMISSIBLE_GRID: {reason}'
        else:
```

*(The `else:` branch below — the offline `_broker_sizing` fallback — is unchanged.)*

---

## PATCH R1 — `Terminal/risk_envelope.py`: operator-configurable equity floor (optional)

The prop-firm $4,775 hard floor and the Delta India personal account are different
regimes. Making the floor env-overridable keeps default semantics for all existing tests
while letting the operator consciously set a personal-account floor (any wiring of
`budget_per_trade`/OR-tools survival budgeting for the Delta venue will consume it).

**FIND (line 29):**

```python
HARD_FLOOR_USD = 4775.00       # Contractual floor (USD 4,700) + Self-imposed safety buffer (USD 75) = USD 4,775
```

**REPLACE WITH:**

```python
# PATCH R1: env-overridable hard floor. Default preserves the FundingPips contract
# semantics exactly ($4,775); a personal Delta India account may consciously set
# RISK_HARD_FLOOR_USD (e.g. '0' or a personal survival floor) without code edits.
HARD_FLOOR_USD = float(os.environ.get("RISK_HARD_FLOOR_USD", "4775.00"))
```

**AND ensure `import os` exists at the top of the file (add if missing):**

```python
import os
```
