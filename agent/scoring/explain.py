"""
Plain-English, comparative explanations for scored candidate rows — turns
"delta 0.22" (a restated field, not a reason) into an actual sentence about
why one strike is a better or worse trade than its peers.

Score (agent/scoring/score.py) already blends yield, delta-fit, liquidity,
and volatility richness into one number; this module is what turns that
number into language, comparing each row against the best-scoring row in its
own peer group — same ticker, same expiration, the exact grouping already
visible in the table (each "📅 ..." header row is one peer group).
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


def _premium(row: Dict[str, Any]) -> float:
    for key in ("_premium", "premium", "mid"):
        val = row.get(key)
        if val is not None:
            try:
                return float(val)
            except (TypeError, ValueError):
                continue
    return 0.0


def explain_relative(
    row: Dict[str, Any],
    peers: List[Dict[str, Any]],
    option_right: str,
) -> str:
    """One sentence-or-two comparing `row` against the best-scoring row in
    `peers` (which should include `row` itself). Empty string if there's
    nothing to compare against (a single-row group).

    `option_right`: "CALL" (covered call — assignment means your shares get
    called away) or "PUT" (cash-secured put — assignment means you're forced
    to buy the shares).
    """
    scored = [p for p in peers if p.get("score") is not None]
    if len(scored) < 2:
        return ""

    strike = row.get("strike")
    delta = row.get("delta")
    yield_pct = float(row.get("annualized_yield") or 0.0) * 100
    premium = _premium(row)
    assignment_pct = abs(float(delta)) * 100 if delta is not None else None
    assignment_word = "called away" if option_right != "PUT" else "assigned (forced to buy)"

    lead = f"${strike:,.2f} pays {yield_pct:.1f}% annualized (${premium:.2f} premium)"
    if assignment_pct is not None:
        lead += f", about a {assignment_pct:.0f}% chance of being {assignment_word} (delta {delta:.2f})."
    else:
        lead += "."

    best = max(scored, key=lambda p: p.get("score") or 0.0)
    is_best = (row.get("strike") == best.get("strike")
              and row.get("expiration") == best.get("expiration"))

    if is_best:
        # Compared against the SAFEST peer (lowest |delta|), not the
        # lowest-scored one — those aren't the same row when yield differs
        # enough to dominate the score despite a smaller delta edge.
        others = [p for p in scored
                 if not (p.get("strike") == strike and p.get("expiration") == row.get("expiration"))]
        if not others:
            return lead + " Best score among this expiration's strikes."
        safest = min(others, key=lambda p: abs(float(p.get("delta"))) if p.get("delta") is not None else 1.0)
        safest_yield = float(safest.get("annualized_yield") or 0.0) * 100
        yield_edge = yield_pct - safest_yield
        safety_gap = None
        if delta is not None and safest.get("delta") is not None:
            # Positive means this row's delta is bigger (less safe) than the
            # safest peer's - i.e. this row gave up that much safety margin.
            safety_gap = abs(float(delta)) - abs(float(safest["delta"]))
        if safety_gap is not None and safety_gap > 0:
            note = (f" (giving up only {safety_gap*100:.0f} points of extra safety "
                    f"vs the ${safest.get('strike'):,.0f} strike)")
        else:
            note = " — and it's already the safest strike in this expiration"
        return (lead + f" Best score in this expiration — {yield_edge:.1f} points more "
               f"annualized yield than the next-safest strike{note}.")

    yield_gap = float(best.get("annualized_yield") or 0.0) * 100 - yield_pct
    best_delta = best.get("delta")
    delta_gap = None
    if delta is not None and best_delta is not None:
        delta_gap = abs(float(delta)) - abs(float(best_delta))

    if yield_gap <= 0:
        # Pays as much or more but still scored lower - liquidity/vol/theta
        # pulled it down; say so rather than claim a yield story that isn't true.
        return (lead + f" Scores below the ${best.get('strike'):,.0f} strike despite similar or "
               f"better yield — likely a wider spread, thinner open interest, or richer "
               f"volatility premium tipped the balance there instead.")

    if delta_gap is not None and delta_gap < 0:
        return (lead + f" Gives up {yield_gap:.1f} points of annualized yield versus the "
               f"${best.get('strike'):,.0f} strike, in exchange for "
               f"{abs(delta_gap)*100:.0f} points less assignment risk — the composite "
               f"score weighs the extra income more.")

    return (lead + f" Pays {yield_gap:.1f} points less than the ${best.get('strike'):,.0f} "
           f"strike without enough of a safety improvement to offset it.")


def explain_group(rows: List[Dict[str, Any]], option_right: str) -> Dict[int, str]:
    """Explanations for every row in one peer group (same ticker+expiration),
    keyed by each row's position in `rows`."""
    return {i: explain_relative(row, rows, option_right) for i, row in enumerate(rows)}


# ── per-criterion breakdown (color-coded, with the typical range) ───────────

def _crit(name: str, value: str, color: str, note: str) -> Dict[str, str]:
    return {"name": name, "value": value, "color": color, "note": note}


def criteria_rows(row: Dict[str, Any], config: Dict[str, Any], option_right: str) -> List[Dict[str, str]]:
    """One entry per underlying criterion behind this candidate's Rec/Score —
    value, color ("green"/"yellow"/"red"), and a note stating the typical
    range so the color is never a black box. Reuses the exact thresholds
    each recommender/filter actually applies (delta band, earnings buffer,
    liquidity minimums) so this never contradicts the real Yes/No logic.
    """
    rec_key = "cc_recommendation" if option_right != "PUT" else "csp_recommendation"
    rec_cfg = config.get(rec_key) or {}
    delta_min = float(rec_cfg.get("delta_min", 0.10))
    delta_max = float(rec_cfg.get("delta_max", 0.25))
    earnings_buffer = int(rec_cfg.get("earnings_buffer_days", 7))
    min_oi_cfg = config.get("min_open_interest")
    max_spread_cfg = config.get("max_spread_pct")

    out: List[Dict[str, str]] = []

    delta = row.get("delta")
    if delta is None:
        out.append(_crit("Delta", "n/a", "yellow", "assignment odds proxy"))
    else:
        d = abs(float(delta))
        color = "green" if delta_min <= d <= delta_max else (
            "yellow" if delta_min * 0.6 <= d <= delta_max * 1.6 else "red")
        out.append(_crit("Delta", f"{d:.2f}", color,
                         f"target {delta_min:.2f}-{delta_max:.2f} · typical 0.10-0.30"))

    ann_yield = row.get("annualized_yield")
    if ann_yield is None:
        out.append(_crit("Annualized Yield", "n/a", "yellow", "premium collected, annualized"))
    else:
        y = float(ann_yield) * 100
        color = "green" if y >= 8 else ("yellow" if y >= 4 else "red")
        out.append(_crit("Annualized Yield", f"{y:.1f}%", color,
                         "target 8-15%+ · under 4% is thin"))

    ivr = row.get("ivr")
    if ivr is None:
        out.append(_crit("IV Rank", "n/a", "yellow", "IV vs its own 1yr range"))
    else:
        v = float(ivr)
        color = "green" if v >= 50 else ("yellow" if v >= 30 else "red")
        out.append(_crit("IV Rank", f"{v:.0f}%", color,
                         "sell when >50% (rich) · avoid under 30% (cheap)"))

    vrp = row.get("vrp")
    if vrp is None:
        out.append(_crit("VRP (IV/HV)", "n/a", "yellow", "implied vs 20d realized vol"))
    else:
        v = float(vrp)
        color = "green" if v >= 1.2 else ("yellow" if v >= 1.0 else "red")
        out.append(_crit("VRP (IV/HV)", f"{v:.2f}×", color,
                         ">1.2 rich · <1.0 cheap · >1.0 favors selling"))

    oi = row.get("open_interest")
    min_oi = float(min_oi_cfg) if min_oi_cfg is not None else 100.0
    if oi is None:
        out.append(_crit("Open Interest", "n/a", "yellow", "contracts outstanding"))
    else:
        v = int(oi)
        color = "green" if v >= min_oi * 3 else ("yellow" if v >= min_oi else "red")
        out.append(_crit("Open Interest", f"{v:,}", color,
                         f"screened min {int(min_oi):,} · thin = hard to exit"))

    spread = row.get("spread_pct")
    max_spread = float(max_spread_cfg) if max_spread_cfg is not None else 0.15
    if spread is None:
        out.append(_crit("Bid-Ask Spread", "n/a", "yellow", "cost to trade immediately"))
    else:
        v = float(spread) * 100
        color = "green" if v < 10 else ("yellow" if v <= max_spread * 100 else "red")
        out.append(_crit("Bid-Ask Spread", f"{v:.1f}%", color,
                         f"screened max {max_spread*100:.0f}% · ideal under 10%"))

    theta = row.get("theta_yield")
    if theta is None:
        out.append(_crit("Theta Yield", "n/a", "yellow", "annualized time-decay income"))
    else:
        v = float(theta) * 100
        color = "green" if v >= 8 else ("yellow" if v >= 4 else "red")
        out.append(_crit("Theta Yield", f"{v:.1f}%", color, "higher = faster, more efficient income"))

    dte = row.get("dte")
    if dte is None:
        out.append(_crit("Days to Expiration", "n/a", "yellow", ""))
    else:
        d = int(dte)
        color = "green" if 21 <= d <= 45 else ("yellow" if 7 <= d <= 60 else "red")
        out.append(_crit("Days to Expiration", str(d), color,
                         "sweet spot ~30-45d · <7d = gamma risk · >60d = idle capital"))

    earnings = row.get("earnings_before_expiry")
    has_earnings = bool(earnings) if earnings is not None else False
    out.append(_crit("Earnings Risk", "Yes" if has_earnings else "No",
                     "red" if has_earnings else "green",
                     f"gap risk on the print · buffer {earnings_buffer}d before expiry"))

    return out
