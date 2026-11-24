// Surge Rider — a pump.fun bonding-curve momentum bot for agent-fun/replay/v2.
//
// Idea: most pump.fun curves die fast; a few catch real buy pressure and run. This bot
// waits for a curve to prove itself (old enough to shake out bundle dumps, several
// distinct buyers, net SOL flowing in, price climbing but not yet parabolic), buys a
// small first tranche, pyramids only while the tape stays strong, and exits on a stop
// loss, a trailing stop once the position is up, a fade in buy pressure, a big single
// dump, or a time limit. A pace guard relaxes the entry bar and, late in the run,
// fires small probes so the entry still clears the 20-coin / 20-round-trip guardrail.
//
// Deterministic by construction: no clock, no randomness, iteration only over
// insertion-ordered maps fed by the batch stream.

import { LAMPORTS_PER_SOL, runBot, type Batch, type BotState, type OrderInput, type Pubkey } from './vendor/makefun-bot.ts';

const SOL = LAMPORTS_PER_SOL;
const MIN_BUY = SOL / 100n; // 0.01 SOL — the guardrail's minimum round-trip buy.
const FIRST_BUY = (SOL * 2n) / 100n; // 0.02 SOL first tranche.
const STAKE = (SOL * 2n) / 100n; // 0.02 SOL max per coin — small stakes, minimal fee drag.
const ADD_BUY = (SOL * 15n) / 1000n; // 0.015 SOL pyramid adds.
const BUY_SLIP_BPS = 500;
const SELL_SLIP_BPS = 10_000;

interface TapeTrade {
  slot: number;
  buy: boolean;
  sol: number; // SOL (float) moved by this trade.
  user: string;
  price: number; // SOL per token, from this trade's post-trade virtual reserves.
}

interface CurveTape {
  createdSlot: number;
  trades: TapeTrade[];
  priceHigh: number; // best price seen in the entry window.
  priceAtWindowStart: number;
}

interface OpenTrade {
  mint: Pubkey;
  spent: bigint; // SOL spent so far (cost basis, fees included).
  tokens: bigint;
  peakValue: bigint; // best mark-to-market value seen (lamports).
  armed: boolean; // trailing stop armed.
  boughtSlot: number;
  entries: number;
  exiting: boolean; // sell order sent; waiting for the position to clear.
}

const tapes = new Map<Pubkey, CurveTape>();
const open = new Map<Pubkey, OpenTrade>();
const lastExitSlot = new Map<Pubkey, number>();
const enteredCoins = new Set<Pubkey>();
let createdCount = 0;
let firstSlot = -1;

/** Price in SOL per token from virtual reserves. */
function priceOf(vq: bigint, vt: bigint): number {
  if (vt === 0n) return 0;
  return Number(vq) / Number(vt);
}

function tapeStats(tape: CurveTape, n: number): {
  buys: number;
  sellers: number;
  buySol: number;
  sellSol: number;
  buyers: Set<string>;
  startPrice: number;
} {
  const recent = tape.trades.slice(-n);
  let buys = 0;
  let buySol = 0;
  let sellSol = 0;
  const buyers = new Set<string>();
  for (const t of recent) {
    if (t.buy) {
      buys += 1;
      buySol += t.sol;
      buyers.add(t.user);
    } else {
      sellSol += t.sol;
    }
  }
  return { buys, sellers: recent.length - buys, buySol, sellSol, buyers, startPrice: 0 };
}

/** Net SOL inflow over the last n trades of a tape. */
function netInflow(tape: CurveTape, n: number): number {
  const s = tapeStats(tape, n);
  return s.buySol - s.sellSol;
}

await runBot({
  onBatch(state: BotState, batch: Batch): OrderInput[] {
    if (firstSlot < 0) firstSlot = batch.slot;
    const orders: OrderInput[] = [];
    const caps = state.hello.caps;
    const txFee = state.txFeeLamports();
    let free = state.freeCashLamports();
    let slots = caps.max_open_positions - state.committedPositionCount();
    const maxOrders = caps.max_orders_per_batch;

    // Fold this batch's events into the tapes first.
    for (const e of batch.events) {
      if (e.event === 'CreateEvent') {
        createdCount += 1;
        if (!tapes.has(e.mint)) {
          tapes.set(e.mint, {
            createdSlot: e.slot,
            trades: [],
            priceHigh: 0,
            priceAtWindowStart: 0,
          });
        }
        continue;
      }
      if (e.event === 'CompleteEvent') {
        tapes.delete(e.mint);
        open.delete(e.mint);
        continue;
      }
      // TradeEvent.
      let tape = tapes.get(e.mint);
      if (tape === undefined) {
        tape = { createdSlot: e.slot, trades: [], priceHigh: 0, priceAtWindowStart: 0 };
        tapes.set(e.mint, tape);
      }
      const solMoved = Number(BigInt(e.sol_amount)) / 1e9;
      const p = priceOf(BigInt(e.virtual_sol_reserves), BigInt(e.virtual_token_reserves));
      tape.trades.push({ slot: e.slot, buy: e.is_buy, sol: solMoved, user: e.user, price: p });
      if (tape.trades.length > 60) tape.trades.splice(0, tape.trades.length - 60);
      if (p > tape.priceHigh) tape.priceHigh = p;
    }

    // Pace guard: how far behind are we on distinct coins?
    const behind = createdCount > 120 && enteredCoins.size < Math.floor(createdCount / 25);
    const lateProbe = createdCount > 300 && enteredCoins.size < 15;

    // --- Exits first: manage every open position. ---
    for (const [mint, pos] of open) {
      const curve = state.curves.get(mint);
      const holding = state.positions.get(mint);
      if (curve === undefined || curve.complete || holding === undefined) {
        open.delete(mint);
        continue;
      }
      const p = priceOf(curve.virtual_quote_reserves, curve.virtual_token_reserves);
      if (p <= 0) continue;
      if (pos.exiting) {
        // Sell already sent; wait for the fill to clear it — unless no sell is
        // pending anymore (rejected/failed), in which case re-arm management.
        let sellPending = false;
        for (const pend of state.pending.values()) {
          if (pend.order.side === 'sell' && pend.order.mint === mint) sellPending = true;
        }
        if (sellPending) continue;
        pos.exiting = false;
      }
      // Mark-to-market value in lamports: tokens * price(SOL/token) * 1e9.
      const value = (holding.token_amount * BigInt(Math.floor(p * 1e12))) / 1000n;
      const cost = holding.cost_lamports;
      if (value > pos.peakValue) pos.peakValue = value;
      if (value >= (cost * 125n) / 100n) pos.armed = true;

      const tape = tapes.get(mint);
      const fade = tape !== undefined ? netInflow(tape, 15) : 0;
      const heldSlots = batch.slot - pos.boughtSlot;
      let bigDump = false;
      for (const e of batch.events) {
        if (e.event === 'TradeEvent' && e.mint === mint && !e.is_buy) {
          if (Number(BigInt(e.sol_amount)) / 1e9 >= 0.5) bigDump = true;
        }
      }

      const stopLoss = value < (cost * 85n) / 100n;
      const trailHit = pos.armed && value < (pos.peakValue * 85n) / 100n;
      const faded = fade < -0.08;
      const tooOld = heldSlots > 7200;
      if (stopLoss || trailHit || faded || tooOld || bigDump) {
        if (orders.length < maxOrders) {
          orders.push({ side: 'sell', mint, token_amount: holding.token_amount, max_slippage_bps: SELL_SLIP_BPS });
          pos.exiting = true;
        }
        lastExitSlot.set(mint, batch.slot);
      }
    }

    // --- Entries: scan curves that traded in this batch. ---
    const seenThisBatch = new Set<Pubkey>();
    for (const e of batch.events) {
      if (e.event !== 'TradeEvent') continue;
      seenThisBatch.add(e.mint);
    }
    for (const mint of seenThisBatch) {
      if (orders.length >= maxOrders || slots <= 0) break;
      if (open.has(mint)) continue;
      const curve = state.curves.get(mint);
      const tape = tapes.get(mint);
      if (curve === undefined || curve.complete || tape === undefined) continue;
      const age = batch.slot - tape.createdSlot;
      if (age < 120) continue; // let bundle dumps shake out.
      const cool = lastExitSlot.get(mint);
      if (cool !== undefined && batch.slot - cool < 600) continue;
      if (pos_entries(mint) >= 2) continue;

      const rqSol = Number(curve.real_quote_reserves) / 1e9;
      if (rqSol < 2 || rqSol > 60) continue;

      const stats = tapeStats(tape, 30);
      const net = stats.buySol - stats.sellSol;
      const total = stats.buySol + stats.sellSol;
      const buyShare = total > 0 ? stats.buySol / total : 0;

      // Entry bar; relaxes when the pace guard says we are behind on coins.
      const minNet = behind ? 0.12 : 0.25;
      const minBuys = behind ? 10 : 15;
      const minBuyers = behind ? 5 : 8;
      if (stats.buys < minBuys) continue;
      if (stats.buyers.size < minBuyers) continue;
      if (net < minNet) continue;
      if (buyShare < 0.7) continue;

      // Acceleration: the newer half of the window must inflow at least as fast
      // as the older half — buying a pump that is still gathering steam, not one
      // whose buying already peaked.
      const older = tape.trades.slice(-30, -15);
      const newer = tape.trades.slice(-15);
      let olderNet = 0;
      let newerNet = 0;
      for (const t of older) olderNet += t.buy ? t.sol : -t.sol;
      for (const t of newer) newerNet += t.buy ? t.sol : -t.sol;
      if (newerNet < 0.05) continue;
      if (olderNet > 0 && newerNet < olderNet) continue;

      // Momentum: price up 5–25% across the window, sitting near its window high.
      const windowTrades = tape.trades.slice(-30);
      const startPrice = windowTrades.length > 0 ? windowTrades[0].price : 0;
      const now = priceOf(curve.virtual_quote_reserves, curve.virtual_token_reserves);
      if (startPrice <= 0 || now <= 0) continue;
      const rise = now / startPrice - 1;
      if (rise < 0.05 || rise > 0.25) continue;
      if (tape.priceHigh > 0 && now < tape.priceHigh * 0.92) continue;

      // Late-run probe: tiny stake, quick flip — keeps the guardrail alive.
      const stake = lateProbe ? MIN_BUY : FIRST_BUY;
      const cap = state.maxBuyLamports(mint);
      if (cap < MIN_BUY) continue;
      const amount = stake < cap ? stake : cap;
      if (amount < MIN_BUY) continue;
      if (free < amount + txFee) continue;

      orders.push({ side: 'buy', mint, quote_lamports: amount, max_slippage_bps: BUY_SLIP_BPS });
      free -= amount + txFee;
      slots -= 1;
      enteredCoins.add(mint);
      open.set(mint, {
        mint,
        spent: amount,
        tokens: 0n,
        peakValue: amount,
        armed: false,
        boughtSlot: batch.slot,
        entries: 1,
        exiting: false,
      });
    }

    // --- Pyramids: add to open positions while the tape stays strong. ---
    for (const [mint, pos] of open) {
      if (orders.length >= maxOrders || slots <= 0) break;
      if (pos.spent >= STAKE) continue;
      const curve = state.curves.get(mint);
      const tape = tapes.get(mint);
      const holding = state.positions.get(mint);
      if (curve === undefined || curve.complete || tape === undefined || holding === undefined) continue;
      const net = netInflow(tape, 15);
      if (net < 0.05) continue;
      const now = priceOf(curve.virtual_quote_reserves, curve.virtual_token_reserves);
      // Best price paid so far, in SOL per token, from the peak mark value.
      const lastBuyPrice = pos.peakValue > 0n && holding.token_amount > 0n
        ? Number(pos.peakValue) / 1e9 / Number(holding.token_amount)
        : 0;
      if (lastBuyPrice > 0 && now < lastBuyPrice) continue; // only add into strength.
      const cap = state.maxBuyLamports(mint);
      if (cap < MIN_BUY) continue;
      let add = ADD_BUY;
      if (pos.spent + add > STAKE) add = STAKE - pos.spent;
      if (add < MIN_BUY) continue;
      if (add > cap) add = cap;
      if (add < MIN_BUY) continue;
      if (free < add + txFee) continue;
      orders.push({ side: 'buy', mint, quote_lamports: add, max_slippage_bps: BUY_SLIP_BPS });
      free -= add + txFee;
      pos.spent += add;
      pos.entries += 1;
    }

    return orders;
  },
});

function pos_entries(mint: Pubkey): number {
  const pos = open.get(mint);
  return pos === undefined ? 0 : pos.entries;
}
