import unittest
from unittest.mock import patch
import json
import qml_h4_bot as bot
from datetime import datetime, timezone, timedelta
from qml_h4_bot import Candle, confirmed_pivots, round_price, atr, format_signal, SYMBOLS, find_qml, structural_targets

class CoreTests(unittest.TestCase):
    def test_round_price(self):
        self.assertEqual(round_price('EUR/USD', 1.123456), 1.12346)
        self.assertEqual(round_price('XAU/USD', 2650.126), 2650.13)
    def test_pivot_requires_right_bars(self):
        base=datetime(2025,1,1,tzinfo=timezone.utc)
        vals=[(1,2,0,1),(1,3,0,2),(2,5,1,4),(3,4,2,3),(2,3,1,2),(2,3,1,2),(2,3,1,2)]
        cs=[Candle(base+timedelta(hours=4*i),*v) for i,v in enumerate(vals)]
        piv=confirmed_pivots(cs,1,2)
        self.assertTrue(any(p.kind=='H' and p.price==5 for p in piv))
    def test_only_requested_symbols(self):
        self.assertEqual(SYMBOLS, ["XAU/USD", "EUR/USD"])
    def test_signal_message_is_russian_and_compact(self):
        msg=format_signal({"symbol":"XAU/USD","direction":"SELL","entry":2650.0,"sl":2660.0,
            "tp1":2640.0,"tp2":2630.0,"tp3":2620.0,"id":"ABCD1234"})
        self.assertIn("ЗОЛОТО",msg)
        self.assertIn("НАПРАВЛЕНИЕ: SELL",msg)
        self.assertIn("Ожидается вход",msg)
        self.assertIn("Сигнал не является исполненным ордером",msg)
        self.assertNotIn("SYMBOL:",msg)
    def test_qml_requires_confirmed_head_and_later_h4_bos(self):
        base=datetime(2025,1,1,tzinfo=timezone.utc)
        highs=[102,103,104,110,105,103,101,102,104,115,108,105,103,102,101,100,99]
        lows =[98,97,96,99,98,96,95,97,99,101,99,97,96,92,91,90,89]
        closes=[100,101,102,105,103,100,98,100,102,108,104,100,98,93,92,91,90]
        candles=[Candle(base+timedelta(hours=4*i),closes[i],highs[i],lows[i],closes[i]) for i in range(len(highs))]
        # At the moment the head's right-side confirmation bars end, BOS has not yet closed.
        self.assertEqual(find_qml(candles[:13],"EUR/USD"),[])
        setups=find_qml(candles,"EUR/USD")
        self.assertEqual(len(setups),1)
        self.assertEqual(setups[0]["direction"],"SELL")
        self.assertEqual(setups[0]["qml"],110)
        self.assertEqual(setups[0]["head"],115)
        self.assertEqual(setups[0]["broken_swing"],95)
        self.assertEqual(setups[0]["bos_dt"],base+timedelta(hours=4*13))
        # One valid structural target is not padded into three arbitrary levels.
        self.assertIsNone(structural_targets(candles,"SELL",110,117,95,115,"EUR/USD"))
        # Price-reflected data must produce the exact bullish mirror.
        mirror=[Candle(c.dt,200-c.close,200-c.low,200-c.high,200-c.close) for c in candles]
        bullish=find_qml(mirror,"EUR/USD")
        self.assertEqual(len(bullish),1)
        self.assertEqual(bullish[0]["direction"],"BUY")
        self.assertEqual(bullish[0]["qml"],90)
        self.assertEqual(bullish[0]["head"],85)
        self.assertEqual(bullish[0]["broken_swing"],105)
        self.assertIsNone(structural_targets(mirror,"BUY",90,83,105,85,"EUR/USD"))
        # With three well-separated confirmed swing levels, target selection is structural and ordered.
        highs=list(highs); lows=list(lows); closes=list(closes)
        while len(highs)<48:
            highs.append(102); lows.append(98); closes.append(100)
        for i,low in [(20,90),(28,85),(36,80),(44,75)]:
            lows[i]=low; closes[i]=low+2
        for i,high in [(24,110),(32,108),(40,106)]:
            highs[i]=high; closes[i]=high-2
        extended=[Candle(base+timedelta(hours=4*i),closes[i],highs[i],lows[i],closes[i]) for i in range(len(highs))]
        self.assertEqual(structural_targets(extended,"SELL",110,117,95,115,"EUR/USD"),(95.0,90.0,85.0))
        extended_mirror=[Candle(c.dt,200-c.close,200-c.low,200-c.high,200-c.close) for c in extended]
        self.assertEqual(structural_targets(extended_mirror,"BUY",90,83,105,85,"EUR/USD"),(105.0,110.0,115.0))
    def test_entry_and_tp_events_persist_once(self):
        original=bot.DB_PATH
        bot.DB_PATH=":memory:"
        con=bot.init_db()
        now=datetime.now(timezone.utc)
        con.execute("""INSERT INTO signals
          (id,symbol,direction,status,qml,entry,sl,tp1,tp2,tp3,created_at,last_bar_at,expires_at,payload,last_price,last_price_at)
          VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
          ("SAMPLE1","XAU/USD","SELL","ENTRY PENDING",110,110,116,100,95,90,
           now.isoformat(),now.isoformat(),(now+timedelta(days=3)).isoformat(),json.dumps({}),109.5,(now-timedelta(minutes=15)).isoformat()))
        con.commit()
        with patch.object(bot,"telegram_send",return_value=True) as sender:
            row=con.execute("SELECT * FROM signals WHERE id='SAMPLE1'").fetchone()
            bot.process_price(con,row,110.1,now)
            row=con.execute("SELECT * FROM signals WHERE id='SAMPLE1'").fetchone()
            self.assertEqual(row["status"],"ACTIVE")
            self.assertIn("ENTRY REACHED",json.loads(row["notified_events"]))
            bot.process_price(con,row,99.5,now+timedelta(minutes=15))
            row=con.execute("SELECT * FROM signals WHERE id='SAMPLE1'").fetchone()
            self.assertEqual(row["status"],"TP1 HIT")
            self.assertIn("TP1 HIT",json.loads(row["notified_events"]))
            bot.process_price(con,row,99.5,now+timedelta(minutes=30))
            self.assertEqual(sender.call_count,2)
        con.close()
        bot.DB_PATH=original
    def test_atr_nonnegative(self):
        base=datetime(2025,1,1,tzinfo=timezone.utc)
        cs=[Candle(base+timedelta(hours=4*i),10,12,9,11) for i in range(20)]
        self.assertGreaterEqual(atr(cs,14),0)

if __name__=='__main__': unittest.main()
