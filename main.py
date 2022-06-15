# -*- coding: utf-8 -*-



from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order
import threading
import time
import pandas as pd
import datetime as dt


class TradingApp(EWrapper, EClient):
    def __init__(self): 
        EClient.__init__(self, self) 
        self.data = {}
        self.order_df = pd.DataFrame(columns = ["PermId","ClientId","OrderId","Account",
                                               "Symbol","SecType","Exchange","Action",
                                               "OrderType","TotalQty","CashQty","LmtPrice",
                                               "AuxPrice","Status"])
        
        self.pos_df = pd.DataFrame(columns = ["Account","Symbol","SecType",
                                               "Currency","Position","Avg cost"])
        
        self.pnl_single_df = pd.DataFrame(columns = ["ReqId","Position","DailyPnL","UnrealizedPnL","RealizedPnL","Value"])
        
        self.pnl_df = pd.DataFrame(columns = ["ReqId","DailyPnL","UnrealizedPnL","RealizedPnL"])
        
        self.summary_df = pd.DataFrame(columns = ["ReqId","Account","Tag","Value","Currency"])
        
    def error(self, reqId, errorCode, errorString):
        print("Error {} {} {}".format(reqId,errorCode,errorString))
        
    def nextValidId(self, orderId):
        super().nextValidId(orderId)
        self.nextValidOrderId = orderId
        print("NextValidId:", orderId)
    
    def historicalData(self, reqId, bar):
        if reqId not in self.data:
            self.data[reqId] = [{"Date":bar.date,"Open":bar.open,"High":bar.high,"Low":bar.low,"Close":bar.close,"Volume":bar.volume}]
        else:
            self.data[reqId].append({"Date":bar.date,"Open":bar.open,"High":bar.high,"Low":bar.low,"Close":bar.close,"Volume":bar.volume})
        print("reqID:{}, date:{}, open:{}, high:{}, low:{}, close:{}, volume:{}".format(reqId,bar.date,bar.open,bar.high,bar.low,bar.close,bar.volume))
        
    def openOrder(self, orderId, contract, order, orderState):
        super().openOrder(orderId, contract, order, orderState)
        dictionary = {"PermId": order.permId, "ClientId": order.clientId, "OrderId": orderId, 
                   "Account": order.account, "Symbol": contract.symbol, "SecType": contract.secType,
                   "Exchange": contract.exchange, "Action": order.action, "OrderType": order.orderType,
                   "TotalQty": order.totalQuantity, "CashQty": order.cashQty, 
                   "LmtPrice": order.lmtPrice, "AuxPrice": order.auxPrice, "Status": orderState.status}
    
        self.order_df = self.order_df.append(dictionary, ignore_index = True)
    
    def position(self, account, contract, position, avgCost):
          super().position(account, contract, position, avgCost)
          dictionary = {"Account": account, "Symbol": contract.symbol, "SecType": contract.secType, 
                "Currency": contract.currency, "Position": position, "Avg cost": avgCost}
          self.pos_df = self.pos_df.append(dictionary, ignore_index=True)
          
    def positionEnd(self):
        print('latest position data extracted')
          
    def accountSummary(self, reqId, account, tag, value, currency):
         super().accountSummary(reqId, account, tag, value, currency)
         dictionary = {"ReqId": reqId, "Account": account,
               "Tag": tag, "Value": value, "Currency": currency}
         
         self.summary_df = self.summary_df.append(dictionary, ignore_index=True)
         
    def pnlSingle(self, reqId, pos, dailyPnL, unrealizedPnL, realizedPnL, value):
         super().pnlSingle(reqId, pos, dailyPnL, unrealizedPnL, realizedPnL, value)
         dictionary = {"ReqId": reqId, "Position": pos,
                   "DailyPnL": dailyPnL, "UnrealizedPnL": unrealizedPnL,
                   "RealizedPnL": realizedPnL, "Value": value}
         
         
         self.pnl_single_df = self.pnl_single_df.append(dictionary, ignore_index=True)
         
    def pnl(self, reqId, dailyPnL, unrealizedPnL, realizedPnL):
        super().pnl(reqId, dailyPnL, unrealizedPnL, realizedPnL)
        dictionary = {"ReqId":reqId, "DailyPnL": dailyPnL, "UnrealizedPnL": unrealizedPnL, "RealizedPnL": realizedPnL}
        
        self.pnl_df = self.pnl_df.append(dictionary, ignore_index=True)
          

########################handling connection to IBAPI###########################
def websocket_con():
    app.run()
    
app = TradingApp()      
app.connect("127.0.0.1", 7497, clientId=1)

# starting a separate daemon thread to execute the websocket connection
con_thread = threading.Thread(target=websocket_con, daemon=True)
con_thread.start()
time.sleep(1) # some latency added to ensure that the connection is established
###############################################################################

###################storing trade app object in dataframe#######################
def histData(req_num,contract,duration,candle_size):
    """extracts historical data"""
    app.reqHistoricalData(reqId=req_num, 
                          contract=contract,
                          endDateTime='',
                          durationStr=duration,
                          barSizeSetting=candle_size,
                          whatToShow='ADJUSTED_LAST',
                          useRTH=1,
                          formatDate=1,
                          keepUpToDate=0,
                          chartOptions=[])	 # EClient function to request contract details

def dataDataframe(TradeApp_obj, symbols, symbol):
    "returns extracted historical data in dataframe format"
    df = pd.DataFrame(TradeApp_obj.data[symbols.index(symbol)])
    df.set_index("Date",inplace=True)
    return df
###############################################################################

##################handling functions (contracts & orders)######################
def usTechStk(symbol,sec_type="STK",currency="USD",exchange="ISLAND"):
    contract = Contract()
    contract.symbol = symbol
    contract.secType = sec_type
    contract.currency = currency
    contract.exchange = exchange
    return contract 

#creating object of the limit order class - will be used as a parameter for other function calls
def limitOrder(direction,quantity,lmt_price):
    order = Order()
    order.action = direction
    order.orderType = "LMT"
    order.totalQuantity = quantity
    order.lmtPrice = lmt_price
    return order

def marketOrder(direction,quantity):
    order = Order()
    order.action = direction
    order.orderType = "MKT"
    order.totalQuantity = quantity
    return order

def stopOrder(direction,quantity,stop_price):
    order = Order()
    order.action = direction
    order.orderType = "STP"
    order.totalQuantity = quantity
    order.auxPrice = stop_price
    return order

def trailStopOrder(direction, quantity, stop_price, tr_stop_price):
    order = Order()
    order.action = direction
    order.orderType = "TRAIL"
    order.totalQuantity = quantity
    order.auxPrice = stop_price
    order.trailStopPrice = tr_stop_price
    return order
###############################################################################

#################strategy functions (MACD and stochOscltr)#####################
def MACD(DF,a=12,b=26,c=9):
    """function to calculate MACD
       typical values a(fast moving average) = 12; 
                      b(slow moving average) =26; 
                      c(signal line ma window) =9"""
    df = DF.copy()
    df["MA_Fast"]=df["Close"].ewm(span=a,min_periods=a).mean()
    df["MA_Slow"]=df["Close"].ewm(span=b,min_periods=b).mean()
    df["MACD"]=df["MA_Fast"]-df["MA_Slow"]
    df["Signal"]=df["MACD"].ewm(span=c,min_periods=c).mean()
    return df

def stochOscltr(DF,a=20,b=3):
    """function to calculate Stochastics
       a = lookback period
       b = moving average window for %D"""
    df = DF.copy()
    df['C-L'] = df['Close'] - df['Low'].rolling(a).min()
    df['H-L'] = df['High'].rolling(a).max() - df['Low'].rolling(a).min()
    df['%K'] = df['C-L']/df['H-L']*100
    #df['%D'] = df['%K'].ewm(span=b,min_periods=b).mean()
    return df['%K'].rolling(b).mean()

def atr(DF,n):
    "function to calculate True Range and Average True Range"
    df = DF.copy()
    df['H-L']=abs(df['High']-df['Low'])
    df['H-PC']=abs(df['High']-df['Close'].shift(1))
    df['L-PC']=abs(df['Low']-df['Close'].shift(1))
    df['TR']=df[['H-L','H-PC','L-PC']].max(axis=1,skipna=False)
    #df['ATR'] = df['TR'].rolling(n).mean()
    df['ATR'] = df['TR'].ewm(com=n,min_periods=n).mean()
    return df['ATR']
###############################################################################

###############################main function###################################
def main():
    app.reqPositions()
    time.sleep(1)
    pos_df = app.pos_df
    pos_df.drop_duplicates(inplace=True, ignore_index=True)
    app.reqOpenOrders()
    time.sleep(1)
    order_df = app.order_df
    for ticker in tickers:
        print("begining passthrough for ", ticker)
        histData(tickers.index(ticker),usTechStk(ticker),'1 M', '15 mins')
        time.sleep(3)
        df = dataDataframe(app, tickers, ticker)
        df["stoch"] = stochOscltr(df)
        df["macd"] = MACD(df)["MACD"]
        df["signal"] = MACD(df)["Signal"]
        df["atr"] = atr(df,60)
        df.dropna(inplace=True)
        quantity = int(capital/df["Close"][-1])
        
        if quantity == 0: 
            continue
        
        # if position dataframe empty
        if len(pos_df) == 0:
            # strategy
            if df['macd'][-1] > df['signal'][-1] and \
            df['stoch'][-1] > 30 and \
            df['stoch'][-1] > df['stoch'][-2]:
                app.reqIds(-1)
                time.sleep(2)
                order_id = app.nextValidOrderId
                app.placeOrder(order_id, usTechStk(ticker), marketOrder('BUY', quantity))
                stop_price = round(df['Close'][-1]-df['atr'][-1])
                app.placeOrder(order_id+1, usTechStk(ticker), stopOrder('SELL', quantity, stop_price))
        
        # if position dataframe not empty and if stock not in position dataframe
        if len(pos_df.columns) != 0 and ticker not in pos_df["Symbol"].tolist():
            # strategy
            if df['macd'][-1] > df['signal'][-1] and \
            df['stoch'][-1] > 30 and \
            df['stoch'][-1] > df['stoch'][-2]:
                app.reqIds(-1)
                time.sleep(2)
                order_id = app.nextValidOrderId
                app.placeOrder(order_id, usTechStk(ticker), marketOrder('BUY', quantity))
                stop_price = round(df['Close'][-1]-df['atr'][-1])
                app.placeOrder(order_id+1, usTechStk(ticker), stopOrder('SELL', quantity, stop_price))
        
        # if position dataframe not empty and if stock in position dataframe
        if len(pos_df.columns) != 0 and ticker in pos_df["Symbol"].tolist():
            
            # if stock not currently owned
            if pos_df[pos_df['Symbol']==ticker]["Position"].values[-1] == 0:
                # strategy
                if df['macd'][-1] > df['signal'][-1] and \
                df['stoch'][-1] > 30 and \
                df['stoch'][-1] > df['stoch'][-2]:
                    app.reqIds(-1)
                    time.sleep(2)
                    order_id = app.nextValidOrderId
                    app.placeOrder(order_id, usTechStk(ticker), marketOrder('BUY', quantity))
                    stop_price = round(df['Close'][-1]-df['atr'][-1])
                    app.placeOrder(order_id+1, usTechStk(ticker), stopOrder('SELL', quantity, stop_price))
                    
            #if stock currently owned
            if pos_df[pos_df['Symbol']==ticker]["Position"].values[0] > 0:
                ord_id = order_df[order_df['Symbol']==ticker]['OrderId'].values[-1]
                app.cancelOrder(ord_id)
                app.reqIds(-1)
                time.sleep(2)
                order_id = app.nextValidOrderId
                stop_price = round(df['Close'][-1]-df['atr'][-1])
                app.placeOrder(order_id+1, usTechStk(ticker), stopOrder('SELL', quantity, stop_price))
                
                
                
                    
        
        
    return None
###############################################################################

tickers = ["FB","AMZN","INTC","MSFT","AAPL","GOOG","CSCO","CMCSA","ADBE","NVDA",
           "NFLX","PYPL","AMGN","AVGO","TXN","CHTR","QCOM","GILD","FISV","BKNG",
           "INTU","ADP","CME","TMUS","MU"]


capital = 1000
starttime = time.time()
timeout = time.time()+60*60*1 #60 seconds * 60 minutes * 1 hour (1 hour in total)

while time.time() <= timeout:
    main()
    time.sleep(900 - ((time.time() - starttime) % 900.0))
    
