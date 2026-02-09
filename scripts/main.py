import yfinance

def example1():
    ticker_String = "AAPL"
    ticker = yfinance.Ticker(ticker_String)

    historical_data = ticker.history(period="1mo")  # data for the last month

    #Display a summary of the fetched data
    print(f"Summary of Historical Data for {ticker_String}:")
    print(historical_data[['Open', 'High', 'Low', 'Close', 'Volume']])
    print(historical_data['Low'])


def message_handler(message):
    print("Received message:", message)

def example2():
    with yfinance.WebSocket() as ws:
        ws.subscribe(["AAPL", "BTC-USD"])
        ws.listen(message_handler)

example1()