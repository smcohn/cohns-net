#!/usr/bin/python3
import time
import datetime
import websocket
import json
import logging, sys
import requests
import random

def subscribe(socket,symbol):
    subscription_request = '{"event": "subscribe", "channel": "trades", "symbol": "' + symbol +'"}'
    socket.send(subscription_request)
    logging.debug("Sent %s" % subscription_request)
    result =  socket.recv()
    logging.debug("Received %s" % result)
    subscription_reply = json.loads(result)
    subscription_status = subscription_reply.get("event")
    if subscription_status == "subscribed":
        subscribed_chan_id = subscription_reply.get("chanId")
        subscribed_symbol = subscription_reply.get("symbol")
        logging.info("Successfully subscribed.")
        return(subscribed_chan_id)
    else:
        return(0)

def output_quote(symbol,quote):
    logging.debug("Decoding quote: %s" % quote)
    seq = quote[0]
    logging.debug("seq: %s" % seq)
    timestamp = quote[1]
    logging.debug("millisecond timestamp: %s" % timestamp)
    now = datetime.datetime.fromtimestamp(timestamp/1000.0)
    now = now.strftime('%Y-%m-%d-%H-%M-%S-%f')[:-3]
    logging.debug("formatted timestamp: %s" % now)
    quantity = quote[2]
    if quantity > 0:  is_buy = "True" 
    else: is_buy = "False"
    volume = abs(quantity)
    logging.debug("volume: %s" % volume)
    price = quote[3]
    logging.debug("price: %s" % price)
    output_string = "[symbol=\"" + symbol + "\", datetime=\"" + str(now) + "\", price=\"" + str(price) + "\", volume=\"" + str(volume) + "\", isbuy:\"" + is_buy + "\"]"
    print(output_string)

def parse_result(result):
        logging.debug("Received %s" % result)
        result_list = eval(result.split()[0])
        logging.debug("Cast that to a list as %s" % result_list)
        update_channel_id = result_list[0]
        logging.debug("ChannelId = %s" % update_channel_id)
        update_symbol = symbol_channel_id[update_channel_id]
        tick = result_list[1]
        tick_type = type(tick)
        logging.debug("Tick is type %s" % tick_type)
        if tick_type ==  list:
            logging.debug("Received a bulk update")
            logging.debug("Tick = %s" % tick)
            for quote in reversed(tick):
                output_quote(update_symbol,quote)
        else:
            update_type = result_list[1]
            logging.debug("ChannelId = %s" % update_channel_id)
            logging.debug("Type = %s" % update_type)
            if update_type == 'te':
                quote = result_list[2]
                logging.debug("Ticker is %s" % quote)
                output_quote(update_symbol,quote)
            else:
                logging.info("Update type is %s, skipping." % update_type)

def main():
    logging.basicConfig(filename='poller.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)
    logging.debug("Started bitfinex poller")
    global symbol_channel_id 
    symbol_channel_id = {}

    websocket_uri = 'wss://api-pub.bitfinex.com/ws/2'
    logging.info("Creating websocket connection to %s." % websocket_uri)
    ws = websocket.create_connection(websocket_uri)
    result =  ws.recv()

    logging.debug("Received %s" % result)
    logging.debug("Retreiving list of valid symbols...")
    valid_symbols_json = requests.get("https://api.bitfinex.com/v1/symbols")
    valid_symbols_list = valid_symbols_json.json()
    logging.debug("Symbol list: %s" % valid_symbols_list)
    ticker_list = random.choices(valid_symbols_list, k=5)
    logging.debug("Five Random symbols to watch: %s" % ticker_list )
    # Subscribe to each symbol, map channel to symbol_channel_id.
    for subscribe_symbol in ticker_list:
        logging.debug("symbol= %s" % subscribe_symbol)
        subscribed_channel = subscribe(ws,subscribe_symbol)
        symbol_channel_id[subscribed_channel] = subscribe_symbol
        if subscribed_channel == 0:
            logging.critical("subscribe(%s) failed" % subscribe_symbol)
            ws.close()
            exit(1)
        result =  ws.recv()
        parse_result(result)
    # Now loop forever on more input
    while 1 > 0:
        result =  ws.recv()
        parse_result(result)
    # We shouldn't find ourselves here, but if we do, clean up.
    logging.info("Closing connection....")
    ws.close()
    logging.debug("Exiting...")
    exit(1)

if __name__ == "__main__":
    main()
