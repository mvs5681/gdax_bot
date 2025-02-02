#!/usr/bin/env python

import argparse
import configparser
import datetime
import json
import sys
import time
import uuid
import boto3
from decimal import Decimal
from coinbase.rest import RESTClient
from json import dumps


def get_timestamp():
    ts = time.time()
    return datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")

def generate_client_order_id():
    return str(uuid.uuid4())

"""
    Basic Coinbase Pro DCA buy/sell bot that executes a market order.
    * CB Pro does not incentivize maker vs taker trading unless you trade over $50k in
        a 30 day period (0.25% taker, 0.15% maker). Current fees are 0.50% if you make
        less than $10k worth of trades over the last 30 days. Drops to 0.35% if you're
        above $10k but below $50k in trades.
    * Market orders can be issued for as little as $5 of value versus limit orders which
        must be 0.001 BTC (e.g. $50 min if btc is at $50k). BTC-denominated market
        orders must be at least 0.0001 BTC.

    This is meant to be run as a crontab to make regular buys/sells on a set schedule.
"""

parser = argparse.ArgumentParser(
    description="""
        This is a basic Coinbase Pro DCA buying/selling bot.

        ex:
            BTC-USD BUY 14 USD          (buy $14 worth of BTC)
            BTC-USD BUY 0.00125 BTC     (buy 0.00125 BTC)
            ETH-BTC SELL 0.00125 BTC    (sell 0.00125 BTC worth of ETH)
            ETH-BTC SELL 0.1 ETH        (sell 0.1 ETH)
    """,
    formatter_class=argparse.RawTextHelpFormatter,
)

# Required positional arguments
parser.add_argument(
    "-market_name", default="BTC-USD", help="(e.g. BTC-USD, ETH-BTC, etc)"
)

parser.add_argument("-order_side", default="BUY", type=str, choices=["BUY", "SELL"])

parser.add_argument(
    "-amount",
    type=Decimal,
    default="4.00",
    help="The quantity to buy or sell in the amount_currency",
)

parser.add_argument(
    "-amount_currency", default="USD", help="The currency the amount is denominated in"
)


# Additional options
parser.add_argument(
    "-sandbox",
    action="store_true",
    default=False,
    dest="sandbox_mode",
    help="Run against sandbox, skips user confirmation prompt",
)

parser.add_argument(
    "-warn_after",
    default=30,
    action="store",
    type=int,
    dest="warn_after",
    help="secs to wait before sending an alert that an order isn't done",
)

parser.add_argument(
    "-j",
    "--job",
    action="store_true",
    default=False,
    dest="job_mode",
    help="Suppresses user confirmation prompt",
)

parser.add_argument(
    "-c",
    "--config",
    default="./settings-local.conf",
    dest="config_file",
    help="Override default config file location",
)


def main(event, context):
    args = parser.parse_args()
    attributes = event.get("attributes", {})

    market_name = attributes.get("market_name", args.market_name)
    order_side = attributes.get("order_side", args.order_side)
    amount = Decimal(attributes.get("amount", args.amount))
    amount_currency = attributes.get("amount_currency", args.amount_currency)
    config_file = attributes.get("config_file", args.config_file)

    job_mode = True if "job" in attributes else args.job_mode

    args.market_name = market_name
    args.order_side = order_side
    args.amount = amount
    args.amount_currency = amount_currency
    args.config_file = config_file
    args.job_mode = job_mode

    print(f"{get_timestamp()}: STARTED: {args}")

    sandbox_mode = args.sandbox_mode
    job_mode = args.job_mode
    warn_after = args.warn_after

    if not sandbox_mode and not job_mode:
        if sys.version_info[0] < 3:
            # python2.x compatibility
            response = raw_input("Production purchase! Confirm [Y]: ")  # noqa: F821
        else:
            response = input("Production purchase! Confirm [Y]: ")
        if response != "Y":
            print("Exiting without submitting purchase.")
            exit()

    # Read settings
    print(f"Reading config file: {config_file}")
    config = configparser.ConfigParser()
    config.read(config_file)
    config_section = "production" 
    
    key = config.get(config_section, "API_KEY")
    secret = config.get(config_section, "SECRET_KEY")
    sns_topic = config.get(config_section, "SNS_TOPIC")

    # Prep boto SNS client for email notifications
    sns = boto3.client('sns')

    # Instantiate public and auth API clients
    client = RESTClient(api_key=key, api_secret=secret, timeout=5)

    # Get product info
    product = client.get_product(market_name)
    
    # Get product info and setup quote and base currency
    base_currency = product.base_currency_id
    quote_currency = product.quote_currency_id
    base_min_size = Decimal(product.base_min_size).normalize()
    base_increment = Decimal(product.base_increment).normalize()
    quote_increment = Decimal(product.quote_increment).normalize()
    if amount_currency == product.quote_currency_id:
        amount_currency_is_quote_currency = True
    elif amount_currency == product.base_currency_id:
        amount_currency_is_quote_currency = False
    else:
        raise Exception(
            f"amount_currency {amount_currency} not in market {market_name}"
        )
    
    print(f"product: {product}")
    print(f"base_min_size: {base_min_size}")
    print(f"quote_increment: {quote_increment}")
 
    quote_size = str(amount.quantize(base_increment))
    print(f"quote_size: {quote_size}")
    print(f"order_side: {order_side}")
    # Put a buy order in
    if order_side == "BUY":
        order = client.market_order_buy(client_order_id=generate_client_order_id(), product_id=market_name, quote_size=quote_size)
        print(dumps(order.to_dict(), indent=2))
    else: # Currently only supports buy orders
        exit()
    
    if "message" in order.to_dict():
    #     # Something went wrong if there's a 'message' field in response
        sns.publish(
            TopicArn=sns_topic,
            Subject=f"Could not place {market_name} {order_side} order",
            Message=json.dumps(order.to_dict(), sort_keys=True, indent=4)
        )
        exit()

    if order and "error_response" in order.to_dict():
        print(f"{get_timestamp()}: {market_name} Order Error")

    # Get Order details to check status
    order_id = order["success_response"]["order_id"]
    client_order_id = order["success_response"]["client_order_id"]
    print(f"order_id: {order_id}")
    print(f"client_order_id: {client_order_id}")

    # Check if the order is still open or unfilled
    wait_time = 5
    total_wait_time = 0

    # Allow time for the order to be processed
    time.sleep(wait_time)

    order_response = client.get_order(order_id)
    print(f"Response: {order_response}")

    order = order_response.order
    print(f'Order: {order}')
    print(f"Status: {order['status']}")
    
   
    while order['status'] in ['OPEN', 'PENDING', 'UNKNOWN_ORDER_STATUS']:

        if total_wait_time > warn_after:
            sns.publish(
                TopicArn=sns_topic,
                Subject=f"{market_name} {order_side} order of {amount} {amount_currency} OPEN/UNFILLED",
                Message=json.dumps(order.to_dict(), sort_keys=True, indent=4)
            )
            exit()

        print(
            f"{get_timestamp()}: Order {order_id} still {order['status']}. Sleeping for {wait_time} (total {total_wait_time})"
        )
        time.sleep(wait_time)
        total_wait_time += wait_time
        order_response = client.get_order(order_id)
        order = order_response.order
        
        if (order["cancel_message"] or  order["reject_message"]) and \
                (order["status"] not in ['OPEN', 'FILLED', 'UNKNOWN_ORDER_STATUS']):
            # Most likely the order was manually cancelled in the UI

            sns.publish(
                TopicArn=sns_topic,
                Subject=f"{market_name} {order_side} order of {amount} {amount_currency} CANCELLED/REJECTED",
                Message=json.dumps(order.to_dict(), sort_keys=True, indent=4)
            )
            exit()
    
    # Order status is no longer pending!
    print('Printing the order')
    print(f"order: {order}")
    print(order)
    

    market_price = Decimal(order.average_filled_price).quantize(quote_increment)

    subject = f"{market_name} {order_side} order of {amount} {amount_currency} {order['status']} @ {market_price} {quote_currency}"
    print(subject)
    sns.publish(
        TopicArn=sns_topic,
        Subject=subject,
        Message=json.dumps(order.to_dict(), sort_keys=True, indent=4)
    )

    
    return {
        'statusCode': 200,
        'body': json.dumps("BTCBOT Job Ended!")
    }


if __name__ == "__main__":
    context = {}
    event = {"attributes": {}}
    # event = {
    #     "attributes": {
    #         "market_name": "BTC-USD",
    #         "order_side": "BUY",
    #         "amount": "1.00",
    #         "amount_currency": "USD",
    #         "config_file": "./settings-local.conf",
    #     }
    # }

    main(event, context)
    