from extract import connect_to_api, extract_json
from config import logger


def main():
    response = connect_to_api()

    data = extract_json(response)

    logger.info(f"{len(data)} records extracted")

    seen_symbols = []

    for stock in data:
        if stock["symbol"] not in seen_symbols:
            logger.info(f"{stock['symbol']} data successfully processed")
            seen_symbols.append(stock["symbol"])


if __name__ == "__main__":
    main()