# senator-filings
Scrape public filings of the buy + sell orders of U.S. senators and calculate their returns. This repo contains a script to scrape the electronic filings at https://efdsearch.senate.gov/search/ and a Jupyter notebook to analyze the results.

## Requirements

python3

## Quick Start

Scrape all of the senators' filings: `python3 main.py`

Analyze the results by starting a Jupyter server and going through the notebook: `jupyter notebook`

## Limitations

1. We only look at electronic publicly filed trades by senators. Some periodic transaction reports are PDFs, which are ignored.
2. We calculate returns only using the trades observed. This is almost definitely not representative of a senator's entire portfolio. A more accurate way of thinking about the returns is a portfolio that mimics the observed buys and sells.
3. If the periodic transaction report specifies a range ($1000 - $5000), then we assume the amount is the lower bound.
4. We ignore trades for tickers that do not have data through the Yahoo Finance API.
5. The portfolio is not allowed to go short.
