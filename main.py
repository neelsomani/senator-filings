""" Scrape the stock transactions from Senator periodic filings. """

from selenium import webdriver
from selenium.common.exceptions import StaleElementReferenceException

import datetime
import logging
import pandas as pd
import pickle
import time
from typing import Any

FILINGS_HOME = 'https://efdsearch.senate.gov/search/home/'
AGREE_CHECKBOX = 'agree_statement'

REPORT_TYPES = 'report_type'
PERIODIC_TRANSACTIONS = '11'
FROM_DATE = 'fromDate'
TO_DATE = 'toDate'

N_ENTRIES_ATTR = 'aria-controls'
N_ENTRIES_VAL = 'filedReports'
SORTING_ELEMENTS = 'sorting'
INNER_TEXT = 'innerText'
SORT_CONTENTS = 'Date Received/Filed'

NEXT_BTN = 'filedReports_next'

LOGGER = logging.getLogger(__name__)


def click_on(driver: webdriver.Chrome, element: Any) -> None:
    """ Click on the specified element. """
    webdriver.ActionChains(driver) \
        .move_to_element(element) \
        .click(element) \
        .perform()


def open_page() -> webdriver.Chrome:
    driver = webdriver.Chrome('./chromedriver')
    driver.get(FILINGS_HOME)
    return driver


def agree_to_terms(driver: webdriver.Chrome) -> None:
    agree_box = driver.find_element_by_id(AGREE_CHECKBOX)
    click_on(driver, agree_box)


def search(driver: webdriver.Chrome) -> None:
    """ Search for the last `years` of data. """
    report_boxes = driver.find_elements_by_name(REPORT_TYPES)
    for r in report_boxes:
        value = r.get_attribute('value')
        if value == PERIODIC_TRANSACTIONS:
            click_on(driver, r)
            break
    today = datetime.datetime.today()
    driver.find_element_by_id(TO_DATE).send_keys(
        today.strftime('%m/%d/%Y')
    )
    driver.find_element_by_id(FROM_DATE).send_keys(
        datetime.datetime(2012, 1, 1).strftime('%m/%d/%Y')
    )
    buttons = driver.find_elements_by_tag_name('button')
    for b in buttons:
        contents = b.get_property(INNER_TEXT)
        if 'Search Reports' in contents:
            click_on(driver, b)
            break


def set_search_results_layout(driver: webdriver.Chrome) -> None:
    """ Sort by oldest filings. """
    selects = driver.find_elements_by_tag_name('select')
    for s in selects:
        if s.get_attribute(N_ENTRIES_ATTR) == N_ENTRIES_VAL:
            s.send_keys('100')
            break
    sort_cols = driver.find_elements_by_class_name(SORTING_ELEMENTS)
    # Give the list time to populate
    time.sleep(2)
    for c in sort_cols:
        header = c.get_attribute(INNER_TEXT)
        if header == SORT_CONTENTS:
            click_on(driver, c)
            break


def parse_page(driver: webdriver.Chrome) -> pd.DataFrame:
    """ Get a page of transactions and extract a DF of the tx_date,
    order_type (buy or sell), ticker, asset_name, tx_amount. """
    stocks = []
    col_names = ['tx_date', 'order_type', 'ticker', 'asset_name', 'tx_amount']
    tbodies = driver.find_elements_by_tag_name('tbody')
    if len(tbodies) == 0:
        return pd.DataFrame()
    for row in tbodies[0].find_elements_by_tag_name('tr'):
        cols = [c.get_attribute(INNER_TEXT)
                for c in row.find_elements_by_tag_name('td')]
        tx_date, ticker, asset_name, asset_type, order_type, tx_amount =\
            cols[1], cols[3], cols[4], cols[5], cols[6], cols[7]
        if asset_type != 'Stock' and ticker.strip() in ('--', ''):
            continue
        stocks.append([tx_date, order_type, ticker, asset_name, tx_amount])
    return pd.DataFrame(stocks).rename(columns=dict(enumerate(col_names)))


def iterate_through_results(driver: webdriver.Chrome) -> pd.DataFrame:
    """ Go through a given page's senators. """
    col_names = ['tx_date',
                 'file_date',
                 'last_name',
                 'first_name',
                 'order_type',
                 'ticker',
                 'asset_name',
                 'tx_amount']
    all_txs = pd.DataFrame().rename(
        columns=dict(enumerate(col_names)))
    no_rows = 0
    n_links = 0
    for row in driver.find_elements_by_tag_name('tbody')[0]\
                     .find_elements_by_tag_name('tr'):
        cols = row.find_elements_by_tag_name('td')
        first, last, report_type, date_received = (
            cols[0].get_attribute(INNER_TEXT),
            cols[1].get_attribute(INNER_TEXT),
            cols[3],
            cols[4].get_attribute(INNER_TEXT)
        )
        link = report_type.find_elements_by_tag_name('a')[0]
        click_on(driver, link)
        driver.switch_to.window(driver.window_handles[-1])
        txs = parse_page(driver)
        if len(txs) == 0:
            no_rows += 1
        driver.close()
        driver.switch_to.window(driver.window_handles[-1])
        all_txs = all_txs.append(
            txs.assign(file_date=date_received,
                       last_name=last,
                       first_name=first))
        time.sleep(2)
        n_links += 1
    LOGGER.info('{} out of {} pages returned no extractable transaction data'
                .format(no_rows, n_links))
    return all_txs


def main():
    driver = open_page()
    agree_to_terms(driver)
    search(driver)
    set_search_results_layout(driver)
    all_txs = pd.DataFrame()
    while True:
        LOGGER.info('Starting page {}'.format(i))
        LOGGER.info('{} rows so far'.format(len(all_txs)))
        time.sleep(2)
        try:
            txs = iterate_through_results(driver)
        except StaleElementReferenceException as e:
            LOGGER.exception('Stale element reference', e)
            # Retry if stale element
            time.sleep(1)
            txs = iterate_through_results(driver)
        all_txs = all_txs.append(txs)
        # Break if we're on the last page
        if 'disabled' in driver.find_element_by_id(NEXT_BTN)\
                               .get_attribute('class'):
            break
        click_on(driver, driver.find_element_by_id(NEXT_BTN))
    return all_txs


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    senator_txs = main()
    LOGGER.info('Dumping to .pickle')
    with open('notebooks/senators.pickle', 'wb') as f:
        pickle.dump(senator_txs, f)
