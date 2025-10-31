📊 Stock Transaction Dataset
📘 Overview

This dataset contains detailed records of stock market transactions executed by customers. Each record represents a single transaction, capturing trade details, customer information, and contextual metadata such as date, sector, and market characteristics.

🧾 Field Descriptions
Each column in the dataset is described below:
| **Column Name**         | **Description**                                                                                                                                                            | **Data Type**        | **Example Value** |
| ----------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------- | ----------------- |
| `transaction_id`        | A unique numeric identifier assigned to each transaction record.                                                                                                           | Integer              | 1                 |
| `timestamp`             | The date on which the stock transaction took place.                                                                                                                        | Date                 | 2023-01-02        |
| `customer_id`           | A unique identifier representing the customer executing the trade.                                                                                                         | Integer              | 4747              |
| `stock_ticker`          | The stock’s unique ticker symbol indicating which security was traded.                                                                                                     | String               | STK006            |
| `transaction_type`      | Specifies whether the transaction was a buy or sell action.                                                                                                                | Categorical (String) | BUY               |
| `quantity`              | The total number of stock units involved in the transaction.                                                                                                               | Integer              | 503               |
| `average_trade_size`    | The average quantity of shares traded per transaction for the customer and stock.                                                                                          | Float                | 195.33            |
| `stock_price`           | **Log-transformed stock price** at the time of transaction. The original stock price can be recovered using the exponential function: `original_price = exp(stock_price)`. | Float (Log scale)    | 4.917154          |
| `total_trade_amount`    | The total value of the transaction calculated as quantity × (original stock price).                                                                                        | Float                | 68716.45          |
| `customer_account_type` | The classification of the customer’s account, such as retail or institutional.                                                                                             | Categorical (String) | Retail            |
| `day_name`              | The day of the week on which the transaction occurred.                                                                                                                     | Categorical (String) | Monday            |
| `is_weekend`            | Indicates whether the transaction took place on a weekend.                                                                                                                 | Boolean              | False             |
| `is_holiday`            | Indicates whether the transaction occurred on a recognized public holiday.                                                                                                 | Boolean              | False             |
| `stock_liquidity_tier`  | The liquidity classification of the traded stock based on its trading volume or activity level.                                                                            | Categorical (String) | High              |
| `stock_sector`          | The broad economic sector to which the traded stock belongs.                                                                                                               | Categorical (String) | Energy            |
| `stock_industry`        | The specific industry category within the broader sector of the stock.                                                                                                     | Categorical (String) | Oil & Gas         |
