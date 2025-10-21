CREATE OR REPLACE TABLE pumpscience.events.bonding_curve_trades AS

with base_events as (
    SELECT 
        --*
        e.block_timestamp
        , e.tx_id
        , TO_CHAR(e.instruction:accounts[0]) as trader
        , TO_CHAR(e.instruction:accounts[3]) as trade_mint
        , t.name
        , t.symbol
        , MAX(
            CASE 
            WHEN tr.TX_TO = trader AND tr.mint='So11111111111111111111111111111111111111111' THEN 'sell'
            ELSE 'buy' END) as side
        , MAX(
            CASE 
            WHEN tr.TX_TO = trader AND tr.mint='So11111111111111111111111111111111111111111' THEN amount
            ELSE NULL END) as trader_sol
        , MAX(
            CASE 
            WHEN tr.TX_TO = 'JBaYBfH8A54Sx1TiuU5191FV8aPMcqCsxFjR4xYko1qQ' AND tr.mint='So11111111111111111111111111111111111111111' THEN amount
            ELSE NULL END) as fee_sol
        , MAX(
            CASE 
            WHEN tr.TX_TO NOT IN ('JBaYBfH8A54Sx1TiuU5191FV8aPMcqCsxFjR4xYko1qQ', trader ) AND tr.mint='So11111111111111111111111111111111111111111' THEN amount
            ELSE NULL END) as curve_sol
        , MAX(
            CASE 
            WHEN tr.TX_TO = trader AND tr.mint!='So11111111111111111111111111111111111111111' THEN amount
            ELSE NULL END) as trader_token
        , MAX(
            CASE 
            WHEN tr.TX_TO NOT IN ('JBaYBfH8A54Sx1TiuU5191FV8aPMcqCsxFjR4xYko1qQ', trader ) AND tr.mint!='So11111111111111111111111111111111111111111' THEN amount
            ELSE NULL END) as curve_token
    from solana.core.fact_events e 
    LEFT JOIN pumpscience.events.token_creations t ON e.instruction:accounts[3] = t.mint
    LEFT JOIN solana.core.fact_transfers tr ON (e.tx_id = tr.tx_id AND e.block_timestamp = tr.block_timestamp AND e.index = FLOOR(tr.index))
    WHERE 1=1
        and e.block_timestamp>='2025-02-10'
        and e.program_id IN (
            '95deBvJ6VrgZC3St8V2weajqDVnU6pF8SjqMnfxnPGcY',
            '7HrXqoWjkgcM7MvVG2smCBDK31ZAhWhvdDbyungWNBcj'
            )
        and substr(utils.helpers.base58_to_hex(e.instruction:data), 3, 16) = 'f8c69e91e17587c8' --curve swap
        and succeeded
    GROUP BY 1, 2, 3, 4, 5, 6
)

, refine as (
    SELECT 
        block_timestamp
        , tx_id
        , trader
        , trade_mint
        , name 
        , symbol
        , side
        , COALESCE(trader_sol, 0) + COALESCE (fee_sol, 0) + COALESCE(curve_sol, 0) as amount_sol
        , COALESCE (fee_sol, 0) as fees_sol
        , COALESCE(trader_token, 0) + COALESCE(curve_token, 0) as amount_token
    from base_events
)

SELECT * from refine


-- TESTCASES

-- and (block_id = 367287871 and tx_id = '39PTrsgBekrBw6P21QLQea7Ei5NshQpLgwVMxe6knf4QFMh24RBk5ijckqqLXjYpT9JYkMK7YXuxCnfg7uicVWHd') --sell
-- and (block_id = 366081357 and tx_id = '4Ji5zZpQnfdJsnwdzFPaxhthwj6LfJidJVa6ddbLdHdQpcQW6GUtjTycwRDzbsmhPnDKdBPCMYNjzq68diRxfXKT')
-- and (block_id = 358110511 and tx_id = '67BW1pm8i9UWPSqD1NJNjvVnc1QS1m9RDLZbrFaUq9ojUtUxogc9TfqNX2abdsti7Vm1BMPvCCQ4FBLrPrWL6unj')
-- and (block_id = 319712810 and tx_id = 'yhf9GAxQ2tKTfqJq75Bf5tJc5B9jLhgYN9et4pviHxYiSNm6ca4Rcav77PNftwKcxX7AV4SnZhZeKkJByw1NTBJ')