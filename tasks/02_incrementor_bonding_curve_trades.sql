CREATE OR REPLACE TASK PUMPSCIENCE.EVENTS.INCREMENTOR_BONDING_CURVE_TRADES

  WAREHOUSE = 'AMB_TASK_WH' -- Specify the warehouse to use for the task
  AFTER PUMPSCIENCE.EVENTS.INCREMENTOR_TOKEN_CREATIONS
  
AS

INSERT INTO PUMPSCIENCE.EVENTS.BONDING_CURVE_TRADES (
    block_timestamp
    , tx_id
    , trader
    , trade_mint
    , name 
    , symbol
    , side
    , amount_sol
    , fees_sol
    , amount_token
)

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
        , MAX(CASE 
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
        and e.block_timestamp > (SELECT MAX(block_timestamp) as max_ts from pumpscience.events.bonding_curve_trades)
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