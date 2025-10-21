CREATE OR REPLACE TASK PUMPSCIENCE.EVENTS.INCREMENTOR_EZ_METEORA_SWAPS

  WAREHOUSE = 'AMB_TASK_WH' -- Specify the warehouse to use for the task
  AFTER PUMPSCIENCE.EVENTS.INCREMENTOR_GRADUATION_EVENTS
  
AS

INSERT INTO PUMPSCIENCE.EVENTS.EZ_METEORA_SWAPS (
    block_timestamp
    , tx_id
    , swapper 
    , swap_from_symbol
    , swap_to_symbol
    , side
    , volume_usd
    , volume_sol
    , swap_from_amount
    , swap_to_amount
    , swap_from_mint
    , swap_to_mint
)

with base_events as (
    SELECT 
        s.block_timestamp
        , s.tx_id
        , s.swapper 
        , COALESCE(swap_from_symbol, grad_name) as swap_from_symbol
        , COALESCE(swap_to_symbol, grad_name) as swap_to_symbol
        , CASE 
            WHEN swap_to_mint='So11111111111111111111111111111111111111112' THEN 'sell'
            ELSE 'buy' END as side
        , CASE 
            WHEN swap_to_mint='So11111111111111111111111111111111111111112' THEN swap_to_amount_usd
            ELSE swap_from_amount_usd END as volume_usd
        , CASE 
            WHEN swap_to_mint='So11111111111111111111111111111111111111112' THEN swap_to_amount
            ELSE swap_from_amount END as volume_sol
        , swap_from_amount
        , swap_to_amount
        , swap_from_mint
        , swap_to_mint
    from solana.defi.ez_dex_swaps s
    join pumpscience.events.graduation_events t ON (s.swap_from_mint = t.token_a_mint OR s.swap_to_mint = t.token_a_mint)
    WHERE 1=1
        AND s.block_timestamp > (SELECT MAX(block_timestamp) as max_ts from pumpscience.events.ez_meteora_swaps)
        --extra check
        AND (s.swap_from_mint='So11111111111111111111111111111111111111112' OR s.swap_to_mint='So11111111111111111111111111111111111111112')
        and s.swap_program = 'meteora DAMM' and program_id = 'cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG'
)

SELECT * from base_events