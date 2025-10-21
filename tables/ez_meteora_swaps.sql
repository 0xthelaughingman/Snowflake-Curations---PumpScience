CREATE OR REPLACE TABLE pumpscience.events.ez_meteora_swaps AS

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
        --AND (block_id=367452876 AND tx_id='2eWFSAhx6TVRXDqyALwTpnLARkF1hjF7cAW8Pm9qQLxqKgNbttSByht5tpWcZWEH14zidSPkr3mwJa44Hgnb1TiN')
        AND s.block_timestamp>='2025-01-01'
        --extra check
        AND (s.swap_from_mint='So11111111111111111111111111111111111111112' OR s.swap_to_mint='So11111111111111111111111111111111111111112')
        and s.swap_program = 'meteora DAMM' and program_id = 'cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG'
)

SELECT * from base_events