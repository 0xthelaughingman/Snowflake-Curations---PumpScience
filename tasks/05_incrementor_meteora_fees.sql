CREATE OR REPLACE TASK PUMPSCIENCE.EVENTS.INCREMENTOR_METEORA_FEES

  WAREHOUSE = 'AMB_TASK_WH' -- Specify the warehouse to use for the task
  AFTER PUMPSCIENCE.EVENTS.INCREMENTOR_EZ_METEORA_SWAPS
  
AS

INSERT INTO PUMPSCIENCE.EVENTS.METEORA_FEES (
    block_timestamp
    , tx_id
    , pool_address
    , pool_name
    , swap_events
    , total_fees_sol
    , lp_fees
    , protocol_fees
    , partner_fees
    , referral_fees
)

with raw_events as (
    SELECT 
        f.block_timestamp
        , f.tx_id
        , inner_index
        , f.instruction
        , row_number() OVER (ORDER BY f.tx_id ASC) as rn
    from solana.core.fact_events_inner f 
    INNER JOIN (
        SELECT 
            tx_id
            , MAX(block_timestamp) as block_timestamp
        from pumpscience.events.ez_meteora_swaps
        WHERE 1=1
            AND block_timestamp > (SELECT MAX(block_timestamp) as max_ts from pumpscience.events.meteora_fees)
        GROUP BY 1
    ) s ON (f.tx_id = s.tx_id AND f.block_timestamp = s.block_timestamp)
    WHERE 1=1
        AND f.block_timestamp > (SELECT MAX(block_timestamp) as max_ts from pumpscience.events.meteora_fees)
        AND f.program_id = 'cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG'
        AND instruction:accounts[0]='3rmHSu74h1ZcmAisVcWerTCiRDQbUrBKmcwptYGjHfet'
        AND succeeded
)

, refine as (
    SELECT 
        block_timestamp
        , tx_id 
        , pool_address
        , COUNT(DISTINCT inner_index) as swap_events
        , SUM(lp_fee) as lp_fees
        , SUM(protocol_fee) as protocol_fees
        , SUM(partner_fee) as partner_fees
        , SUM(referral_fee) as referral_fees
        , protocol_fees + lp_fees + partner_fees + referral_fees as total_fees_sol
    FROM (
        SELECT 
            block_timestamp
            , tx_id
            , inner_index
            , utils.helpers.base58_to_hex(instruction:data) as data_hex
            -- , substr(data_hex, 3, 32) as identifier_hex
            , utils.helpers.hex_to_base58('0x' || substr(data_hex, 3 + 32, 64)) as pool_address
            -- , utils.helpers.hex_to_int('0x' || substr(data_hex, 3 + 32 + 64, 2)) as trade_direction -- 1 = buy, 0 = sell
            -- , utils.helpers.hex_to_int('0x' || substr(data_hex, 3 + 32 + 64 + 2, 2)) as has_ref
            -- , utils.helpers.hex_to_int('s2c', to_char(reverse(to_binary(substr(data_hex, 3 + 32 + 64 + 2 + 2, 16))))) as amount_in
            -- , utils.helpers.hex_to_int('s2c', to_char(reverse(to_binary(substr(data_hex, 3 + 32 + 64 + 2 + 2 + 16, 16))))) as min_amt_out
            -- , utils.helpers.hex_to_int('s2c', to_char(reverse(to_binary(substr(data_hex, 135, 16))))) as amount_out
            -- , utils.helpers.hex_to_int('s2c', to_char(reverse(to_binary(substr(data_hex, 135 + 16, 32))))) as nextSqrtPrice
            , TO_NUMBER(utils.helpers.hex_to_int('s2c', to_char(reverse(to_binary(substr(data_hex, 135 + 16 + 32, 16))))))/1e9 as lp_fee
            , TO_NUMBER(utils.helpers.hex_to_int('s2c', to_char(reverse(to_binary(substr(data_hex, 135 + 16 + 32 + 16, 16))))))/1e9 as protocol_fee
            , TO_NUMBER(utils.helpers.hex_to_int('s2c', to_char(reverse(to_binary(substr(data_hex, 135 + 16 + 32 + 16 + 16, 16))))))/1e9 as partner_fee
            , TO_NUMBER(utils.helpers.hex_to_int('s2c', to_char(reverse(to_binary(substr(data_hex, 135 + 16 + 32 + 16 + 16 + 16, 16))))))/1e9 as referral_fee
            , (lp_fee + protocol_fee + partner_fee + referral_fee) as total_fees_sol
            -- , utils.helpers.hex_to_int('s2c', to_char(reverse(to_binary(substr(data_hex, 135 + 16 + 32 + 16 + 16 + 32, 16))))) as actual_amount_in
            -- , utils.helpers.hex_to_int('s2c', to_char(reverse(to_binary(substr(data_hex, 135 + 16 + 32 + 16 + 16 + 48, 16))))) as current_ts 
        from raw_events
        WHERE 1=1
            AND substr(utils.helpers.base58_to_hex(instruction:data), 3, 32) = 'e445a52e51cb9a1d1b3c15d58aaabb93' --anchor log
        -- AND (tx_id= '2gmj85Mft4c8GW7dnS2gZhXrwJbELVZvdLfKS29kQSmUJr4CCAtyS6CePViNhdyEon86FXqGkMHsJ2bimoJ1k5xc')
    )
    GROUP BY 1, 2, 3
)
        
SELECT
    r.block_timestamp
    , r.tx_id
    , r.pool_address
    , g.pool_name
    , swap_events
    , total_fees_sol
    , lp_fees
    , protocol_fees
    , partner_fees
    , referral_fees
from refine r
LEFT JOIN pumpscience.events.graduation_events g on r.pool_address = g.pool_address
WHERE 1=1
    and g.pool_name IS NOT NULL