CREATE OR REPLACE TABLE pumpscience.events.graduation_events AS

with base_events as (
    SELECT
        e.block_timestamp
        , e.tx_id
        , TO_CHAR(e.signers[0]) as signer
        , TO_CHAR(t.decoded_instruction:accounts[0]:pubkey) as creator
        , TO_CHAR(t.decoded_instruction:accounts[5]:pubkey) as pool_auth
        , TO_CHAR(t.decoded_instruction:accounts[6]:pubkey) as pool_address
        , TO_CHAR(t.decoded_instruction:accounts[8]:pubkey) as token_a_mint
        , TO_CHAR(t.decoded_instruction:accounts[9]:pubkey) as token_b_mint
        , m.name as grad_name
        , m.symbol || '-' || 'WSOL' as pool_name
    from solana.core.fact_events e
    LEFT JOIN (
        SELECT
            tx_id
            , decoded_instruction
        from solana.core.fact_decoded_instructions
        WHERE 1=1
            --AND (block_id=358255426 AND tx_id='43BrrtX9oBCSdonmhF4Mw1XSzmQV24vuHxfhcwk5rUoVRzygwoSCYYij1CfNEcqGDSoA1wdgRqtzc1kk3gtJoRND')
            AND program_id = 'cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG'
            AND event_type = 'initialize_pool'
    ) t on e.tx_id = t.tx_id
    LEFT JOIN (
        SELECT 
            mint
            , symbol
            , name
        from pumpscience.events.token_creations
    ) m ON t.decoded_instruction:accounts[8]:pubkey = m.mint
    WHERE 1=1
        and e.block_timestamp>='2025-02-10'
        --AND (block_id=358255426 AND e.tx_id='43BrrtX9oBCSdonmhF4Mw1XSzmQV24vuHxfhcwk5rUoVRzygwoSCYYij1CfNEcqGDSoA1wdgRqtzc1kk3gtJoRND')
        AND e.program_id IN (
            '95deBvJ6VrgZC3St8V2weajqDVnU6pF8SjqMnfxnPGcY',
            '7HrXqoWjkgcM7MvVG2smCBDK31ZAhWhvdDbyungWNBcj'
        )
        and substr(utils.helpers.base58_to_hex(e.instruction:data), 3, 16) = 'f0eadc3196e9013c' --graduation
        AND e.succeeded
)

SELECT * from base_events
WHERE 1=1
    AND pool_address IS NOT NULL