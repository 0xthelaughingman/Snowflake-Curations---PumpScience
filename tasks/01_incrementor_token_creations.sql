CREATE OR REPLACE TASK PUMPSCIENCE.EVENTS.INCREMENTOR_TOKEN_CREATIONS
  WAREHOUSE = 'AMB_TASK_WH'
  SCHEDULE = 'USING CRON 30 1 * * * UTC' 


AS

INSERT INTO PUMPSCIENCE.EVENTS.TOKEN_CREATIONS (
    block_timestamp
    , tx_id
    , platform_program_id
    , creator
    , mint
    , symbol
    , name
    , uri_image
)

with mint_txs as (
    SELECT 
        e.block_timestamp
        , e.tx_id
        , e.program_id as platform_program_id
        --, substr(utils.helpers.base58_to_hex(instruction:data), 3, 16)
        , TO_CHAR(t.signers[0]) as creator
        , TO_CHAR(t.decoded_instruction:accounts[1]:pubkey) as mint
        , TO_CHAR(t.decoded_instruction:args:createMetadataAccountArgsV3:data:symbol) as symbol
        , TO_CHAR(t.decoded_instruction:args:createMetadataAccountArgsV3:data:name) as name 
        , TO_CHAR(t.decoded_instruction:args:createMetadataAccountArgsV3:data:uri) as uri_image
    from SOLANA.CORE.FACT_EVENTS e
    LEFT JOIN SOLANA.CORE.FACT_DECODED_INSTRUCTIONS t on e.tx_id = t.tx_id
    WHERE 1=1
        and e.block_timestamp > (SELECT max(block_timestamp) as max_ts from PUMPSCIENCE.EVENTS.TOKEN_CREATIONS)
        -- and e.block_id = 358110511
        -- and e.tx_id='67BW1pm8i9UWPSqD1NJNjvVnc1QS1m9RDLZbrFaUq9ojUtUxogc9TfqNX2abdsti7Vm1BMPvCCQ4FBLrPrWL6unj'
        and e.program_id IN (
            '95deBvJ6VrgZC3St8V2weajqDVnU6pF8SjqMnfxnPGcY',
            '7HrXqoWjkgcM7MvVG2smCBDK31ZAhWhvdDbyungWNBcj'
            )
        and substr(utils.helpers.base58_to_hex(e.instruction:data), 3, 16) = '5e8b9e32455f082d' --mint/creation
        and e.succeeded
        and t.program_id = 'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s'
        and t.event_type = 'CreateMetadataAccountV3'

)


SELECT * from mint_txs