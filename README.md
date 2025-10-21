# Snowflake Curations - PumpScience
Data modelling/curation for the PumpScience project with FlipsideCrypto Solana Snowflake Datashares.

# Structure/Notes - 
- The majority of the curation was done in the form of creating the tables for the project's events:
  - token_creations/mints
  - bonding curve trades
  - token graduations
  - post graduation trades/swaps on meteora
  - meteora pools' fees

- The tasks are used to periodically update/sync the tables with newer events. As some of the curated tables have dependencies on parent events/tables, there is a particular order in which the tables as well as the tasks have to be created/executed.

- The order of execution/creation being: 
    - token_creations
        - bonding_curve_trades
        - graduation_events
          - ez_meteora_swaps
          - meteora_fees
          
![tasks execution flow](https://github.com/0xthelaughingman/Snowflake-Curations---PumpScience/blob/main/images/tasks%20graph.png)


