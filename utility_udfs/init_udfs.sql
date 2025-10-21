-- These are copied from FSCrypto's livequery util functions available here:
-- https://github.com/FlipsideCrypto/livequery-models/blob/main/macros/core/functions.py.sql
-- https://github.com/FlipsideCrypto/livequery-models/blob/main/macros/core/utils.yaml.sql


-- REPLACE DATABASE AND SCHEMA AS PER YOUR PREF!
CREATE OR REPLACE DATABASE UTILS;
USE database UTILS;
CREATE OR REPLACE SCHEMA helpers;


CREATE OR REPLACE FUNCTION utils.helpers.hex_to_int(hex STRING)
RETURNS TEXT
LANGUAGE PYTHON
RETURNS NULL ON NULL INPUT
IMMUTABLE
RUNTIME_VERSION = '3.10'
HANDLER = 'hex_to_int'

AS $$ 

def hex_to_int(hex) -> str:
    """
    Converts hex (of any size) to int (as a string). Snowflake and java script can only handle up to 64-bit (38 digits of precision)
    hex_to_int('200000000000000000000000000000211');
    >> 680564733841876926926749214863536423441
    hex_to_int('0x200000000000000000000000000000211');
    >> 680564733841876926926749214863536423441
    hex_to_int(NULL);
    >> NULL
    """
    try:
        return str(int(hex, 16)) if hex and hex != "0x" else None
    except:
        return None

$$;


CREATE OR REPLACE FUNCTION utils.helpers.hex_to_int(encoding STRING, hex STRING)
RETURNS TEXT
LANGUAGE PYTHON
RETURNS NULL ON NULL INPUT
IMMUTABLE
RUNTIME_VERSION = '3.10'
HANDLER = 'hex_to_int'

AS $$ 

def hex_to_int(encoding, hex) -> str:
    """
    Converts hex (of any size) to int (as a string). Snowflake and java script can only handle up to 64-bit (38 digits of precision)
    hex_to_int('hex', '200000000000000000000000000000211');
    >> 680564733841876926926749214863536423441
    hex_to_int('hex', '0x200000000000000000000000000000211');
    >> 680564733841876926926749214863536423441
    hex_to_int('hex', NULL);
    >> NULL
    hex_to_int('s2c', 'ffffffffffffffffffffffffffffffffffffffffffffffffffffffffe5b83acf');
    >> -440911153
    """
    try:
        if not hex:
            return None
        if encoding.lower() == 's2c':
            if hex[0:2].lower() != '0x':
                hex = f'0x{hex}'

            bits = len(hex[2:]) * 4
            value = int(hex, 0)
            if value & (1 << (bits - 1)):
                value -= 1 << bits
            return str(value)
        else:
            return str(int(hex, 16))
    except:
        return None

$$;


CREATE OR REPLACE FUNCTION utils.helpers.base58_to_hex(base58 STRING)
RETURNS TEXT
LANGUAGE PYTHON
RETURNS NULL ON NULL INPUT
RUNTIME_VERSION = '3.10'
HANDLER = 'transform_base58_to_hex'

AS $$ 

def transform_base58_to_hex(base58):
    if base58 is None:
        return 'Invalid input'

    ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    base_count = len(ALPHABET)

    num = 0
    leading_zeros = 0

    for char in base58:
        if char == '1':
            leading_zeros += 1
        else:
            break

    for char in base58:
        num *= base_count
        if char in ALPHABET:
            num += ALPHABET.index(char)
        else:
            return 'Invalid character in input'

    hex_string = hex(num)[2:]

    if len(hex_string) % 2 != 0:
        hex_string = '0' + hex_string

    hex_leading_zeros = '00' * leading_zeros

    return '0x' + hex_leading_zeros + hex_string

$$;


CREATE OR REPLACE FUNCTION utils.helpers.hex_to_base58(hex STRING)
RETURNS TEXT
LANGUAGE PYTHON
RETURNS NULL ON NULL INPUT
RUNTIME_VERSION = '3.10'
HANDLER = 'transform_hex_to_base58'

AS $$ 

def transform_hex_to_base58(hex):
    if hex is None or not hex.startswith('0x'):
        return 'Invalid input'

    hex = hex[2:]

    ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    byte_array = bytes.fromhex(hex)
    num = int.from_bytes(byte_array, 'big')

    encoded = ''
    while num > 0:
        num, remainder = divmod(num, 58)
        encoded = ALPHABET[remainder] + encoded

    for byte in byte_array:
        if byte == 0:
            encoded = '1' + encoded
        else:
            break

    return encoded

$$;