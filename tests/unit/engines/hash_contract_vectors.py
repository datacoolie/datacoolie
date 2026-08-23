"""Shared golden vectors for portable DataCoolie hash algorithms."""

from datetime import date


HASH_CONTRACT_ROWS = [
    ("VN", 123, True, date(2026, 8, 1)),
    ("Việt Nam", -4, False, date(2024, 1, 2)),
    ("", 0, True, date(1970, 1, 1)),
    (None, None, None, None),
]

SHA256_HASHES = [
    "842577920fb330d701994d15e8e4fb4a0a2ab2e0042d7b6f8aebb8251f9bfb8c",
    "5cb808ee58dfd69c938d9ecacf7f4c39b923a0b9c38a966b8d9ab8341424e0c6",
    "036414af1fb43b2fd0761dea6c72827f810d4cb1134a471ee1d41e63864f2c2b",
    "3486794cdeaf9e4af12ee78b4cd9738d29b833149974c7aff14eccc86192dc52",
]

XXHASH64_HASHES = [
    -1252426240253896258,
    -8970267855725418729,
    3323983074380353824,
    -3005774564412403364,
]
