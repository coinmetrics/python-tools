from typing import Tuple, Dict


def postgres_connection_argument(value: str) -> Tuple[str, int, str, str, str]:
    pieces = value.split(":")
    if len(pieces) != 5:
        raise ValueError()
    return (pieces[0], int(pieces[1]), pieces[2], pieces[3], pieces[4])


def bitcoin_node_connection_argument(value: str) -> Tuple[str, int, str, str]:
    pieces = value.split(":")
    if len(pieces) != 4:
        raise ValueError()
    return (pieces[0], int(pieces[1]), pieces[2], pieces[3])
