"""
This file handles CMD configuration of Host & Port of the server.
Command line configuration

Config is kept different from the server so in later stages
(Replication's --replicaof, RDB's --dir/--dbfilename, AOF's --appendonlyfile) just add fields here
wihtout touching event loop.
"""

import argparse
from dataclasses import dataclass


@dataclass
class Config:
    host: str = '0.0.0.0'
    port: int = 6379
    replicaof_host: str = None
    replicaof_port: str = None

def parse_args(argv=None) -> Config:
    parser = argparse.ArgumentParser(description="A REdis like server in python from scratch.")
    parser.add_argument("--port", type=int, default=6379, help="Port to listen on (default: 6379)")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="Host/Interface to bind to")
    parser.add_argument("--replicaof", nargs=2, metavar=("HOST", "PORT"), default=None, help="Replicate from the master at HOST PORT (e.g  --replicaof localhost 6379)")
    args = parser.parse_args(argv)

    replicaof_host, replicaof_port = None, None
    if args.replicaof is not None:
        replicaof_host = args.replicaof[0]
        replicaof_port = int(args.replicaof[1])

    return Config(
        host=args.host, 
        port=args.port,
        replicaof_host=replicaof_host,
        replicaof_port=replicaof_port,
    )
