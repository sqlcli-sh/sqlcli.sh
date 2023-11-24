from dataclasses import dataclass
import os
import stat
from typing import Callable, Dict, List, Tuple, Any

import re
import sys

from enum import Enum

from sqlcli.dburl.dsn import gen_from_url

class Transport(Enum):
    """
    Transport types.
    """
    TransportNone = 0
    TransportTCP = 1
    TransportUDP = 2
    TransportUnix = 4
    TransportAny = 8

@dataclass
class Scheme:
    """
    Scheme is the Python equivalent of the Go struct with the same name.
    """
    Driver: str
    Generator: Callable[[Any], Tuple[str, str, Any]]
    Transport: Any
    Opaque: bool
    Aliases: List[str]
    Override: str

import re

class FileType:
    def __init__(self, driver, f, ext):
        self.driver = driver
        self.f = f
        self.ext = re.compile(ext)

BaseSchemes: List[Scheme] = [
    Scheme(Driver="file", Generator=gen_opaque, Transport=0, Opaque=True, Aliases=["file"], Override=""),

    # Core Databases
    Scheme(Driver="mysql", Generator=gen_mysql, Transport=Transport.TransportTCP | Transport.TransportUDP | Transport.TransportUnix, Opaque=False, Aliases=["mariadb", "maria", "percona", "aurora"], Override=""),
    Scheme(Driver="oracle", Generator=gen_from_url("oracle://localhost:1521"), Transport=0, Opaque=False, Aliases=["ora", "oci", "oci8", "odpi", "odpi-c"], Override=""),
    Scheme(Driver="postgres", Generator=gen_postgres, Transport=Transport.TransportUnix, Opaque=False, Aliases=["pg", "postgresql", "pgsql"], Override=""),
    Scheme(Driver="sqlite3", Generator=gen_opaque, Transport=0, Opaque=True, Aliases=["sqlite"], Override=""),
    Scheme(Driver="sqlserver", Generator=GenSqlserver, Transport=0, Opaque=False, Aliases=["ms", "mssql", "azuresql"], Override="")

    # Wire Compatible
    Scheme(Driver="cockroachdb", Generator=gen_from_url("postgres://localhost:26257/?sslmode=disable"), Transport=0, Opaque=False, Aliases=["cr", "cockroach", "crdb", "cdb"], Override="postgres"),
    Scheme(Driver="memsql", Generator=gen_mysql, Transport=0, Opaque=False, Aliases=[], Override="mysql"),
    Scheme(Driver="redshift", Generator=gen_from_url("postgres://localhost:5439/"), Transport=0, Opaque=False, Aliases=["rs"], Override="postgres"),
    Scheme(Driver="tidb", Generator=gen_mysql, Transport=0, Opaque=False, Aliases=[], Override="mysql"),
    Scheme(Driver="vitess", Generator=gen_mysql, Transport=0, Opaque=False, Aliases=["vt"], Override="mysql")

    # Other Databases
    Scheme(Driver="adodb", Generator=gen_adodb, Transport=0, Opaque=False, Aliases=["ado"], Override=""),
    Scheme(Driver="awsathena", Generator=gen_scheme("s3"), Transport=0, Opaque=False, Aliases=["s3", "aws", "athena"], Override=""),
    Scheme(Driver="avatica", Generator=gen_from_url("http://localhost:8765/"), Transport=0, Opaque=False, Aliases=["phoenix"], Override=""),
    Scheme(Driver="bigquery", Generator=gen_scheme("bigquery"), Transport=0, Opaque=False, Aliases=["bq"], Override=""),
    Scheme(Driver="clickhouse", Generator=gen_from_url("clickhouse://localhost:9000/"), Transport=0, Opaque=False, Aliases=["ch"], Override=""),
    Scheme(Driver="duckdb", Generator=gen_opaque, Transport=0, Opaque=True, Aliases=["dk", "ddb", "duck"], Override=""),
    Scheme(Driver="cosmos", Generator=gen_cosmos, Transport=0, Opaque=False, Aliases=["cm"], Override=""),
    Scheme(Driver="cql", Generator=gen_cassandra, Transport=0, Opaque=False, Aliases=["ca", "cassandra", "datastax", "scy", "scylla"], Override=""),
    Scheme(Driver="csvq", Generator=gen_opaque, Transport=0, Opaque=True, Aliases=["csv", "tsv", "json"], Override=""),
    Scheme(Driver="databend", Generator=gen_databend, Transport=0, Opaque=False, Aliases=["dd", "bend"], Override=""),
    Scheme(Driver="exasol", Generator=gen_exasol, Transport=0, Opaque=False, Aliases=["ex", "exa"], Override=""),
    Scheme(Driver="firebirdsql", Generator=gen_firebird, Transport=0, Opaque=False, Aliases=["fb", "firebird"], Override=""),
    Scheme(Driver="flightsql", Generator=gen_scheme("flightsql"), Transport=0, Opaque=False, Aliases=["fl", "flight"], Override=""),
    Scheme(Driver="genji", Generator=gen_opaque, Transport=0, Opaque=True, Aliases=["gj"], Override=""),
    Scheme(Driver="h2", Generator=gen_from_url("h2://localhost:9092/"), Transport=0, Opaque=False, Aliases=[], Override=""),
    Scheme(Driver="hdb", Generator=gen_scheme("hdb"), Transport=0, Opaque=False, Aliases=["sa", "saphana", "sap", "hana"], Override=""),
    Scheme(Driver="hive", Generator=gen_scheme_truncate, Transport=0, Opaque=False, Aliases=[], Override=""),
    Scheme(Driver="ignite", Generator=gen_ignite, Transport=0, Opaque=False, Aliases=["ig", "gridgain"], Override=""),
    Scheme(Driver="impala", Generator=gen_scheme("impala"), Transport=0, Opaque=False, Aliases=[], Override=""),
    Scheme(Driver="maxcompute", Generator=gen_scheme_truncate, Transport=0, Opaque=False, Aliases=["mc"], Override=""),
    Scheme(Driver="n1ql", Generator=gen_from_url("http://localhost:9000/"), Transport=0, Opaque=False, Aliases=["couchbase"], Override=""),
    Scheme(Driver="nzgo", Generator=gen_postgres, Transport=Transport.TransportUnix, Opaque=False, Aliases=["nz", "netezza"], Override=""),
    Scheme(Driver="odbc", Generator=gen_odbc, Transport=Transport.TransportAny, Opaque=False, Aliases=[], Override=""),
    Scheme(Driver="oleodbc", Generator=gen_ole_odbc, Transport=Transport.TransportAny, Opaque=False, Aliases=["oo", "ole"], Override="adodb"),
    Scheme(Driver="ots", Generator=gen_table_store, Transport=Transport.TransportAny, Opaque=False, Aliases=["tablestore"], Override=""),
    Scheme(Driver="presto", Generator=gen_presto, Transport=0, Opaque=False, Aliases=["prestodb", "prestos", "prs", "prestodbs"], Override=""),
    Scheme(Driver="ql", Generator=gen_opaque, Transport=0, Opaque=True, Aliases=["ql", "cznic", "cznicql"], Override=""),
    Scheme(Driver="snowflake", Generator=gen_snowflake, Transport=0, Opaque=False, Aliases=["sf"], Override=""),
    Scheme(Driver="spanner", Generator=gen_spanner, Transport=0, Opaque=False, Aliases=["sp"], Override=""),
    Scheme(Driver="tds", Generator=gen_from_url("http://localhost:5000/"), Transport=0, Opaque=False, Aliases=["ax", "ase", "sapase"], Override=""),
    Scheme(Driver="trino", Generator=gen_presto, Transport=0, Opaque=False, Aliases=["trino", "trinos", "trs"], Override=""),
    Scheme(Driver="vertica", Generator=gen_from_url("vertica://localhost:5433/"), Transport=0, Opaque=False, Aliases=[], Override=""),
    Scheme(Driver="voltdb", Generator=gen_voltdb, Transport=0, Opaque=False, Aliases=["volt", "vdb"], Override="")
]


RegisteredSchemeta: Dict[str, Scheme] = {}

def register_alias(name, alias, do_sort):
    scheme = RegisteredSchemeta.get(name)
    if scheme is None:
        sys.exit(f"scheme {name} not registered")
    if do_sort and alias in scheme.Aliases:
        sys.exit(f"scheme {name} already has alias {alias}")
    if alias in RegisteredSchemeta:
        sys.exit(f"scheme {alias} already registered")
    scheme.Aliases.append(alias)
    if do_sort:
        scheme.Aliases.sort(key=len)
    RegisteredSchemeta[alias] = scheme

def register(scheme):
    if scheme.Generator is None:
        sys.exit("must specify Generator when registering Scheme")
    if scheme.Opaque and scheme.Transport & Transport.TransportUnix != 0:
        sys.exit("scheme must support only Opaque or Unix protocols, not both")
    # check if registered
    if scheme.Driver in RegisteredSchemeta:
        sys.exit(f"scheme {scheme.Driver} already registered")
    sz = Scheme(
        Driver=scheme.Driver,
        Generator=scheme.Generator,
        Transport=scheme.Transport,
        Opaque=scheme.Opaque,
        Override=scheme.Override,
    )
    RegisteredSchemeta[scheme.Driver] = sz
    # add aliases
    has_short = False
    for alias in scheme.Aliases:
        if len(alias) == 2:
            has_short = True
        if scheme.Driver != alias:
            register_alias(scheme.Driver, alias, False)
    if not has_short and len(scheme.Driver) > 2:
        register_alias(scheme.Driver, scheme.Driver[:2], False)
    # ensure always at least one alias, and that if Driver is 2 characters,
    # that it gets added as well
    if len(sz.Aliases) == 0 or len(scheme.Driver) == 2:
        sz.Aliases.append(scheme.Driver)
    # sort
    sz.Aliases.sort(key=len)

def unregister(name):
    scheme = RegisteredSchemeta.get(name)
    if scheme is None:
        raise KeyError(f"Scheme {name} not found")
    del RegisteredSchemeta[name]
    return scheme

file_types: List[FileType] = []

def register_file_type(driver, f, ext):
    try:
        ext_re = re.compile(ext)
    except re.error:
        sys.exit(f"invalid extension regexp {ext}")
    file_types.append(FileType(driver=driver, f=f, ext=ext_re))

sqlite3_header = b"SQLite format 3\000"

def is_sqlite3_header(buf):
    return buf.startswith(sqlite3_header)

duckdb_re = re.compile(b'^.{8}DUCK.{8}')

def is_duckdb_header(buf):
    return bool(duckdb_re.match(buf))

def init():
    global RegisteredSchemeta
    # register schemes
    schemes = BaseSchemes
    RegisteredSchemeta = {scheme.Driver: None for scheme in schemes}
    for scheme in schemes:
        register(scheme)
    register_file_type("duckdb", is_duckdb_header, r'(?i)\.duckdb$')
    register_file_type("sqlite3", is_sqlite3_header, r'(?i)\.(db|sqlite|sqlite3)$')

def scheme_type(name: str) -> str:
    # try to resolve the path on unix systems
    if os.name != "nt":
        return resolve_type(name)
    else:
        with open(name, 'rb') as f:
            # file exists, match header
            buf = f.read(64)
            if len(buf) == 0:
                return "sqlite3", None
            for typ in file_types:
                if typ.f(buf):
                    return typ.driver, None

        # doesn't exist, match file extension
        ext = os.path.splitext(name)[1]
        for typ in file_types:
            if re.match(typ.ext, ext):
                return typ.driver, None

        return ErrUnknownFileType()

def resolve_type(s: str) -> str:
    if '?' in s:
        i = s.rindex('?')
        if os.path.exists(s[:i]):
            s = s[:i]

    dir = s
    while dir and dir != "/" and dir != ".":
        # chop off :4444 port
        i, j = dir.rfind(":"), dir.rfind("/")
        if i != -1 and i > j:
            dir = dir[:i]

        st = os.stat(dir)
        if stat.S_ISDIR(st.st_mode):
            return "postgres"
        elif stat.S_ISSOCK(st.st_mode):
            return "mysql"

        if j != -1:
            dir = dir[:j]
        else:
            dir = ""

    return ""