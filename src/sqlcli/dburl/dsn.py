import os
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode
from typing import Callable, List, Tuple

from sqlcli.dburl import URL
from sqlcli.dburl.exceptions import ErrMissingHost, ErrMissingPath, ErrMissingUser

def convert_options(q: dict, pairs: List[str]) -> dict:
    n = {}
    for k, v in q.items():
        x = v.copy()
        for i, z in enumerate(v):
            for j in range(0, len(pairs), 2):
                if pairs[j] == z:
                    z = pairs[j+1]
            x[i] = z
        n[k] = x
    return n

def gen_query_options(q: dict) -> str:
    s = urlencode(q)
    if s != "":
        return "?" + s
    return ""

def gen_options_odbc(q: dict, skip_when_empty: bool, ignore: List[str]) -> str:
    return gen_options(q, "", "=", ";", ",", skip_when_empty, ignore)

def gen_options(q: dict, joiner: str, assign: str, sep: str, val_sep: str, skip_when_empty: bool, ignore: List[str]) -> str:
    if len(q) == 0:
        return ""
    # make ignore map
    ig = {i.lower(): True for i in ignore}
    # sort keys
    s = sorted(q.keys())
    opts = []
    for k in s:
        if not ig.get(k.lower()):
            val = val_sep.join(q[k])
            if not skip_when_empty or val != "":
                if val != "":
                    val = assign + val
                opts.append(k + val)
    if len(opts) != 0:
        return joiner + sep.join(opts)
    return ""

def gen_scheme(scheme: str) -> Callable[[URL], Tuple[str, str]]:
    def inner(u: URL) -> Tuple[str, str]:
        u = u._replace(scheme=scheme, netloc=u.netloc or 'localhost')
        return urlunparse(u), ""
    return inner

def gen_scheme_truncate(u: URL) -> Tuple[str, str]:
    s = str(u)
    i = s.find('://')
    if i != -1:
        return s[i+3:], ""
    return s, ""

def gen_from_url(urlstr: str) -> Callable[[URL], Tuple[str, str]]:
    z = urlparse(urlstr)
    def inner(u: URL) -> Tuple[str, str]:
        opaque = z.path if u.path == '' else u.path
        user = z.username if u.username is None else u.username
        host = z.hostname if u.hostname == '' else u.hostname
        port = z.port if u.port == '' else u.port
        if port != '':
            host += ':' + port
        pstr = z.path if u.path == '' else u.path
        raw_path = z.path if u.path == '' else u.path
        q = parse_qs(z.query)
        for k, v in parse_qs(u.query).items():
            q[k] = ' '.join(v)
        fragment = z.fragment if u.fragment == '' else u.fragment
        y = z._replace(scheme=z.scheme, path=opaque, username=user, netloc=host,
                       path=pstr, query=urlencode(q), fragment=fragment)
        return urlunparse(y), ""
    return inner

def gen_opaque(u: URL) -> Tuple[str, str]:
    if u.path == '':
        raise ErrMissingPath("Missing path")
    return u.path + urlencode(parse_qs(u.query)), ""

def gen_adodb(u: URL) -> Tuple[str, str]:
    host, port = u.hostname, u.port
    dsname, dbname = u.path.lstrip('/'), ""
    if dsname == "":
        dsname = "."
    if os.path.exists(dsname) == 0:
        i = dsname.find('/')
        if i != -1:
            dbname = dsname[i+1:]
            dsname = dsname[:i]
    q = parse_qs(u.query)
    q["Provider"] = host
    q["Port"] = port
    q["Data Source"] = dsname
    q["Database"] = dbname
    if u.username is not None:
        q["User ID"] = u.username
        q["Password"] = u.password
    if u.hostPortDB is None:
        n = dsname
        if dbname != "":
            n += "/" + dbname
        u.hostPortDB = [host, port, n]
    return gen_options_odbc(q, True), ""

def gen_cassandra(u: URL) -> Tuple[str, str]:
    u = urlparse(u)
    host, port, dbname = "localhost", "9042", u.path.lstrip('/')
    if u.hostname != "":
        host = u.hostname
    if u.port != "":
        port = u.port
    q = parse_qs(u.query)
    if u.username is not None:
        q["username"] = u.username
        if u.password != "":
            q["password"] = u.password
    if dbname != "":
        q["keyspace"] = dbname
    return host + ":" + port + gen_query_options(q), ""

def gen_cosmos(u: URL) -> Tuple[str, str]:
    u = urlparse(u)
    host, port, dbname = u.hostname, u.port, u.path.lstrip('/')
    if port != "":
        port = ":" + port
    q = parse_qs(u.query)
    q["AccountEndpoint"] = "https://" + host + port
    if u.username is None:
        raise ErrMissingUser("Missing user")
    q["AccountKey"] = u.username
    if dbname != "":
        q["Db"] = dbname
    return gen_options_odbc(q, True), ""

def gen_databend(u: URL) -> Tuple[str, str]:
    u = urlparse(u)
    if u.hostname == "":
        raise ErrMissingHost("Missing host")
    return str(u), ""

def gen_exasol(u: URL) -> Tuple[str, str, None]:
    u = urlparse(u)
    host, port, dbname = u.hostname, u.port, u.path.lstrip('/')
    if host == "":
        host = "localhost"
    if port == "":
        port = "8563"
    q = parse_qs(u.query)
    if dbname != "":
        q["schema"] = dbname
    if u.username is not None:
        q["user"] = u.username
        q["password"] = u.password
    return f"exa:{host}:{port}{gen_options(q, ';', '=', ';', ',', True)}", ""

def gen_firebird(u: URL) -> Tuple[str, str]:
    z = urlparse(u)
    return z.geturl().lstrip('//'), ""

def gen_godror(u: URL) -> Tuple[str, str]:
    u = urlparse(u)
    host, port, service = u.hostname, u.port, u.path.lstrip('/')
    instance = ""
    i = service.rfind('/')
    if i != -1:
        instance, service = service[i+1:], service[:i]
    dsn = host
    if port != "":
        dsn += ":" + port
    if u.username is not None:
        n = u.username
        if u.password != "":
            n += "/" + u.password
        dsn = n + "@//" + dsn
    if service != "":
        dsn += "/" + service
    if instance != "":
        dsn += "/" + instance
    return dsn, ""

def gen_ignite(u: URL) -> Tuple[str, str]:
    u = urlparse(u)
    host, port, dbname = "localhost", "10800", u.path.lstrip('/')
    if u.hostname != "":
        host = u.hostname
    if u.port != "":
        port = u.port
    q = parse_qs(u.query)
    if u.username is not None:
        q["username"] = u.username
        if u.password != "":
            q["password"] = u.password
    if dbname != "":
        dbname = "/" + dbname
    return "tcp://" + host + ":" + port + dbname + urlencode(q), ""

def gen_mymysql(u: URL) -> Tuple[str, str]:
    u = urlparse(u)
    host, port, dbname = u.hostname, u.port, u.path.lstrip('/')
    if u.scheme == "unix":
        if host == "":
            dbname = "/" + dbname
        host, dbname = resolve_socket(os.path.join(host, dbname))
        port = ""
    if u.scheme != "unix":
        if host == "":
            host = "localhost"
        if port == "":
            port = "3306"
    if port != "":
        port = ":" + port
    dsn = u.scheme + ":" + host + port
    dsn += gen_options(convert_options(parse_qs(u.query), "true", ""), ",", "=", ",", " ", False)
    dsn += "*" + dbname
    if u.username is not None:
        dsn += "/" + u.username + "/" + u.password
    elif dsn.endswith("*"):
        dsn += "//"
    return dsn, ""

def gen_mysql(u: URL) -> Tuple[str, str]:
    u = urlparse(u)
    host, port, dbname = u.hostname, u.port, u.path.lstrip('/')
    dsn = ""
    if u.username is not None:
        n = u.username
        if u.password != "":
            n += ":" + u.password
        dsn += n + "@"
    if u.scheme == "unix":
        if host == "":
            dbname = "/" + dbname
        host, dbname = resolve_socket(os.path.join(host, dbname))
        port = ""
    if u.scheme != "unix":
        if host == "":
            host = "localhost"
        if port == "":
            port = "3306"
    if port != "":
        port = ":" + port
    dsn += u.scheme + "(" + host + port + ")" + "/" + dbname
    return dsn + urlencode(parse_qs(u.query)), ""

def gen_odbc(u: URL) -> Tuple[str, str]:
    host, port, dbname = u.hostname, u.port, u.path.lstrip('/')
    q = parse_qs(u.query)
    q["Driver"] = "{" + u.scheme.replace("+", " ") + "}"
    if u.hostPortDB is None:
        u.hostPortDB = [host, port, dbname]
    q = parse_qs(u.query)
    q["Server"] = host
    if port == "":
        proto = u.scheme.lower()
        if "mysql" in proto:
    q['Driver'] = "{" + u.transport.replace("+", " ") + "}"
    q['Server'] = host
    if port == "":
        proto = u.transport.lower()
        q["Port"] = "3306"
    elif "postgres" in proto:
        q["Port"] = "5432"
    if "mysql" in proto:
        q['Port'] = "3306"
    elif "postgres" in proto:
    elif "db2" in proto or "ibm" in proto:
        q["ServiceName"] = "50000"
    else:
            q["Port"] = "1433"
    else:
        q["Port"] = port
    q["Database"] = dbname
    if u.username is not None:
        q["UID"] = u.username
        q['Port'] = "5432"
    elif "db2" in proto or "ibm" in proto:
        q['ServiceName'] = "50000"
    else:
        q['Port'] = "1433"
    else:
        q['Port'] = port
    q['Database'] = dbname
        q["PWD"] = u.password
    return urlencode(q), "", None

    if u.username is not None:
        q['UID'] = u.username
        q['PWD'] = u.password
def gen_oleodbc(u: URL) -> Tuple[str, str, None]:
    props, _, _ = gen_odbc(u)
    return gen_options_odbc(q, True), "", None

def gen_oleodbc(u: URL) -> Tuple[str, str, None]:
    return 'Provider=MSDASQL.1;Extended Properties="' + props + '"', "", None

def gen_postgres(u: URL) -> Tuple[str, str, None]:
    host, port, dbname = u.hostname, u.port, u.path.lstrip('/')
    if host == ".":
        raise ErrRelativePathNotSupported
    if u.scheme == "unix":
    props, _, err = gen_odbc(u)
    if err is not None:
        return "", "", None
    return 'Provider=MSDASQL.1;Extended Properties="' + props + '"', "", None

def gen_postgres(u: URL) -> Tuple[str, str, None]:
        if host == "":
            dbname = "/" + dbname
        host, port, dbname = resolve_dir(os.path.join(host, dbname))
    host, port, dbname = u.hostname, u.port, u.path.lstrip('/')
    if host == ".":
        return "", "", ErrRelativePathNotSupported
    if u.transport == "unix":
    q = parse_qs(u.query)
    q["host"] = host
    q["port"] = port
    q["dbname"] = dbname
        if host == "":
            dbname = "/" + dbname
        host, port, dbname = resolve_dir(os.path.join(host, dbname))
    if u.username is not None:
        q["user"] = u.username
        q["password"] = u.password
    return urlencode(q), "", None

    q = parse_qs(u.query)
    q['host'] = host
    q['port'] = port
    q['dbname'] = dbname

def gen_presto(u: URL) -> Tuple[str, str, None]:
    z = urlparse(u.geturl())
    if z.scheme.endswith("s"):
    if u.username is not None:
        q['user'] = u.username
        q['password'] = u.password
    if u.hostPortDB is None:
        z.scheme = "https"
    if z.username is None:
        z.username = "user"
    if z.hostname == "":
        z.hostname = "localhost"
        u.hostPortDB = [host, port, dbname]
    return gen_options(q, "", "=", " ", ",", True), "", None

    if z.port == "":
        if z.scheme == "http":
            z.hostname += ":8080"
        else:
            z.hostname += ":8443"


def gen_presto(u: URL) -> Tuple[str, str, None]:
    z = urlparse(u)
    if z.scheme.endswith("s"):
        z.scheme = "https"
    if z.username is None:
        z.username = "user"
    if z.hostname == "":
        z.hostname = "localhost"
    if z.port == "":
        if z.scheme == "http":
            z.hostname += ":8080"
        else:
            z.hostname += ":8443"
    q = parse_qs(z.query)
    dbname, schema = z.path.lstrip('/'), ""
    if dbname == "":
        dbname = "default"
    elif "/" in dbname:
        schema, dbname = dbname.split("/", 1)
    q["catalog"] = dbname
    if schema != "":
        q["schema"] = schema
    z.query = urlencode(q)
    return z.geturl(), "", None

def gen_snowflake(u: URL) -> Tuple[str, str, None]:
    host, port, dbname = u.hostname, u.port, u.path.lstrip('/')
    if host == "":
        raise ErrMissingHost
    if port != "":
        port = ":" + port
    if u.username is None:
        raise ErrMissingUser
    user = u.username
    if u.password != "":
        user += ":" + u.password
    return user + "@" + host + port + "/" + dbname + urlencode(parse_qs(u.query)), "", None

def gen_spanner(u: URL) -> Tuple[str, str, None]:
    project, instance, dbname = u.hostname, "", u.path.lstrip('/')
    if project == "":
        raise ErrMissingHost
    if "/" not in dbname:
        raise ErrMissingPath
    instance, dbname = dbname.split("/", 1)
    if instance == "" or dbname == "":
        raise ErrMissingPath
    return f'projects/{project}/instances/{instance}/databases/{dbname}', "", None

def gen_sqlserver(u: URL) -> Tuple[str, str, None]:
    z = urlparse(u)
    if z.hostname == "":
        z.hostname = "localhost"
    driver = "sqlserver"
    if "azuresql" in z.scheme.lower() or "fedauth" in z.query:
        driver = "azuresql"
    v = z.path.lstrip('/').split("/")
    q = parse_qs(z.query)
    if "database" not in q and v and v[0]:
        q["database"] = v[-1]
        z.path, z.query = "/" + "/".join(v[:-1]), urlencode(q)
    return z.geturl(), driver, None

def gen_tablestore(u: URL) -> Tuple[str, str, None]:
    transport = ""
    splits = u.scheme.split("+")
    if not splits:
        raise ErrInvalidDatabaseScheme
    elif len(splits) == 1 or splits[1] == "https":
        transport = "https"
    elif splits[1] == "http":
        transport = "http"
    else:
        raise ErrInvalidTransportProtocol
    z = urlparse(u)
    z.scheme = transport
    return z.geturl(), "", None

def gen_voltdb(u: URL) -> Tuple[str, str, None]:
    host, port = u.hostname or "localhost", u.port or "21212"
    return host + ":" + port, "", None
