from dataclasses import dataclass
from urllib.parse import ParseResult, urlparse
from sqlcli.dburl.exceptions import ErrInvalidTransportProtocol, ErrMissingPath

from sqlcli.dburl.scheme import RegisteredSchemeta, Scheme, Transport, scheme_type

from urllib.parse import urlparse
import re


@dataclass
class URL:
    # Member variables from urlparse.ParseResult
    scheme: str
    netloc: str
    path: str
    params: str
    query: str
    fragment: str
    username: str
    password: str
    hostname: str
    port: int

    original_scheme: str
    transport: str
    driver: str
    unaliased_driver: str
    dsn: str

    def __init__(self, v: ParseResult):
        self.scheme = v.scheme
        self.netloc = v.netloc
        self.path = v.path
        self.params = v.params
        self.query = v.query
        self.fragment = v.fragment
        self.username = v.username
        self.password = v.password
        self.hostname = v.hostname
        self.port = v.port

        self.original_scheme = ""
        self.transport = ""
        self.driver = ""
        self.unaliased_driver = ""
        self.dsn = ""


def parse(urlstr) -> URL:
    v: ParseResult = urlparse(urlstr)

    if v.scheme == "":
        return parse(scheme_type(urlstr) + ":" + urlstr)

    # create url
    u: URL = URL(v)
    u.original_scheme = urlstr[: len(v.scheme)]
    u.transport = "tcp"

    # check for +transport in scheme
    check_transport = False
    if "+" in u.scheme:
        i = u.scheme.index("+")
        u.transport = urlstr[i + 1 : len(v.scheme)]
        u.scheme = u.scheme[:i]
        check_transport = True

    # get dsn generator
    scheme: Scheme = RegisteredSchemeta.get(u.scheme, (None, False))

    if scheme.driver == "file":
        # determine scheme for file
        s = u.opaque_or_path()

        if u.transport != "tcp" or "+" in u.original_scheme:
            raise ErrInvalidTransportProtocol()
        elif s == "":
            raise ErrMissingPath()

        typ = scheme_type(s)
        return parse(typ + "://" + u.build_opaque())
    elif not scheme.opaque and u.url.path != "":
        # if scheme does not understand opaque URLs, retry parsing after
        # building fully qualified URL
        return parse(u.original_scheme + "://" + u.build_opaque())
    elif scheme.opaque and u.url.path == "":
        # force Opaque
        u.url.path, u.url.netloc, u.url.path, u.url.path = (
            u.url.netloc + u.url.path,
            "",
            "",
            "",
        )
    elif u.url.netloc == "." or (
        u.url.netloc == "" and re.sub("^/", "", u.url.path) != ""
    ):
        # force unix proto
        u.transport = "unix"

    # check transport
    if check_transport or u.transport != "tcp":
        if scheme.transport == Transport.TransportNone:
            raise ErrInvalidTransportProtocol()
        elif (
            (scheme.transport != Transport.TransportAny and u.transport != "")
            or (scheme.transport == Transport.TransportTCP and u.transport == "tcp")
            or (scheme.transport == Transport.TransportUDP and u.transport == "udp")
            or (scheme.transport == Transport.TransportUnix and u.transport == "unix")
        ):
            pass
        else:
            raise ErrInvalidTransportProtocol()

    # set driver
    u.driver, u.unaliased_driver = scheme.driver, scheme.driver
    if scheme.override != "":
        u.driver = scheme.override

    # generate dsn
    u.dsn, u.go_driver = scheme.generator(u)
    return u
