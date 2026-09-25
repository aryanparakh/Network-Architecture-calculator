"""
Automated Unit and Integration Tests for Persistent HTTP/1.1 Calculator Server
"""

import unittest
import socket
import threading
import time
from server import CalculatorServer, HTTPRequestParser, CalculatorHandler, HTTPRequest


def get_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class TestCalculatorLogic(unittest.TestCase):
    """Direct unit tests for CalculatorHandler logic."""

    def _make_req(self, method="GET", path="/add", query_params=None, headers=None, body=b""):
        if query_params is None:
            query_params = {"a": ["2"], "b": ["3"]}
        if headers is None:
            headers = {"host": "localhost"}
        return HTTPRequest(
            method=method,
            target=path,
            path=path,
            query_params=query_params,
            version="HTTP/1.1",
            headers=headers,
            body=body,
        )

    def test_add_endpoint(self):
        req = self._make_req(path="/add", query_params={"a": ["2"], "b": ["3"]})
        resp, close = CalculatorHandler.handle_request(req)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.body, b"5")
        self.assertFalse(close)

    def test_sub_endpoint(self):
        req = self._make_req(path="/sub", query_params={"a": ["10"], "b": ["4"]})
        resp, close = CalculatorHandler.handle_request(req)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.body, b"6")
        self.assertFalse(close)

    def test_mul_endpoint(self):
        req = self._make_req(path="/mul", query_params={"a": ["6"], "b": ["7"]})
        resp, close = CalculatorHandler.handle_request(req)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.body, b"42")
        self.assertFalse(close)

    def test_div_endpoint(self):
        req = self._make_req(path="/div", query_params={"a": ["9"], "b": ["3"]})
        resp, close = CalculatorHandler.handle_request(req)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.body, b"3")
        self.assertFalse(close)

    def test_div_by_zero(self):
        req = self._make_req(path="/div", query_params={"a": ["1"], "b": ["0"]})
        resp, close = CalculatorHandler.handle_request(req)
        self.assertEqual(resp.status_code, 400)
        self.assertFalse(close)

    def test_invalid_parameter(self):
        req = self._make_req(path="/add", query_params={"a": ["x"], "b": ["3"]})
        resp, close = CalculatorHandler.handle_request(req)
        self.assertEqual(resp.status_code, 400)
        self.assertFalse(close)

    def test_missing_parameter(self):
        req = self._make_req(path="/add", query_params={"a": ["2"]})
        resp, close = CalculatorHandler.handle_request(req)
        self.assertEqual(resp.status_code, 400)
        self.assertFalse(close)

    def test_unknown_operation(self):
        req = self._make_req(path="/pow", query_params={"a": ["2"], "b": ["8"]})
        resp, close = CalculatorHandler.handle_request(req)
        self.assertEqual(resp.status_code, 404)
        self.assertFalse(close)

    def test_unsupported_method(self):
        req = self._make_req(method="POST", path="/add", query_params={"a": ["2"], "b": ["3"]})
        resp, close = CalculatorHandler.handle_request(req)
        self.assertEqual(resp.status_code, 405)
        self.assertFalse(close)

    def test_missing_host_header(self):
        req = self._make_req(headers={})  # Missing Host
        resp, close = CalculatorHandler.handle_request(req)
        self.assertEqual(resp.status_code, 400)
        self.assertFalse(close)


class TestBufferFraming(unittest.TestCase):
    """Unit tests for the stream framing buffer parser."""

    def test_incomplete_headers(self):
        buf = bytearray(b"GET /add?a=2&b=3 HTTP/1.1\r\nHost: local")
        self.assertIsNone(HTTPRequestParser.inspect_buffer(buf))

    def test_complete_no_body(self):
        buf = bytearray(b"GET /add?a=2&b=3 HTTP/1.1\r\nHost: localhost\r\n\r\n")
        framing = HTTPRequestParser.inspect_buffer(buf)
        self.assertIsNotNone(framing)
        header_end, total_len = framing
        self.assertEqual(total_len, len(buf))

    def test_incomplete_body_with_content_length(self):
        buf = bytearray(b"POST /add HTTP/1.1\r\nHost: localhost\r\nContent-Length: 10\r\n\r\n12345")
        # 5 bytes body out of 10
        self.assertIsNone(HTTPRequestParser.inspect_buffer(buf))

    def test_complete_body_with_content_length(self):
        buf = bytearray(b"POST /add HTTP/1.1\r\nHost: localhost\r\nContent-Length: 10\r\n\r\n1234567890")
        framing = HTTPRequestParser.inspect_buffer(buf)
        self.assertIsNotNone(framing)
        header_end, total_len = framing
        self.assertEqual(total_len, len(buf))

    def test_coalesced_requests_in_buffer(self):
        req1 = b"GET /add?a=1&b=2 HTTP/1.1\r\nHost: localhost\r\n\r\n"
        req2 = b"GET /mul?a=3&b=4 HTTP/1.1\r\nHost: localhost\r\n\r\n"
        buf = bytearray(req1 + req2)

        framing = HTTPRequestParser.inspect_buffer(buf)
        self.assertIsNotNone(framing)
        header_end, total_len = framing
        self.assertEqual(total_len, len(req1))

        # Consume req1
        req1_bytes = bytes(buf[:total_len])
        del buf[:total_len]
        self.assertEqual(req1_bytes, req1)

        # req2 remains intact
        framing2 = HTTPRequestParser.inspect_buffer(buf)
        self.assertIsNotNone(framing2)
        _, total_len2 = framing2
        self.assertEqual(total_len2, len(req2))


class TestServerIntegration(unittest.TestCase):
    """End-to-end integration tests over real TCP sockets."""

    @classmethod
    def setUpClass(cls):
        cls.port = get_free_port()
        cls.server = CalculatorServer(host="127.0.0.1", port=cls.port, idle_timeout=10.0)
        cls.server_thread = threading.Thread(target=cls.server.start, daemon=True)
        cls.server_thread.start()
        # Allow server to bind and listen
        time.sleep(0.1)

    @classmethod
    def tearDownClass(cls):
        cls.server.stop()

    def _read_response(self, sock, buf):
        delimiter = b"\r\n\r\n"
        while True:
            idx = buf.find(delimiter)
            if idx != -1:
                header_end = idx
                delim_len = 4
                break
            chunk = sock.recv(4096)
            if not chunk:
                raise ConnectionError("Socket closed")
            buf.extend(chunk)

        header_bytes = bytes(buf[:header_end])
        lines = header_bytes.decode("iso-8859-1").splitlines()
        code = int(lines[0].split()[1])

        headers = {}
        for line in lines[1:]:
            if ":" in line:
                k, v = line.split(":", 1)
                headers[k.strip().lower()] = v.strip()

        content_len = int(headers.get("content-length", 0))
        total_len = header_end + delim_len + content_len

        while len(buf) < total_len:
            chunk = sock.recv(4096)
            if not chunk:
                raise ConnectionError("Socket closed")
            buf.extend(chunk)

        body = bytes(buf[header_end + delim_len : total_len])
        del buf[:total_len]
        return code, headers, body

    def test_slide_marking_sequence_one_socket(self):
        """Replicates exact slide marking sequence over a single TCP connection."""
        s = socket.create_connection(("127.0.0.1", self.port))
        buf = bytearray()

        cases = [
            ("GET /add?a=2&b=3 HTTP/1.1\r\nHost: localhost\r\n\r\n", 200, b"5"),
            ("GET /sub?a=10&b=4 HTTP/1.1\r\nHost: localhost\r\n\r\n", 200, b"6"),
            ("GET /mul?a=6&b=7 HTTP/1.1\r\nHost: localhost\r\n\r\n", 200, b"42"),
            ("GET /div?a=1&b=0 HTTP/1.1\r\nHost: localhost\r\n\r\n", 400, None),
            ("GET /pow?a=2&b=8 HTTP/1.1\r\nHost: localhost\r\n\r\n", 404, None),
            ("POST /add HTTP/1.1\r\nHost: localhost\r\n\r\n", 405, None),
        ]

        responses = 0
        for req, exp_code, exp_body in cases:
            s.sendall(req.encode("latin1"))
            code, headers, body = self._read_response(s, buf)
            responses += 1
            self.assertEqual(code, exp_code)
            if exp_body is not None:
                self.assertEqual(body, exp_body)

        self.assertEqual(responses, 6)

        # Check socket still open
        s.setblocking(False)
        try:
            peek = s.recv(1, socket.MSG_PEEK)
            # If not closed, no error or blocking
            still_open = True
        except BlockingIOError:
            still_open = True
        except OSError:
            still_open = False
        s.setblocking(True)
        self.assertTrue(still_open, "Socket should remain open after all 6 requests!")
        s.close()

    def test_pipelining(self):
        """Sends multiple requests in a single TCP sendall and reads ordered responses."""
        s = socket.create_connection(("127.0.0.1", self.port))
        buf = bytearray()

        batch = (
            "GET /add?a=100&b=200 HTTP/1.1\r\nHost: localhost\r\n\r\n"
            "GET /sub?a=500&b=200 HTTP/1.1\r\nHost: localhost\r\n\r\n"
            "GET /div?a=99&b=3 HTTP/1.1\r\nHost: localhost\r\n\r\n"
        )
        s.sendall(batch.encode("latin1"))

        c1, _, b1 = self._read_response(s, buf)
        self.assertEqual((c1, b1), (200, b"300"))

        c2, _, b2 = self._read_response(s, buf)
        self.assertEqual((c2, b2), (200, b"300"))

        c3, _, b3 = self._read_response(s, buf)
        self.assertEqual((c3, b3), (200, b"33"))

        s.close()

    def test_fragmented_delivery(self):
        """Sends request bytes incrementally to test partial buffering."""
        s = socket.create_connection(("127.0.0.1", self.port))
        buf = bytearray()

        msg = b"GET /mul?a=9&b=9 HTTP/1.1\r\nHost: localhost\r\n\r\n"
        for byte in [msg[i:i+1] for i in range(len(msg))]:
            s.sendall(byte)
            time.sleep(0.001)

        code, _, body = self._read_response(s, buf)
        self.assertEqual((code, body), (200, b"81"))
        s.close()


if __name__ == "__main__":
    unittest.main()
