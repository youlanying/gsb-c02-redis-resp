from typing import Any, Union

from ..exceptions import ConnectionError, InvalidResponse, ResponseError
from ..typing import EncodableT
from ..utils import SENTINEL
from .base import _AsyncRESPBase, _RESPBase, parse_frame_int
from .socket import SERVER_CLOSED_CONNECTION_ERROR


class _RESP2Parser(_RESPBase):
    """RESP2 protocol implementation"""

    def read_response(
        self, disable_decoding=False, timeout: Union[float, object] = SENTINEL
    ):
        if self._buffer is None:
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)
        pos = self._buffer.get_pos() if self._buffer else None
        try:
            result = self._read_response(
                disable_decoding=disable_decoding, timeout=timeout
            )
        except InvalidResponse:
            # A protocol/framing error is unrecoverable: the buffered bytes
            # can no longer be trusted to hold reply boundaries. Drop them
            # instead of rewinding, so a subsequent read on this connection
            # fails instead of re-parsing the same bad frame forever.
            if self._buffer is not None:
                self._buffer.close()
            raise
        except BaseException:
            if self._buffer:
                self._buffer.rewind(pos)
            raise
        else:
            self._buffer.purge()
            return result

    def _read_response(
        self, disable_decoding=False, timeout: Union[float, object] = SENTINEL
    ):
        raw = self._buffer.readline(timeout=timeout)
        if not raw:
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)

        byte, response = raw[:1], raw[1:]

        # server returned an error
        if byte == b"-":
            response = response.decode("utf-8", errors="replace")
            error = self.parse_error(response)
            # if the error is a ConnectionError, raise immediately so the user
            # is notified
            if isinstance(error, ConnectionError):
                raise error
            # otherwise, we're dealing with a ResponseError that might belong
            # inside a pipeline response. the connection's read_response()
            # and/or the pipeline's execute() will raise this error if
            # necessary, so just return the exception instance here.
            return error
        # single value
        elif byte == b"+":
            pass
        # int value
        elif byte == b":":
            return parse_frame_int(response)
        # bulk response
        elif byte == b"$" and response == b"-1":
            return None
        elif byte == b"$":
            response = self._buffer.read(parse_frame_int(response), timeout=timeout)
        # multi-bulk response
        elif byte == b"*" and response == b"-1":
            return None
        elif byte == b"*":
            response = [
                self._read_response(disable_decoding=disable_decoding, timeout=timeout)
                for i in range(parse_frame_int(response))
            ]
        else:
            raise InvalidResponse(f"Protocol Error: {raw!r}")

        if disable_decoding is False:
            response = self.encoder.decode(response)
        return response


class _AsyncRESP2Parser(_AsyncRESPBase):
    """Async class for the RESP2 protocol"""

    async def read_response(self, disable_decoding: bool = False):
        if not self._connected:
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)
        if self._chunks:
            # augment parsing buffer with previously read data
            self._buffer += b"".join(self._chunks)
            self._chunks.clear()
        self._pos = 0
        try:
            response = await self._read_response(disable_decoding=disable_decoding)
        except InvalidResponse:
            # A protocol/framing error is unrecoverable: drop the parse state
            # and invalidate the connection, so a subsequent read fails
            # instead of re-parsing the same bad frame forever.
            self._clear()
            self._connected = False
            raise
        # Successfully parsing a response allows us to clear our parsing buffer
        self._clear()
        return response

    async def _read_response(
        self, disable_decoding: bool = False
    ) -> Union[EncodableT, ResponseError, None]:
        raw = await self._readline()
        response: Any
        byte, response = raw[:1], raw[1:]

        # server returned an error
        if byte == b"-":
            response = response.decode("utf-8", errors="replace")
            error = self.parse_error(response)
            # if the error is a ConnectionError, raise immediately so the user
            # is notified
            if isinstance(error, ConnectionError):
                self._clear()  # Successful parse
                raise error
            # otherwise, we're dealing with a ResponseError that might belong
            # inside a pipeline response. the connection's read_response()
            # and/or the pipeline's execute() will raise this error if
            # necessary, so just return the exception instance here.
            return error
        # single value
        elif byte == b"+":
            pass
        # int value
        elif byte == b":":
            return parse_frame_int(response)
        # bulk response
        elif byte == b"$" and response == b"-1":
            return None
        elif byte == b"$":
            response = await self._read(parse_frame_int(response))
        # multi-bulk response
        elif byte == b"*" and response == b"-1":
            return None
        elif byte == b"*":
            response = [
                (await self._read_response(disable_decoding))
                for _ in range(parse_frame_int(response))  # noqa
            ]
        else:
            raise InvalidResponse(f"Protocol Error: {raw!r}")

        if disable_decoding is False:
            response = self.encoder.decode(response)
        return response
