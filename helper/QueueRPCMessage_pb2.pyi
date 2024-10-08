"""
@generated by mypy-protobuf.  Do not edit manually!
isort:skip_file
"""
import builtins
import google.protobuf.descriptor
import google.protobuf.internal.containers
import google.protobuf.message
import typing
import typing_extensions

DESCRIPTOR: google.protobuf.descriptor.FileDescriptor = ...

class MapFieldEntry(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor = ...
    KEY_FIELD_NUMBER: builtins.int
    VALUE_FIELD_NUMBER: builtins.int
    key: typing.Text = ...
    value: typing.Text = ...

    def __init__(self,
        *,
        key : typing.Text = ...,
        value : typing.Text = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal[u"key",b"key",u"value",b"value"]) -> None: ...
global___MapFieldEntry = MapFieldEntry

class QueueRequestBodyMessage(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor = ...
    class CookiesEntry(google.protobuf.message.Message):
        DESCRIPTOR: google.protobuf.descriptor.Descriptor = ...
        KEY_FIELD_NUMBER: builtins.int
        VALUE_FIELD_NUMBER: builtins.int
        key: typing.Text = ...
        value: typing.Text = ...

        def __init__(self,
            *,
            key : typing.Text = ...,
            value : typing.Text = ...,
            ) -> None: ...
        def ClearField(self, field_name: typing_extensions.Literal[u"key",b"key",u"value",b"value"]) -> None: ...

    FULL_URL_FIELD_NUMBER: builtins.int
    METHOD_FIELD_NUMBER: builtins.int
    HEADERS_FIELD_NUMBER: builtins.int
    COOKIES_FIELD_NUMBER: builtins.int
    BODY_BYTES_FIELD_NUMBER: builtins.int
    TIMEOUT_FIELD_NUMBER: builtins.int
    full_url: typing.Text = ...
    method: typing.Text = ...
    body_bytes: builtins.bytes = ...
    timeout: builtins.int = ...

    @property
    def headers(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___MapFieldEntry]: ...

    @property
    def cookies(self) -> google.protobuf.internal.containers.ScalarMap[typing.Text, typing.Text]: ...

    def __init__(self,
        *,
        full_url : typing.Text = ...,
        method : typing.Text = ...,
        headers : typing.Optional[typing.Iterable[global___MapFieldEntry]] = ...,
        cookies : typing.Optional[typing.Mapping[typing.Text, typing.Text]] = ...,
        body_bytes : builtins.bytes = ...,
        timeout : builtins.int = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal[u"body_bytes",b"body_bytes",u"cookies",b"cookies",u"full_url",b"full_url",u"headers",b"headers",u"method",b"method",u"timeout",b"timeout"]) -> None: ...
global___QueueRequestBodyMessage = QueueRequestBodyMessage

class QueueResponseDataMessage(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor = ...
    class CookiesEntry(google.protobuf.message.Message):
        DESCRIPTOR: google.protobuf.descriptor.Descriptor = ...
        KEY_FIELD_NUMBER: builtins.int
        VALUE_FIELD_NUMBER: builtins.int
        key: typing.Text = ...
        value: typing.Text = ...

        def __init__(self,
            *,
            key : typing.Text = ...,
            value : typing.Text = ...,
            ) -> None: ...
        def ClearField(self, field_name: typing_extensions.Literal[u"key",b"key",u"value",b"value"]) -> None: ...

    IS_ERROR_FIELD_NUMBER: builtins.int
    STATUS_CODE_FIELD_NUMBER: builtins.int
    HEADERS_FIELD_NUMBER: builtins.int
    COOKIES_FIELD_NUMBER: builtins.int
    BODY_BYTES_FIELD_NUMBER: builtins.int
    WORKER_NAME_FIELD_NUMBER: builtins.int
    WORKER_MAC_FIELD_NUMBER: builtins.int
    is_error: builtins.bool = ...
    status_code: builtins.int = ...
    body_bytes: builtins.bytes = ...
    worker_name: typing.Text = ...
    worker_mac: typing.Text = ...

    @property
    def headers(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___MapFieldEntry]: ...

    @property
    def cookies(self) -> google.protobuf.internal.containers.ScalarMap[typing.Text, typing.Text]: ...

    def __init__(self,
        *,
        is_error : builtins.bool = ...,
        status_code : builtins.int = ...,
        headers : typing.Optional[typing.Iterable[global___MapFieldEntry]] = ...,
        cookies : typing.Optional[typing.Mapping[typing.Text, typing.Text]] = ...,
        body_bytes : builtins.bytes = ...,
        worker_name : typing.Text = ...,
        worker_mac : typing.Text = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal[u"body_bytes",b"body_bytes",u"cookies",b"cookies",u"headers",b"headers",u"is_error",b"is_error",u"status_code",b"status_code",u"worker_mac",b"worker_mac",u"worker_name",b"worker_name"]) -> None: ...
global___QueueResponseDataMessage = QueueResponseDataMessage
