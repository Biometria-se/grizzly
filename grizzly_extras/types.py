from enum import Enum, auto


class ResponseContentType(Enum):
    GUESS = 0
    JSON = auto()
    XML = auto()
    PLAIN = auto()


def str_response_content_type(value: str) -> ResponseContentType:
    if value.strip() in ['application/json', 'json']:
        return ResponseContentType.JSON
    elif value.strip() in ['application/xml', 'xml']:
        return ResponseContentType.XML
    elif value.strip() in ['text/plain', 'plain']:
        return ResponseContentType.PLAIN
    else:
        raise ValueError(f'"{value}" is an unknown response content type')

